// Package bridge соединяет шину с телеграм-каналом.
package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TopicBucket — KV с соответствием «тред → тема супергруппы».
//
// В KV, а не в файле рядом с мостом: у моста тогда нет состояния на диске,
// нечего бэкапить и незачем чинить при переезде на другой VPS.
const TopicBucket = "bridge_topics"

// Вид записи в бакете тем.
//
// Записи двух видов лежат вместе, потому что живут одинаково долго — вечно, —
// а раздельное хранение оправдано разницей в СРОКЕ ЖИЗНИ, а не в смысле
// (см. RouteBucket). Но различать их надо явно: поиск темы разговора
// перебирает бакет целиком, и запись проекта не должна быть принята за
// разговор — участников в ней нет, и ответ ушёл бы в пустоту.
const (
	// KindThreadTopic — тема под один разговор. Пустая строка означает то же
	// самое: так выглядят все записи, сделанные до появления видов, и менять
	// их трактовку нельзя — иначе существующие разговоры оборвутся разом.
	KindThreadTopic = "thread_topic"
	// KindProjectTopic — тема под проект целиком.
	KindProjectTopic = "project_topic"
)

// topicVersion — версия формата записи темы.
const topicVersion = 1

// Topic — тема канала под разговор или под проект.
type Topic struct {
	// Version и Kind добавлены позже остальных полей. У старых записей их
	// нет, и пустой Kind читается как KindThreadTopic — это не умолчание для
	// удобства, а единственная возможная трактовка: других видов тогда не
	// существовало.
	Version         int      `json:"v,omitempty"`
	Kind            string   `json:"kind,omitempty"`
	MessageThreadID int      `json:"message_thread_id"`
	Participants    []string `json:"participants"`

	// Project и ProjectKnown добавлены позже и только для тем ПРОЕКТОВ.
	//
	// Имя нужно потому, что ключ записи — необратимый хеш: по номеру темы
	// проект иначе не восстановить, и письмо, начатое командой в такой теме,
	// уходит в «Общее» вместе со всей веткой.
	//
	// Признак обязателен и отдельным полем: пустое имя — законное значение,
	// под ним живёт тема «Общего» (`projectKey("")`). Без признака пустая
	// строка означала бы сразу и «Общее», и «имени в записи нет».
	//
	// Версия НЕ поднята намеренно. Добавление необязательных полей обратно
	// совместимо в обе стороны: старая сборка их игнорирует, новая читает
	// старую запись как «имя неизвестно». А ProjectTopic сверяет версию
	// строгим равенством и отдаёт ОШИБКУ при расхождении — то есть bump
	// сломал бы чтение всех существующих тем разом.
	Project      string `json:"project,omitempty"`
	ProjectKnown bool   `json:"project_known,omitempty"`
}

// ProjectName — что известно об имени проекта темы.
//
// Known отделяет «имя записано» от «поля в записи нет»: пустое Name при
// Known означает тему «Общего», а без Known — старую запись, сделанную до
// того, как имена стали храниться.
type ProjectName struct {
	Name  string
	Known bool
}

// IsThreadTopic — запись описывает разговор, а не проект.
func (t Topic) IsThreadTopic() bool {
	return t.Kind == "" || t.Kind == KindThreadTopic
}

// projectKey — ключ темы проекта.
//
// Хеш по той же причине, что и у маршрутов: имя проекта задаёт человек, а
// ключи KV принимают ограниченный набор символов. Проект с пробелом или
// кириллицей в названии иначе получил бы отказ при записи — и обнаружилось
// бы это на первом же письме.
//
// Приставка отделяет проекты от тредов в общем пространстве ключей.
func projectKey(project string) string {
	sum := sha256.Sum256([]byte(project))
	return fmt.Sprintf("%s%x", projectPrefix, sum)
}

// projectPrefix — приставка ключей тем проектов.
//
// Именованной она стала, когда по ней потребовалось не только СОБИРАТЬ ключ,
// но и отбирать записи при обратном поиске. Разъехаться этим двум местам
// нельзя: поиск молча перестал бы находить темы, собранные другой строкой.
const projectPrefix = "project-"

// PutProjectTopic запоминает тему, отведённую проекту.
func (s *TopicStore) PutProjectTopic(ctx context.Context, project string, messageThreadID int) error {
	payload, err := json.Marshal(Topic{
		Version:         topicVersion,
		Kind:            KindProjectTopic,
		MessageThreadID: messageThreadID,
		// Имя уже здесь, в аргументе, — поэтому новые темы приходят
		// заполненными, и дозаполнять придётся только те, что заведены
		// прежним кодом.
		Project:      project,
		ProjectKnown: true,
	})
	if err != nil {
		return fmt.Errorf("сериализация темы проекта: %w", err)
	}
	if _, err := s.kv.Put(ctx, projectKey(project), payload); err != nil {
		return fmt.Errorf("запись темы проекта %q: %w", project, err)
	}
	return nil
}

// ProjectTopic возвращает тему проекта. Отсутствие — не ошибка: тему заведут.
//
// А вот запись НЕ ТОГО ВИДА под этим ключом — ошибка, и молчать о ней нельзя.
// Вернуть «темы нет» значит завести вторую тему поверх существующей записи,
// то есть тихо разделить проект надвое. Лучше сказать прямо: в бакете лежит
// не то, что мы ожидали, и это чинит человек, а не код.
func (s *TopicStore) ProjectTopic(ctx context.Context, project string) (int, bool, error) {
	topic, ok, err := s.Get(ctx, projectKey(project))
	if err != nil || !ok {
		return 0, false, err
	}

	if topic.Kind != KindProjectTopic {
		return 0, false, fmt.Errorf(
			"под ключом проекта %q лежит запись вида %q, ожидалась %q",
			project, topic.Kind, KindProjectTopic)
	}
	// Ровно известная версия, включая отказ на ноль: темы проектов появились
	// сразу с версией, и запись без неё сделана не нами. Легаси-записи
	// разговоров тут ни при чём — у них свой ключ и свой вид, проверенный выше.
	if topic.Version != topicVersion {
		return 0, false, fmt.Errorf(
			"тема проекта %q записана версией %d, эта сборка понимает только %d",
			project, topic.Version, topicVersion)
	}
	return topic.MessageThreadID, true, nil
}

// PostedBucket — KV с отметками о показанных письмах.
//
// Отдельный бакет, а не префикс внутри TopicBucket: TTL в JetStream задаётся
// на бакет целиком, и держать вместе вечные записи о темах и временные
// отметки означало бы либо вечные отметки, либо исчезающие темы.
//
// Раньше отметки лежали среди тем и не истекали никогда. Их по одной на
// письмо, а поиск темы по идентификатору перебирает бакет целиком — то есть
// каждое сообщение человека дорожало с каждым показанным письмом. Замер на
// живом сервере: пустой бакет 1.0 мс, 1000 ключей 7 мс, 5000 ключей 32 мс.
const PostedBucket = "bridge_posted"

// PostedTTL — сколько живёт отметка о показе.
//
// Отметка нужна, пока письмо может прийти повторно: копии на нескольких
// адресатов идут подряд, а неподтверждённое письмо возвращается в поток
// столько раз, сколько нужно. Двух суток хватает и на многочасовой простой
// моста, и на любую разумную череду повторов.
//
// Верхней границы «навсегда» здесь быть не должно: письма идут постоянно,
// и бакет без TTL растёт до конца жизни системы.
const PostedTTL = 48 * time.Hour

// TopicStore хранит соответствие тредов и тем, а также отметки о показах.
//
// Без него после рестарта моста для продолжающегося разговора создавалась бы
// вторая тема-дубль.
type TopicStore struct {
	kv     jetstream.KeyValue
	posted jetstream.KeyValue
	routes jetstream.KeyValue
}

func NewTopicStore(ctx context.Context, js jetstream.JetStream) (*TopicStore, error) {
	kv, err := openBucket(ctx, js, jetstream.KeyValueConfig{
		Bucket:      TopicBucket,
		Description: "тред разговора -> тема телеграм-супергруппы",
	})
	if err != nil {
		return nil, err
	}

	// Бакет заводится ТОЛЬКО так, через KV-обёртку. Созданный мимо неё — руками
	// через nats CLI или сырым CreateStream — окажется без прямого чтения, а
	// прав на обходной путь у моста нет и не должно быть: запись будет
	// проходить, чтение молча висеть до таймаута.
	posted, err := openBucket(ctx, js, jetstream.KeyValueConfig{
		Bucket:      PostedBucket,
		Description: "письмо уже показано человеку",
		TTL:         PostedTTL,
	})
	if err != nil {
		return nil, err
	}

	// Маршруты ответов: свой бакет со своим сроком жизни. Почему не рядом с
	// темами — в комментарии к RouteBucket.
	routes, err := openBucket(ctx, js, jetstream.KeyValueConfig{
		Bucket:      RouteBucket,
		Description: "пост в чате -> разговор, участники и проект",
		TTL:         RouteTTL,
	})
	if err != nil {
		return nil, err
	}

	return &TopicStore{kv: kv, posted: posted, routes: routes}, nil
}

// openBucket создаёт бакет или открывает существующий.
//
// Порядок «создать, при отказе открыть» сохранён с прежнего кода: право
// создавать бакеты есть только у моста, и на пустом хабе первым приходит
// именно он.
func openBucket(ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	kv, err := js.CreateKeyValue(ctx, cfg)
	if err == nil {
		return kv, nil
	}
	if !errors.Is(err, jetstream.ErrBucketExists) {
		return nil, fmt.Errorf("бакет %s: %w", cfg.Bucket, err)
	}

	kv, err = js.KeyValue(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("открытие бакета %s: %w", cfg.Bucket, err)
	}
	return kv, nil
}

// Get возвращает тему разговора. Отсутствие — не ошибка.
func (s *TopicStore) Get(ctx context.Context, threadID string) (Topic, bool, error) {
	entry, err := s.kv.Get(ctx, threadID)
	// Недопустимый ключ приравниваем к отсутствующему. Ключи KV ограничены
	// набором символов, и всё, что в него не укладывается, не может там
	// лежать по построению — для вызывающего это то же самое «темы нет».
	// Иначе он был бы обязан разбирать два случая, означающих одно.
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
		return Topic{}, false, nil
	}
	if err != nil {
		return Topic{}, false, fmt.Errorf("чтение темы %s: %w", threadID, err)
	}

	var topic Topic
	if err := json.Unmarshal(entry.Value(), &topic); err != nil {
		return Topic{}, false, fmt.Errorf("разбор темы %s: %w", threadID, err)
	}
	return topic, true, nil
}

// postedPrefix — приставка к ключам отметок в СТАРОМ бакете тем.
//
// Отметки давно переехали в PostedBucket, но в bridge_topics могли остаться
// записи, сделанные до переезда, и поиск темы обязан их пропускать: тема с
// таким ключом не найдётся никогда, а вот принять отметку за тему он может.
const postedPrefix = "posted-"

// WasPosted говорит, показывали ли уже это письмо.
//
// Нужно потому, что одно письмо нескольким адресатам лежит в потоке
// НЕСКОЛЬКИМИ сообщениями — по копии на получателя, так устроена доставка и
// дедупликация на публикации. Витрина читает поток целиком и без этой
// проверки показала бы человеку одинаковый пост столько раз, сколько было
// адресатов.
func (s *TopicStore) WasPosted(ctx context.Context, key string) (bool, error) {
	_, err := s.posted.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("проверка отметки о показе %s: %w", key, err)
	}
	return true, nil
}

// MarkPosted отмечает письмо показанным.
//
// Вызывается ПОСЛЕ успешного показа, и порядок здесь — главное решение всей
// задачи. Раньше отметка ставилась до отправки: письмо, не ушедшее из-за
// икоты сети, возвращалось в поток, но повторная попытка видела отметку,
// считала письмо показанным и подтверждала его. Человек не видел письма
// никогда и не мог узнать, что оно было.
//
// Обратная сторона выбрана сознательно: если мост упадёт между показом и
// отметкой, человек увидит письмо дважды. Дубль он видит и понимает, потерю —
// нет.
//
// Существующий ключ ошибкой не считается: он означает, что письмо показано,
// а это ровно то, чего мы добивались.
func (s *TopicStore) MarkPosted(ctx context.Context, key string) error {
	_, err := s.posted.Create(ctx, key, []byte("1"))
	if err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return fmt.Errorf("отметка о показе %s: %w", key, err)
	}
	return nil
}

// Forget убирает запись о теме разговора.
//
// Нужна, когда тема в телеграме перестала существовать: закрыта, удалена или
// потеряна. Запись о ней после этого не просто бесполезна — она вредна, и
// каждое следующее письмо разговора спотыкалось бы об неё заново. Отсутствие
// ключа ошибкой не считается: цель — чтобы записи не было.
func (s *TopicStore) Forget(ctx context.Context, threadID string) error {
	err := s.kv.Delete(ctx, threadID)
	if err == nil || errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
		return nil
	}
	return fmt.Errorf("удаление темы %s: %w", threadID, err)
}

// Put запоминает тему разговора.
func (s *TopicStore) Put(ctx context.Context, threadID string, topic Topic) error {
	payload, err := json.Marshal(topic)
	if err != nil {
		return fmt.Errorf("сериализация темы: %w", err)
	}
	if _, err := s.kv.Put(ctx, threadID, payload); err != nil {
		return fmt.Errorf("запись темы %s: %w", threadID, err)
	}
	return nil
}

// ProjectByTopic ищет тему ПРОЕКТА по номеру темы супергруппы.
//
// Второе значение отвечает на вопрос «это вообще тема проекта», а не «нашли
// ли имя»: у старых записей имени нет, и различать эти два случая обязано
// само API. Вызывающему они говорят разное. Записи нет — значит тема чужая
// или это тема разговора, и объяснять человеку нечего. Запись есть без
// имени — значит тема проекта, но какого, мост пока не знает, и сказать об
// этом надо.
//
// Перебор ключей, а не прямое чтение: ключ выводится из ИМЕНИ проекта, а мы
// идём от номера темы.
//
// Стоимость складывается из перечисления ключей и чтения записей, но читаются
// только ПРОЕКТНЫЕ: их единицы, тогда как разговоров сотни. Замер на живом
// сервере дал около 0.1 мс на прочитанную запись (2.8 мс на десяти, 10.2 на
// сотне, 33.8 на трёхстах). Цифра «7 мс на 1000 ключей» из комментария к
// PostedBucket сюда не относится: там мерено только перечисление.
//
// Проход всегда полный, в отличие от findByTopic, который выходит на первом
// совпадении: дубликат номера темы обязан быть замечен, а увидеть его можно
// только дочитав до конца.
func (s *TopicStore) ProjectByTopic(ctx context.Context, messageThreadID int) (ProjectName, bool, error) {
	keys, err := s.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return ProjectName{}, false, nil
	}
	if err != nil {
		return ProjectName{}, false, fmt.Errorf("поиск проекта темы %d: %w", messageThreadID, err)
	}

	var found *Topic
	for _, key := range keys {
		// Смотрим ТОЛЬКО на ключи проектов. Записи разговоров и отметки о
		// показах сюда не относятся, и читать их не просто лишнее: любая
		// повреждённая запись разговора возвращала бы ошибку и блокировала
		// адресную команду во всех проектах разом — при том что к проектам
		// она отношения не имеет.
		if !strings.HasPrefix(key, projectPrefix) {
			continue
		}
		topic, ok, err := s.Get(ctx, key)
		if err != nil {
			// Повреждённая запись — это неизвестность, а не «темы нет».
			// Сказать «не знаю проект» при нечитаемом бакете значит увести
			// письмо в «Общее» тихо и необратимо.
			return ProjectName{}, false, fmt.Errorf("чтение записи %s: %w", key, err)
		}
		if !ok || topic.Kind != KindProjectTopic {
			continue
		}
		if topic.MessageThreadID != messageThreadID {
			continue
		}
		if topic.Version != topicVersion {
			return ProjectName{}, false, fmt.Errorf(
				"тема проекта под ключом %s записана версией %d, эта сборка понимает только %d",
				key, topic.Version, topicVersion)
		}
		if found != nil {
			// Две темы проектов с одним номером — противоречие в данных.
			// Выбрать первую попавшуюся значит адресовать письма наугад,
			// причём стабильно наугад: порядок ключей не гарантирован.
			return ProjectName{}, false, fmt.Errorf(
				"номеру темы %d соответствуют две записи проектов", messageThreadID)
		}
		copied := topic
		found = &copied
	}

	if found == nil {
		return ProjectName{}, false, nil
	}
	return ProjectName{Name: found.Project, Known: found.ProjectKnown}, true, nil
}

// FillProjectName дописывает имя в СУЩЕСТВУЮЩУЮ запись темы проекта.
//
// Записи не создаёт, и это главное её свойство. Отсутствие записи означает
// «темы у проекта ещё нет», а заводить её вправе только витрина при первом
// письме: имя проекта приходит мосту из визиток живых агентов, и создание по
// нему темы наплодило бы пустых веток под каждый проект из чужого конфига.
//
// Повтор безвреден: заполненная запись отвечает первым же чтением и ничего
// не пишет. Это важно, потому что вызывать её будут на каждой визитке, то
// есть примерно раз в минуту на агента.
func (s *TopicStore) FillProjectName(ctx context.Context, project string) (bool, error) {
	key := projectKey(project)

	for attempt := 0; attempt < 2; attempt++ {
		entry, err := s.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("чтение темы проекта %q: %w", project, err)
		}

		var topic Topic
		if err := json.Unmarshal(entry.Value(), &topic); err != nil {
			return false, fmt.Errorf("разбор темы проекта %q: %w", project, err)
		}
		if topic.Kind != KindProjectTopic {
			return false, fmt.Errorf(
				"под ключом проекта %q лежит запись вида %q, ожидалась %q",
				project, topic.Kind, KindProjectTopic)
		}
		if topic.Version != topicVersion {
			return false, fmt.Errorf(
				"тема проекта %q записана версией %d, эта сборка понимает только %d",
				project, topic.Version, topicVersion)
		}
		if topic.ProjectKnown {
			if topic.Project == project {
				return false, nil // уже заполнено, делать нечего
			}
			// Имя записано и не наше: под ключом, выведенным из имени,
			// лежит чужое имя. Это повреждение, и молчаливо переписать его
			// нельзя — вторая тема того же проекта хуже отсутствующей.
			return false, fmt.Errorf(
				"под ключом проекта %q записано имя %q", project, topic.Project)
		}

		topic.Project = project
		topic.ProjectKnown = true
		payload, err := json.Marshal(topic)
		if err != nil {
			return false, fmt.Errorf("сериализация темы проекта %q: %w", project, err)
		}

		// Сравнение с ревизией, а не Put: между чтением и записью запись мог
		// поменять кто-то ещё, и слепая перезапись стёрла бы его правку.
		_, err = s.kv.Update(ctx, key, payload, entry.Revision())
		if err == nil {
			return true, nil
		}
		// Конфликт определяется ИМЕННО этой ошибкой: ErrKeyExists для той же
		// цели не годится, о чём предупреждает и документация клиента —
		// на реплицированных потоках коды расходятся.
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return false, fmt.Errorf("запись имени проекта %q: %w", project, err)
		}
		// Кто-то опередил. Перечитываем и смотрим, что там теперь: если он
		// записал то же имя, работа сделана; если нет — следующий круг
		// покажет причину. Молчаливое «ничего не делали» здесь недопустимо:
		// оно неотличимо от успеха, а запись могла остаться пустой.
	}

	return false, fmt.Errorf("имя проекта %q не записано: запись меняли параллельно", project)
}
