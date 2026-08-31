package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/tg"
	"github.com/nats-io/nats.go/jetstream"
)

// errNotForum — чат не форумный либо у бота нет can_manage_topics.
var errNotForum = errors.New("тему создать не удалось")

// Poster — то, через что витрина пишет в канал.
//
// Интерфейс, а не клиент: в тестах подставляется двойник, и вся логика
// раскладки по темам проверяется без единого запроса в Telegram.
type Poster interface {
	// Send возвращает идентификаторы отправленных сообщений: длинное письмо
	// уходит несколькими постами, и связать с разговором нужно каждый —
	// человек отвечает на любой из них.
	Send(ctx context.Context, threadID int, post tg.Post) ([]int, error)
	CreateTopic(ctx context.Context, name string) (int, error)
}

// showcaseConsumer — имя durable-консьюмера моста.
//
// Ровно это имя, и менять его нельзя: права в nats/hub.conf выданы под него
// поимённо. Переименование не даст ошибки — витрина просто перестанет
// получать письма.
const showcaseConsumer = "telegram-bridge"

// Showcase читает поток писем и постит их в канал.
//
// Durable consumer с подтверждением, а не эфемерная подписка: мост один, и
// ему нужна доставка каждого письма ровно один раз. Пропущенное письмо
// человек не увидит никогда, а лишнее — увидит дважды. Поэтому подтверждение
// ставится ПОСЛЕ успешной отправки в телеграм.
type Showcase struct {
	js     jetstream.JetStream
	store  *TopicStore
	poster Poster
	// routes — куда писать маршруты постов. Обычно это тот же store; в
	// тестах подменяется, чтобы проверить поведение при отказе хранилища.
	//
	// Отдельное поле, а не аргумент: продакшн-путь от этого не усложняется,
	// зато отказ записи маршрута можно воспроизвести, не ломая настоящий KV.
	routes routeWriter
	// chatID нужен для ключей маршрутов: номера сообщений уникальны в
	// пределах чата, и без него маршруты двух супергрупп склеились бы.
	chatID    string
	forumMode bool
	logger    *log.Logger
	// creating сериализует ЗАВЕДЕНИЕ темы проекта.
	//
	// Два письма одного проекта, пришедшие подряд, иначе оба увидят «темы
	// нет» и создадут по теме. Ключ в KV откатить можно, тему в Telegram —
	// нельзя, поэтому дешевле не допустить.
	//
	// Мьютекс в процессе, а не распределённый захват: мост работает в одном
	// экземпляре, и второй сломал бы куда больше, чем темы (getUpdates
	// отдаёт обновление одному потребителю). Если экземпляров когда-нибудь
	// станет два, здесь понадобится захват в KV — и это надо будет вспомнить.
	creating sync.Mutex
}

func NewShowcase(js jetstream.JetStream, store *TopicStore, poster Poster, chatID string, forumMode bool) *Showcase {
	return &Showcase{
		js: js, store: store, poster: poster, routes: store,
		chatID: chatID, forumMode: forumMode,
		logger: log.Default(),
	}
}

// Run крутится, пока жив контекст.
func (s *Showcase) Run(ctx context.Context) error {
	stream, err := s.js.Stream(ctx, bus.StreamName)
	if err != nil {
		return fmt.Errorf("поток %s: %w", bus.StreamName, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       showcaseConsumer,
		FilterSubject: "mail.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		// Мост лежал — письма ждут его в потоке и придут при старте.
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("консьюмер моста: %w", err)
	}

	iter, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("итератор сообщений: %w", err)
	}
	defer iter.Stop()

	go func() {
		<-ctx.Done()
		iter.Stop()
	}()

	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("чтение потока: %w", err)
		}

		if err := s.handle(ctx, msg); err != nil {
			// Канал недоступен целиком — работать больше не с чем. Возвращаем
			// ошибку наружу, чтобы мост завершился: висеть в повторах, которые
			// заведомо не пройдут, хуже честного отказа. Видимым его делает
			// systemd — в юните для этого выставлены StartLimitIntervalSec
			// и StartLimitBurst.
			if errors.Is(err, errChannelDown) {
				return fmt.Errorf("витрина остановлена: %w", err)
			}

			s.logger.Printf("мост: не смог опубликовать письмо: %v", err)
			// Не подтверждаем: письмо вернётся и уйдёт в канал позже.
			//
			// С задержкой, а не немедленно: Nak() возвращает письмо тут же, и
			// на неустранимой ошибке витрина уходила в цикл на полной скорости.
			// Замерено на стенде: 624 попытки за 150 мс — шквал запросов в
			// Telegram, залитый журнал и занятый процессор ради письма, которое
			// всё равно не проходит.
			if nakErr := msg.NakWithDelay(retryDelay(msg)); nakErr != nil {
				// Молча проглоченный отказ означал бы, что письмо не вернётся
				// и человек его не увидит вовсе.
				s.logger.Printf("мост: не смог вернуть письмо в поток: %v", nakErr)
			}
			continue
		}
		if ackErr := msg.Ack(); ackErr != nil {
			// Без подтверждения письмо придёт повторно и человек увидит дубль.
			s.logger.Printf("мост: письмо показано, но не подтверждено: %v", ackErr)
		}
	}
}

func (s *Showcase) handle(ctx context.Context, msg jetstream.Msg) error {
	var m mail.Message
	if err := json.Unmarshal(msg.Data(), &m); err != nil {
		// Повторять разбор бессмысленно, но и молчать нельзя: человек не
		// отличит «писем не было» от «мост их не понял». Показываем
		// повреждённое письмо как есть, с номером в потоке — по нему его
		// можно найти. Договорённость симметрична с чтением ящика у агента.
		return s.postCorrupted(ctx, msg)
	}

	// Отправитель берётся из ТЕМЫ, а не из тела: тему удостоверил хаб правом
	// publish: mail.*.<свой_id>, а поле from — заявление отправителя. Витрина
	// — это то, что читает человек, и подделанное «human» здесь означало бы,
	// что любой узел может изобразить в телеграме самого владельца.
	m.From = bus.SenderForDisplay(msg.Subject())

	// Письмо нескольким адресатам лежит в потоке несколькими копиями:
	// человеку показываем ровно одну.
	key := postedKey("m", m.ID)
	shown, err := s.store.WasPosted(ctx, key)
	if err != nil {
		return err
	}
	if shown {
		return nil
	}

	threadID, err := s.topicFor(ctx, &m)
	if err != nil {
		return err
	}

	ids, showErr := s.show(ctx, threadID, tg.FormatMessage(&m), m.ThreadID)
	if showErr != nil && len(ids) == 0 {
		// Не показано ничего: письмо вернётся в поток и будет показано позже.
		// Здесь повтор безопасен — дублировать нечего.
		return showErr
	}

	// Маршруты — до отметки о показе, но их провал отметку не отменяет.
	//
	// Порядок такой, потому что маршрут нужен для ответа на уже видимый пост.
	// А не отменяет — потому что иначе витрина показала бы письмо ВТОРОЙ раз:
	// отметки нет, письмо вернулось в поток, ушло в чат опять. Пост в
	// Telegram не удалить, дубль необратим; потерянный маршрут — обратим,
	// человек получит внятное «разговор не найден» и напишет заново.
	s.saveRoutes(ctx, ids, &m)

	if showErr != nil {
		// Хвост длинного письма не ушёл. Повторять показ нельзя — уже
		// показанные части задвоились бы; человек увидит письмо неполным.
		s.logger.Printf("мост: письмо %s показано частично (%d частей), хвост потерян: %v",
			m.ID, len(ids), showErr)
	}

	// Только теперь. Отметка означает «человек это видел», и ставить её
	// авансом значит обещать за Telegram.
	if err := s.store.MarkPosted(ctx, key); err != nil {
		return err
	}

	// Канал отвалился посреди показа — мост обязан остановиться, и systemd
	// об этом узнает. Но ошибка возвращается ПОСЛЕ фиксации показанного:
	// иначе письмо вернулось бы в поток, и при следующем запуске уже
	// показанная часть ушла бы в чат второй раз.
	return showErr
}

// routeWriter — то, что умеет запомнить маршрут поста.
//
// Интерфейс ровно на один метод: витрине от хранилища здесь больше ничего не
// нужно, а тесту нужно уметь отказать в записи, не поднимая сломанный KV.
type routeWriter interface {
	PutRoute(ctx context.Context, chatID string, messageID int, route Route) error
}

// saveRoutes связывает показанные посты с разговором.
//
// Пишется маршрут для КАЖДОЙ части: длинное письмо уходит несколькими
// постами, и человек отвечает на любой из них — чаще на первый, потому что
// он выше.
//
// Ошибки не возвращаются наверх сознательно. Единственное, что мы могли бы
// сделать в ответ, — не отметить письмо показанным, а это означает второй
// пост в чате при следующей попытке. Дубль необратим, отсутствие маршрута —
// нет, поэтому здесь мы предпочитаем сказать в лог и жить дальше.
func (s *Showcase) saveRoutes(ctx context.Context, ids []int, m *mail.Message) {
	route := Route{
		ThreadID:     m.ThreadID,
		Project:      m.Project,
		Participants: m.Participants(),
	}

	for _, id := range ids {
		if err := s.putRouteWithRetry(ctx, id, route); err != nil {
			s.logger.Printf("мост: пост %d остался без маршрута (%v) — ответ на него получит отказ", id, err)
		}
	}
}

// putRouteWithRetry повторяет ТОЛЬКО запись маршрута, никогда не показ.
//
// Повтор здесь честно идемпотентен: ключ выведен из уже полученного номера
// поста, значение то же самое. Повторять показ на этом месте было бы ошибкой
// — он уже состоялся.
func (s *Showcase) putRouteWithRetry(ctx context.Context, messageID int, route Route) error {
	const attempts = 3

	var err error
	for i := 0; i < attempts; i++ {
		if err = s.routes.PutRoute(ctx, s.chatID, messageID, route); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return err
}

// show показывает текст человеку, обходя беды, которые можно обойти.
//
// Отказы Telegram делятся по области поражения, и лечатся они
// противоположным образом:
//
//   - канал недоступен целиком (нет прав, бот выкинут, чата нет) — обходить
//     нечем, дальше работать бессмысленно;
//   - не годится конкретная тема — показываем в общий поток, письмо доходит;
//   - не годится сам текст — этим занимается клиент, повторяя показ без
//     разметки.
//
// Без такого различения одно негодное письмо останавливало бы витрину для
// всех остальных: оно возвращалось бы в поток бесконечно.
func (s *Showcase) show(ctx context.Context, threadID int, post tg.Post, conversation string) ([]int, error) {
	ids, err := s.poster.Send(ctx, threadID, post)
	if err == nil {
		return ids, nil
	}
	if channelUnavailable(err) {
		return ids, fmt.Errorf("%w: %w", errChannelDown, err)
	}

	// Обход возможен, только пока НИЧЕГО не показано. Если часть письма уже
	// в чате, повторная отправка всего текста задвоит её, а пост не удалить.
	// Тогда честнее вернуть частичный успех наверх: письмо неполное, но без
	// дубля.
	if len(ids) == 0 && threadID != 0 && topicUnusable(err) {
		// Тема испортилась: закрыта, удалена или её больше нет. Запись о ней
		// теперь врёт, и оставить её значит спотыкаться о неё на каждом
		// следующем письме этого разговора.
		s.logger.Printf("мост: тема %d не годится (%v), показываю в общий поток", threadID, err)
		if conversation != "" {
			if delErr := s.store.Forget(ctx, conversation); delErr != nil {
				s.logger.Printf("мост: не смог забыть тему разговора %s: %v", conversation, delErr)
			}
		}
		return s.show(ctx, 0, post, "")
	}

	// Часть письма могла уйти до отказа: длинное режется на куски, и
	// идентификаторы уже показанных возвращаются вместе с ошибкой. Терять их
	// нельзя — эти посты человек видит, и ответ на них должен куда-то вести.
	return ids, err
}

// permanentTopicFailure отличает «темы здесь невозможны» от «сейчас не вышло».
func permanentTopicFailure(err error) bool {
	if errors.Is(err, errNotForum) {
		return true
	}
	var apiErr *tg.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Permanent()
	}
	return false
}

// errChannelDown — канал недоступен целиком, обходить нечем.
var errChannelDown = errors.New("телеграм-канал недоступен")

// apiFailure достаёт отказ Telegram, если он там есть.
func apiFailure(err error) (*tg.APIError, bool) {
	var apiErr *tg.APIError
	if errors.As(err, &apiErr) {
		return apiErr, true
	}
	return nil, false
}

// channelUnavailable — беда с самим чатом или ботом, а не с письмом.
//
// Именно этот случай оправдывает остановку моста: он не может ни показать
// письмо, ни пожаловаться человеку — жалоба идёт тем же каналом.
func channelUnavailable(err error) bool {
	apiErr, ok := apiFailure(err)
	if !ok {
		return false
	}
	if apiErr.Code == http.StatusUnauthorized || apiErr.Code == http.StatusForbidden {
		return true
	}
	return matchesAny(apiErr.Description,
		"CHAT NOT FOUND", "BOT WAS KICKED", "BOT IS NOT A MEMBER", "BOT WAS BLOCKED",
		"CHAT_WRITE_FORBIDDEN", "NOT ENOUGH RIGHTS", "CHAT_ADMIN_REQUIRED",
		"GROUP CHAT WAS DEACTIVATED")
}

// topicUnusable — не годится конкретная тема, но чат жив.
func topicUnusable(err error) bool {
	apiErr, ok := apiFailure(err)
	if !ok || apiErr.Code != http.StatusBadRequest {
		return false
	}
	return matchesAny(apiErr.Description,
		"MESSAGE THREAD NOT FOUND", "TOPIC_CLOSED", "TOPIC_DELETED",
		"TOPIC CLOSED", "TOPIC DELETED", "THREAD NOT FOUND")
}

func matchesAny(description string, markers ...string) bool {
	d := strings.ToUpper(description)
	for _, marker := range markers {
		if strings.Contains(d, marker) {
			return true
		}
	}
	return false
}

// Пределы задержки перед повторной доставкой.
//
// Первая пауза короткая: обычная беда — мигнувшая сеть, и ждать минуту ради
// неё незачем. Дальше пауза удваивается: если не прошло трижды, дело не в
// мигании. Потолок нужен, чтобы письмо не улеглось спать на сутки — беда
// может кончиться в любой момент, а человек письма ждёт.
const (
	retryDelayFirst = 2 * time.Second
	retryDelayMax   = time.Minute
)

// retryDelay считает паузу по числу доставок этого письма.
func retryDelay(msg jetstream.Msg) time.Duration {
	delivered := uint64(1)
	if meta, err := msg.Metadata(); err == nil && meta.NumDelivered > 0 {
		delivered = meta.NumDelivered
	}

	delay := retryDelayFirst
	for range delivered - 1 {
		delay *= 2
		if delay >= retryDelayMax {
			return retryDelayMax
		}
	}
	return delay
}

// postedKey строит ключ отметки о показе.
//
// Всегда хеш, никогда не исходная строка. Идентификатор письма приходит из
// ТЕЛА и не проверяется ничем: mail.Validate смотрит на отправителя,
// получателей, тему, размер и hops, но не на id. А ключи KV принимают лишь
// ограниченный набор символов.
//
// Отправитель с id вроде «не ключ вовсе» получал отказ на записи отметки,
// письмо возвращалось в поток и показывалось СНОВА — не терялось, а
// размножалось: по посту на каждую доставку. Проверено тестом, на прежнем
// коде он красный.
//
// Хеш снимает вопрос целиком: любая строка становится допустимым ключом
// фиксированной длины, и подобрать чужой ключ отправитель не может.
func postedKey(kind string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return fmt.Sprintf("%s-%x", kind, sum)
}

// corruptedKey — ключ отметки для письма, которое не разобралось.
//
// У такого письма нет ни идентификатора, ни вообще полей: доверять его
// содержимому нельзя по построению. Зато копии на разных адресатов делаются
// из ОДНОГО сериализованного тела — bus.Publish сериализует письмо один раз
// и рассылает один и тот же payload, — поэтому хеш тела различает письма и
// отождествляет копии. Проверено: три копии дают один sha256.
//
// К телу добавляется отправитель из ТЕМЫ, за которого поручился хаб. Не весь
// subject: в нём есть получатель, и с ним у каждой копии был бы свой ключ —
// человек увидел бы столько постов, сколько адресатов.
func corruptedKey(msg jetstream.Msg) string {
	return postedKey("c", string(msg.Data()), bus.SenderFromSubject(msg.Subject()))
}

// corruptedPreview — сколько байт повреждённого письма показываем.
const corruptedPreview = 200

// postCorrupted показывает человеку письмо, которое не удалось разобрать.
func (s *Showcase) postCorrupted(ctx context.Context, msg jetstream.Msg) error {
	key := corruptedKey(msg)
	shown, err := s.store.WasPosted(ctx, key)
	if err != nil {
		return err
	}
	if shown {
		return nil
	}

	seq := uint64(0)
	if meta, err := msg.Metadata(); err == nil {
		seq = meta.Sequence.Stream
	}

	raw := msg.Data()
	if len(raw) > corruptedPreview {
		raw = raw[:corruptedPreview]
	}

	s.logger.Printf("мост: письмо seq=%d не разобрано, показываю как повреждённое", seq)
	// Маршрут повреждённому письму не нужен: разговора у него нет, отвечать
	// в него некому — оно показано, чтобы человек знал о проблеме.
	if _, err := s.show(ctx, 0, tg.Post{Text: tg.FormatCorrupted(seq, string(raw))}, ""); err != nil {
		return err
	}
	return s.store.MarkPosted(ctx, key)
}

// topicFor находит или заводит тему под разговор.
//
// Если чат не форумный, возвращает 0 — письмо уйдёт в общий поток. Это
// деградация, а не отказ: молчать хуже, чем показать без раскладки по темам.
func (s *Showcase) topicFor(ctx context.Context, m *mail.Message) (int, error) {
	if !s.forumMode {
		return 0, nil
	}

	// Разговор, у которого уже есть СВОЯ тема, в ней и остаётся.
	//
	// Так работали все письма до перехода на темы проектов, и обрывать
	// существующие обсуждения нельзя: человек продолжает читать их там, где
	// начал. Новые разговоры такой записи не имеют и идут в тему проекта.
	existing, ok, err := s.store.Get(ctx, m.ThreadID)
	if err != nil {
		return 0, err
	}
	if ok && existing.IsThreadTopic() {
		return existing.MessageThreadID, nil
	}

	return s.projectTopic(ctx, m.Project)
}

// projectTopic находит или заводит тему проекта.
//
// Заведение сериализовано: два письма одного проекта, пришедшие подряд, иначе
// оба увидели бы «темы нет» и создали по теме. Запись в KV откатить можно,
// тему в Telegram — нельзя.
func (s *Showcase) projectTopic(ctx context.Context, project string) (int, error) {
	if id, ok, err := s.store.ProjectTopic(ctx, project); err != nil {
		return 0, err
	} else if ok {
		return id, nil
	}

	s.creating.Lock()
	defer s.creating.Unlock()

	// Пока ждали мьютекс, тему мог завести сосед — проверяем ещё раз.
	if id, ok, err := s.store.ProjectTopic(ctx, project); err != nil {
		return 0, err
	} else if ok {
		return id, nil
	}

	created, err := s.poster.CreateTopic(ctx, tg.ProjectTopicName(project))
	if err != nil {
		// Деградируем только на постоянной причине: нет прав, чат не форумный.
		// Таймаут или 5xx — временная беда, и раньше любая из них навсегда,
		// до рестарта, выключала раскладку по темам.
		if permanentTopicFailure(err) {
			s.logger.Printf("мост: темы недоступны (%v), перехожу в общий поток", err)
			s.forumMode = false
			return 0, nil
		}
		return 0, fmt.Errorf("создание темы: %w", err)
	}

	// Тема заведена в Telegram; если запись о ней не ляжет, тема останется
	// сиротой — при следующем письме проекта заведётся вторая. Отменить
	// созданную тему нечем, поэтому это явный остаточный риск, а не случай,
	// который можно обойти кодом.
	if err := s.store.PutProjectTopic(ctx, project, created); err != nil {
		return 0, err
	}
	return created, nil
}
