package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/nats-io/nats.go/jetstream"
)

// defaultInboxLimit — сколько писем отдаём, если агент не попросил иного.
const defaultInboxLimit = 50

// inboxAttempts — сколько раз повторить чтение, если соседняя сессия снесла
// общий консьюмер посреди работы.
const inboxAttempts = 3

// batchSize — размер одной выборки при чтении ящика.
const batchSize = 50

// ScanCap — сколько писем максимум просмотреть за один вызов Inbox.
//
// Нужен из-за фильтра важности: без предела запрос urgent в ящике с тысячами
// обычных писем шёл бы по всему архиву. Ограничение видимое: если предел
// исчерпан и ничего не найдено, Inbox возвращает ошибку, а не пустоту.
//
// Экспортирован намеренно, и это не удобство вызывающего. Предел действует
// НЕЗАВИСИМО от Limit: цикл чтения останавливается по любому из двух, поэтому
// вызов с Limit больше ScanCap молча получает меньше писем, чем просил. Тот,
// кто считает диапазоны по позициям потока, обязан знать настоящую ширину
// чтения — иначе между его окнами остаются незакрытые полосы, а выглядит это
// как исправный поиск. Ровно так и вышло: окно шириной 2000 при пределе 1000
// не доставало до конца ящика на боевых значениях, и тесты с малым пределом
// этого не видели, потому что при четырёх письмах предел в игру не вступает.
const ScanCap = 1000

// inboxConsumerTTL — через сколько простоя сервер уберёт брошенный консьюмер.
//
// Консьюмер нужен только на время чтения. Минуты хватает с запасом, а
// переживать перезапуск ему незачем: позиция хранится в KV.
const inboxConsumerTTL = time.Minute

// Page — выдача ящика вместе с признаком её полноты.
//
// HasMore отвечает на единственный вопрос, который нельзя задать самой
// выдаче: «это всё?». Срез из limit писем неотличим от полного ящика, и
// читающий, взявший одну порцию, искренне считает, что видел всё. Он при
// этом видит САМЫЕ СТАРЫЕ письма — выдача идёт от позиции прочитанного
// вперёд, — то есть отвечает по устаревшему состоянию и не знает об этом.
// Так и разъехались узлы 30.08: указания приходили по коммиту, которого
// в ветке уже не было.
//
// Признак стоит одной выборки сверх лимита, а не обхода ящика: нам нужен
// факт «дальше есть», а не их число.
type Page struct {
	Envelopes []Envelope
	HasMore   bool
}

// Envelope — письмо вместе с его позицией в потоке.
//
// Позиция нужна, чтобы отметить прочтение: сам агент оперирует письмами,
// а состояние ящика двигается по номерам в потоке.
type Envelope struct {
	Message *mail.Message
	Seq     uint64
}

// InboxOptions — фильтры чтения.
type InboxOptions struct {
	UnreadOnly    bool
	Limit         int
	MinImportance string // "", normal, high, urgent

	// StartSeq — с какой позиции потока начинать чтение.
	//
	// Ноль означает «как обычно»: с начала ящика либо с позиции прочитанного
	// при UnreadOnly. Ненулевое значение нужно ровно для одного случая —
	// поиска письма у КОНЦА ящика, где лежит свежее. Чтение с начала для
	// этого не годится: в выросшем ящике свежее письмо лежит за любым
	// разумным пределом просмотра.
	//
	// Позиция прочитанного при заданном StartSeq не учитывается: поиск не
	// зависит от того, отмечено письмо прочитанным или нет.
	StartSeq uint64
}

// importanceRank задаёт порядок важности для фильтра.
var importanceRank = map[string]int{
	mail.ImportanceNormal: 0,
	mail.ImportanceHigh:   1,
	mail.ImportanceUrgent: 2,
}

// InboxConsumer — имя consumer'а, которым агент читает свой ящик.
//
// Имя ФИКСИРОВАННОЕ и содержит agent_id, потому что на нём держится вся
// изоляция. Права на хабе выданы по точному имени:
//
//	$JS.API.CONSUMER.CREATE.MAIL.inbox-<ID>.mail.<ID>
//	$JS.API.CONSUMER.MSG.NEXT.MAIL.inbox-<ID>
//
// Ordered consumer здесь не годится: он получает случайное имя, и право на
// выборку пришлось бы давать шаблоном MSG.NEXT.MAIL.>. Тогда любой агент
// сырым запросом вытянул бы письма из консьюмера моста, который durable и
// называется предсказуемо. Проверено эксплойтом, см. perms_test.go.
func InboxConsumer(agentID string) string {
	return "inbox-" + agentID
}

// StreamLastSeq возвращает номер последнего сообщения потока.
//
// Нужен, чтобы искать письмо у конца ящика: собственного «последнего номера»
// у ящика агента нет, а прямое чтение сообщения по номеру агенту недоступно
// намеренно — право $JS.API.STREAM.MSG.GET отдаёт любое сообщение потока в
// обход фильтра по теме.
//
// Номер общий на весь поток, то есть включает письма других агентов. Для
// поиска это годится: он задаёт окно у конца, а фильтр по теме применяет сам
// консьюмер.
func StreamLastSeq(ctx context.Context, js jetstream.JetStream) (uint64, error) {
	stream, err := js.Stream(ctx, StreamName)
	if err != nil {
		return 0, fmt.Errorf("поток %s: %w", StreamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("состояние потока %s: %w", StreamName, err)
	}
	return info.State.LastSeq, nil
}

// Inbox читает ящик агента.
//
// Чтение не потребляет письма: consumer каждый раз пересоздаётся с нужной
// позиции, а сами письма остаются в потоке (retention limits). Позицию
// прочитанного двигает только MarkRead, поэтому несколько сессий одного
// агента видят одинаковый ящик, а сторож, слушающий ту же тему, ничего
// не «съедает».
func Inbox(ctx context.Context, js jetstream.JetStream, agentID string, opts InboxOptions) ([]Envelope, error) {
	page, err := InboxPage(ctx, js, agentID, opts)
	if err != nil {
		return nil, err
	}
	return page.Envelopes, nil
}

// InboxPage читает ящик и говорит, осталось ли что-то за выдачей.
//
// Отдельная функция, а не смена сигнатуры Inbox: у Inbox десятки вызовов в
// тестах, которым признак полноты безразличен, и правка ради него размыла бы
// диффы там, где ничего не менялось по сути.
func InboxPage(ctx context.Context, js jetstream.JetStream, agentID string, opts InboxOptions) (Page, error) {
	// Консьюмер общий на агента, а сессий у него может быть несколько — и это
	// разные ПРОЦЕССЫ, поэтому блокировкой их не развести. Соседняя сессия
	// способна снести консьюмер посреди нашего чтения; повторяем.
	//
	// Повтор безопасен: чтение ничего не меняет, позиция живёт в KV, и
	// худшее, что бывает при гонке, — прочитать те же письма дважды.
	var lastErr error
	for attempt := 0; attempt < inboxAttempts; attempt++ {
		page, err := readInbox(ctx, js, agentID, opts)
		if err == nil {
			return page, nil
		}
		if !isConsumerRace(err) {
			return Page{}, err
		}
		lastErr = err
	}
	return Page{}, fmt.Errorf("чтение ящика %s не удалось за %d попыток "+
		"(параллельные сессии мешают друг другу): %w", agentID, inboxAttempts, lastErr)
}

// isConsumerRace — консьюмер исчез из-под нас, пока соседняя сессия
// пересоздавала его.
func isConsumerRace(err error) bool {
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return true
	}
	text := err.Error()
	return strings.Contains(text, "consumer deleted") ||
		strings.Contains(text, "consumer not found")
}

func readInbox(ctx context.Context, js jetstream.JetStream, agentID string, opts InboxOptions) (Page, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultInboxLimit
	}

	stream, err := js.Stream(ctx, StreamName)
	if err != nil {
		return Page{}, fmt.Errorf("поток %s: %w", StreamName, err)
	}

	var startSeq uint64 = 1
	if opts.StartSeq > 0 {
		startSeq = opts.StartSeq
	} else if opts.UnreadOnly {
		pos, err := ReadPosition(ctx, js, agentID)
		if err != nil {
			return Page{}, err
		}
		startSeq = pos + 1
	}

	name := InboxConsumer(agentID)

	// Удаление обязательно: CreateOrUpdateConsumer существующему консьюмеру
	// позицию НЕ меняет, и второе чтение вернуло бы пустоту. Отсутствие
	// консьюмера при этом нормально — он эфемерный и мог истечь сам.
	if err := stream.DeleteConsumer(ctx, name); err != nil &&
		!errors.Is(err, jetstream.ErrConsumerNotFound) {
		return Page{}, fmt.Errorf("сброс консьюмера %s: %w", name, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          name,
		FilterSubject: MailInboxFilter(agentID),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   startSeq,
		// Консьюмер живёт только на время чтения; брошенный подчистится сам.
		InactiveThreshold: inboxConsumerTTL,
	})
	if err != nil {
		return Page{}, fmt.Errorf("консьюмер для ящика %s: %w", agentID, err)
	}

	// Читаем порциями, пока не наберём limit ПОДХОДЯЩИХ писем.
	//
	// Раньше делалась одна выборка на limit, а фильтр важности применялся
	// после неё. Из-за этого срочное письмо, перед которым лежит limit
	// обычных, не возвращалось никогда: следующий вызов пересоздавал
	// консьюмер с той же позиции и снова упирался в те же обычные. Выглядело
	// это как «срочных писем нет».
	var out []Envelope
	scanned := 0

	// hasMore ставится ФАКТОМ доставленного письма сверх лимита, а не
	// выводом из числа отданных: len(out) == limit одинаково выглядит и когда
	// ящик кончился ровно на лимите, и когда за ним ещё сотня. Повторная
	// выборка тут не годится — пачка уже доставлена консьюмеру, и второй
	// Fetch вернёт пустоту при непустом ящике. Проверено тестом: он краснел
	// именно на этом.
	hasMore := false
	lastGot := 0

	for len(out) < limit && !hasMore && scanned < ScanCap {
		batch, err := cons.Fetch(batchSize, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			return Page{}, fmt.Errorf("чтение ящика %s: %w", agentID, err)
		}

		got := 0
		for msg := range batch.Messages() {
			got++
			scanned++

			// Подтверждаем сразу: письмо из потока при этом не исчезает
			// (retention limits), подтверждение лишь двигает позицию самого
			// консьюмера, который мы всё равно пересоздаём. Без него сервер
			// передоставит те же письма в следующей пачке.
			_ = msg.Ack()

			meta, err := msg.Metadata()
			if err != nil {
				return Page{}, fmt.Errorf("метаданные сообщения: %w", err)
			}

			// Отправитель берётся из ТЕМЫ: её удостоверил хаб правом
			// publish: mail.*.<свой_id>. Поле from в теле — всего лишь
			// заявление, и при расхождении верить надо теме.
			sender := SenderForDisplay(msg.Subject())

			var m mail.Message
			if err := json.Unmarshal(msg.Data(), &m); err != nil {
				// Битое письмо не роняет чтение ящика, но и не исчезает молча:
				// пропущенное снаружи неотличимо от «его не было». Отдаём
				// заглушку — агент увидит её сам и покажет человеку.
				if len(out) >= limit {
					hasMore = true
					continue
				}
				out = append(out, Envelope{
					Message: damagedMessage(agentID, meta.Sequence.Stream, msg.Data()),
					Seq:     meta.Sequence.Stream,
				})
				continue
			}
			// Отправитель ВСЕГДА из темы, даже когда тема невалидна.
			//
			// Раньше здесь стояло `if sender != ""`, и при неудостоверённой
			// теме в поле оставалось заявление из тела — то есть ровно то,
			// от чего защищает вся схема. Такие письма в потоке есть:
			// двухтокенные темы остались с тех пор, когда отправитель жил
			// только в JSON.
			m.From = sender
			if !passesImportance(&m, opts.MinImportance) {
				continue
			}

			if len(out) >= limit {
				// Письмо сверх лимита в выдачу не идёт, но доказывает, что
				// выдача неполна. Дальше пачку дочитываем только ради этого
				// признака — она уже доставлена, лишних запросов нет.
				hasMore = true
				continue
			}
			out = append(out, Envelope{Message: &m, Seq: meta.Sequence.Stream})
		}
		if err := batch.Error(); err != nil {
			return Page{}, fmt.Errorf("пачка писем: %w", err)
		}
		lastGot = got
		if got == 0 {
			break // ящик кончился
		}
	}

	// Упёрлись в предел просмотра и ничего не нашли — это «не досмотрел»,
	// а не «ничего нет». Молчаливое «пусто» тут неотличимо от исправного
	// ответа, поэтому говорим вслух.
	if len(out) == 0 && scanned >= ScanCap {
		return Page{}, fmt.Errorf("просмотрено %d писем ящика %s без совпадений: "+
			"сузьте фильтр или отметьте прочитанное", scanned, agentID)
	}

	// Лимит мог совпасть с границей ПАЧКИ, и тогда письма сверх него в ней
	// физически не было — а в ящике оно есть. Именно так признак молчал при
	// умолчании: defaultInboxLimit и batchSize оба равны пятидесяти, то есть
	// в самом частом случае он не работал вовсе.
	//
	// Спрашиваем сервер об одном письме сверх выдачи. Дополнительная доставка
	// безопасна: позицию прочитанного двигает ТОЛЬКО MarkRead, а консьюмер
	// эфемерный и пересоздаётся при каждом чтении — значит доставленное сверх
	// лимита вернётся в следующей выдаче и не потеряется. Свойство держится на
	// внешнем хранении позиции: перенесут её в консьюмер — письмо начнёт
	// пропадать молча.
	//
	// Условие lastGot == batchSize бережёт отзывчивость: если последняя пачка
	// пришла неполной, ящик исчерпан, и ждать ответа сервера незачем.
	//
	// Признак здесь КОНСЕРВАТИВЕН и потому шире, чем в цикле: там hasMore
	// ставят только письма, прошедшие фильтр, здесь — любое доставленное.
	// При фильтре по важности добор скажет «есть ещё», когда подходящих
	// больше нет, и читающий сделает одно лишнее чтение с пустым ответом.
	// Выбор намеренный: узнать дёшево, есть ли ДАЛЬШЕ подходящее, нельзя —
	// за неподходящим письмом может лежать нужное. Лишний вызов дешевле
	// пропущенного письма, ради которого всё и затевалось.
	if !hasMore && len(out) >= limit && lastGot == batchSize {
		// Отказ ВСПОМОГАТЕЛЬНОГО шага не отменяет собранную выдачу: письма
		// уже прочитаны, а неудача проверки остатка — не повод терять их.
		// Отвечаем консервативно: «возможно, есть ещё». Худшее следствие —
		// лишнее чтение, тогда как возврат ошибки отменял бы успешную работу
		// из-за необязательной проверки.
		probe, err := cons.Fetch(1, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			// Отказ здесь безвреден для выдачи, но невидимым быть не должен:
			// если добор откажет НАВСЕГДА, признак навсегда встанет в «есть
			// ещё», читающие начнут делать лишнее чтение на каждую полную
			// страницу, и причину искать будет нечем.
			log.Printf("bus: проверка остатка ящика %s не удалась: %v", agentID, err)
			return Page{Envelopes: out, HasMore: true}, nil
		}
		for msg := range probe.Messages() {
			_ = msg.Ack()
			hasMore = true
		}
		if err := probe.Error(); err != nil {
			log.Printf("bus: проверка остатка ящика %s не удалась: %v", agentID, err)
			return Page{Envelopes: out, HasMore: true}, nil
		}
	}

	// Упёрлись в предел просмотра — за ним заведомо осталось непросмотренное.
	if scanned >= ScanCap {
		hasMore = true
	}

	return Page{Envelopes: out, HasMore: hasMore}, nil
}

// damagedPreview — сколько байт испорченного письма показать.
const damagedPreview = 200

// DamagedSender — отправитель, который подставляется вместо неразобранного.
//
// Не пустая строка и не имя настоящего агента: человек в канале должен сразу
// видеть, что письмо повреждено, а не гадать, кто прислал мусор.
const DamagedSender = "(повреждено)"

// damagedMessage — заглушка на месте письма, которое не разобралось.
func damagedMessage(agentID string, seq uint64, raw []byte) *mail.Message {
	preview := string(raw)
	if len(preview) > damagedPreview {
		preview = preview[:damagedPreview] + "…"
	}

	return &mail.Message{
		ID:         fmt.Sprintf("damaged-%d", seq),
		ThreadID:   fmt.Sprintf("damaged-%d", seq),
		From:       DamagedSender,
		To:         []string{agentID},
		Subject:    fmt.Sprintf("не удалось разобрать письмо seq=%d", seq),
		Body:       preview,
		Importance: mail.ImportanceNormal,
		CreatedAt:  time.Now().UTC(),
	}
}

func passesImportance(m *mail.Message, min string) bool {
	if min == "" {
		return true
	}
	return importanceRank[m.Importance] >= importanceRank[min]
}

// ReadPosition возвращает номер последнего прочитанного письма.
//
// Отсутствие ключа — не ошибка: агент просто ещё ничего не читал.
func ReadPosition(ctx context.Context, js jetstream.JetStream, agentID string) (uint64, error) {
	pos, _, err := readPosition(ctx, js, agentID)
	return pos, err
}

// readPosition отдаёт позицию вместе с ревизией ключа.
//
// Ревизия нужна MarkRead для CAS: без неё две сессии одного агента,
// прочитавшие одинаковое «текущее», затирают друг друга, и курсор может
// уехать назад.
func readPosition(ctx context.Context, js jetstream.JetStream, agentID string) (uint64, uint64, error) {
	kv, err := js.KeyValue(ctx, StateBucket)
	if err != nil {
		return 0, 0, fmt.Errorf("бакет %s: %w", StateBucket, err)
	}

	entry, err := kv.Get(ctx, agentID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("позиция ящика %s: %w", agentID, err)
	}

	pos, err := strconv.ParseUint(string(entry.Value()), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("испорченная позиция ящика %s: %w", agentID, err)
	}
	return pos, entry.Revision(), nil
}

// markReadAttempts — сколько раз повторить CAS при конкурентной записи.
//
// Соперников не больше числа сессий одного агента на машине, поэтому
// нескольких попыток достаточно с запасом.
const markReadAttempts = 5

// MarkRead двигает позицию прочитанного вперёд.
//
// Две защиты, и обе не косметические.
//
// Первая: позиция не может уйти дальше конца потока. Инструмент принимает
// номер от модели, а та может ошибиться или получить его из письма-инъекции;
// огромное значение спрятало бы всю будущую почту до тех пор, пока поток не
// дорастёт до этого номера, — и выглядело бы это как «писем нет».
//
// Вторая: запись идёт через CAS по ревизии. Раньше две сессии одного агента,
// прочитавшие одинаковое «текущее», писали друг поверх друга, и курсор мог
// откатиться назад вопреки обещанию «только вперёд».
func MarkRead(ctx context.Context, js jetstream.JetStream, agentID string, seq uint64) error {
	kv, err := js.KeyValue(ctx, StateBucket)
	if err != nil {
		return fmt.Errorf("бакет %s: %w", StateBucket, err)
	}

	stream, err := js.Stream(ctx, StreamName)
	if err != nil {
		return fmt.Errorf("поток %s: %w", StreamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("состояние потока %s: %w", StreamName, err)
	}
	if last := info.State.LastSeq; seq > last {
		return fmt.Errorf("позиция %d за концом потока (последняя %d): "+
			"так можно спрятать всю будущую почту", seq, last)
	}

	for attempt := 0; attempt < markReadAttempts; attempt++ {
		current, revision, err := readPosition(ctx, js, agentID)
		if err != nil {
			return err
		}
		if seq <= current {
			return nil
		}

		value := []byte(strconv.FormatUint(seq, 10))
		if revision == 0 {
			// Ключа ещё нет: Create падает, если его успел завести сосед.
			if _, err := kv.Create(ctx, agentID, value); err == nil {
				return nil
			} else if !errors.Is(err, jetstream.ErrKeyExists) {
				return fmt.Errorf("запись позиции ящика %s: %w", agentID, err)
			}
			continue
		}

		if _, err := kv.Update(ctx, agentID, value, revision); err == nil {
			return nil
		} else if !isCASConflict(err) {
			return fmt.Errorf("запись позиции ящика %s: %w", agentID, err)
		}
		// Ревизия устарела — сосед успел раньше; перечитываем и пробуем снова.
	}

	return fmt.Errorf("позиция ящика %s не записана за %d попыток: слишком много "+
		"одновременных сессий", agentID, markReadAttempts)
}

// isCASConflict — не удалось записать, потому что ревизия устарела.
func isCASConflict(err error) bool {
	return errors.Is(err, jetstream.ErrKeyExists) ||
		strings.Contains(err.Error(), "wrong last sequence")
}
