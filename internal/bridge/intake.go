package bridge

import (
	"context"
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/tg"
	"github.com/nats-io/nats.go/jetstream"
)

// HumanID — отправитель писем, пришедших из телеграма.
//
// Человек в этой сети такой же адресат, как агенты: ему отвечают, его видно
// в тредах. Отдельного механизма для «сообщений от оператора» нет намеренно.
const HumanID = "human"

// longPollTimeout — сколько держать запрос обновлений.
const longPollTimeout = 25

// Updater — источник сообщений человека.
type Updater interface {
	GetUpdates(ctx context.Context, offset, timeout int) ([]tg.Update, error)
}

// FileFetcher скачивает файл из Telegram по его file_id.
//
// Отдельно от Updater намеренно: скачивание нужно только для вложений, и токен
// живёт здесь, у моста. Байты мост кладёт в ObjectStore (bus.PutAttachment), а
// адресат достаёт их оттуда своим NKey — токен ему не выдаётся.
type FileFetcher interface {
	FetchFile(ctx context.Context, fileID string) ([]byte, error)
}

// Intake превращает сообщения человека в письма.
type Intake struct {
	js      jetstream.JetStream
	store   *TopicStore
	updater Updater
	files   FileFetcher
	reg     *bus.Registry
	poster  Poster
	logger  *log.Logger

	// botUsername — имя бота из GetMe, без «@».
	//
	// Нужно ровно для одного: в группах Telegram дописывает к команде адресата
	// (`/to@наш_бот`), и человек, выбравший команду из подсказки, получает
	// именно такой текст. Без имени пришлось бы либо принимать любой суффикс —
	// то есть и команды чужим ботам, — либо молча терять основной способ
	// набора. Запроса к Telegram отсюда нет: имя уже добыто при старте моста.
	botUsername string

	// guided — когда в теме последний раз объясняли правило ответа.
	//
	// В памяти, а не в KV: подсказка — вежливость, а не состояние. После
	// рестарта человек в худшем случае получит её ещё раз, и это дешевле,
	// чем ещё одно хранилище ради строки текста.
	guidedMu sync.Mutex
	guided   map[int]time.Time

	// warnedProject — когда в теме последний раз объясняли, что имя проекта
	// мосту неизвестно.
	//
	// Счётчик СВОЙ, а не общий с guided, хотя частота у них одна. Общий
	// означал бы, что два разных объяснения глушат друг друга: человек
	// получит то, чей вызов случился раньше, и не получит второго — причём
	// какое именно, зависит от порядка, а не от смысла.
	warnedProject map[int]time.Time

	// offset — позиция чтения обновлений Telegram.
	//
	// Живёт в KV, а не только в памяти. Подтверждение обновления уходит в
	// Telegram лишь со следующим запросом getUpdates, и до тех пор оно
	// остаётся неподтверждённым; рестарт моста в этом окне — а окно длиной
	// в long-poll, до двадцати пяти секунд — возвращал ту же пачку заново.
	// Сообщение человека превращалось во второе письмо с новым UUID, и
	// дедупликация потока о нём ничего не знала.
	offset int
	state  offsetStore

	// chatID — единственный чат, из которого мост принимает сообщения.
	//
	// Без этой проверки любой человек в Telegram может написать боту в личку
	// (username бота публичен) — и его текст уйдёт агентам письмом от имени
	// human. То же с любой группой, куда бота добавят.
	chatID string

	// allowedUsers — числовые идентификаторы тех, кому позволено говорить
	// от имени human.
	//
	// Пустым он сюда не приходит: bridge.Run отказывается стартовать без
	// списка. Но проверка ниже написана так, чтобы пустота означала отказ
	// и здесь тоже — Intake создаётся и в тестах, и защита не должна
	// зависеть от того, кто его собрал.
	allowedUsers map[int64]bool
}

// SetPoster задаёт обратный канал к человеку.
//
// Без него сообщение, которое некому доставить, исчезает молча: человек
// написал в канал и не отличит «никого нет в сети» от сломанного моста.
// Реестр визиток пуст не только когда агентов нет, но и первую минуту после
// старта моста — визитки приходят по таймеру, и до первой из них мост
// считает сеть пустой.
func (i *Intake) SetPoster(p Poster) { i.poster = p }

// SetBotUsername сообщает имя бота, добытое при старте моста.
func (i *Intake) SetBotUsername(u string) { i.botUsername = strings.TrimPrefix(u, "@") }

// SetFileFetcher включает скачивание вложений. Без него сообщение с файлом
// доходит письмом, но с пометкой, что файл получить не удалось.
func (i *Intake) SetFileFetcher(f FileFetcher) { i.files = f }

// SetState включает устойчивую позицию чтения.
//
// Отдельно от конструктора, потому что бакет состояния — это обращение к
// сети, а Intake создаётся и там, где сети нет. Без хранилища приём работает
// по-старому: позиция живёт в памяти и обнуляется при рестарте.
func (i *Intake) SetState(s *StateStore) { i.state = s }

// setState — тот же переключатель для тестов пакета.
//
// Нужен ровно затем, чтобы подставить двойник хранилища и увидеть МОМЕНТ
// записи позиции. Публичная сигнатура при этом остаётся прежней: приватный
// интерфейс не должен вылезать в API, которым пользуется мост.
func (i *Intake) setState(s offsetStore) { i.state = s }

// setLogger подменяет журнал. Только для тестов пакета.
//
// Публичной такую настройку делать незачем: мосту она не нужна, а строка
// маршрута проверяется изнутри пакета.
func (i *Intake) setLogger(l *log.Logger) { i.logger = l }

// offsetStore — устойчивая позиция чтения.
//
// Интерфейс узкий и приватный: приёму от хранилища нужны ровно две операции,
// и брать зависимость от всего StateStore незачем. Заодно порядок «позиция
// сохраняется ПОСЛЕ обработки» становится проверяемым — двойник видит момент
// вызова, а на конкретном типе этот порядок держался только комментарием.
type offsetStore interface {
	Offset(ctx context.Context) (int, error)
	SetOffset(ctx context.Context, offset int) error
}

func NewIntake(js jetstream.JetStream, store *TopicStore, updater Updater, reg *bus.Registry,
	chatID string, allowedUsers []int64,
) *Intake {
	allowed := make(map[int64]bool, len(allowedUsers))
	for _, id := range allowedUsers {
		allowed[id] = true
	}
	return &Intake{
		js: js, store: store, updater: updater, reg: reg,
		chatID: chatID, allowedUsers: allowed,
		logger: log.Default(),
	}
}

// accepts решает, слушать ли это сообщение вообще.
//
// Отказ здесь молчаливый и это осознанно: писать «вам сюда нельзя» в ответ
// на сообщение из чужого чата означало бы отвечать неизвестно кому и
// подтверждать, что бот жив. В лог отказ пишется.
func (i *Intake) accepts(update tg.Update) bool {
	if i.chatID != "" && update.ChatID != i.chatID {
		i.logger.Printf("мост: сообщение из чужого чата %s отброшено", update.ChatID)
		return false
	}
	// Пустой список — отказ всем, а не разрешение всем. Раньше условие
	// начиналось с len(...) > 0, и незаполненный список снимал проверку
	// целиком: право говорить от имени человека получал любой участник чата.
	if !i.allowedUsers[update.FromID] {
		i.logger.Printf("мост: отправитель %d (@%s) не в списке разрешённых", update.FromID, update.From)
		return false
	}
	return true
}

// Пауза после неудачного запроса обновлений: растёт до потолка и сбрасывается
// первым успехом.
//
// Раньше неудача вела к немедленному повтору. При обрыве сети до Telegram это
// давало цикл HTTP-запросов на полной скорости и строку в лог на каждый
// оборот: журнал забивался за минуты, а причина — «сети нет» — тонула в
// собственных повторах.
const (
	retryPauseMin = time.Second
	retryPauseMax = time.Minute
)

// Run опрашивает телеграм, пока жив контекст.
func (i *Intake) Run(ctx context.Context) error {
	if err := i.restoreOffset(ctx); err != nil {
		// Без позиции мост разберёт заново всё, что Telegram ещё хранит:
		// человек получит дубли за сутки. Это не повод не запускаться, но
		// сказать об этом надо.
		i.logger.Printf("мост: позиция чтения обновлений не восстановлена, "+
			"возможны повторы сообщений человека: %v", err)
	}

	pause := retryPauseMin
	quiet := false // об устойчивой недоступности говорим один раз, а не каждый оборот

	for {
		if ctx.Err() != nil {
			return nil
		}

		updates, err := i.updater.GetUpdates(ctx, i.offset, longPollTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			// Конфликт экземпляров — не сетевая беда, и повторять его нельзя.
			//
			// Проверяется КОД ответа, а не текст описания: текст Telegram
			// волен поменять, а 409 у этого случая один. Смотрим только
			// ошибку чтения обновлений: 409 от отправки сообщения означает
			// совсем другое и сюда не попадает.
			var apiErr *tg.APIError
			if errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict {
				return fmt.Errorf("%w: %w", ErrPollingConflict, err)
			}

			if !quiet {
				i.logger.Printf("мост: не смог получить обновления (повторяю с паузой до %s): %v",
					retryPauseMax, err)
				quiet = true
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(pause):
			}
			if pause *= 2; pause > retryPauseMax {
				pause = retryPauseMax
			}
			continue
		}
		if quiet {
			i.logger.Printf("мост: обновления снова приходят")
		}
		pause, quiet = retryPauseMin, false

		for _, update := range updates {
			// Сдвигаем offset даже на неудачной обработке: иначе одно
			// проблемное сообщение заклинит приём навсегда.
			if update.ID >= i.offset {
				i.offset = update.ID + 1
			}
			i.deliver(ctx, update)
			i.rememberOffset(ctx)
		}
	}
}

// restoreOffset поднимает позицию чтения с прошлого запуска.
func (i *Intake) restoreOffset(ctx context.Context) error {
	if i.state == nil {
		return nil
	}
	saved, err := i.state.Offset(ctx)
	if err != nil {
		return err
	}
	if saved > i.offset {
		i.offset = saved
	}
	return nil
}

// rememberOffset сохраняет позицию ПОСЛЕ обработки обновления.
//
// Порядок именно такой и он осознанный. Запись до обработки закрывала бы окно
// дубля, но открывала окно потери: упавший между записью и публикацией мост
// сообщение человека уже не увидит. Дубль человек заметит и повторит мысль,
// потерю — не заметит никто.
//
// Окно дубля при этом закрыто с другой стороны: письму от человека даётся
// идентификатор, выведенный из chat_id и update_id, а окно дедупликации
// потока поднято до суток.
func (i *Intake) rememberOffset(ctx context.Context) {
	if i.state == nil {
		return
	}
	if err := i.state.SetOffset(ctx, i.offset); err != nil {
		// Не смертельно: позиция останется в памяти, и до рестарта приём
		// работает как прежде.
		i.logger.Printf("мост: позиция чтения обновлений не сохранена: %v", err)
	}
}

// handleAttempts — сколько раз пробуем превратить сообщение в письмо.
//
// Подтверждение обновления в Telegram нельзя отозвать, поэтому временный сбой
// шины означал бы потерю сообщения навсегда. Повторяем на месте, и только
// исчерпав попытки, говорим человеку вслух.
const handleAttempts = 3

// retryPause — пауза между попытками.
const retryPause = 500 * time.Millisecond

func (i *Intake) deliver(ctx context.Context, update tg.Update) {
	var err error
	for attempt := 1; attempt <= handleAttempts; attempt++ {
		if err = i.handle(ctx, update); err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if attempt < handleAttempts {
			i.logger.Printf("мост: попытка %d для сообщения %d не удалась: %v", attempt, update.ID, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryPause):
			}
		}
	}

	i.logger.Printf("мост: сообщение %d не превратилось в письмо за %d попыток: %v",
		update.ID, handleAttempts, err)
	// Сказать вслух обязательно: подтверждение в Telegram уже не отозвать,
	// и человек иначе решит, что его услышали.
	i.tellHuman(ctx, update.ThreadID,
		"⚠️ Сообщение не доставлено: мост не смог передать его агентам. Повторите, пожалуйста.")
}

func (i *Intake) handle(ctx context.Context, update tg.Update) error {
	if !i.accepts(update) {
		return nil
	}

	text := strings.TrimSpace(update.Text)
	// Пустой текст без файла игнорируем: служебные и медиа-без-подписи обновления
	// письмом не становятся. Но файл без подписи — становится: сам файл и есть
	// сообщение, адресат заберёт его по file_id.
	if text == "" && update.Document == nil {
		return nil
	}

	if startsWithBotCommand(update) {
		// `/to` — единственное исключение, и оно поимённое: команда адресует
		// письмо одному агенту вместо участников разговора.
		if agentID, body, isTo := i.parseToCommand(update); isTo {
			return i.deliverTo(ctx, update, agentID, body)
		}

		// Остальные команды адресованы боту, а не агентам, и письмом
		// становиться не должны: `/start` из супергруппы разошёлся всем троим, разбудил
		// каждого и завёл в витрине тему со своим именем.
		//
		// Возврат nil здесь означает «обновление обработано»: приём двигает
		// offset дальше. Пропустить обновление, не подтвердив, нельзя —
		// Telegram отдаст ту же пачку снова, и всё написанное человеком
		// после команды не дойдёт вовсе.
		//
		// В лог идёт только номер обновления. Текст пришёл из сети, и
		// тащить его в лог незачем: команда и так известна по типу.
		i.logger.Printf("мост: обновление %d — команда боту, письмом не становится", update.ID)
		return nil
	}

	dest, err := i.route(ctx, update)
	if errors.Is(err, errNoRoute) {
		i.guide(ctx, update.ThreadID)
		return nil
	}
	if err != nil {
		return err
	}
	m := mail.New(HumanID, dest.recipients,
		subjectForMessage(text, update.Document), i.bodyForMessage(ctx, text, update.Document))

	// Решение записывается ДО публикации, а не после.
	//
	// Строка отвечает на вопрос «кому мост адресовал письмо и почему», и
	// восстановить это иначе нечем: выбор живёт в памяти процесса и нигде не
	// сохраняется. Когда человек написал «всем», а услышали двое, разбор
	// упирался в рассуждение о коде, потому что журнал молчал.
	//
	// До публикации — чтобы строка была и в том случае, когда публикация
	// упала: там она нужнее всего.
	//
	// Список берётся ИЗ ПИСЬМА, а не из маршрута: `Recipients` убирает
	// повторы, и письмо уходит по дедуплицированному списку. Журнал, взявший
	// сырой, показал бы адресата дважды там, где доставка была одна, — а
	// дубли в участниках у нас уже случались и стоили двойного пробуждения.
	//
	// В строке только источник выбора и идентификаторы агентов. Ни текста
	// сообщения, ни темы письма, ни идентификатора автора в телеграме: всё
	// это пришло из сети и в журнале ни на один вопрос не отвечает.
	i.logger.Printf("мост: маршрут обновления %d — источник=%s адресаты=[%s]",
		update.ID, dest.source, strings.Join(m.Recipients(), " "))

	if len(m.Recipients()) == 0 {
		i.logger.Printf("мост: некому доставить сообщение из телеграма — в сети нет живых агентов")
		i.tellHuman(ctx, update.ThreadID,
			"⚠️ Сообщение не доставлено: сейчас в сети нет ни одного агента. "+
				"Если мост только что запущен, визитки подтянутся в течение минуты — повторите.")
		return nil
	}

	// Идентификатор письма выводится из самого обновления, а не случаен.
	//
	// Случайный UUID означал, что рестарт моста в окне подтверждения делает
	// из одного сообщения человека два разных письма, и дедупликация потока
	// бессильна — она сверяет идентификаторы. Детерминированный ключ делает
	// повторную обработку того же update_id безвредной.
	m.ID = telegramMessageID(update)
	if dest.threadID != "" {
		m.ThreadID = dest.threadID
	}
	// Проект берётся из уже прочитанного маршрута, без единого лишнего
	// обращения к хранилищу. Без него ответ агента показывался бы в «Общее»,
	// хотя человек писал в тему проекта: `Reply` переносит проект дальше по
	// цепочке, и пустой проект уводил туда всю ветку.
	m.Project = dest.project

	if err := bus.Publish(ctx, i.js, m); err != nil {
		return fmt.Errorf("публикация письма от человека: %w", err)
	}
	return nil
}

// tellHuman отвечает в тот же чат или тему, куда человек написал.
//
// Молчание неотличимо от исправной работы, поэтому отказ произносится вслух.
// Ошибку отправки только логируем: если и обратный канал не работает, делать
// с этим здесь нечего.
func (i *Intake) tellHuman(ctx context.Context, threadID int, text string) {
	if i.poster == nil {
		return
	}
	if _, err := i.poster.Send(ctx, threadID, tg.Post{Text: text}); err != nil {
		i.logger.Printf("мост: не смог ответить человеку: %v", err)
	}
}

// startsWithBotCommand — сообщение начинается командой боту.
//
// Смотрит на разметку Telegram, а не на первый символ, и это существенно:
// путь `/etc/nats/tls/privkey.pem` начинается так же, а такие строки в
// рабочей переписке обычное дело. Признак команды даёт сам Telegram
// сущностью `bot_command`.
//
// Проверяется ИМЕННО нулевое смещение. Та же сущность приходит и во фразе
// «напиши /start боту», где команда — часть человеческой речи, а не
// обращение к боту; терять такие сообщения нельзя.
//
// Команды чужим ботам отсеиваются наравне со своими: в переписке агентов
// человеческого смысла у них нет, а разбирать, кому адресована команда,
// значит хранить имя бота ещё в одном месте.
func startsWithBotCommand(update tg.Update) bool {
	for _, e := range update.Entities {
		if e.Type == "bot_command" && e.Offset == 0 {
			return true
		}
	}
	return false
}

// guideEvery — как часто мост объясняет человеку правило ответа.
//
// Раз в час на тему, а не на каждое сообщение. В живом обсуждении человек
// пишет подряд — «ага», «понял», «давай завтра», — и подсказка на каждую
// реплику превратила бы мост в собеседника, который перебивает. Хуже того,
// у постов есть пауза в три секунды: десять подсказок заняли бы полминуты,
// в течение которых витрина не покажет ни одного письма.
const guideEvery = time.Hour

// guide объясняет человеку, почему сообщение никуда не ушло.
//
// Молчать нельзя: человек написал и ждёт. Повторять при каждой реплике —
// тоже, поэтому подсказка выдаётся не чаще guideEvery на тему.
func (i *Intake) guide(ctx context.Context, messageThreadID int) {
	i.guidedMu.Lock()
	last, seen := i.guided[messageThreadID]
	now := time.Now()
	if seen && now.Sub(last) < guideEvery {
		i.guidedMu.Unlock()
		return
	}
	if i.guided == nil {
		i.guided = make(map[int]time.Time)
	}
	i.guided[messageThreadID] = now
	i.guidedMu.Unlock()

	i.tellHuman(ctx, messageThreadID,
		"⚠️ Не понял, к какому разговору это относится. "+
			"Ответьте (Reply) на конкретное сообщение бота — тогда письмо уйдёт участникам именно того обсуждения.")
}

// errNoRoute — сообщение некуда адресовать точно.
//
// Не ошибка обработки: так выглядит реплика в тему проекта без ответа на
// пост, ответ на пост из прежней жизни или на истёкший маршрут. Веерная
// рассылка здесь была бы хуже отказа — переписка ушла бы посторонним.
var errNoRoute = errors.New("разговор не определён")

// ErrPollingConflict — Telegram сказал, что обновления забирает кто-то ещё.
//
// Экспортируется намеренно: это единственная ошибка чтения обновлений, при
// которой повторять бессмысленно и вредно. Два процесса с одним токеном
// вытесняют друг друга по кругу, и каждое вытеснение теряет часть сообщений
// человека — Telegram отдаёт пачку тому, кто успел, и второму она уже не
// придёт. В логе это выглядит как одна строка про недоступность, то есть
// неотличимо от моргнувшей сети.
//
// Решение, что делать дальше — останавливать процесс, менять политику
// systemd, брать аренду, — принимается снаружи. Здесь задача только одна:
// сделать конфликт отличимым от сети.
var ErrPollingConflict = errors.New("обновления Telegram забирает другой экземпляр моста")

// route решает, кому адресовать сообщение.
//
// Написано в тему разговора — участникам этого разговора. Написано в общий
// поток — всем, кто сейчас в сети. Ушедшим агентам не шлём: письмо пролежит
// без толку, а человек будет ждать ответа.
// destination — кому уходит письмо человека, в какой разговор и по какому
// проекту.
//
// Проект здесь не украшение: витрина выбирает по нему тему, и письмо без
// проекта попадает в «Общее» независимо от того, где человек его написал.
// Раньше intake проект отбрасывал, хотя маршрут поста его хранит.
type destination struct {
	recipients []string
	threadID   string
	project    string
	// source — откуда взят список адресатов.
	//
	// Нужен не для логики, а для журнала: снаружи «письмо ушло двоим» и
	// «письмо ушло всем, кого мост считал живыми» неразличимы, а разница
	// между ними — это разница между работающей адресацией и тихо
	// потерянным участником.
	source routeSource
}

// routeSource — способ, которым выбраны адресаты.
type routeSource string

const (
	// routeFromPost — ответ на конкретный пост: адресаты взяты из маршрута.
	routeFromPost routeSource = "post"
	// routeFromTopic — тема разговора: адресаты взяты из записи темы.
	routeFromTopic routeSource = "topic"
	// routeFromAlive — общий чат: адресаты взяты из реестра живых.
	routeFromAlive routeSource = "alive"
)

func (i *Intake) route(ctx context.Context, update tg.Update) (destination, error) {
	// Ответ на конкретный пост — единственный способ попасть точно в
	// разговор, когда посты разных обсуждений лежат в одной теме проекта.
	if update.ReplyToMessageID != 0 {
		route, ok, err := i.store.Route(ctx, i.chatID, update.ReplyToMessageID)
		if err != nil {
			return destination{}, err
		}
		if ok {
			return destination{
				recipients: route.Participants,
				threadID:   route.ThreadID,
				project:    route.Project,
				source:     routeFromPost,
			}, nil
		}
		// Маршрута нет. Это либо пост, показанный до перехода на темы
		// проектов (маршруты появились позже), либо истёкший срок, либо
		// ответ не на сообщение бота вовсе.
		//
		// Различает их признак «ответили боту». Ответ на пост бота в теме
		// разговора — продолжение обсуждения, и терять его нельзя: до
		// перехода так работали ВСЕ ответы. Ответ на человеческую реплику
		// адресатов не имеет, и рассылать его участникам темы значит будить
		// людей чужим уточнением.
		if update.ReplyToBot && update.ThreadID != 0 {
			threadID, topic, found, err := i.findByTopic(ctx, update.ThreadID)
			if err != nil {
				return destination{}, err
			}
			if found {
				// Проекта у старых тем нет: запись темы его не хранит, а
				// маршрута у поста не оказалось. Письмо уйдёт в «Общее», как
				// и уходило до сих пор, — здесь это не потеря, а прежнее
				// поведение legacy-разговоров.
				return destination{recipients: topic.Participants, threadID: threadID, source: routeFromTopic}, nil
			}
		}

		i.logger.Printf("мост: маршрут поста %d неизвестен", update.ReplyToMessageID)
		return destination{}, errNoRoute
	}

	if update.ThreadID != 0 {
		threadID, topic, found, err := i.findByTopic(ctx, update.ThreadID)
		if err != nil {
			return destination{}, err
		}
		if found {
			// Тема разговора: она сама означает адресатов, ответ на пост не
			// нужен. Так работают обсуждения, начатые до перехода.
			return destination{recipients: topic.Participants, threadID: threadID, source: routeFromTopic}, nil
		}

		// Тема есть, но разговора за ней нет — это тема проекта либо чужая.
		// Раньше отсюда шла рассылка всем живым; в общей теме проекта это
		// значит, что каждое «ага» будит четверых и каждый гадает, к чему оно.
		return destination{}, errNoRoute
	}

	// Общий чат вне тем: адресатов неоткуда взять, кроме как «все, кто в
	// сети». Здесь это осмысленно — человек обращается не к разговору, а ко
	// всем сразу.
	var all []string
	for _, card := range i.reg.Alive() {
		all = append(all, card.AgentID)
	}
	return destination{recipients: all, source: routeFromAlive}, nil
}

// findByTopic ищет разговор по идентификатору темы.
//
// KV индексирован по треду, поэтому обратный поиск линейный. Тем немного
// (один разговор — одна тема), и заводить второй индекс пока незачем.
func (i *Intake) findByTopic(ctx context.Context, messageThreadID int) (string, Topic, bool, error) {
	keys, err := i.store.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		// Пустой бакет — не ошибка: тем ещё не заводили.
		return "", Topic{}, false, nil
	}
	if err != nil {
		// Раньше любая ошибка выглядела как «тем нет», и ответ в приватную
		// тему уходил веером всем живым агентам. Таймаут или отказ прав —
		// это неизвестность, а не отсутствие темы, и молчать о ней нельзя.
		return "", Topic{}, false, fmt.Errorf("поиск темы %d: %w", messageThreadID, err)
	}

	for _, key := range keys {
		if strings.HasPrefix(key, postedPrefix) {
			continue // отметки о показанных письмах темами не являются
		}
		topic, ok, err := i.store.Get(ctx, key)
		if err != nil {
			// Повреждённая запись — тоже неизвестность: продолжать поиск
			// можно, но делать вид, что темы нет, нельзя.
			return "", Topic{}, false, fmt.Errorf("чтение темы %s: %w", key, err)
		}
		if !ok {
			continue
		}
		// Запись проекта хранится в том же бакете и тоже несёт номер темы,
		// но участников в ней нет. Приняв её за разговор, мост адресовал бы
		// ответ человека пустому списку — то есть никому.
		if !topic.IsThreadTopic() {
			continue
		}
		if topic.MessageThreadID == messageThreadID {
			return key, topic, true, nil
		}
	}
	return "", Topic{}, false, nil
}

// telegramNamespace — пространство имён для идентификаторов писем от человека.
//
// Фиксированный UUID: он определяет отображение «обновление Telegram → письмо»
// и обязан быть одним и тем же во всех запусках моста, иначе смысл
// детерминированного идентификатора теряется.
var telegramNamespace = uuid.MustParse("6f9619ff-8b86-d011-b42d-00c04fc964ff")

// telegramMessageID — идентификатор письма, выведенный из обновления.
//
// В ключ входят чат и update_id: их пара уникальна и не меняется при
// повторной выдаче обновления. Времени в ключе нет намеренно — иначе повтор
// того же обновления дал бы другой идентификатор, ради чего всё и затевалось.
func telegramMessageID(update tg.Update) string {
	return uuid.NewSHA1(telegramNamespace,
		[]byte("tg:"+update.ChatID+":"+strconv.Itoa(update.ID))).String()
}

// subjectLimit — сколько символов сообщения уходит в тему письма.
const subjectLimit = 60

func subjectFrom(text string) string {
	line := text
	if idx := strings.IndexByte(line, '\n'); idx >= 0 {
		line = line[:idx]
	}

	runes := []rune(line)
	if len(runes) > subjectLimit {
		return string(runes[:subjectLimit]) + "…"
	}
	if len(runes) == 0 {
		return "сообщение от человека"
	}
	return line
}

// subjectForMessage — тема письма из текста, а для файла без подписи — из
// имени файла: иначе у переданного архива была бы безликая тема «сообщение от
// человека», и в витрине не разобрать, что прислали.
func subjectForMessage(text string, doc *tg.Attachment) string {
	if strings.TrimSpace(text) == "" && doc != nil && doc.FileName != "" {
		return subjectFrom("файл: " + doc.FileName)
	}
	return subjectFrom(text)
}

// bodyForMessage дописывает к тексту блок вложения, предварительно СКАЧАВ файл
// и положив его байты в ObjectStore.
//
// Байты в письмо не кладутся: письмо — текст (лимит 64 КБ). В тело идёт лишь
// ключ объекта, по которому адресат достаёт файл своим NKey (fetch_attachment).
// Скачивание может не удаться (нет токена в тестах, отказ Telegram) — тогда
// письмо всё равно доходит, но с пометкой, что файл не получен: потерять само
// сообщение из-за файла хуже, чем доставить его без файла.
func (i *Intake) bodyForMessage(ctx context.Context, text string, doc *tg.Attachment) string {
	if doc == nil {
		return text
	}

	var note string
	if key, err := i.storeAttachment(ctx, doc); err != nil {
		i.logger.Printf("мост: вложение не сохранено: %v", err)
		note = failedAttachmentNote(doc)
	} else {
		note = attachmentNote(doc, key)
	}

	if strings.TrimSpace(text) == "" {
		return note
	}
	return text + "\n\n" + note
}

// storeAttachment качает файл из Telegram и кладёт его байты в ObjectStore.
// Возвращает ключ объекта для тела письма.
func (i *Intake) storeAttachment(ctx context.Context, doc *tg.Attachment) (string, error) {
	if i.files == nil {
		return "", fmt.Errorf("загрузчик файлов не настроен")
	}
	data, err := i.files.FetchFile(ctx, doc.FileID)
	if err != nil {
		return "", fmt.Errorf("скачивание: %w", err)
	}
	key, err := bus.PutAttachment(ctx, i.js, doc.FileName, data)
	if err != nil {
		return "", fmt.Errorf("сохранение в ObjectStore: %w", err)
	}
	return key, nil
}

// attachmentNote — блок про вложение с КЛЮЧОМ ОБЪЕКТА (не file_id).
//
// По ключу адресат зовёт fetch_attachment и достаёт байты из ObjectStore своим
// NKey. Ни file_id, ни токена в письме нет: качать нечем, да и незачем.
func attachmentNote(doc *tg.Attachment, key string) string {
	name := doc.FileName
	if name == "" {
		name = "файл"
	}
	b := &strings.Builder{}
	fmt.Fprintf(b, "📎 ВЛОЖЕНИЕ: %s", name)
	switch {
	case doc.FileSize > 0 && doc.MimeType != "":
		fmt.Fprintf(b, " (%d байт, %s)", doc.FileSize, doc.MimeType)
	case doc.FileSize > 0:
		fmt.Fprintf(b, " (%d байт)", doc.FileSize)
	case doc.MimeType != "":
		fmt.Fprintf(b, " (%s)", doc.MimeType)
	}
	fmt.Fprintf(b, "\nобъект: %s\nЗабери файл инструментом fetch_attachment (object=\"%s\").", key, key)
	return b.String()
}

// failedAttachmentNote — блок для случая, когда файл скачать/сохранить не вышло.
func failedAttachmentNote(doc *tg.Attachment) string {
	name := doc.FileName
	if name == "" {
		name = "файл"
	}
	return fmt.Sprintf("📎 ВЛОЖЕНИЕ: %s — получить не удалось, попроси прислать снова", name)
}

// toCommand — единственная команда, которую мост разбирает сам.
//
// Остальные команды по-прежнему отбрасываются: `/start` из супергруппы
// однажды разошёлся письмом всем троим агентам. Исключение делается
// поимённо, а не «все команды пропускаем».
const toCommand = "/to"

// parseToCommand разбирает «/to <agent-id> <текст>».
//
// Третье значение отвечает на вопрос «это вообще наша команда», а не «разбор
// удался»: `/to` без адресата или без текста — тоже наш случай, и человеку
// надо ответить, а не промолчать, как молчим на чужие команды.
//
// Признак команды берётся из разметки Telegram, а разбиение — по словам.
// Смещения в разметке Telegram считаются в кодовых единицах UTF-16, и
// арифметика по ним в Go требует пересчёта; здесь она не нужна вовсе:
// команда стоит первой, значит первое слово текста и есть команда.
func (i *Intake) parseToCommand(update tg.Update) (agentID, body string, isTo bool) {
	if !startsWithBotCommand(update) {
		return "", "", false
	}

	// Принимается `/to` и `/to@имя_нашего_бота` — и только они.
	//
	// Суффикс сравнивается с именем ЭТОГО бота, а не принимается любой:
	// `/to@чужой_бот` адресован другому боту, и превращать его в письмо
	// значит отвечать за чужой разговор. Регистр не учитывается: имена в
	// Telegram регистронезависимы, и клиент волен подставить любое написание.
	cmd, rest := splitFirstWord(strings.TrimSpace(update.Text))
	if !i.isToCommand(cmd) {
		return "", "", false
	}

	agentID, body = splitFirstWord(rest)
	return agentID, strings.TrimSpace(body), true
}

// splitFirstWord делит строку на первое слово и остаток.
//
// Остаток возвращается КАК ЕСТЬ, без схлопывания пробелов: это тело письма,
// и переносы строк в нём значимы.
func splitFirstWord(s string) (first, rest string) {
	s = strings.TrimLeft(s, " \t\n")
	idx := strings.IndexAny(s, " \t\n")
	if idx < 0 {
		return s, ""
	}
	return s[:idx], strings.TrimLeft(s[idx:], " \t")
}

// aliveAgent ищет агента в реестре ТОЧНЫМ совпадением.
//
// Без приведения регистра и без поиска по вхождению: идентификатор агента —
// это часть темы NATS (`mail.*.<id>`), где регистр значим, а «похожее» имя
// означает другого адресата. Ошибиться здесь — значит отправить переписку не
// тому.
func (i *Intake) aliveAgent(id string) bool {
	if id == "" {
		return false
	}
	for _, card := range i.reg.Alive() {
		if card.AgentID == id {
			return true
		}
	}
	return false
}

// aliveList — имена живых агентов для подсказки человеку.
//
// Берутся из реестра, а не из сообщения человека, и это существенно: ответ
// уходит в Telegram с разметкой HTML, а идентификаторы в реестре удостоверены
// темой визитки (`card.AgentID = owner` в WatchPresence), то есть правами
// хаба. Эхо введённого человеком имени здесь было бы возвратом в чат строки
// из сети без экранирования.
func (i *Intake) aliveList() string {
	var names []string
	for _, card := range i.reg.Alive() {
		names = append(names, card.AgentID)
	}
	if len(names) == 0 {
		return "сейчас в сети никого нет"
	}
	sort.Strings(names)
	// Экранируется каждое имя, хотя тема визитки и удостоверяет владельца
	// строки. Удостоверение отвечает на вопрос «чьё имя», а не «безопасно ли
	// оно в HTML»: ответ уходит с parse_mode HTML, и один `<` в имени
	// превратит подсказку в отказ Telegram или в чужую разметку.
	for idx, name := range names {
		names[idx] = html.EscapeString(name)
	}
	return strings.Join(names, ", ")
}

// deliverTo отправляет письмо ОДНОМУ названному агенту.
//
// Смысл команды — не тревожить остальных, поэтому любой отказ здесь тихий для
// сети и громкий для человека: письма не возникает вовсе, а человек получает
// объяснение в ту же тему. Рассылка «всем живым» при неузнанном имени была бы
// худшим из возможных ответов: команда просила обратного.
func (i *Intake) deliverTo(ctx context.Context, update tg.Update, agentID, body string) error {
	// Пустой текст допустим, если приложен файл: `/to pm` + архив без подписи —
	// это адресная передача файла, сам файл и есть содержимое.
	if agentID == "" || (body == "" && update.Document == nil) {
		i.tellHuman(ctx, update.ThreadID,
			"⚠️ Формат: <code>/to агент текст</code>. Сейчас в сети: "+i.aliveList())
		return nil
	}

	if !i.aliveAgent(agentID) {
		// Имя не эхоим: оно пришло из сети, а ответ уходит с разметкой HTML.
		i.tellHuman(ctx, update.ThreadID, i.noSuchAgent())
		return nil
	}

	where, err := i.conversationFor(ctx, update)
	if err != nil {
		return fmt.Errorf("разговор для адресного письма: %w", err)
	}
	if where.projectUnknown {
		i.warnUnknownProject(ctx, update.ThreadID)
	}

	m := mail.New(HumanID, []string{agentID},
		subjectForMessage(body, update.Document), i.bodyForMessage(ctx, body, update.Document))
	m.ID = telegramMessageID(update)
	if where.threadID != "" {
		m.ThreadID = where.threadID
	}
	m.Project = where.project

	if err := bus.Publish(ctx, i.js, m); err != nil {
		return fmt.Errorf("публикация адресного письма от человека: %w", err)
	}
	return nil
}

// conversationFor — разговор, к которому отнести адресное письмо.
//
// Команда переопределяет АДРЕСАТА, но не контекст: `/to` в ответ на пост или
// внутри темы разговора ложится в ту же нитку, иначе ответ человека
// оторвётся от обсуждения, которое он читает.
//
// Отказ хранилища возвращается наверх, а не подменяется новым разговором:
// в `handle` письмо повторят. Молчаливая подмена при живом хранилище, которое
// просто моргнуло, необратима для читателя канала — письмо уже висит в чужой
// нитке, и понять это по чату нечем.
// addressedContext — что удалось узнать о месте, куда написали команду.
//
// projectUnknown отделяет «тема проекта, имя которой мост пока не знает» от
// «это не тема проекта». Первое человеку объясняют, второе — нет: говорить
// «не знаю проект» про чужую тему бессмысленно, там никакого проекта и нет.
type addressedContext struct {
	threadID       string
	project        string
	projectUnknown bool
}

func (i *Intake) conversationFor(ctx context.Context, update tg.Update) (addressedContext, error) {
	if update.ReplyToMessageID == 0 {
		// Не ответ, но, возможно, тема разговора: до перехода на темы
		// проектов каждое обсуждение жило в своей теме, и сообщение в ней
		// адресовалось её участникам без всякого Reply. Команда меняет
		// адресата, а нитку тема даёт по-прежнему. Тему ПРОЕКТА findByTopic
		// отбрасывает сам (`IsThreadTopic`), поэтому там разговор останется
		// новым — как и должен.
		if update.ThreadID == 0 {
			return addressedContext{}, nil
		}

		// Сначала тема ПРОЕКТА — узким поиском, не зависящим от чужих записей.
		// Виды тем взаимоисключающи: один номер темы Telegram не бывает сразу
		// проектным и разговорным.
		where, found, err := i.projectOfTopic(ctx, update.ThreadID)
		if err != nil {
			return addressedContext{}, err
		}
		if found {
			return where, nil
		}

		// Проектной записи нет — значит это тема разговора либо чужая. У темы
		// разговора проекта нет и быть не может, запись его не хранит: письмо
		// уходит без проекта и молча, как уходило всегда.
		conversation, err := i.topicConversation(ctx, update.ThreadID)
		if err != nil {
			return addressedContext{}, err
		}
		return addressedContext{threadID: conversation}, nil
	}

	route, ok, err := i.store.Route(ctx, i.chatID, update.ReplyToMessageID)
	if err != nil {
		// Ошибка чтения возвращается наверх, а не проглатывается: в handle её
		// ждёт повтор. Начать новый разговор при живом хранилище, которое
		// просто моргнуло, значит оторвать ответ человека от обсуждения —
		// тихо и необратимо для читателя канала.
		return addressedContext{}, err
	}
	if ok {
		return addressedContext{threadID: route.ThreadID, project: route.Project}, nil
	}

	// Маршрута нет — тот же запасной путь, что и у обычного ответа: пост мог
	// быть показан до перехода на темы проектов. Разговор берётся по теме,
	// адресат остаётся тем, кого назвал человек.
	if update.ThreadID == 0 {
		return addressedContext{}, nil
	}

	// Пост показан до появления маршрутов. Тему спрашиваем в том же порядке,
	// что и без ответа: сперва узкий поиск проекта, потом широкий — разговора.
	where, found, err := i.projectOfTopic(ctx, update.ThreadID)
	if err != nil {
		return addressedContext{}, err
	}
	if found {
		// Тема проектная: адресат задан командой, нитка начнётся новая, но
		// письмо окажется в своей теме, а не в «Общем».
		return where, nil
	}

	// Запасной путь по теме разговора — только для ответа боту, как и прежде:
	// ответ на человеческую реплику адресатов не имеет.
	if update.ReplyToBot {
		conversation, err := i.topicConversation(ctx, update.ThreadID)
		if err != nil {
			return addressedContext{}, err
		}
		if conversation != "" {
			return addressedContext{threadID: conversation}, nil
		}
	}
	return addressedContext{}, nil
}

// projectOfTopic — что тема говорит о проекте.
//
// Второе значение отделяет «проектной записи нет» от «есть, но без имени»:
// известное пустое имя «Общего» иначе не отличить от промаха.
//
// Спрашивается ПЕРВЫМ, до поиска разговора, и это не вопрос вкуса. Поиск
// проекта читает только ключи с приставкой `project-`, а поиск разговора
// перебирает бакет целиком и падает на любой повреждённой записи — в том
// числе чужой, не имеющей к теме отношения. Спросив проект первым, мы
// отвечаем там, где ответ структурно точен, и не зависим от соседей.
//
// Полной изоляции это не даёт и дать не может: в теме РАЗГОВОРА широкий
// перебор неизбежен, потому что записи разговоров только так и находятся.
func (i *Intake) projectOfTopic(ctx context.Context, messageThreadID int) (addressedContext, bool, error) {
	name, ok, err := i.store.ProjectByTopic(ctx, messageThreadID)
	if err != nil {
		return addressedContext{}, false, err
	}
	if !ok {
		return addressedContext{}, false, nil
	}
	if !name.Known {
		return addressedContext{projectUnknown: true}, true, nil
	}
	return addressedContext{project: name.Name}, true, nil
}

// topicConversation — разговор темы, если это тема разговора, а не проекта.
//
// Пустая строка означает «темы разговора за этим номером нет», а не ошибку:
// в теме проекта записи нет по устройству, и это нормальный ход.
func (i *Intake) topicConversation(ctx context.Context, messageThreadID int) (string, error) {
	threadID, _, found, err := i.findByTopic(ctx, messageThreadID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return threadID, nil
}

// noSuchAgent объясняет, почему адресат не найден.
//
// Различает две причины, потому что они требуют от человека разного. Реестр
// наполняется ТОЛЬКО подпиской на визитки (`bus.WatchPresence`), запроса при
// старте нет, а узлы переизлучают их раз в минуту. Значит первую минуту после
// рестарта моста реестр пуст, хотя вся сеть жива, — и ответ «такого агента
// нет» был бы уверенной неправдой. Мост перезапускается при каждой раскатке,
// так что окно попадается регулярно.
func (i *Intake) noSuchAgent() string {
	if len(i.reg.Alive()) == 0 {
		return "⚠️ Мост только что поднялся и ещё не слышал ни одной визитки — они приходят раз в минуту. " +
			"Повторите через минуту: скорее всего, агент на месте."
	}
	// Формулировка наблюдаемая, а не онтологическая: «мосту не видно» вместо
	// «такого агента нет». Частично прогретый реестр знает не всех, и
	// уверенное «нет» было бы неправдой ровно так же, как при пустом.
	return "⚠️ Этот агент сейчас мосту не виден; после перезапуска список обновляется до минуты. " +
		"Видны: " + i.aliveList()
}

// isToCommand — это наша команда или чужая.
func (i *Intake) isToCommand(cmd string) bool {
	if cmd == toCommand {
		return true
	}
	at := strings.IndexByte(cmd, '@')
	if at < 0 || cmd[:at] != toCommand {
		return false
	}
	// Имя бота неизвестно — суффикс не принимаем вовсе. Иначе `/to@кто_угодно`
	// стал бы письмом, а имя как раз и отличает нашу команду от чужой.
	if i.botUsername == "" {
		return false
	}
	return strings.EqualFold(cmd[at+1:], i.botUsername)
}

// warnUnknownProject объясняет, что имя проекта этой темы мосту неизвестно.
//
// Отдельный счётчик от `guide`, ключ тот же — номер темы. Частота одинакова
// по той же причине: объяснение полезно один раз, а на каждом сообщении
// превращается в шум. Но счётчики не общие, иначе два разных объяснения
// глушили бы друг друга.
func (i *Intake) warnUnknownProject(ctx context.Context, messageThreadID int) {
	i.guidedMu.Lock()
	last, seen := i.warnedProject[messageThreadID]
	now := time.Now()
	if seen && now.Sub(last) < guideEvery {
		i.guidedMu.Unlock()
		return
	}
	if i.warnedProject == nil {
		i.warnedProject = make(map[int]time.Time)
	}
	i.warnedProject[messageThreadID] = now
	i.guidedMu.Unlock()

	i.tellHuman(ctx, messageThreadID,
		"ℹ️ Проект этой темы мосту пока неизвестен, поэтому письмо уйдёт в «Общее». "+
			"Если этот проект указан у живого агента, имя появится после его следующей визитки; "+
			"если такого агента нет, письма из темы так и будут уходить в «Общее».")
}
