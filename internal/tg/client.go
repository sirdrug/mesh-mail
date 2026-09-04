// Package tg — клиент Telegram Bot API для моста.
//
// Исходящие вызовы идут через github.com/go-telegram/bot: типы Bot API,
// multipart и новые методы приходят из библиотеки, а не пишутся здесь. Это
// понадобилось, когда к четырём методам добавились файлы и клавиатуры.
//
// ВХОДЯЩИЙ getUpdates остаётся собственным и останется им: позиция чтения
// живёт в JetStream и сдвигается ПОСЛЕ обработки обновления, а библиотека
// двигает свою до передачи обработчику. Это не неудобство, а несовместимость
// с нашей гарантией «сообщение человека не теряется».
//
// Здесь же остаётся всё, что нельзя отдать наружу: разрезание длинного
// текста, ограничитель частоты, повтор на 429 и лечение отвергнутой разметки
// безопасным моноширинным показом. Повторная отправка создаёт ВТОРОЙ пост, а
// пост в Telegram не удалить, поэтому политика повторов должна принадлежать
// мосту.
//
// Ограничения Telegram, о которые легко споткнуться:
//   - чтобы бот писал в супергруппу, он должен быть в ней АДМИНИСТРАТОРОМ;
//   - message_thread_id работает только в супергруппе с включёнными Topics,
//     и передавать его нулевым нельзя — API вернёт ошибку;
//   - предел одного сообщения 4096 символов, режем сами.
package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

// MaxMessageRunes — с запасом ниже реального предела в 4096: остаток
// уходит на служебную обвязку вроде пометки «часть 2».
const MaxMessageRunes = 3800

const defaultBaseURL = "https://api.telegram.org"

// MinSendGap — пауза между сообщениями в один чат.
//
// Telegram разрешает примерно двадцать сообщений в минуту на групповой чат и
// отвечает 429 на превышение. Раньше мост в это ограничение упирался только
// постфактум: несколько агентов отвечали разом, витрина выкладывала посты
// подряд, и дальше всё зависело от повторов. Три секунды дают ровно двадцать
// в минуту — очередь идёт медленнее, зато без отказов.
//
// Пауза общая на клиента, а не на вызывающего: ограничение у Telegram
// на чат, и делить его между витриной и ответами человеку бессмысленно.
const MinSendGap = 3 * time.Second

// Client — бот.
type Client struct {
	token   string
	baseURL string
	http    *http.Client

	// sendMu держит очередь на отправку: пауза считается от последнего
	// РЕАЛЬНО отправленного сообщения, поэтому её нельзя вести в вызывающем.
	sendMu   sync.Mutex
	lastSend time.Time
	minGap   time.Duration

	// api — библиотека Bot API для ИСХОДЯЩИХ вызовов.
	//
	// Создаётся в конструкторе и без единого сетевого обращения: `WithSkipGetMe`
	// отключает проверку токена, которую библиотека иначе делает сама. Входящий
	// getUpdates остаётся нашим — там свой durable offset и свой порядок.
	api *bot.Bot
	// initErr — отказ создания библиотеки, отложенный до первого вызова.
	//
	// Сигнатура New ошибки не возвращает, и менять её значило бы трогать всех
	// вызывающих ради случая, которого у нас не бывает: пустой токен отсеивается
	// в конфигурации раньше. Ошибка не выдаётся за отказ Telegram — иначе
	// классификация сочла бы её бедой чата.
	initErr error
}

// Option настраивает клиента.
type Option func(*Client)

// WithBaseURL подменяет адрес API. Нужен тестам.
func WithBaseURL(url string) Option {
	return func(c *Client) { c.baseURL = url }
}

// WithHTTPClient подменяет HTTP-клиента.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

// WithMinSendGap меняет паузу между сообщениями. Нужен тестам: ждать по три
// секунды на каждое сообщение они не должны.
func WithMinSendGap(gap time.Duration) Option {
	return func(c *Client) { c.minGap = gap }
}

func New(token string, opts ...Option) *Client {
	c := &Client{
		token:   token,
		baseURL: defaultBaseURL,
		http:    &http.Client{Timeout: 60 * time.Second},
		minGap:  MinSendGap,
	}
	for _, opt := range opts {
		opt(c)
	}

	// Бот создаётся ПОСЛЕ опций: адрес и HTTP-клиент к этому моменту уже свои.
	api, err := bot.New(c.token,
		bot.WithSkipGetMe(),
		bot.WithServerURL(c.baseURL),
		// Второй аргумент — таймаут длинного опроса библиотеки. Мы им не
		// пользуемся, но подменить клиент без него нельзя.
		bot.WithHTTPClient(c.http.Timeout, &capturingClient{inner: c.http}),
	)
	if err != nil {
		c.initErr = fmt.Errorf("создание клиента Telegram: %w", err)
		return c
	}
	c.api = api
	return c
}

// outbound выполняет исходящий вызов через библиотеку.
//
// Здесь же остаётся наш повтор на 429: библиотека ничего не переигрывает сама,
// и политика повторов должна принадлежать мосту — повторная отправка создаёт
// ВТОРОЙ пост, а пост в Telegram не удалить.
func (c *Client) outbound(ctx context.Context, method string, do func(context.Context) error) error {
	if c.initErr != nil {
		return c.initErr
	}

	for attempt := 0; attempt < 2; attempt++ {
		callCtx, capture := withCapture(ctx)
		err := do(callCtx)
		if err == nil {
			return nil
		}

		converted := asAPIError(method, capture, err)
		var apiErr *APIError
		if attempt == 0 && errors.As(converted, &apiErr) && apiErr.Code == http.StatusTooManyRequests {
			wait := time.Duration(retryAfterOf(capture, err)) * time.Second
			if wait <= 0 {
				wait = 3 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		return converted
	}
	return nil
}

// waitTurn выдерживает паузу перед отправкой.
//
// Блокировка держится всё ожидание намеренно: иначе двое отсчитали бы паузу
// от одного и того же прошлого сообщения и ушли бы в Telegram одновременно —
// то есть ограничение не ограничивало бы ничего.
func (c *Client) waitTurn(ctx context.Context) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	if !c.lastSend.IsZero() {
		if wait := c.minGap - time.Since(c.lastSend); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	c.lastSend = time.Now()
	return nil
}

// SendRequest — параметры отправки сообщения.
type SendRequest struct {
	ChatID   string
	Text     string
	ThreadID int // 0 — писать в общий поток
	// MarkedLines — обычные строки текста начинаются со служебной приставки.
	//
	// Признак нужен аварийному показу: он собирает моноширинный блок из
	// готового текста и не может ОТЛИЧИТЬ служебную приставку от той же черты,
	// написанной человеком. Скажем, тело `│ пользовательские данные` внутри
	// блока кода — обычный текст письма, и стереть у него первый символ
	// значит потерять данные.
	//
	// Маркированы не все строки: внутри блока кода приставок нет намеренно,
	// и черта в начале такой строки принадлежит письму. Разбирается с этим
	// сам аварийный показ — он смотрит разметку до снятия тегов.
	//
	// По умолчанию — false: ничего не снимаем, поведение прежнее. Ставит
	// признак только тот, кто сам эти приставки и поставил.
	MarkedLines bool
}

// Update — входящее сообщение от человека.
// Entity — размеченный кусок текста в сообщении Telegram.
//
// Нужен ровно для одного решения: команда боту это или текст человека.
// По первому символу их не различить — путь `/etc/nats/tls` начинается так
// же, а такие строки в рабочей переписке обычное дело. Telegram помечает
// команды типом `bot_command`, и это единственный признак, которому можно
// верить.
//
// Хранится смещение, а не только тип: команда в начале строки адресована
// боту, а та же команда посреди фразы («напиши /start в бота») — обычный
// текст, и терять его нельзя.
type Entity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

type Update struct {
	ID       int
	ChatID   string
	ThreadID int
	Text     string
	// Entities — разметка текста. Пустая, если Telegram её не прислал;
	// подставлять сюда что-либо самим нельзя, иначе признак перестанет
	// быть признаком.
	Entities []Entity
	// ReplyToMessageID — на какой пост человек ответил. Ноль, если это не
	// ответ.
	//
	// В общей теме проекта рядом идут посты разных разговоров, и только это
	// поле говорит, к какому из них относится реплика. Без него ответ пришлось
	// бы рассылать всем живым — то есть посторонним.
	ReplyToMessageID int
	// ReplyToBot — ответили на сообщение БОТА, а не человека.
	//
	// Различие существенное. Ответ на пост бота — продолжение разговора,
	// который бот показал. Ответ на собственную или чужую человеческую
	// реплику разговором не является: адресатов у него нет, и рассылать его
	// участникам темы значит будить людей чужой репликой.
	ReplyToBot bool
	From       string
	// FromID — числовой идентификатор отправителя. Именно он, а не username:
	// username меняется владельцем в любой момент и не годится на роль
	// удостоверения.
	FromID int64
	// Document — приложенный файл. nil, если сообщение без файла.
	//
	// Байты сюда НЕ кладутся: мост файл не качает и в письмо не тащит — письмо
	// остаётся текстом (лимит 64 КБ). Здесь только ссылка (FileID), по которой
	// адресат забирает файл из Bot API сам. У сообщения с файлом текст лежит в
	// caption, поэтому парсер кладёт caption в Text: команда `/to` и разбор
	// разговора работают тем же путём, что и для обычного сообщения.
	Document *Attachment
}

// Attachment — ссылка на файл в Telegram, без самих байтов.
type Attachment struct {
	FileID   string
	FileName string
	FileSize int64
	MimeType string
}

// APIError — отказ, пришедший от самого Telegram.
//
// Отдельный тип нужен вызывающему, чтобы отличить постоянную причину
// («нет прав», «чат не форумный») от временной (таймаут, 5xx). Без этого
// различения мост на любой икоте сети навсегда переключался бы в
// деградированный режим.
type APIError struct {
	Method      string
	Code        int
	Description string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram отклонил %s (%d): %s", e.Method, e.Code, e.Description)
}

// Permanent сообщает, что повторять запрос бессмысленно: дело в правах или
// в устройстве чата, а не в сети.
func (e *APIError) Permanent() bool {
	if e.Code == http.StatusForbidden || e.Code == http.StatusUnauthorized {
		return true
	}
	if e.Code != http.StatusBadRequest {
		return false
	}
	d := strings.ToUpper(e.Description)
	for _, marker := range []string{
		"NOT ENOUGH RIGHTS", "CHAT_ADMIN_REQUIRED", "NOT A FORUM",
		"TOPIC", "CHAT NOT FOUND", "BOT WAS KICKED", "BOT IS NOT A MEMBER",
	} {
		if strings.Contains(d, marker) {
			return true
		}
	}
	return false
}

// call — собственный транспорт ВХОДЯЩЕГО пути.
//
// После перехода на библиотеку им пользуется только getUpdates: исходящие
// методы идут через outbound. Здесь JSON, а не multipart, и это постоянная
// разница, а не остаток миграции.
//
// Одна повторная попытка на 429: чаще всего это всплеск после того, как
// несколько агентов ответили разом, и повтор через retry_after проходит.
func (c *Client) call(ctx context.Context, method string, payload map[string]any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("сериализация запроса %s: %w", method, err)
	}

	url := fmt.Sprintf("%s/bot%s/%s", c.baseURL, c.token, method)

	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("запрос %s: %w", method, err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("вызов %s: %w", method, err)
		}

		var envelope struct {
			OK          bool            `json:"ok"`
			Result      json.RawMessage `json:"result"`
			Description string          `json:"description"`
			Parameters  struct {
				RetryAfter int `json:"retry_after"`
			} `json:"parameters"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&envelope)
		// Ответ уже разобран: ошибка закрытия тела ни на что не влияет.
		_ = resp.Body.Close()
		if decodeErr != nil {
			return fmt.Errorf("разбор ответа %s: %w", method, decodeErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt == 0 {
			wait := time.Duration(envelope.Parameters.RetryAfter) * time.Second
			if wait <= 0 {
				wait = 3 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		if !envelope.OK {
			return &APIError{Method: method, Code: resp.StatusCode, Description: envelope.Description}
		}
		if out != nil && len(envelope.Result) > 0 {
			if err := json.Unmarshal(envelope.Result, out); err != nil {
				return fmt.Errorf("разбор результата %s: %w", method, err)
			}
		}
		return nil
	}

	return fmt.Errorf("метод %s не прошёл после повтора", method)
}

// Split режет текст по границам строк, не разрывая их без нужды.
func Split(text string) []string {
	if len([]rune(text)) <= MaxMessageRunes {
		return []string{text}
	}

	var parts []string
	var current strings.Builder
	currentLen := 0

	flush := func() {
		if currentLen > 0 {
			parts = append(parts, current.String())
			current.Reset()
			currentLen = 0
		}
	}

	for _, line := range strings.SplitAfter(text, "\n") {
		runes := []rune(line)

		if currentLen+len(runes) > MaxMessageRunes {
			flush()
		}
		// Одна строка длиннее лимита — режем грубо, деваться некуда.
		for len(runes) > MaxMessageRunes {
			parts = append(parts, string(runes[:MaxMessageRunes]))
			runes = runes[MaxMessageRunes:]
		}

		current.WriteString(string(runes))
		currentLen += len(runes)
	}
	flush()

	return parts
}

// SendMessage отправляет сообщение, разбивая длинное на части.
//
// Возвращает идентификаторы ВСЕХ отправленных кусков, по одному на часть.
//
// Раньше возвращался только последний, и этого хватало, пока идентификатор
// никому не был нужен. Он понадобился, когда посты в общей теме проекта
// стали связываться с разговорами: человек отвечает на конкретное сообщение,
// и по нему надо понять, кому адресовать ответ. Длинное письмо разрезано на
// части, отвечают нередко на первую — с одним последним идентификатором
// такой ответ не нашёл бы разговора.
//
// При ошибке возвращаются идентификаторы уже отправленных частей: они в чате
// и связать их с разговором всё равно нужно.
func (c *Client) SendMessage(ctx context.Context, req SendRequest) ([]int, error) {
	var ids []int

	for _, chunk := range Split(req.Text) {
		if err := c.waitTurn(ctx); err != nil {
			return ids, err
		}

		messageID, err := c.sendChunk(ctx, req, chunk, true)
		if err != nil && markupRejected(err) {
			// Разметку не приняли. Содержание письма важнее оформления, но
			// голый текст отправлять нельзя: Telegram сам превратит /команды,
			// адреса и контакты в активные сущности. Сводим показ к одному
			// заново экранированному <pre>. В аварийном показе моноширинным
			// станет весь кусок, включая шапку: сохранять её оформление после
			// отказа означало бы снова угадывать, какой тег Telegram отверг.
			// Если отвергнут и <pre>, возвращаем ошибку, а не снимаем защиту.
			messageID, err = c.sendChunk(ctx, req, safeFallbackMarkup(chunk, req.MarkedLines), true)
			if err == nil {
				ids = append(ids, messageID)
				continue
			}
		}
		if err != nil {
			return ids, err
		}
		ids = append(ids, messageID)
	}

	return ids, nil
}

// sendChunk отправляет один кусок текста.
//
// Предпросмотр ссылок выключен и разметка HTML — ровно как раньше: письма
// агентов содержат пути и код, и раскрытые превью забивали бы канал.
func (c *Client) sendChunk(ctx context.Context, req SendRequest, text string, markup bool) (int, error) {
	params := &bot.SendMessageParams{
		ChatID:             req.ChatID,
		Text:               text,
		LinkPreviewOptions: &models.LinkPreviewOptions{IsDisabled: bot.True()},
	}
	if markup {
		params.ParseMode = models.ParseModeHTML
	}
	// Нулевую тему не передаём вовсе: в неформумном чате это ошибка API.
	if req.ThreadID != 0 {
		params.MessageThreadID = req.ThreadID
	}

	var messageID int
	err := c.outbound(ctx, "sendMessage", func(ctx context.Context) error {
		sent, err := c.api.SendMessage(ctx, params)
		if err != nil {
			return err
		}
		messageID = sent.ID
		return nil
	})
	return messageID, err
}

// markupRejected — Telegram отверг именно разметку, а не сообщение целиком.
func markupRejected(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusBadRequest {
		return false
	}

	d := strings.ToUpper(apiErr.Description)
	for _, marker := range []string{
		"CAN'T PARSE ENTITIES", "CAN'T FIND END TAG", "UNSUPPORTED START TAG",
		"UNCLOSED START TAG", "ENTITIES",
	} {
		if strings.Contains(d, marker) {
			return true
		}
	}
	return false
}

// CreateForumTopic заводит тему в форумной супергруппе.
func (c *Client) CreateForumTopic(ctx context.Context, chatID, name string) (int, error) {
	runes := []rune(name)
	if len(runes) > 128 {
		name = string(runes[:128])
	}

	var threadID int
	err := c.outbound(ctx, "createForumTopic", func(ctx context.Context) error {
		topic, err := c.api.CreateForumTopic(ctx, &bot.CreateForumTopicParams{
			ChatID: chatID,
			Name:   name,
		})
		if err != nil {
			return err
		}
		threadID = topic.MessageThreadID
		return nil
	})
	if err != nil {
		return 0, err
	}
	return threadID, nil
}

// GetUpdates — long polling входящих сообщений.
//
// Запрос собирается вручную, ответ разбирается моделями библиотеки. Разделение
// не случайное: своим остаётся то, чем мы управляем, — позиция чтения, набор
// типов обновлений и таймаут; чужим становится описание полей Bot API, которое
// всё равно ведёт не наш проект.
//
// Пачка разбирается ПОЭЛЕМЕНТНО, и у каждого элемента есть запасной путь.
// Причина не в аккуратности, а в том, как устроены модели библиотеки: поля с
// вариантами — forward_origin, paid_media, external_reply.origin — разбираются
// своим кодом, который на НЕИЗВЕСТНОМ значении type возвращает ошибку для
// всего обновления. Telegram новые варианты добавляет молча.
//
// Прежний разбор такие поля просто не описывал и потому не замечал. Пересланное
// сообщение нового вида уронило бы разбор всей пачки, а приём двигает позицию
// по тому, что разобрал: пачка пришла бы снова, и снова, и снова — то самое
// заклинивание, которое мы уже ловили на живом стенде. Один незнакомый вариант
// у Telegram — и мост перестаёт слышать человека, не сказав ни слова в журнал.
func (c *Client) GetUpdates(ctx context.Context, offset, timeoutSeconds int) ([]Update, error) {
	payload := map[string]any{
		"timeout":         timeoutSeconds,
		"allowed_updates": []string{"message"},
	}
	if offset > 0 {
		payload["offset"] = offset
	}

	// Сырые элементы: конверт и то, что result — массив, проверяет c.call, а
	// каждый элемент разбирается отдельно.
	var raw []json.RawMessage
	if err := c.call(ctx, "getUpdates", payload, &raw); err != nil {
		return nil, err
	}

	// Служебные обновления — создание темы, вход участника — возвращаются
	// наравне с текстовыми, хотя мосту с ними делать нечего.
	//
	// Отсеивать их здесь нельзя: приём двигает offset по тому, что ему
	// вернули, и пропущенный update_id означает, что подтверждения не будет
	// никогда. Пачка из одних служебных сообщений заклинивала приём намертво:
	// Telegram отдавал её снова и снова, а всё написанное человеком после
	// не доходило вовсе. Мост при этом выглядел здоровым — ни ошибки в логе,
	// ни остановки. Поймано на живом стенде: за семь минут работы моста
	// pending_update_count не сдвинулся с трёх.
	//
	// Решение, что делать с текстом, принимает Intake.handle: пустой текст
	// он и так пропускает молча.
	updates := make([]Update, 0, len(raw))
	for _, item := range raw {
		update, err := parseUpdate(item)
		if err != nil {
			// Вся пачка целиком, а не разобранная часть. Частичный результат
			// опаснее отказа: приём подтвердил бы позицию по тому, что сумел
			// прочесть, и неразобранное не пришло бы больше никогда. Отказ же
			// вернёт ту же пачку на следующем круге.
			return nil, err
		}
		updates = append(updates, update)
	}
	return updates, nil
}

// parseUpdate разбирает одно обновление, отступая на узкий разбор при отказе.
//
// Сначала модели библиотеки: они описывают Bot API полнее, чем мы, и держать
// это описание у себя незачем. Если модель отказалась — а отказывается она на
// незнакомом варианте поля, которым мост не пользуется, — берётся запасной
// узкий разбор ровно тех полей, что нужны приёму.
//
// Отступление не смягчает требований: если запасной разбор не понял
// используемое поле, идентификатор или сам JSON, обновление отвергается, и
// вместе с ним вся пачка.
func parseUpdate(raw json.RawMessage) (Update, error) {
	var item models.Update
	if err := json.Unmarshal(raw, &item); err == nil {
		return updateFromModel(item)
	}

	var narrow wireUpdate
	if err := json.Unmarshal(raw, &narrow); err != nil {
		return Update{}, fmt.Errorf("разбор обновления: %w", err)
	}
	return narrow.toUpdate()
}

// wireUpdate — запасное описание обновления ровно из используемых полей.
//
// Это прежний разбор, существовавший до перехода на модели, — теперь у него
// есть имя и одна задача: пережить незнакомое поле, которого мост всё равно
// не читает. Добавлять сюда поля «на будущее» не надо: чем шире описание, тем
// больше поводов отказать там, где отказывать не за что.
type wireUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  *struct {
		Text     string   `json:"text"`
		Entities []Entity `json:"entities"`
		// У сообщения с файлом текст человека лежит в caption, а разметка —
		// в caption_entities. Читаем их наравне с text/entities.
		Caption         string   `json:"caption"`
		CaptionEntities []Entity `json:"caption_entities"`
		Document        *struct {
			FileID   string `json:"file_id"`
			FileName string `json:"file_name"`
			FileSize int64  `json:"file_size"`
			MimeType string `json:"mime_type"`
		} `json:"document"`
		ReplyToMessage *struct {
			MessageID int `json:"message_id"`
			From      *struct {
				IsBot bool `json:"is_bot"`
			} `json:"from"`
		} `json:"reply_to_message"`
		ThreadID int `json:"message_thread_id"`
		Chat     struct {
			ID int64 `json:"id"`
		} `json:"chat"`
		From *struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
		} `json:"from"`
	} `json:"message"`
}

// toUpdate повторяет отображение моделей поле в поле.
//
// Расхождение между двумя путями было бы хуже отсутствия запасного: одно и то
// же письмо доходило бы по-разному в зависимости от того, попалось ли рядом
// незнакомое поле.
func (w wireUpdate) toUpdate() (Update, error) {
	id, err := updateID(w.UpdateID)
	if err != nil {
		return Update{}, err
	}

	update := Update{ID: id}
	if w.Message == nil {
		update.ChatID = strconv.FormatInt(0, 10)
		return update, nil
	}

	update.ChatID = strconv.FormatInt(w.Message.Chat.ID, 10)
	update.ThreadID = w.Message.ThreadID

	var doc *Attachment
	if d := w.Message.Document; d != nil {
		doc = &Attachment{FileID: d.FileID, FileName: d.FileName, FileSize: d.FileSize, MimeType: d.MimeType}
	}
	applyContent(&update, w.Message.Text, w.Message.Entities,
		w.Message.Caption, w.Message.CaptionEntities, doc)

	if from := w.Message.From; from != nil {
		update.From = from.Username
		update.FromID = from.ID
	}

	if reply := w.Message.ReplyToMessage; reply != nil {
		update.ReplyToMessageID = reply.MessageID
		update.ReplyToBot = reply.From != nil && reply.From.IsBot
	}

	return update, nil
}

// updateFromModel переводит обновление библиотеки в наше.
//
// Перевод явный и узкий: наружу выходит только то, чем пользуется мост.
// Библиотечные типы дальше этой функции не идут — иначе смена библиотеки
// стала бы изменением протокола всей сети.
//
// Отсутствующее сообщение — не ошибка. Служебное обновление приходит без
// текстового message, и вернуть его всё равно надо: приём двигает позицию
// чтения по тому, что получил, и пропуск одного update_id останавливает
// приём навсегда.
func updateFromModel(item models.Update) (Update, error) {
	id, err := updateID(item.ID)
	if err != nil {
		return Update{}, err
	}

	update := Update{ID: id}

	message := item.Message
	if message == nil {
		// Ни чата, ни отправителя: поля остаются нулевыми, как и при разборе
		// голого update_id прежним кодом. ChatID при этом «0» — строковый
		// ноль, а не пустая строка, и менять это здесь нельзя: маршрутизация
		// уже написана под такой вид.
		update.ChatID = strconv.FormatInt(0, 10)
		return update, nil
	}

	update.ChatID = strconv.FormatInt(message.Chat.ID, 10)
	update.ThreadID = message.MessageThreadID

	var doc *Attachment
	if d := message.Document; d != nil {
		doc = &Attachment{FileID: d.FileID, FileName: d.FileName, FileSize: d.FileSize, MimeType: d.MimeType}
	}
	applyContent(&update, message.Text, entitiesFromModel(message.Entities),
		message.Caption, entitiesFromModel(message.CaptionEntities), doc)

	if from := message.From; from != nil {
		update.From = from.Username
		update.FromID = from.ID
	}

	if reply := message.ReplyToMessage; reply != nil {
		update.ReplyToMessageID = reply.ID
		update.ReplyToBot = reply.From != nil && reply.From.IsBot
	}

	return update, nil
}

// applyContent кладёт в обновление текст, разметку и файл. Оба пути разбора
// зовут её, чтобы не разойтись: у сообщения с файлом человеческий текст лежит
// в caption, и если самого text нет — читаем caption как текст, а
// caption_entities как разметку. Так `/to` и разбор разговора работают для
// файла тем же кодом, что и для обычного сообщения.
//
// Различие nil и пустого списка разметки сохраняется: подставляем ровно то,
// что пришло, и ничего не «схлопываем».
func applyContent(u *Update, text string, entities []Entity,
	caption string, captionEntities []Entity, doc *Attachment,
) {
	u.Text = text
	u.Entities = entities
	if u.Text == "" && caption != "" {
		u.Text = caption
		u.Entities = captionEntities
	}
	u.Document = doc
}

// entitiesFromModel переносит разметку, сохраняя смещения нетронутыми.
//
// Смещения Telegram считает в кодовых единицах UTF-16, и пересчитывать их
// здесь нельзя ни во что: тот, кто их читает, знает об этом и рассчитывает на
// исходные числа.
//
// Отсутствие разметки остаётся отсутствием, а пустой список — пустым списком.
// Различать их надо по nil, а не по длине: прямой разбор JSON давал nil на
// отсутствующем поле и непустой указатель на `entities: []`, и схлопывание
// этих двух случаев сделало бы «Telegram ничего не прислал» неотличимым от
// «прислал пусто». Признак перестал бы быть признаком.
func entitiesFromModel(source []models.MessageEntity) []Entity {
	if source == nil {
		return nil
	}
	entities := make([]Entity, 0, len(source))
	for _, item := range source {
		entities = append(entities, Entity{
			Type:   string(item.Type),
			Offset: item.Offset,
			Length: item.Length,
		})
	}
	return entities
}

// updateID сужает идентификатор обновления до нашего int.
//
// Библиотека объявляет его int64, мост хранит int. На 64-битной платформе это
// одно и то же, на 32-битной — нет, и молчаливое усечение там означало бы
// сдвиг позиции чтения на чужое число: приём подтвердил бы обновления,
// которых не видел, и они не пришли бы больше никогда.
//
// Прежний код такой ошибки допустить не мог — он разбирал update_id прямо в
// int, и переполнение возвращалось ошибкой разбора JSON. Проверка здесь
// сохраняет ровно то поведение: отказ вместо тихой порчи.
func updateID(id int64) (int, error) {
	return narrowUpdateID(id, strconv.IntSize)
}

// narrowUpdateID — та же проверка с явно заданной разрядностью.
//
// Разрядность параметром не ради красоты: на 64-битной машине значения,
// не помещающегося в int, не существует, и ветка отказа недостижима. Тест с
// t.Skip доказывал бы только то, что тест пропущен. С явными битами проверка
// становится обычной функцией, и обе её стороны проверяются здесь же, без
// эмулятора и кросс-сборки.
func narrowUpdateID(id int64, bits int) (int, error) {
	if bits < 64 {
		limit := int64(1) << (bits - 1)
		if id >= limit || id < -limit {
			return 0, fmt.Errorf("update_id %d не помещается в %d-битный int", id, bits)
		}
	}
	return int(id), nil
}

// GetMe проверяет токен на старте.
//
// Ранняя диагностика: молчащий из-за опечатки бот отлаживается мучительно.
func (c *Client) GetMe(ctx context.Context) (string, error) {
	var username string
	err := c.outbound(ctx, "getMe", func(ctx context.Context) error {
		me, err := c.api.GetMe(ctx)
		if err != nil {
			return err
		}
		username = me.Username
		return nil
	})
	if err != nil {
		return "", err
	}
	return username, nil
}
