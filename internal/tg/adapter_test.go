package tg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-telegram/bot"
)

// telegramDouble отвечает заданным кодом и телом.
func telegramDouble(t *testing.T, status int, body map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// вызовЧерезАдаптер повторяет то, что будет делать клиент: контекст с
// накопителем, вызов метода библиотеки, перевод ошибки.
func вызовЧерезАдаптер(t *testing.T, srv *httptest.Server) error {
	t.Helper()
	b, err := bot.New("123:тест",
		bot.WithSkipGetMe(),
		bot.WithServerURL(srv.URL),
		bot.WithHTTPClient(0, &capturingClient{inner: srv.Client()}),
	)
	if err != nil {
		t.Fatalf("создание бота: %v", err)
	}

	ctx, capture := withCapture(context.Background())
	_, callErr := b.GetMe(ctx)
	return asAPIError("getMe", capture, callErr)
}

// Код 500 доезжает числом, а не теряется.
//
// У библиотеки для 5xx нет своего значения: она отдаёт текст, в котором код
// только напечатан. Перехват ответа ниже библиотеки сохраняет число.
func TestКод500ДоезжаетЧислом(t *testing.T) {
	srv := telegramDouble(t, 500, map[string]any{"ok": false, "description": "Internal Server Error"})

	err := вызовЧерезАдаптер(t, srv)

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("ошибка не APIError: %v", err)
	}
	if apiErr.Code != 500 {
		t.Fatalf("код %d, ожидался 500: число потеряно", apiErr.Code)
	}
	if apiErr.Description != "Internal Server Error" {
		t.Fatalf("описание %q — взято не из ответа Telegram", apiErr.Description)
	}
}

// Код берётся из HTTP-ответа, а не из тела.
//
// Прежний клиент клал в APIError.Code именно статус ответа, и на этом стоит
// классификация отказов. Библиотека смотрит только на error_code в теле;
// когда они расходятся, parity даёт перехват, а не библиотека.
func TestКодБерётсяИзОтветаАНеИзТела(t *testing.T) {
	srv := telegramDouble(t, 502, map[string]any{
		"ok": false, "error_code": 400, "description": "Bad Gateway",
	})

	err := вызовЧерезАдаптер(t, srv)

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("ошибка не APIError: %v", err)
	}
	if apiErr.Code != 502 {
		t.Fatalf("код %d, ожидался 502 — как в прежнем клиенте", apiErr.Code)
	}
}

// Пауза из retry_after достаётся, а публичный тип не меняется.
func TestПаузаИзОтветаДоступнаВнутри(t *testing.T) {
	srv := telegramDouble(t, 429, map[string]any{
		"ok": false, "description": "Too Many Requests: retry after 7",
		"parameters": map[string]any{"retry_after": 7},
	})

	b, err := bot.New("123:тест", bot.WithSkipGetMe(), bot.WithServerURL(srv.URL),
		bot.WithHTTPClient(0, &capturingClient{inner: srv.Client()}))
	if err != nil {
		t.Fatalf("создание бота: %v", err)
	}
	ctx, capture := withCapture(context.Background())
	_, callErr := b.GetMe(ctx)

	if got := retryAfterOf(capture, callErr); got != 7 {
		t.Fatalf("пауза %d, ожидалась 7", got)
	}

	apiErr := &APIError{}
	if !errors.As(asAPIError("getMe", capture, callErr), &apiErr) || apiErr.Code != 429 {
		t.Fatalf("код %d, ожидался 429", apiErr.Code)
	}
}

// Два одновременных вызова не смешивают перехваченное.
//
// Накопитель живёт в контексте вызова, а не в клиенте: клиент один на всех, и
// общий изменяемый накопитель отдал бы одному вызову чужой отказ.
//
// Устройство проверки важнее её наличия. Два вызова просто «одновременно»
// ничего не различают: при общем накопителе они могут разойтись во времени и
// случайно получить каждый своё. Поэтому медленный вызов начинается первым и
// заканчивается последним — при общем накопителе он гарантированно застанет
// там чужой, только что записанный отказ.
func TestОдновременныеВызовыНеСмешиваютОтказы(t *testing.T) {
	медленный := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(403)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "description": "Forbidden: bot was kicked"})
	}))
	defer медленный.Close()

	быстрый := telegramDouble(t, 400, map[string]any{"ok": false, "description": "Bad Request: TOPIC_CLOSED"})

	var wg sync.WaitGroup
	var медленнаяОшибка, быстраяОшибка error

	wg.Add(1)
	go func() {
		defer wg.Done()
		медленнаяОшибка = вызовЧерезАдаптер(t, медленный)
	}()

	// Даём медленному уйти в запрос, затем быстрый успевает ответить целиком.
	time.Sleep(50 * time.Millisecond)
	быстраяОшибка = вызовЧерезАдаптер(t, быстрый)
	wg.Wait()

	var медленная, быстрая *APIError
	if !errors.As(медленнаяОшибка, &медленная) || !errors.As(быстраяОшибка, &быстрая) {
		t.Fatalf("ошибки не APIError: %v, %v", медленнаяОшибка, быстраяОшибка)
	}
	if медленная.Code != 403 {
		t.Fatalf("медленный вызов получил код %d вместо 403: перехваченное смешалось", медленная.Code)
	}
	if быстрая.Code != 400 {
		t.Fatalf("быстрый вызов получил код %d вместо 400", быстрая.Code)
	}
}

// Библиотека получает тело нетронутым и разбирает удачный ответ.
//
// Перехват читает поток целиком, поэтому обязан вернуть его на место —
// иначе разбор результата у библиотеки увидит пустоту.
func TestУспешныйОтветРазбираетсяПослеПерехвата(t *testing.T) {
	srv := telegramDouble(t, 200, map[string]any{
		"ok": true, "result": map[string]any{"id": 42, "is_bot": true, "username": "двойник_бот"},
	})

	b, err := bot.New("123:тест", bot.WithSkipGetMe(), bot.WithServerURL(srv.URL),
		bot.WithHTTPClient(0, &capturingClient{inner: srv.Client()}))
	if err != nil {
		t.Fatalf("создание бота: %v", err)
	}
	ctx, capture := withCapture(context.Background())
	me, callErr := b.GetMe(ctx)
	if err := asAPIError("getMe", capture, callErr); err != nil {
		t.Fatalf("удачный вызов дал ошибку: %v", err)
	}
	if me == nil || me.Username != "двойник_бот" {
		t.Fatalf("результат не разобран: %+v", me)
	}
}

// Беда без ответа Telegram не выдаётся за отказ Telegram.
//
// Оборванная связь — не APIError: классификация отказов ниже по коду решит,
// что чат недоступен, хотя чат ни при чём.
func TestОбрывСвязиНеСтановитсяAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // сервер мёртв: соединение не установится

	b, err := bot.New("123:тест", bot.WithSkipGetMe(), bot.WithServerURL(srv.URL),
		bot.WithHTTPClient(0, &capturingClient{inner: &http.Client{}}))
	if err != nil {
		t.Fatalf("создание бота: %v", err)
	}
	ctx, capture := withCapture(context.Background())
	_, callErr := b.GetMe(ctx)

	converted := asAPIError("getMe", capture, callErr)
	var apiErr *APIError
	if errors.As(converted, &apiErr) {
		t.Fatalf("обрыв связи выдан за отказ Telegram: %+v", apiErr)
	}
	if converted == nil {
		t.Fatal("обрыв связи потерян")
	}
}

// Перехваченное принадлежит своему вызову, а не клиенту.
//
// Проверка прямая, на уровне самого механизма, и это нужно: сквозной тест с
// двумя запросами не различает изоляцию от общего накопителя, потому что при
// удачной последовательности каждый вызов и там прочитает своё. Здесь оба
// ответа перехватываются ДО того, как хоть один прочитан, — при общем
// накопителе первый неизбежно увидит второй.
func TestПерехваченноеПринадлежитСвоемуВызову(t *testing.T) {
	первый := telegramDouble(t, 403, map[string]any{"ok": false, "description": "Forbidden: bot was kicked"})
	второй := telegramDouble(t, 400, map[string]any{"ok": false, "description": "Bad Request: TOPIC_CLOSED"})

	client := &capturingClient{inner: &http.Client{}}

	прогнать := func(ctx context.Context, srv *httptest.Server) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
		if err != nil {
			t.Fatalf("запрос: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("вызов: %v", err)
		}
		_ = resp.Body.Close()
	}

	ctx1, capture1 := withCapture(context.Background())
	ctx2, capture2 := withCapture(context.Background())

	// Оба ответа перехвачены раньше, чем прочитан хоть один.
	прогнать(ctx1, первый)
	прогнать(ctx2, второй)

	failure1, ok1 := capture1.taken()
	failure2, ok2 := capture2.taken()
	if !ok1 || !ok2 {
		t.Fatalf("отказ не перехвачен: первый=%v второй=%v", ok1, ok2)
	}
	if failure1.status != 403 {
		t.Fatalf("первый вызов видит код %d вместо 403 — накопитель общий", failure1.status)
	}
	if failure2.status != 400 {
		t.Fatalf("второй вызов видит код %d вместо 400", failure2.status)
	}
}

// Забытый накопитель даёт обычную ошибку, а не панику.
//
// Витрина и приём живут в одном процессе с этим клиентом: паника внутри
// одного вызова остановила бы мост целиком. Случай не выдуманный — методов
// станет несколько, и завести накопитель в одном из них легко забыть.
func TestБезНакопителяНетПаники(t *testing.T) {
	err := asAPIError("sendMessage", nil, errors.New("что-то пошло не так"))
	if err == nil {
		t.Fatal("ошибка потеряна")
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		t.Fatalf("отказ без накопителя выдан за отказ Telegram: %+v", apiErr)
	}
	if got := retryAfterOf(nil, errors.New("другая беда")); got != 0 {
		t.Fatalf("пауза %d без накопителя, ожидался ноль", got)
	}
}

// Пустой токен не создаёт клиента, но и не роняет процесс.
//
// Библиотека отвергает пустой токен в конструкторе, а наш New ошибки не
// возвращает — сигнатура публичная, и менять её ради случая, который
// отсеивается в конфигурации раньше, значило бы трогать всех вызывающих.
// Поэтому отказ откладывается до первого вызова и не выдаётся за отказ
// Telegram: иначе классификация решит, что нас выгнали из чата.
func TestПустойТокенОтказываетПриПервомВызове(t *testing.T) {
	client := New("")

	_, err := client.GetMe(context.Background())
	if err == nil {
		t.Fatal("пустой токен принят молча")
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		t.Fatalf("отказ создания выдан за отказ Telegram: %+v", apiErr)
	}
}

// Создание клиента не ходит в сеть.
//
// Библиотека умеет проверять токен в конструкторе; нам это не нужно и вредно:
// мост создаёт клиент до того, как решит, работать ли вообще, а сеть в
// конструкторе превращает его в отказ старта при первой же беде со связью.
func TestСозданиеКлиентаНеХодитВСеть(t *testing.T) {
	var обращения int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		обращения++
		_, _ = w.Write([]byte(`{"ok":true,"result":{}}`))
	}))
	defer srv.Close()

	_ = New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()))

	if обращения != 0 {
		t.Fatalf("конструктор сделал %d обращений к Telegram, ожидалось ноль", обращения)
	}
}

// Отказ по частоте лечится одним повтором, а не бесконечным.
//
// Поведение существовало и до перехода на библиотеку, но тестом закреплено не
// было — обнаружилось мутацией «не повторять», которая не покрасила ничего.
// Повтор здесь ровно один: второй отказ отдаётся вызывающему, иначе мост
// молотил бы паузами, пока Telegram не сдастся.
func TestОтказПоЧастотеПовторяетсяОдинРаз(t *testing.T) {
	var mu sync.Mutex
	var обращения int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		обращения++
		первое := обращения == 1
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if первое {
			w.WriteHeader(http.StatusTooManyRequests)
			// Пауза в одну секунду: тест ждёт её по-настоящему, потому что
			// проверяется в том числе то, что мост её соблюдает.
			_, _ = w.Write([]byte(`{"ok":false,"description":"Too Many Requests","parameters":{"retry_after":1}}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
	}))
	defer srv.Close()

	client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
	начало := time.Now()
	ids, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-100", Text: "привет"})
	if err != nil {
		t.Fatalf("сообщение не доставлено: %v", err)
	}
	if len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("идентификаторы %v, ожидался [7]", ids)
	}

	mu.Lock()
	defer mu.Unlock()
	if обращения != 2 {
		t.Fatalf("обращений %d, ожидалось два: отказ и повтор", обращения)
	}
	if прошло := time.Since(начало); прошло < time.Second {
		t.Fatalf("повтор ушёл через %v — пауза из retry_after не соблюдена", прошло)
	}
}

// Отмена во время паузы прерывает ожидание, а не досиживает его.
func TestОтменаВоВремяПаузыПрерываетОжидание(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		// Пауза заведомо больше, чем ждёт тест.
		_, _ = w.Write([]byte(`{"ok":false,"description":"Too Many Requests","parameters":{"retry_after":30}}`))
	}))
	defer srv.Close()

	client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	начало := time.Now()
	_, err := client.SendMessage(ctx, SendRequest{ChatID: "-100", Text: "привет"})
	if err == nil {
		t.Fatal("отмена не вернула ошибку")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("вернулась ошибка %v, ожидалась отмена контекста", err)
	}
	if прошло := time.Since(начало); прошло > 5*time.Second {
		t.Fatalf("ожидание длилось %v — отмена не прервала паузу", прошло)
	}
}

// Текст, состоящий из цифр, остаётся текстом.
//
// Приведение типов идёт по имени поля, а не по виду значения. Иначе
// сообщение «123» приходило бы из multipart числом, а из JSON строкой, и
// тест, сравнивающий текст, показал бы расхождение, которого нет.
func TestЧисловойТекстНеСтановитсяЧислом(t *testing.T) {
	var got map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fields, err := readRequestFields(r)
		if err != nil {
			t.Errorf("двойник не разобрал запрос: %v", err)
		}
		got = fields
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer srv.Close()

	client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
	if _, err := client.SendMessage(context.Background(), SendRequest{
		ChatID: "-100", Text: "123", ThreadID: 7,
	}); err != nil {
		t.Fatalf("отправка: %v", err)
	}

	if text, ok := got["text"].(string); !ok || text != "123" {
		t.Errorf("text = %#v, ожидалась строка «123»", got["text"])
	}
	if thread, ok := got["message_thread_id"].(float64); !ok || thread != 7 {
		t.Errorf("message_thread_id = %#v, ожидалось число 7", got["message_thread_id"])
	}
}

// Предпросмотр ссылок выключен, и это проверяется.
//
// Поведение прежнее, но носитель сменился: было поле disable_web_page_preview,
// стало link_preview_options.is_disabled. Мутация «убрать выключение» не
// красила ничего — дыру нашло ревью mbp-claude.
//
// Держится на этом много: письма агентов полны путей и ссылок, и раскрытые
// превью забили бы канал так, что читать его стало бы нельзя.
func TestПредпросмотрСсылокВыключен(t *testing.T) {
	var got map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fields, err := readRequestFields(r)
		if err != nil {
			t.Errorf("двойник не разобрал запрос: %v", err)
		}
		got = fields
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer srv.Close()

	client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
	if _, err := client.SendMessage(context.Background(), SendRequest{
		ChatID: "-100", Text: "смотри https://example.com/очень/длинный/путь",
	}); err != nil {
		t.Fatalf("отправка: %v", err)
	}

	raw, ok := got["link_preview_options"].(string)
	if !ok {
		t.Fatalf("поле link_preview_options не передано вовсе: %#v", got["link_preview_options"])
	}
	var options struct {
		IsDisabled bool `json:"is_disabled"`
	}
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		t.Fatalf("разбор link_preview_options %q: %v", raw, err)
	}
	if !options.IsDisabled {
		t.Fatal("предпросмотр ссылок не выключен — канал забьётся раскрытыми превью")
	}
}

// Не-JSON ответ остаётся ошибкой разбора и не выдаётся за отказ Telegram.
//
// Проверяется через настоящий метод клиента, а не мимо него: важно, что путь
// целиком — библиотека, перехват, перевод ошибки — не выдумывает APIError из
// мусора. Иначе страница от прокси выглядела бы как «нас выгнали из чата».
func TestНеJSONОтветНеСтановитсяОтказомTelegram(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html><body>502 Bad Gateway</body></html>"))
	}))
	defer srv.Close()

	client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
	_, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-100", Text: "привет"})
	if err == nil {
		t.Fatal("мусор вместо ответа принят за успех")
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		t.Fatalf("не-JSON ответ выдан за отказ Telegram: %+v", apiErr)
	}
}

// Отказ Telegram доезжает точным APIError через КАЖДЫЙ из трёх методов.
//
// Поштучно, а не «через адаптер»: обход outbound в одном из методов иначе
// остался бы незамеченным — там строится накопитель, повтор и перевод ошибки.
func TestОтказДоезжаетЧерезКаждыйМетод(t *testing.T) {
	вызовы := []struct {
		имя   string
		звать func(*Client) error
	}{
		{"getMe", func(c *Client) error { _, err := c.GetMe(context.Background()); return err }},
		{"sendMessage", func(c *Client) error {
			_, err := c.SendMessage(context.Background(), SendRequest{ChatID: "-100", Text: "привет"})
			return err
		}},
		{"createForumTopic", func(c *Client) error {
			_, err := c.CreateForumTopic(context.Background(), "-100", "тема")
			return err
		}},
	}

	for _, вызов := range вызовы {
		t.Run(вызов.имя, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"ok":false,"description":"Forbidden: bot was kicked from the group chat"}`))
			}))
			defer srv.Close()

			client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
			err := вызов.звать(client)

			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("ошибка не APIError: %v", err)
			}
			if apiErr.Code != http.StatusForbidden {
				t.Errorf("код %d, ожидался 403", apiErr.Code)
			}
			if !strings.Contains(apiErr.Description, "bot was kicked") {
				t.Errorf("описание %q — взято не из ответа Telegram", apiErr.Description)
			}
		})
	}
}

// Без retry_after пауза всё равно есть, и отмена её прерывает.
//
// Пара к тесту с заданной паузой: там проверялось, что мост слушается
// Telegram, здесь — что при молчании Telegram он не бросается повторять
// немедленно. Три секунды по умолчанию тест не досиживает: он отменяет
// контекст и убеждается, что второго запроса не случилось.
func TestБезRetryAfterПаузаЕстьИПрерываетсяОтменой(t *testing.T) {
	var mu sync.Mutex
	var обращения int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		обращения++
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		// Telegram промолчал о сроке — мост обязан выждать своё умолчание.
		_, _ = w.Write([]byte(`{"ok":false,"description":"Too Many Requests"}`))
	}))
	defer srv.Close()

	client := New("123:тест", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithMinSendGap(0))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	начало := time.Now()
	_, err := client.SendMessage(ctx, SendRequest{ChatID: "-100", Text: "привет"})
	if err == nil {
		t.Fatal("отказ по частоте не вернул ошибки")
	}
	// Именно отмена, а не любая ошибка: иначе в ветке ожидания могла бы
	// возвращаться посторонняя беда, и тест этого не отличил бы.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("вернулась ошибка %v, ожидалась отмена контекста", err)
	}
	if прошло := time.Since(начало); прошло >= 3*time.Second {
		t.Fatalf("ожидание длилось %v — отмена не прервала паузу", прошло)
	}

	mu.Lock()
	defer mu.Unlock()
	if обращения != 1 {
		t.Fatalf("обращений %d, ожидалось одно: повтор ушёл, не выждав паузы", обращения)
	}
}
