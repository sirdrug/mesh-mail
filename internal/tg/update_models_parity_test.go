package tg_test

// Паритетный набор для перевода разбора getUpdates на models.Update.
//
// Проверки НЕ знают, чем разобран ответ Telegram: они смотрят только на то,
// что вернул публичный Client.GetUpdates. Поэтому набор обязан быть зелёным
// и до миграции, и после — расхождение любого поля означает, что миграция
// изменила поведение, а не только устройство.
//
// Пакет внешний (tg_test) намеренно: изнутри пакета легко нечаянно опереться
// на внутреннюю структуру разбора, и тогда тест перестанет отвечать на вопрос
// «изменилось ли наблюдаемое поведение».

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// apiDouble — двойник Bot API, отдающий заготовленный СЫРОЙ result.
//
// Сырой, а не собранный из map: часть проверок подсовывает намеренно
// негодный JSON (переполнение, чужой тип поля), и через map такое тело не
// выразить — encoding/json собрал бы его обратно правильным.
type apiDouble struct {
	client *tg.Client

	mu    sync.Mutex
	calls []recordedCall
}

// recordedCall — что двойник получил: имя метода и поля тела, приведённые
// к общему виду.
type recordedCall struct {
	method string
	fields map[string]any
}

// newDouble поднимает двойника, отвечающий одним и тем же result на любой
// вызов, и возвращает клиента, настроенного на него.
func newDouble(t *testing.T, rawResult string) *apiDouble {
	t.Helper()
	return newDoubleWithEnvelope(t, `{"ok":true,"result":`+rawResult+`}`)
}

// newDoubleWithEnvelope нужен там, где проверяется реакция на негодный ответ
// целиком, а не только на его result.
func newDoubleWithEnvelope(t *testing.T, envelope string) *apiDouble {
	t.Helper()

	double := &apiDouble{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		method := parts[len(parts)-1]

		fields, err := requestFields(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"ok":false,"description":"двойник не разобрал запрос"}`)
			return
		}

		double.mu.Lock()
		double.calls = append(double.calls, recordedCall{method: method, fields: fields})
		double.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelope)
	}))
	t.Cleanup(srv.Close)

	double.client = tg.New("тестовый-токен", tg.WithBaseURL(srv.URL), tg.WithHTTPClient(srv.Client()))
	return double
}

// lastCall — последний запрос, дошедший до двойника.
func (d *apiDouble) lastCall(t *testing.T) recordedCall {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.calls) == 0 {
		t.Fatalf("двойник не получил ни одного запроса")
	}
	return d.calls[len(d.calls)-1]
}

// requestFields приводит тело запроса к общему виду.
//
// Понимает и JSON, и multipart. Сейчас getUpdates идёт своим путём и шлёт
// JSON, но утверждения набора говорят о СОДЕРЖИМОМ запроса, а не о способе
// его передачи: тест, падающий от смены транспорта, сообщал бы не о том, о
// чём написан.
func requestFields(r *http.Request) (map[string]any, error) {
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil && contentType != "" {
		return nil, fmt.Errorf("тип содержимого %q: %w", contentType, err)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		if params["boundary"] == "" {
			return nil, errors.New("multipart без границы")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			if errors.Is(err, io.EOF) {
				return map[string]any{}, nil
			}
			return nil, fmt.Errorf("разбор multipart: %w", err)
		}
		out := make(map[string]any, len(r.MultipartForm.Value))
		for name, values := range r.MultipartForm.Value {
			if len(values) > 0 {
				out[name] = values[0]
			}
		}
		return out, nil
	}

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		if errors.Is(err, io.EOF) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("разбор JSON: %w", err)
	}
	if body == nil {
		body = map[string]any{}
	}
	return body, nil
}

// numberField достаёт числовое поле независимо от вида тела.
func numberField(t *testing.T, fields map[string]any, name string) (float64, bool) {
	t.Helper()
	value, ok := fields[name]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case string:
		var parsed float64
		if _, err := fmt.Sscanf(typed, "%g", &parsed); err != nil {
			t.Fatalf("поле %s=%q не число", name, typed)
		}
		return parsed, true
	default:
		t.Fatalf("поле %s неожиданного типа %T", name, value)
		return 0, false
	}
}

// stringList достаёт список строк: в JSON это массив, в multipart — та же
// строка в JSON-виде.
func stringList(t *testing.T, fields map[string]any, name string) ([]string, bool) {
	t.Helper()
	value, ok := fields[name]
	if !ok {
		return nil, false
	}
	switch typed := value.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("в списке %s не строка: %T", name, item)
			}
			out = append(out, text)
		}
		return out, true
	case string:
		var out []string
		if err := json.Unmarshal([]byte(typed), &out); err != nil {
			t.Fatalf("список %s=%q не разобран: %v", name, typed, err)
		}
		return out, true
	default:
		t.Fatalf("поле %s неожиданного типа %T", name, value)
		return nil, false
	}
}

// TestParityПолноеОтображениеПолей — сквозная проверка всех полей Update.
//
// Собрана одним сообщением намеренно: поля разбираются вместе, и ошибка
// миграции чаще всего теряет одно из них, оставляя остальные на месте.
func TestParityПолноеОтображениеПолей(t *testing.T) {
	double := newDouble(t, `[{
		"update_id": 4242,
		"message": {
			"message_id": 77,
			"text": "/to pi-claude посмотри",
			"message_thread_id": 19,
			"chat": {"id": -1001234567890},
			"from": {"id": 987654321, "username": "tester"},
			"entities": [
				{"type": "bot_command", "offset": 0, "length": 3},
				{"type": "mention", "offset": 4, "length": 10}
			],
			"reply_to_message": {"message_id": 512, "from": {"is_bot": true}}
		}
	}]`)

	got, err := double.client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("обновлений %d, ожидалось 1", len(got))
	}
	update := got[0]

	if update.ID != 4242 {
		t.Errorf("ID = %d, ожидалось 4242", update.ID)
	}
	// Идентификатор супергруппы не помещается в int32: сужение типа при
	// миграции проявилось бы именно здесь и только на боевом чате.
	if update.ChatID != "-1001234567890" {
		t.Errorf("ChatID = %q, ожидалось -1001234567890", update.ChatID)
	}
	if update.ThreadID != 19 {
		t.Errorf("ThreadID = %d, ожидалось 19", update.ThreadID)
	}
	if update.Text != "/to pi-claude посмотри" {
		t.Errorf("Text = %q", update.Text)
	}
	if update.From != "tester" {
		t.Errorf("From = %q, ожидалось tester", update.From)
	}
	if update.FromID != 987654321 {
		t.Errorf("FromID = %d, ожидалось 987654321", update.FromID)
	}
	if update.ReplyToMessageID != 512 {
		t.Errorf("ReplyToMessageID = %d, ожидалось 512", update.ReplyToMessageID)
	}
	if !update.ReplyToBot {
		t.Errorf("ReplyToBot = false, ожидалось true")
	}

	want := []tg.Entity{
		{Type: "bot_command", Offset: 0, Length: 3},
		{Type: "mention", Offset: 4, Length: 10},
	}
	if len(update.Entities) != len(want) {
		t.Fatalf("разметки %d, ожидалось %d: %+v", len(update.Entities), len(want), update.Entities)
	}
	for i, entity := range want {
		if update.Entities[i] != entity {
			t.Errorf("разметка %d = %+v, ожидалась %+v", i, update.Entities[i], entity)
		}
	}
}

// TestParityСмещенияРазметкиНеПересчитываются.
//
// Telegram считает смещения в единицах UTF-16, а Go — в байтах или рунах.
// Разбор обязан отдавать число КАК ПРИСЛАЛИ: пересчёт здесь — тихая порча
// признака, по которому мост отличает команду от текста.
func TestParityСмещенияРазметкиНеПересчитываются(t *testing.T) {
	// В тексте эмодзи вне BMP: одна руна, но две единицы UTF-16 и четыре
	// байта. Смещение 3 не совпадает ни с рунным (2), ни с байтовым (5).
	double := newDouble(t, `[{
		"update_id": 1,
		"message": {
			"text": "😀 /tmp",
			"chat": {"id": -1001},
			"from": {"id": 7},
			"entities": [{"type": "bot_command", "offset": 3, "length": 4}]
		}
	}]`)

	got, err := double.client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got[0].Entities) != 1 {
		t.Fatalf("разметки %d, ожидалась 1", len(got[0].Entities))
	}
	entity := got[0].Entities[0]
	if entity.Offset != 3 {
		t.Errorf("Offset = %d, ожидалось 3 (единицы UTF-16, как прислал Telegram)", entity.Offset)
	}
	if entity.Length != 4 {
		t.Errorf("Length = %d, ожидалось 4", entity.Length)
	}
	if entity.Type != "bot_command" {
		t.Errorf("Type = %q", entity.Type)
	}
}

// TestParityРазметкаБезПоляИПустаяРазличаются.
//
// «Разметки нет вовсе» и «Telegram прислал пустой список» — разные новости, и
// мост на них смотрит: nil означает, что признака нет, пустой непустой слайс —
// что Telegram посмотрел и ничего не нашёл. Миграция, прогоняющая разметку
// через make([]Entity, 0, n), стирает это различие молча.
func TestParityРазметкаБезПоляИПустаяРазличаются(t *testing.T) {
	t.Run("поля нет — nil", func(t *testing.T) {
		double := newDouble(t, `[{"update_id":1,"message":{"text":"без разметки","chat":{"id":-1001},"from":{"id":7}}}]`)
		got, err := double.client.GetUpdates(context.Background(), 0, 1)
		if err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}
		if got[0].Entities != nil {
			t.Errorf("Entities = %+v, ожидался nil", got[0].Entities)
		}
	})

	t.Run("пустой список — не nil", func(t *testing.T) {
		double := newDouble(t, `[{"update_id":1,"message":{"text":"пусто","entities":[],"chat":{"id":-1001},"from":{"id":7}}}]`)
		got, err := double.client.GetUpdates(context.Background(), 0, 1)
		if err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}
		if got[0].Entities == nil {
			t.Errorf("Entities = nil, ожидался пустой непустой слайс")
		}
		if len(got[0].Entities) != 0 {
			t.Errorf("Entities = %+v, ожидался пустым", got[0].Entities)
		}
	})
}

// TestParityНеполныеОбновленияНеПадаютИДаютНули.
//
// В models.Update message, from и reply_to_message — указатели, а сейчас это
// вложенные структуры. Именно здесь миграция получает nil там, где раньше был
// нулевой объект, и именно здесь она уронит мост, если разыменует его без
// проверки. Ожидания записаны как ПРЕЖНИЕ нулевые значения, включая ChatID
// "0": это строка от числа, а не пустая строка.
func TestParityНеполныеОбновленияНеПадаютИДаютНули(t *testing.T) {
	cases := []struct {
		имя    string
		result string
		want   tg.Update
	}{
		{
			имя:    "сообщения нет вовсе",
			result: `[{"update_id":10}]`,
			want:   tg.Update{ID: 10, ChatID: "0"},
		},
		{
			имя:    "служебное сообщение без текста",
			result: `[{"update_id":11,"message":{"message_thread_id":27,"chat":{"id":-1001},"from":{"id":1},"forum_topic_created":{"name":"тема"}}}]`,
			want:   tg.Update{ID: 11, ChatID: "-1001", ThreadID: 27, FromID: 1},
		},
		{
			имя:    "отправителя нет",
			result: `[{"update_id":12,"message":{"text":"из канала","chat":{"id":-1001}}}]`,
			want:   tg.Update{ID: 12, ChatID: "-1001", Text: "из канала"},
		},
		{
			имя:    "ответа нет",
			result: `[{"update_id":13,"message":{"text":"просто реплика","chat":{"id":-1001},"from":{"id":7,"username":"tester"}}}]`,
			want:   tg.Update{ID: 13, ChatID: "-1001", Text: "просто реплика", From: "tester", FromID: 7},
		},
		{
			// Ответ есть, а отправителя внутри ответа нет: пост от имени
			// канала. ReplyToMessageID обязан сохраниться — по нему мост
			// находит разговор, — а ReplyToBot стать false, потому что
			// «ответили боту» здесь не подтверждено.
			имя:    "внутри ответа нет отправителя",
			result: `[{"update_id":14,"message":{"text":"ага","chat":{"id":-1001},"from":{"id":7},"reply_to_message":{"message_id":900}}}]`,
			want:   tg.Update{ID: 14, ChatID: "-1001", Text: "ага", FromID: 7, ReplyToMessageID: 900},
		},
		{
			имя:    "ответ человеку, а не боту",
			result: `[{"update_id":15,"message":{"text":"ага","chat":{"id":-1001},"from":{"id":7},"reply_to_message":{"message_id":901,"from":{"is_bot":false}}}}]`,
			want:   tg.Update{ID: 15, ChatID: "-1001", Text: "ага", FromID: 7, ReplyToMessageID: 901},
		},
	}

	for _, c := range cases {
		t.Run(c.имя, func(t *testing.T) {
			double := newDouble(t, c.result)

			got, err := double.client.GetUpdates(context.Background(), 0, 1)
			if err != nil {
				t.Fatalf("получение обновлений: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("обновлений %d, ожидалось 1", len(got))
			}
			// Сравнение целиком, а не по полям: пропущенное поле — ровно та
			// ошибка, которую ищет паритетный набор.
			if got[0].Entities != nil {
				t.Errorf("Entities = %+v, ожидался nil", got[0].Entities)
			}
			got[0].Entities = nil
			if !reflect.DeepEqual(got[0], c.want) {
				t.Errorf("Update = %+v,\nожидалось     %+v", got[0], c.want)
			}
		})
	}
}

// TestParityЗапросНесётТаймаутИТолькоMessage.
//
// allowed_updates ровно ["message"] — не косметика: подписка на прочие типы
// вернула бы обновления, которых приём не разбирает, а offset двигался бы по
// ним. Смещение при нуле не шлётся вовсе — таков нынешний контракт.
func TestParityЗапросНесётТаймаутИТолькоMessage(t *testing.T) {
	t.Run("offset нулевой — поля нет", func(t *testing.T) {
		double := newDouble(t, `[]`)
		if _, err := double.client.GetUpdates(context.Background(), 0, 25); err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}

		call := double.lastCall(t)
		if call.method != "getUpdates" {
			t.Errorf("метод %q, ожидался getUpdates", call.method)
		}
		if _, ok := call.fields["offset"]; ok {
			t.Errorf("offset передан при нуле: %+v", call.fields)
		}
		timeout, ok := numberField(t, call.fields, "timeout")
		if !ok || timeout != 25 {
			t.Errorf("timeout = %v (есть: %v), ожидалось 25", timeout, ok)
		}
		allowed, ok := stringList(t, call.fields, "allowed_updates")
		if !ok {
			t.Fatalf("allowed_updates не передан: %+v", call.fields)
		}
		if len(allowed) != 1 || allowed[0] != "message" {
			t.Errorf("allowed_updates = %v, ожидалось [message]", allowed)
		}
	})

	t.Run("offset положительный — передан как есть", func(t *testing.T) {
		double := newDouble(t, `[]`)
		if _, err := double.client.GetUpdates(context.Background(), 100500, 1); err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}

		offset, ok := numberField(t, double.lastCall(t).fields, "offset")
		if !ok {
			t.Fatalf("offset не передан")
		}
		if offset != 100500 {
			t.Errorf("offset = %v, ожидалось 100500", offset)
		}
	})
}

// TestParityПачкаСохраняетПорядокИВсеИдентификаторы.
//
// Отсев служебных обновлений здесь запрещён: приём двигает offset по тому,
// что ему вернули, и пропущенный update_id означает, что подтверждения не
// будет никогда — пачка придёт снова, и так до бесконечности. Ловилось на
// живом стенде, поэтому проверяется и порядок, и полнота.
func TestParityПачкаСохраняетПорядокИВсеИдентификаторы(t *testing.T) {
	double := newDouble(t, `[
		{"update_id":200,"message":{"chat":{"id":-1001},"from":{"id":1},"forum_topic_created":{"name":"тема"}}},
		{"update_id":201,"message":{"text":"первый","chat":{"id":-1001},"from":{"id":7,"username":"tester"}}},
		{"update_id":202},
		{"update_id":203,"message":{"text":"второй","chat":{"id":-1001},"from":{"id":7,"username":"tester"}}},
		{"update_id":204,"message":{"chat":{"id":-1001},"from":{"id":1},"new_chat_members":[{"id":9}]}}
	]`)

	got, err := double.client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}

	wantIDs := []int{200, 201, 202, 203, 204}
	if len(got) != len(wantIDs) {
		t.Fatalf("обновлений %d, ожидалось %d: служебные отсеивать нельзя", len(got), len(wantIDs))
	}
	for i, want := range wantIDs {
		if got[i].ID != want {
			t.Fatalf("на месте %d update_id = %d, ожидался %d: порядок изменён", i, got[i].ID, want)
		}
	}
	if got[1].Text != "первый" || got[3].Text != "второй" {
		t.Errorf("тексты перепутаны: %q и %q", got[1].Text, got[3].Text)
	}
	if got[0].Text != "" || got[2].Text != "" || got[4].Text != "" {
		t.Errorf("у служебных появился текст: %q, %q, %q", got[0].Text, got[2].Text, got[4].Text)
	}
}

// TestParityНегодныйОтветОтвергаетсяЦеликом.
//
// Частичный результат опаснее отказа: мост подвинул бы offset по тому, что
// сумел разобрать, и неразобранное потерялось бы навсегда. Поэтому у каждого
// случая проверяется И ошибка, И отсутствие обновлений — одного мало.
func TestParityНегодныйОтветОтвергаетсяЦеликом(t *testing.T) {
	cases := []struct {
		имя      string
		envelope string
	}{
		{
			// Первый элемент годный, второй с чужим типом поля. Соблазн
			// «вернуть хотя бы разобранное» здесь стоит потерянного письма.
			имя:      "чужой тип update_id во втором элементе",
			envelope: `{"ok":true,"result":[{"update_id":1,"message":{"text":"годное","chat":{"id":-1001}}},{"update_id":"сто"}]}`,
		},
		{
			// Переполнение int: число больше, чем помещается в целое.
			имя:      "переполнение update_id",
			envelope: `{"ok":true,"result":[{"update_id":99999999999999999999,"message":{"text":"годное","chat":{"id":-1001}}}]}`,
		},
		{
			имя:      "битый JSON в результате",
			envelope: `{"ok":true,"result":[{"update_id":1,]}`,
		},
		{
			имя:      "результат не массив",
			envelope: `{"ok":true,"result":{"update_id":1}}`,
		},
	}

	for _, c := range cases {
		t.Run(c.имя, func(t *testing.T) {
			double := newDoubleWithEnvelope(t, c.envelope)

			got, err := double.client.GetUpdates(context.Background(), 0, 1)
			if err == nil {
				t.Fatalf("ошибки нет, вернулось %d обновлений: %+v", len(got), got)
			}
			if got != nil {
				t.Errorf("вместе с ошибкой вернулось %d обновлений: %+v", len(got), got)
			}
		})
	}
}

// TestParityОтказТелеграмаДоходитДоВызывающего.
//
// Отказ обязан остаться отказом, а не превратиться в пустую пачку: приёму
// нужно отличать «ничего нового» от «спросить не удалось».
func TestParityОтказТелеграмаДоходитДоВызывающего(t *testing.T) {
	double := newDoubleWithEnvelope(t, `{"ok":false,"description":"Unauthorized"}`)

	got, err := double.client.GetUpdates(context.Background(), 0, 1)
	if err == nil {
		t.Fatalf("отказ Telegram не дошёл, вернулось %+v", got)
	}
	if got != nil {
		t.Errorf("вместе с отказом вернулось %d обновлений", len(got))
	}
	if !strings.Contains(err.Error(), "Unauthorized") {
		t.Errorf("описание отказа потеряно: %v", err)
	}
}

// TestParityПустаяПачкаНеОшибка — «ничего нового» это обычный ответ long
// polling, и он не должен выглядеть как сбой.
func TestParityПустаяПачкаНеОшибка(t *testing.T) {
	double := newDouble(t, `[]`)

	got, err := double.client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("пустая пачка обернулась ошибкой: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("обновлений %d, ожидалось 0", len(got))
	}
}

// Незнакомое вложенное поле не должно отменять обновление.
//
// Прежний разбор смотрел ровно на семь наших полей, а всё остальное в
// сообщении молча пропускал. Разбор полной моделью библиотеки видит объект
// целиком, и часть вложенных типов имеет строгий разбор: незнакомый вариант
// возвращает ошибку. Поле при этом может быть таким, которое мосту не нужно
// вовсе.
//
// Цена отказа несоразмерна: ошибка отвергает ВСЮ пачку, позиция чтения не
// двигается, Telegram отдаёт её снова — и приём заклинивает навсегда, а мост
// выглядит здоровым. Ровно этот отказ уже ловили на живом стенде.
//
// Список вариантов ведёт не наш проект: Bot API пополняется, и «сегодня все
// типы известны» — утверждение с истекающим сроком.
func TestParityНезнакомоеВложенноеПолеНеОтменяетОбновление(t *testing.T) {
	// Тело собирается из общей части и одного незнакомого поля: так видно,
	// что проверяется именно поле, а не разница сообщений.
	const общее = `"message_id": 77,
		"text": "/to pi-claude посмотри",
		"message_thread_id": 19,
		"chat": {"id": -1001234567890},
		"from": {"id": 987654321, "username": "tester"},
		"entities": [{"type": "bot_command", "offset": 0, "length": 3}],
		"reply_to_message": {"message_id": 512, "from": {"is_bot": true}}`

	хочу := tg.Update{
		ID:               4242,
		ChatID:           "-1001234567890",
		ThreadID:         19,
		Text:             "/to pi-claude посмотри",
		From:             "tester",
		FromID:           987654321,
		ReplyToMessageID: 512,
		ReplyToBot:       true,
		Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
	}

	cases := []struct {
		имя        string
		незнакомое string
	}{
		{
			// Пересланное сообщение. Поле обязательное для любой пересылки,
			// то есть путь самый обычный.
			имя:        "forward_origin незнакомого вида",
			незнакомое: `"forward_origin": {"type": "вид_которого_ещё_нет"}`,
		},
		{
			// Вторая точка входа того же типа: ответ на сообщение из другого
			// чата. Закрыв одну, легко забыть про эту.
			имя:        "external_reply.origin незнакомого вида",
			незнакомое: `"external_reply": {"origin": {"type": "вид_которого_ещё_нет"}}`,
		},
		{
			имя:        "paid_media незнакомого вида",
			незнакомое: `"paid_media": {"star_count": 1, "paid_media": [{"type": "вид_которого_ещё_нет"}]}`,
		},
	}

	for _, c := range cases {
		t.Run(c.имя, func(t *testing.T) {
			double := newDouble(t, `[{"update_id": 4242, "message": {`+общее+`, `+c.незнакомое+`}}]`)

			got, err := double.client.GetUpdates(context.Background(), 0, 1)
			if err != nil {
				t.Fatalf("незнакомое поле отвергло обновление целиком: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("обновлений %d, ожидалось 1", len(got))
			}
			// Проверяется не «хоть что-то вернулось», а весь доменный
			// объект: обеднённый ответ с одним update_id — тоже потеря.
			if !reflect.DeepEqual(got[0], хочу) {
				t.Errorf("Update = %+v,\nожидалось     %+v", got[0], хочу)
			}
		})
	}
}

// Различие «разметки нет» и «разметка пуста» переживает и незнакомое поле.
//
// Отдельным тестом, потому что запасной путь разбора — это ВТОРОЕ место, где
// живёт тот же контракт. Проверка результата не спрашивает, каким путём он
// получен, и потому годится для обоих; но пройти по второму пути она обязана
// явно, иначе он останется непроверенным.
func TestParityНезнакомоеПолеНеМеняетСемантикуРазметки(t *testing.T) {
	const незнакомое = `"forward_origin": {"type": "вид_которого_ещё_нет"}`

	t.Run("разметки нет — nil", func(t *testing.T) {
		double := newDouble(t, `[{"update_id":1,"message":{"text":"без разметки","chat":{"id":-1001},"from":{"id":7},`+незнакомое+`}}]`)
		got, err := double.client.GetUpdates(context.Background(), 0, 1)
		if err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}
		if got[0].Entities != nil {
			t.Errorf("Entities = %+v, ожидался nil", got[0].Entities)
		}
	})

	t.Run("разметка пуста — не nil", func(t *testing.T) {
		double := newDouble(t, `[{"update_id":1,"message":{"text":"пусто","entities":[],"chat":{"id":-1001},"from":{"id":7},`+незнакомое+`}}]`)
		got, err := double.client.GetUpdates(context.Background(), 0, 1)
		if err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}
		if got[0].Entities == nil {
			t.Errorf("Entities = nil, ожидался пустой непустой слайс")
		}
		if len(got[0].Entities) != 0 {
			t.Errorf("Entities = %+v, ожидался пустым", got[0].Entities)
		}
	})
}

// Пачка с обычным и незнакомым сохраняет оба и порядок.
//
// Соседство важнее каждого случая по отдельности: отказ на одном элементе
// уносит всю пачку, а значит и письмо человека, пришедшее рядом с чужой
// пересылкой.
func TestParityПачкаСНезнакомымВложеннымСохраняетОба(t *testing.T) {
	double := newDouble(t, `[
		{"update_id":300,"message":{"text":"первое","chat":{"id":-1001},"from":{"id":7,"username":"tester"}}},
		{"update_id":301,"message":{"text":"второе","chat":{"id":-1001},"from":{"id":7,"username":"tester"},
			"forward_origin":{"type":"вид_которого_ещё_нет"}}}
	]`)

	got, err := double.client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("пачка отвергнута из-за одного незнакомого поля: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("обновлений %d, ожидалось 2: сосед по пачке не должен страдать", len(got))
	}
	if got[0].ID != 300 || got[1].ID != 301 {
		t.Fatalf("порядок или идентификаторы изменились: %d, %d", got[0].ID, got[1].ID)
	}
	if got[0].Text != "первое" || got[1].Text != "второе" {
		t.Errorf("тексты потеряны: %q и %q", got[0].Text, got[1].Text)
	}
	if got[1].From != "tester" || got[1].FromID != 7 {
		t.Errorf("отправитель второго потерян: %+v", got[1])
	}
}
