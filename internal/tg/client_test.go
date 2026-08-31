package tg

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// fakeAPI изображает Bot API: отвечает заготовленным телом и считает вызовы.
func fakeAPI(t *testing.T, handler func(method string, body map[string]any) (any, int)) *Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		method := parts[len(parts)-1]

		// Тело читается по типу содержимого и приводится к одному виду:
		// исходящие методы идут через библиотеку и приходят multipart, наш
		// getUpdates — JSON. Утверждения тестов от этого не зависят.
		body, err := readRequestFields(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": false, "description": "двойник не разобрал запрос: " + err.Error(),
			})
			return
		}

		result, status := handler(method, body)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": status == http.StatusOK, "result": result})
	}))
	t.Cleanup(srv.Close)

	return New("тестовый-токен", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()))
}

func TestSendMessageПередаётТемуИТекст(t *testing.T) {
	var gotThread float64
	var gotText string

	client := fakeAPI(t, func(method string, body map[string]any) (any, int) {
		if method != "sendMessage" {
			t.Errorf("вызван метод %q вместо sendMessage", method)
		}
		gotThread, _ = body["message_thread_id"].(float64)
		gotText, _ = body["text"].(string)
		return map[string]any{"message_id": 42}, http.StatusOK
	})

	ids, err := client.SendMessage(context.Background(), SendRequest{
		ChatID: "-1001", Text: "привет", ThreadID: 7,
	})
	if err != nil {
		t.Fatalf("отправка: %v", err)
	}
	if len(ids) != 1 || ids[0] != 42 {
		t.Errorf("идентификаторы %v, ожидался [42]", ids)
	}
	if int(gotThread) != 7 {
		t.Errorf("message_thread_id = %v, ожидался 7", gotThread)
	}
	if gotText != "привет" {
		t.Errorf("text = %q", gotText)
	}
}

func TestSendMessageБезТемыНеШлётПолеТемы(t *testing.T) {
	var hadThread bool

	client := fakeAPI(t, func(_ string, body map[string]any) (any, int) {
		_, hadThread = body["message_thread_id"]
		return map[string]any{"message_id": 1}, http.StatusOK
	})

	if _, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-1001", Text: "т"}); err != nil {
		t.Fatalf("отправка: %v", err)
	}
	// В обычной группе message_thread_id=0 приводит к ошибке API,
	// поэтому поле не должно уходить вовсе.
	if hadThread {
		t.Fatal("message_thread_id ушёл в запрос при нулевой теме")
	}
}

func TestSendMessageРежетДлинныйТекст(t *testing.T) {
	var calls int32

	client := fakeAPI(t, func(_ string, body map[string]any) (any, int) {
		atomic.AddInt32(&calls, 1)
		text, _ := body["text"].(string)
		if len([]rune(text)) > MaxMessageRunes {
			t.Errorf("кусок длиной %d рун превышает лимит", len([]rune(text)))
		}
		return map[string]any{"message_id": 1}, http.StatusOK
	})

	long := strings.Repeat("я", MaxMessageRunes*2+10)
	if _, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-1001", Text: long}); err != nil {
		t.Fatalf("отправка: %v", err)
	}
	if atomic.LoadInt32(&calls) < 3 {
		t.Fatalf("длинный текст ушёл за %d вызовов, ожидалось не меньше 3", calls)
	}
}

func TestCreateForumTopicВозвращаетИдентификатор(t *testing.T) {
	client := fakeAPI(t, func(method string, _ map[string]any) (any, int) {
		if method != "createForumTopic" {
			t.Errorf("метод %q", method)
		}
		return map[string]any{"message_thread_id": 15}, http.StatusOK
	})

	id, err := client.CreateForumTopic(context.Background(), "-1001", "тема")
	if err != nil {
		t.Fatalf("создание темы: %v", err)
	}
	if id != 15 {
		t.Fatalf("message_thread_id = %d, ожидался 15", id)
	}
}

func TestОшибкаAPIВозвращаетсяВызывающему(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return nil, http.StatusBadRequest
	})

	if _, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-1001", Text: "т"}); err == nil {
		t.Fatal("отказ API не превратился в ошибку")
	}
}

func TestGetUpdatesРазбираетСообщение(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{
				"update_id": 100,
				"message": map[string]any{
					"text":              "спроси у всех про routes",
					"message_thread_id": 7,
					"chat":              map[string]any{"id": -1001},
					"from":              map[string]any{"username": "tester"},
				},
			},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("обновлений %d, ожидалось 1", len(got))
	}
	if got[0].Text != "спроси у всех про routes" {
		t.Errorf("текст %q", got[0].Text)
	}
	if got[0].ThreadID != 7 {
		t.Errorf("тема %d, ожидалась 7", got[0].ThreadID)
	}
	if got[0].ChatID != "-1001" {
		t.Errorf("чат %q, ожидался -1001", got[0].ChatID)
	}
	if got[0].ID != 100 {
		t.Errorf("update_id %d", got[0].ID)
	}
}

func TestSplitРежетПоСтрокам(t *testing.T) {
	text := strings.Repeat("строка текста\n", 500)

	parts := Split(text)

	if len(parts) < 2 {
		t.Fatalf("длинный текст не разрезан: кусков %d", len(parts))
	}
	for i, p := range parts {
		if len([]rune(p)) > MaxMessageRunes {
			t.Fatalf("кусок %d длиной %d рун", i, len([]rune(p)))
		}
	}
	if strings.Join(parts, "") != text {
		t.Fatal("склейка кусков не равна исходному тексту")
	}
}

func TestОшибкаAPIРазличаетПостоянноеИВременное(t *testing.T) {
	cases := []struct {
		code      int
		desc      string
		permanent bool
		what      string
	}{
		{403, "Forbidden: bot was kicked", true, "бота выгнали"},
		{400, "Bad Request: not enough rights to manage topics", true, "нет прав на темы"},
		{400, "Bad Request: the chat is not a forum", true, "чат не форумный"},
		{400, "Bad Request: message text is empty", false, "ошибка запроса, но не про права"},
		{500, "Internal Server Error", false, "сбой на стороне Telegram"},
		{429, "Too Many Requests", false, "лимит частоты"},
	}
	for _, c := range cases {
		err := &APIError{Method: "sendMessage", Code: c.code, Description: c.desc}
		if got := err.Permanent(); got != c.permanent {
			t.Errorf("%s: Permanent()=%v, ожидалось %v (%d %q)", c.what, got, c.permanent, c.code, c.desc)
		}
	}
}

// Обновления без текста обязаны возвращаться, а не отсеиваться здесь.
//
// Пока клиент выбрасывал их молча, их update_id не доходил до приёма, и тот
// не двигал offset: служебное сообщение вроде «создана тема» оседало в
// очереди навсегда, Telegram отдавал ту же пачку снова, а все написанные
// после сообщения человека не доходили никогда. Мост при этом выглядел
// здоровым — ни ошибки в логе, ни остановки.
func TestGetUpdatesВозвращаетСлужебныеОбновления(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{
				"update_id": 100,
				"message": map[string]any{
					"message_thread_id":   27,
					"chat":                map[string]any{"id": -1001},
					"from":                map[string]any{"id": 1},
					"forum_topic_created": map[string]any{"name": "тема"},
				},
			},
			map[string]any{
				"update_id": 101,
				"message": map[string]any{
					"text": "человек тут",
					"chat": map[string]any{"id": -1001},
					"from": map[string]any{"id": 987654321},
				},
			},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("вернулось %d обновлений, ожидалось 2: без служебного offset не сдвинется", len(got))
	}
	if got[0].ID != 100 || got[0].Text != "" {
		t.Fatalf("служебное обновление искажено: %+v", got[0])
	}
	if got[1].Text != "человек тут" {
		t.Fatalf("текстовое обновление потеряно: %+v", got[1])
	}
}

// Разметка сообщения доходит до вызывающего.
//
// Нужна ровно для одного решения: команда боту это или текст человека.
// Отличить их по первому символу нельзя — путь `/etc/nats/tls` начинается
// так же, а такие строки мы шлём друг другу постоянно. Telegram помечает
// команды сущностью `bot_command`, и это единственный надёжный признак,
// но до сих пор разбор его выбрасывал.
func TestGetUpdatesСохраняетРазметкуСообщения(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{
				"update_id": 100,
				"message": map[string]any{
					"text": "/start@agent_mesh_bot",
					"chat": map[string]any{"id": -1001},
					"from": map[string]any{"username": "tester"},
					"entities": []any{
						map[string]any{"type": "bot_command", "offset": 0, "length": 21},
					},
				},
			},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("обновлений %d, ожидалось 1", len(got))
	}
	if len(got[0].Entities) != 1 {
		t.Fatalf("разметки %d, ожидалась одна: без неё команду не отличить от текста",
			len(got[0].Entities))
	}
	e := got[0].Entities[0]
	if e.Type != "bot_command" || e.Offset != 0 || e.Length != 21 {
		t.Fatalf("разметка = {%q, %d, %d}, ожидалась {bot_command, 0, 21}", e.Type, e.Offset, e.Length)
	}
}

// Сообщение без разметки её и не приобретает.
//
// Контроль: разбор, подставляющий пустую сущность или считающий разметку по
// тексту, прошёл бы предыдущий тест и сломал бы обычные сообщения.
func TestGetUpdatesБезРазметкиОставляетЕёПустой(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{
				"update_id": 101,
				"message": map[string]any{
					"text": "/etc/nats/tls/privkey.pem",
					"chat": map[string]any{"id": -1001},
					"from": map[string]any{"username": "tester"},
				},
			},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got[0].Entities) != 0 {
		t.Fatalf("разметки %d, ожидалось ноль: Telegram её не присылал", len(got[0].Entities))
	}
	if got[0].Text != "/etc/nats/tls/privkey.pem" {
		t.Errorf("текст %q — путь должен доходить как есть", got[0].Text)
	}
}

// Длинное письмо разрезано на части, и известна каждая.
//
// Идентификаторы нужны, чтобы связать пост в чате с разговором: человек
// отвечает на конкретное сообщение, и по нему надо понять, кому адресовать
// ответ. Раньше возвращался только последний кусок — ответ на первую половину
// длинного письма не нашёл бы разговора.
func TestSendMessageВозвращаетИдентификаторыВсехЧастей(t *testing.T) {
	var next int64
	client := fakeAPI(t, func(method string, _ map[string]any) (any, int) {
		if method != "sendMessage" {
			return nil, http.StatusOK
		}
		next++
		return map[string]any{"message_id": 1000 + next}, http.StatusOK
	})
	// Без этого тест ждал бы по три секунды на каждую часть.
	WithMinSendGap(0)(client)

	// Текст заведомо длиннее предела, чтобы Split дал несколько кусков.
	long := strings.Repeat("строка кода в письме\n", 400)
	ids, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-1001", Text: long})
	if err != nil {
		t.Fatalf("отправка: %v", err)
	}

	want := len(Split(long))
	if want < 2 {
		t.Fatalf("текст не разрезался на части (%d), тест бессмыслен", want)
	}
	if len(ids) != want {
		t.Fatalf("идентификаторов %d, частей %d: по одному на часть", len(ids), want)
	}
	for i, id := range ids {
		if id != 1000+i+1 {
			t.Fatalf("часть %d получила id %d, ожидался %d", i, id, 1000+i+1)
		}
	}
}

// Короткое письмо — один идентификатор, а не пустой список.
func TestSendMessageВозвращаетОдинИдентификаторДляКороткого(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return map[string]any{"message_id": 777}, http.StatusOK
	})

	ids, err := client.SendMessage(context.Background(), SendRequest{ChatID: "-1001", Text: "коротко"})
	if err != nil {
		t.Fatalf("отправка: %v", err)
	}
	if len(ids) != 1 || ids[0] != 777 {
		t.Fatalf("идентификаторы %v, ожидался [777]", ids)
	}
}

// Ответ человека на конкретный пост опознаётся.
//
// Без этого поля нельзя понять, к какому разговору относится ответ, а на этом
// держится вся маршрутизация в общей теме проекта.
func TestGetUpdatesРазбираетОтветНаСообщение(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{
				"update_id": 500,
				"message": map[string]any{
					"text": "согласен",
					"chat": map[string]any{"id": -1001},
					"from": map[string]any{"username": "tester"},
					"reply_to_message": map[string]any{
						"message_id": 1042,
					},
				},
			},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if got[0].ReplyToMessageID != 1042 {
		t.Fatalf("ответ на сообщение %d, ожидалось 1042", got[0].ReplyToMessageID)
	}
}

// Обычное сообщение ответом не притворяется.
func TestGetUpdatesБезОтветаОставляетПолеПустым(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{
				"update_id": 501,
				"message": map[string]any{
					"text": "просто сообщение",
					"chat": map[string]any{"id": -1001},
					"from": map[string]any{"username": "tester"},
				},
			},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if got[0].ReplyToMessageID != 0 {
		t.Fatalf("поле ответа = %d, ожидался ноль", got[0].ReplyToMessageID)
	}
}

// Различается, кому человек ответил: боту или человеку.
//
// Ответ на пост бота — продолжение показанного им разговора. Ответ на чужую
// или собственную реплику разговором не является, и адресатов у него нет.
func TestGetUpdatesРазличаетОтветБотуИЧеловеку(t *testing.T) {
	for _, случай := range []struct {
		имя     string
		isBot   bool
		ожидаем bool
	}{
		{"ответ боту", true, true},
		{"ответ человеку", false, false},
	} {
		t.Run(случай.имя, func(t *testing.T) {
			client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
				return []any{
					map[string]any{
						"update_id": 600,
						"message": map[string]any{
							"text": "отвечаю",
							"chat": map[string]any{"id": -1001},
							"from": map[string]any{"username": "tester"},
							"reply_to_message": map[string]any{
								"message_id": 42,
								"from":       map[string]any{"is_bot": случай.isBot},
							},
						},
					},
				}, http.StatusOK
			})

			got, err := client.GetUpdates(context.Background(), 0, 1)
			if err != nil {
				t.Fatalf("получение обновлений: %v", err)
			}
			if got[0].ReplyToBot != случай.ожидаем {
				t.Fatalf("ReplyToBot=%v, ожидалось %v", got[0].ReplyToBot, случай.ожидаем)
			}
			if got[0].ReplyToMessageID != 42 {
				t.Fatalf("номер поста %d, ожидался 42", got[0].ReplyToMessageID)
			}
		})
	}
}

// Отсутствующая разметка и пустой список различимы.
//
// Разница тонкая и на глаз незаметная, но она несёт смысл: nil значит
// «Telegram про разметку ничего не сказал», пустой непустой срез — «сказал,
// что её нет». Прямой разбор JSON различал эти случаи сам собой; перевод на
// модели библиотеки мог схлопнуть их молча, и признак перестал бы быть
// признаком.
func TestРазметкаРазличаетОтсутствиеИПустойСписок(t *testing.T) {
	случаи := map[string]struct {
		message map[string]any
		nilExp  bool
	}{
		"поля нет вовсе": {
			message: map[string]any{
				"text": "текст",
				"chat": map[string]any{"id": -1001},
			},
			nilExp: true,
		},
		"пустой список": {
			message: map[string]any{
				"text":     "текст",
				"entities": []any{},
				"chat":     map[string]any{"id": -1001},
			},
			nilExp: false,
		},
	}

	for имя, случай := range случаи {
		t.Run(имя, func(t *testing.T) {
			client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
				return []any{map[string]any{"update_id": 7, "message": случай.message}}, http.StatusOK
			})

			got, err := client.GetUpdates(context.Background(), 0, 1)
			if err != nil {
				t.Fatalf("получение обновлений: %v", err)
			}
			if len(got[0].Entities) != 0 {
				t.Fatalf("разметки не должно быть вовсе: %+v", got[0].Entities)
			}
			if isNil := got[0].Entities == nil; isNil != случай.nilExp {
				t.Errorf("разметка nil=%v, ожидалось nil=%v: отсутствие и пустой список неразличимы",
					isNil, случай.nilExp)
			}
		})
	}
}

// Обновление без сообщения не роняет разбор и не теряется.
//
// Служебные обновления приходят без текстового message, а иногда и вовсе
// одним update_id. Вернуть их обязательно: приём двигает позицию чтения по
// тому, что получил, и пропуск одного идентификатора останавливает приём
// навсегда — Telegram будет отдавать ту же пачку снова и снова.
func TestОбновлениеБезСообщенияНеРоняетРазбор(t *testing.T) {
	client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
		return []any{
			map[string]any{"update_id": 500},
			map[string]any{"update_id": 501, "message": map[string]any{
				"chat": map[string]any{"id": -1001},
			}},
			map[string]any{"update_id": 502, "message": map[string]any{
				"text": "человек тут",
				"chat": map[string]any{"id": -1001},
				"from": map[string]any{"id": 987654321, "username": "tester"},
			}},
		}, http.StatusOK
	})

	got, err := client.GetUpdates(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("получение обновлений: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("вернулось %d обновлений, ожидалось 3: пропуск остановит приём", len(got))
	}

	// Голое обновление: прежний разбор давал строковый ноль, а не пустую
	// строку, и маршрутизация написана под такой вид.
	if got[0].ID != 500 || got[0].ChatID != "0" || got[0].Text != "" {
		t.Errorf("голое обновление разобрано иначе, чем прежде: %+v", got[0])
	}
	if got[0].From != "" || got[0].FromID != 0 || got[0].ReplyToMessageID != 0 || got[0].ReplyToBot {
		t.Errorf("у голого обновления появились поля отправителя: %+v", got[0])
	}
	if got[1].ID != 501 || got[1].ChatID != "-1001" || got[1].From != "" {
		t.Errorf("сообщение без отправителя разобрано неверно: %+v", got[1])
	}
	if got[2].From != "tester" || got[2].FromID != 987654321 {
		t.Errorf("отправитель потерян: %+v", got[2])
	}
}

// Тело запроса getUpdates остаётся нашим и неизменным.
//
// Перевод ОТВЕТА на модели библиотеки не должен был затронуть ЗАПРОС: позиция
// чтения, набор типов обновлений и таймаут — наши решения, и библиотека к ним
// отношения не имеет. Проверяется буквально, поле за полем.
func TestЗапросGetUpdatesНеИзменился(t *testing.T) {
	for _, offset := range []int{0, 42} {
		t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
			var got map[string]any
			client := fakeAPI(t, func(method string, body map[string]any) (any, int) {
				if method == "getUpdates" {
					got = body
				}
				return []any{}, http.StatusOK
			})

			if _, err := client.GetUpdates(context.Background(), offset, 25); err != nil {
				t.Fatalf("получение обновлений: %v", err)
			}

			if got["timeout"] != float64(25) {
				t.Errorf("таймаут %v, ожидался 25", got["timeout"])
			}
			allowed, ok := got["allowed_updates"].([]any)
			if !ok || len(allowed) != 1 || allowed[0] != "message" {
				t.Errorf("allowed_updates %v, ожидалось [message]", got["allowed_updates"])
			}
			if offset == 0 {
				if _, есть := got["offset"]; есть {
					t.Errorf("нулевая позиция чтения попала в запрос: %v", got)
				}
				return
			}
			if got["offset"] != float64(offset) {
				t.Errorf("позиция чтения %v, ожидалась %d", got["offset"], offset)
			}
		})
	}
}

// Идентификатор обновления не усекается по дороге.
//
// Библиотека объявляет update_id как int64, мост хранит int. Молчаливое
// сужение сдвинуло бы позицию чтения на чужое число: приём подтвердил бы
// обновления, которых не видел, и они не пришли бы больше никогда.
func TestБольшойИдентификаторОбновленияНеУсекается(t *testing.T) {
	большие := []int64{1 << 31, 1 << 40, math.MaxInt32 + 1}
	for _, id := range большие {
		if strconv.IntSize < 64 && id > math.MaxInt32 {
			continue
		}
		got, err := updateID(id)
		if err != nil {
			t.Fatalf("идентификатор %d отвергнут: %v", id, err)
		}
		if int64(got) != id {
			t.Errorf("идентификатор %d стал %d — позиция чтения сдвинется на чужое число", id, got)
		}
	}
}

// Идентификатор проверяется на разрядность, а не на удачу платформы.
//
// На 64-битной машине значения, не помещающегося в int, не существует, и
// прежний тест этой ветки только пропускал себя. Пропуск доказывает, что тест
// пропущен, и ничего больше. С явной разрядностью обе стороны проверки
// становятся обычной функцией.
func TestРазрядностьГраницыИдентификатора(t *testing.T) {
	const границаInt32 = int64(1) << 31

	помещаются := []int64{0, 1, -1, границаInt32 - 1, -границаInt32, math.MaxInt32}
	for _, id := range помещаются {
		got, err := narrowUpdateID(id, 32)
		if err != nil {
			t.Errorf("идентификатор %d отвергнут 32-битной проверкой: %v", id, err)
			continue
		}
		if int64(got) != id {
			t.Errorf("идентификатор %d стал %d", id, got)
		}
	}

	неПомещаются := []int64{границаInt32, -границаInt32 - 1, math.MaxInt64, math.MinInt64}
	for _, id := range неПомещаются {
		if _, err := narrowUpdateID(id, 32); err == nil {
			t.Errorf("идентификатор %d принят 32-битной проверкой — позиция чтения была бы испорчена", id)
		}
	}

	// На своей разрядности не отвергается ничего: иначе проверка ломала бы
	// работающий приём вместо того, чтобы стеречь чужую платформу.
	for _, id := range []int64{math.MaxInt64, math.MinInt64} {
		if _, err := narrowUpdateID(id, 64); err != nil {
			t.Errorf("идентификатор %d отвергнут 64-битной проверкой: %v", id, err)
		}
	}
}

// Незнакомый вариант поля не отнимает у нас обновление.
//
// Поля с вариантами — forward_origin, paid_media, external_reply.origin —
// модели библиотеки разбирают своим кодом и на НЕИЗВЕСТНОМ type возвращают
// ошибку для всего обновления. Telegram добавляет варианты молча, а приём
// двигает позицию по тому, что разобрал: одно пересланное сообщение нового
// вида заклинило бы мост навсегда, и в журнале не появилось бы ни строчки.
//
// Прежний разбор эти поля просто не описывал и потому переживал их не глядя.
// Запасной путь возвращает ровно то поведение.
func TestНезнакомыйВариантПоляНеТеряетОбновление(t *testing.T) {
	случаи := map[string]string{
		"пересылка нового вида": `"forward_origin":{"type":"новый_вид"},`,
		"платное вложение":      `"paid_media":{"star_count":1,"paid_media":[{"type":"новый"}]},`,
		"внешний ответ":         `"external_reply":{"origin":{"type":"новый_вид"},"chat":{"id":-1}},`,
	}

	for имя, поле := range случаи {
		t.Run(имя, func(t *testing.T) {
			client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
				return json.RawMessage(`[{
					"update_id": 4242,
					"message": {
						"message_id": 77,
						"text": "/to pi-claude посмотри",
						"message_thread_id": 19,
						` + поле + `
						"chat": {"id": -1001234567890},
						"from": {"id": 987654321, "username": "tester"},
						"entities": [{"type": "bot_command", "offset": 0, "length": 3}],
						"reply_to_message": {"message_id": 512, "from": {"is_bot": true}}
					}
				}]`), http.StatusOK
			})

			got, err := client.GetUpdates(context.Background(), 0, 1)
			if err != nil {
				t.Fatalf("незнакомое поле уронило разбор пачки: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("обновлений %d, ожидалось 1", len(got))
			}

			want := Update{
				ID:       4242,
				ChatID:   "-1001234567890",
				ThreadID: 19,
				Text:     "/to pi-claude посмотри",
				Entities: []Entity{{Type: "bot_command", Offset: 0, Length: 3}},
				From:     "tester",
				FromID:   987654321,

				ReplyToMessageID: 512,
				ReplyToBot:       true,
			}
			if !reflect.DeepEqual(got[0], want) {
				t.Errorf("обновление разобрано иначе:\nполучено %+v\nожидалось %+v", got[0], want)
			}
		})
	}
}

// Запасной путь отдаёт то же самое, что основной.
//
// Проверяется одним телом в двух видах: с незнакомым вариантом поля и без
// него. Первое идёт запасным путём, второе — моделями библиотеки, и результат
// обязан совпасть до последнего поля. Расхождение было бы хуже отсутствия
// запасного пути: одно и то же письмо доходило бы по-разному в зависимости от
// того, попалось ли рядом незнакомое поле.
func TestЗапаснойПутьНеРасходитсяСОсновным(t *testing.T) {
	тело := func(лишнее string) json.RawMessage {
		return json.RawMessage(`[{
			"update_id": 7,
			"message": {
				"message_id": 3,
				"text": "текст",
				"message_thread_id": 11,
				` + лишнее + `
				"chat": {"id": -1001},
				"from": {"id": 42, "username": "tester"},
				"entities": [{"type": "url", "offset": 2, "length": 3}],
				"reply_to_message": {"message_id": 5, "from": {"is_bot": false}}
			}
		}]`)
	}

	получить := func(лишнее string) []Update {
		t.Helper()
		client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
			return тело(лишнее), http.StatusOK
		})
		got, err := client.GetUpdates(context.Background(), 0, 1)
		if err != nil {
			t.Fatalf("получение обновлений: %v", err)
		}
		return got
	}

	основной := получить("")
	запасной := получить(`"forward_origin":{"type":"новый_вид"},`)

	if !reflect.DeepEqual(основной, запасной) {
		t.Errorf("пути разошлись:\nмодели  %+v\nзапасной %+v", основной, запасной)
	}
}

// Негодный элемент отвергает всю пачку, а не только себя.
//
// Частичный результат опаснее отказа: приём подтвердил бы позицию по тому,
// что сумел прочесть, и неразобранное не пришло бы больше никогда. Отказ
// вернёт ту же пачку на следующем круге — это переживаемо.
//
// Годный первый элемент здесь обязателен: без него тест зелен и у
// реализации, которая молча пропускает нечитаемые элементы.
func TestНегодныйЭлементОтвергаетВсюПачку(t *testing.T) {
	случаи := map[string]string{
		"чужой тип идентификатора": `{"update_id":"сто"}`,
		"чужой тип текста":         `{"update_id":2,"message":{"text":123,"chat":{"id":-1001}}}`,
		"чужой тип разметки":       `{"update_id":3,"message":{"text":"а","entities":"нет","chat":{"id":-1001}}}`,
		"чужой тип чата":           `{"update_id":4,"message":{"text":"а","chat":{"id":"минус тысяча"}}}`,
	}

	for имя, негодный := range случаи {
		t.Run(имя, func(t *testing.T) {
			client := fakeAPI(t, func(_ string, _ map[string]any) (any, int) {
				return json.RawMessage(`[
					{"update_id":1,"message":{"text":"годное","chat":{"id":-1001},"from":{"id":7}}},
					` + негодный + `
				]`), http.StatusOK
			})

			got, err := client.GetUpdates(context.Background(), 0, 1)
			if err == nil {
				t.Fatalf("негодный элемент принят, вернулось %d обновлений: %+v", len(got), got)
			}
			if got != nil {
				t.Errorf("вместе с ошибкой вернулась разобранная часть: %+v", got)
			}
		})
	}
}
