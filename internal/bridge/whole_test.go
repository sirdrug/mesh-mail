package bridge

// Мост поднимается ЦЕЛИКОМ, через тот же bridge.Run, что и в бою.
//
// Остальные тесты пакета собирают его половинки по отдельности: отдельно
// Showcase, отдельно Intake, а clientPoster — тот десяток строк, что склеивает
// их с Telegram, — не проверяется вовсе. Интеграционный стенд повторял его у
// себя, то есть проверял копию боевого пути.
//
// Здесь настоящее всё, кроме одного: адрес Bot API указывает на двойник, а
// пауза ограничителя укорочена, иначе тест ждал бы по три секунды на каждое
// сообщение. Подменяется клиент, а не Poster, — именно чтобы clientPoster,
// GetMe и разбор ответов API остались в проверяемом пути.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// botDouble — двойник Bot API: отвечает на четыре метода, которыми пользуется
// мост, и запоминает, что бот отправил бы человеку.
type botDouble struct {
	mu      sync.Mutex
	posts   []string
	threads []int
	topics  []string
	pending []tg.Update // отдаётся один раз, как настоящий getUpdates
	// holdUntil — до этого момента обновлений «ещё нет».
	//
	// Человек не пишет в первую миллисекунду после старта моста, а мост
	// узнаёт о живых агентах не мгновенно: визитки приходят по таймеру.
	// Без задержки сообщение приходило раньше первой визитки, мост честно
	// отвечал «некому доставить», и тест проверял бы гонку, а не доставку.
	holdUntil time.Time
	nextID    int
	lastSend  time.Time
	gaps      []time.Duration
}

func (b *botDouble) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]

		// Тело разбирается по типу содержимого: исходящие методы идут через
		// библиотеку и приходят multipart, наш getUpdates — JSON.
		req, err := readBotRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": false, "description": "двойник не разобрал запрос: " + err.Error(),
			})
			return
		}

		b.mu.Lock()
		defer b.mu.Unlock()

		reply := func(v any) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": v})
		}

		switch method {
		case "getMe":
			reply(map[string]any{"username": "двойник_бот"})

		case "sendMessage":
			// Интервалы меряем здесь, а не в клиенте: так проверяется то, что
			// увидел бы Telegram, а не то, что намеревался сделать мост.
			if !b.lastSend.IsZero() {
				b.gaps = append(b.gaps, time.Since(b.lastSend))
			}
			b.lastSend = time.Now()
			b.posts = append(b.posts, req.Text)
			b.threads = append(b.threads, req.ThreadID)
			b.nextID++
			reply(map[string]any{"message_id": b.nextID})

		case "createForumTopic":
			b.topics = append(b.topics, req.Name)
			b.nextID++
			reply(map[string]any{"message_thread_id": b.nextID})

		case "getUpdates":
			var out []tg.Update
			if time.Now().After(b.holdUntil) {
				out, b.pending = b.pending, nil
			}
			if len(out) == 0 {
				// Держим запрос, как настоящий long polling: иначе мост
				// крутил бы цикл на полной скорости и тест мерил бы скорость
				// двойника вместо поведения моста.
				b.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				b.mu.Lock()
				reply([]any{})
				return
			}
			raw := make([]map[string]any, 0, len(out))
			for _, u := range out {
				message := map[string]any{
					"text":              u.Text,
					"message_thread_id": u.ThreadID,
					"chat":              map[string]any{"id": mustInt(u.ChatID)},
					"from":              map[string]any{"id": u.FromID, "username": u.From},
				}
				// Разметка нужна командам: по ней мост отличает `/to` от пути
				// `/etc/nats/...`, и без неё целый мост проверялся бы на
				// сообщениях, которых в бою не бывает.
				if len(u.Entities) > 0 {
					entities := make([]map[string]any, 0, len(u.Entities))
					for _, e := range u.Entities {
						entities = append(entities, map[string]any{
							"type": e.Type, "offset": e.Offset, "length": e.Length,
						})
					}
					message["entities"] = entities
				}
				raw = append(raw, map[string]any{"update_id": u.ID, "message": message})
			}
			reply(raw)

		default:
			reply(map[string]any{})
		}
	})
}

func mustInt(s string) int64 {
	var v int64
	var neg bool
	for i, c := range s {
		if i == 0 && c == '-' {
			neg = true
			continue
		}
		v = v*10 + int64(c-'0')
	}
	if neg {
		return -v
	}
	return v
}

func (b *botDouble) snapshot() ([]string, []int, []string, []time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.posts...), append([]int(nil), b.threads...),
		append([]string(nil), b.topics...), append([]time.Duration(nil), b.gaps...)
}

// Мост, поднятый через Run, работает в обе стороны.
func TestМостЦеликомРаботаетВОбеСтороны(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, conn := newStore(t)

	bot := &botDouble{
		pending: []tg.Update{
			{ID: 500, ChatID: "-1001", Text: "как продвигается?", From: "tester", FromID: 42},
		},
		holdUntil: time.Now().Add(600 * time.Millisecond),
	}
	server := httptest.NewServer(bot.handler())
	defer server.Close()

	// Визитка нужна, чтобы сообщению человека было кому уйти.
	//
	// Переизлучается по таймеру, а не публикуется один раз: мост подписывается
	// на визитки внутри Run, и единственная, отправленная до старта, до него
	// просто не дойдёт. В бою узлы шлют их так же — периодически, — и первую
	// минуту после старта мост считает сеть пустой.
	go func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		for {
			card, err := json.Marshal(bus.Card{
				AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC(),
			})
			if err != nil {
				return
			}
			_ = conn.NC().Publish("agents.pi-claude.presence", card)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, conn, Config{
			ChatID:         "-1001",
			Token:          "123:двойник",
			ForumTopics:    true,
			AllowedUserIDs: []int64{42},
			TelegramOptions: []tg.Option{
				tg.WithBaseURL(server.URL),
				// Ограничитель настоящий, но пауза короче: три секунды на
				// сообщение превратили бы тест в ожидание.
				tg.WithMinSendGap(50 * time.Millisecond),
			},
		})
	}()

	// Сторона витрины: письмо между агентами доходит до канала.
	m := mail.New("pi-claude", []string{"m1-codex"}, "отчёт по сборке", "готово")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация письма: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _, _ := bot.snapshot()
		for _, p := range posts {
			if strings.Contains(p, "отчёт по сборке") {
				return true
			}
		}
		return false
	}, "письмо агента дошло до канала через целый мост")

	// Сторона приёма: сообщение человека стало письмом.
	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{Limit: 10})
		if err != nil {
			return false
		}
		for _, item := range got {
			if item.Message.From == HumanID {
				return true
			}
		}
		return false
	}, "сообщение человека стало письмом через целый мост")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{Limit: 10})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	var fromHuman *mail.Message
	for i := range got {
		if got[i].Message.From == HumanID {
			fromHuman = got[i].Message
			break
		}
	}
	if fromHuman == nil {
		t.Fatal("письма от человека нет")
	}
	if fromHuman.Body != "как продвигается?" {
		t.Errorf("тело письма от человека: %q", fromHuman.Body)
	}
	// Идентификатор выведен из обновления — тот же путь, что проверяет
	// отдельный тест приёма, но здесь он пройден через целый мост.
	if want := telegramMessageID(tg.Update{ID: 500, ChatID: "-1001"}); fromHuman.ID != want {
		t.Errorf("идентификатор письма %q, ожидался выведенный из обновления %q",
			fromHuman.ID, want)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("Run не вернулся после отмены контекста")
	}
}

// Мост через Run отказывается стартовать с пустым списком разрешённых.
//
// Тот же отказ проверяется и на уровне Config, но здесь он проходит весь
// боевой путь: с настоящим клиентом, до обращения к Telegram.
func TestЦелыйМостНеСтартуетБезСпискаРазрешённых(t *testing.T) {
	_, conn := newStore(t)

	bot := &botDouble{}
	server := httptest.NewServer(bot.handler())
	defer server.Close()

	err := Run(context.Background(), conn, Config{
		ChatID:          "-1001",
		Token:           "123:двойник",
		TelegramOptions: []tg.Option{tg.WithBaseURL(server.URL)},
	})
	if err == nil {
		t.Fatal("мост поднялся с пустым allowed_user_ids")
	}
	if !strings.Contains(err.Error(), "allowed_user_ids") {
		t.Fatalf("ошибка не называет поле: %v", err)
	}

	// И до Telegram дело не дошло: отказ конфигурации не должен зависеть от
	// того, ответил ли API.
	if posts, _, _, _ := bot.snapshot(); len(posts) != 0 {
		t.Errorf("мост успел сходить в Telegram до проверки конфигурации")
	}
}

// Целый мост принимает `/to@своё_имя` — значит имя бота из GetMe доходит до приёма.
//
// Проверяется именно проводка: в юнит-тестах имя задаётся сеттером, и ошибка
// «сеттер есть, но в Run его не зовут» осталась бы незамеченной. Здесь имя
// приходит от двойника Bot API тем же путём, что в бою, — ответом на getMe.
func TestЦелыйМостПринимаетКомандуСоСвоимСуффиксом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, conn := newStore(t)

	bot := &botDouble{
		pending: []tg.Update{
			{
				ID: 700, ChatID: "-1001", FromID: 42, From: "tester",
				// Суффикс — ровно тот, что двойник отдаёт на getMe.
				Text:     "/to@двойник_бот pi-claude через целый мост",
				Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 15}},
			},
		},
		holdUntil: time.Now().Add(600 * time.Millisecond),
	}
	server := httptest.NewServer(bot.handler())
	defer server.Close()

	go func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		for {
			card, err := json.Marshal(bus.Card{
				AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC(),
			})
			if err != nil {
				return
			}
			_ = conn.NC().Publish("agents.pi-claude.presence", card)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, conn, Config{
			ChatID:         "-1001",
			Token:          "123:двойник",
			ForumTopics:    true,
			AllowedUserIDs: []int64{42},
			TelegramOptions: []tg.Option{
				tg.WithBaseURL(server.URL),
				tg.WithMinSendGap(50 * time.Millisecond),
			},
		})
	}()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{Limit: 10})
		if err != nil {
			return false
		}
		for _, item := range got {
			if item.Message.From == HumanID {
				return true
			}
		}
		return false
	}, "команда со своим суффиксом стала письмом через целый мост")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{Limit: 10})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	for _, item := range got {
		if item.Message.From != HumanID {
			continue
		}
		if item.Message.Body != "через целый мост" {
			t.Errorf("тело письма %q — команда с суффиксом разобрана неверно", item.Message.Body)
		}
		if len(item.Message.To) != 1 || item.Message.To[0] != "pi-claude" {
			t.Errorf("адресаты %v, ожидался один pi-claude", item.Message.To)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("Run не вернулся после отмены контекста")
	}
}
