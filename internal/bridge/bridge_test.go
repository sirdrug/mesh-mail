package bridge

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
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

func TestClientPosterПодставляетЧат(t *testing.T) {
	var gotChat string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := readBotRequest(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": false, "description": "двойник не разобрал запрос: " + err.Error(),
			})
			return
		}
		gotChat = req.ChatID

		result := map[string]any{"message_id": 1}
		if strings.HasSuffix(r.URL.Path, "createForumTopic") {
			result = map[string]any{"message_thread_id": 5}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
	}))
	defer srv.Close()

	client := tg.New("токен", tg.WithBaseURL(srv.URL), tg.WithHTTPClient(srv.Client()))
	poster := &clientPoster{client: client, chatID: "-1001234"}

	if _, err := poster.Send(context.Background(), 7, tg.Post{Text: "текст"}); err != nil {
		t.Fatalf("отправка: %v", err)
	}
	if gotChat != "-1001234" {
		t.Fatalf("chat_id = %q", gotChat)
	}

	id, err := poster.CreateTopic(context.Background(), "тема")
	if err != nil {
		t.Fatalf("создание темы: %v", err)
	}
	if id != 5 {
		t.Fatalf("message_thread_id = %d", id)
	}
}

func TestОбъявляетТолькоИзменения(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	go func() { _ = handlePresence(ctx, conn, poster, store) }()

	card := bus.Card{
		AgentID: "pi-claude", Host: "raspberrypi", Engine: "claude",
		Projects: []string{"mesh-mail"}, TTLSeconds: 180, AnnouncedAt: time.Now().UTC(),
	}
	// Повторяем объявление, пока не долетит: подписка сторожа поднимается в
	// горутине, а core NATS не хранит историю — визитка, отправленная до
	// подписки, теряется навсегда. В бою этой гонки нет, потому что узел
	// переизлучает визитку по таймеру; здесь воспроизводим то же самое.
	announced := make(chan struct{})
	go func() {
		defer close(announced)
		for {
			if posts, _, _ := poster.snapshot(); len(posts) > 0 {
				return
			}
			if err := bus.Announce(ctx, conn.NC(), card); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) == 1
	}, "объявление о появлении агента")
	<-announced

	// Повтор по таймеру: канал не должен засыпать одинаковыми постами.
	card.AnnouncedAt = time.Now().UTC()
	if err := bus.Announce(ctx, conn.NC(), card); err != nil {
		t.Fatalf("повторная визитка: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if posts, _, _ := poster.snapshot(); len(posts) != 1 {
		t.Fatalf("повтор визитки дал %d постов вместо 1", len(posts))
	}

	// А смена профиля — новость.
	card.Projects = []string{"mesh-mail", "kumo"}
	card.AnnouncedAt = time.Now().UTC()
	if err := bus.Announce(ctx, conn.NC(), card); err != nil {
		t.Fatalf("изменённая визитка: %v", err)
	}
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) == 2
	}, "объявление о смене проектов")
}

// Мост не поднимается без списка разрешённых.
//
// Отказ при старте, а не предупреждение в лог: лог читают, когда что-то уже
// случилось, а здесь цена — чужие письма от имени владельца. Проверяется до
// сети и до Telegram, чтобы ошибка конфигурации не пряталась за таймаутом.
func TestМостНеСтартуетБезСпискаРазрешённых(t *testing.T) {
	err := Run(context.Background(), nil, Config{
		ChatID: "-1001",
		Token:  "123:AA",
	})
	if err == nil {
		t.Fatal("мост поднялся с пустым allowed_user_ids")
	}
	if !strings.Contains(err.Error(), "allowed_user_ids") {
		t.Fatalf("ошибка не называет поле: %v", err)
	}
	// Текст обязан объяснить смену поведения: раньше пустота означала «всем»,
	// и оператор, обновивший мост, иначе решит, что сломалась конфигурация.
	if !strings.Contains(err.Error(), "@userinfobot") {
		t.Fatalf("ошибка не подсказывает, где взять id: %v", err)
	}
}

// Отказ хранилища не теряет имя проекта навсегда.
//
// Дозаполнение идёт на КАЖДОЙ визитке, а не только на изменившейся: узел
// переизлучает одну и ту же визитку раз в минуту, и если привязать заполнение
// к изменению, один отказ хранилища оставил бы проект без имени до
// перезапуска моста — то есть, возможно, навсегда.
func TestИмяПроектаЗаполняетсяПослеОтказаНаТойЖеВизитке(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}

	// Первая попытка обречена: под ключом проекта лежит запись чужого вида.
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		Version: 1, Kind: KindThreadTopic, MessageThreadID: 51,
	}); err != nil {
		t.Fatalf("подготовка испорченной записи: %v", err)
	}

	go func() { _ = handlePresence(ctx, conn, poster, store) }()

	card := bus.Card{
		AgentID: "pi-claude", Host: "raspberrypi", Engine: "claude",
		Projects: []string{"mesh-mail"}, TTLSeconds: 180,
		AnnouncedAt: time.Now().UTC().Truncate(time.Second),
	}
	payload, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("визитка: %v", err)
	}

	// Шлём визитку, пока не убедимся, что подписка её увидела: core NATS
	// истории не хранит, а подписка поднимается в горутине.
	отправить := func() {
		for i := 0; i < 20; i++ {
			_ = conn.NC().Publish("agents.pi-claude.presence", payload)
			time.Sleep(50 * time.Millisecond)
		}
	}
	отправить()

	// Отказ не должен был ничего записать.
	got, _, err := store.ProjectByTopic(ctx, 51)
	if err == nil && got.Known {
		t.Fatal("имя записано поверх записи чужого вида")
	}

	// Чиним запись — теперь это настоящая тема проекта без имени, как у тех,
	// что заведены прежним кодом.
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		Version: 1, Kind: KindProjectTopic, MessageThreadID: 51,
	}); err != nil {
		t.Fatalf("починка записи: %v", err)
	}

	// Визитка ТА ЖЕ САМАЯ — байт в байт. Если бы заполнение шло только на
	// изменение, здесь бы ничего не произошло.
	отправить()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		name, found, err := store.ProjectByTopic(ctx, 51)
		if err == nil && found && name.Known && name.Name == "mesh-mail" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("имя проекта не записалось после повторной неизменной визитки")
}

// Тема «Общего», заведённая прежним кодом, получает имя при старте моста.
//
// Её имя пусто, а пустой проект не перечисляет ни одна визитка: «Общее» ничей
// не проект. Значит источник, закрывающий все остальные темы, эту не закроет
// никогда — и тема, в которую уходят письма без проекта, осталась бы
// «неизвестной» навсегда.
//
// Нашлось мутацией: без этого теста удаление вызова при старте не красило
// ничего.
func TestТемаОбщегоПолучаетИмяПриСтарте(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)

	// Запись прежнего образца: вид и номер темы есть, имени нет.
	if err := store.Put(ctx, projectKey(""), Topic{
		Version: 1, Kind: KindProjectTopic, MessageThreadID: 61,
	}); err != nil {
		t.Fatalf("подготовка записи: %v", err)
	}

	bot := &botDouble{holdUntil: time.Now().Add(time.Hour)}
	server := httptest.NewServer(bot.handler())
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, conn, Config{
			ChatID:          "-1001",
			Token:           "123:двойник",
			ForumTopics:     true,
			AllowedUserIDs:  []int64{42},
			TelegramOptions: []tg.Option{tg.WithBaseURL(server.URL)},
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		name, found, err := store.ProjectByTopic(ctx, 61)
		if err == nil && found && name.Known && name.Name == "" {
			cancel()
			<-done
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("тема «Общего» не получила имени при старте моста")
}

// Отказ при заполнении имени «Общего» останавливает старт, а не проглатывается.
//
// Успешный тест рядом доказывает, что вызов есть, но не доказывает, что его
// ошибка кому-то видна. Молчаливое «не знаю» оставило бы тему, в которую всё
// и уходит, навсегда неизвестной — и заметить это было бы нечем.
func TestОшибкаИмениОбщегоОстанавливаетСтарт(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Под ключом «Общего» лежит запись чужого вида: заполнение обязано
	// вернуть ошибку, а не молча ничего не сделать.
	if err := store.Put(ctx, projectKey(""), Topic{
		Version: 1, Kind: KindThreadTopic, MessageThreadID: 62,
	}); err != nil {
		t.Fatalf("подготовка записи: %v", err)
	}

	bot := &botDouble{holdUntil: time.Now().Add(time.Hour)}
	server := httptest.NewServer(bot.handler())
	defer server.Close()

	// Run запускается в горутине с ожиданием по времени: если ошибку
	// проглотить, он не вернётся вовсе, и тест обязан упасть, а не висеть.
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, conn, Config{
			ChatID:          "-1001",
			Token:           "123:двойник",
			ForumTopics:     true,
			AllowedUserIDs:  []int64{42},
			TelegramOptions: []tg.Option{tg.WithBaseURL(server.URL)},
		})
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run стартовал, проглотив отказ при заполнении имени «Общего»")
		}
		if !strings.Contains(err.Error(), "Общего") {
			t.Fatalf("ошибка не называет причину: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулся: отказ при заполнении имени «Общего» проглочен")
	}
}

// Признак приставок доезжает от показа до запроса в Telegram.
//
// Он решает, снимать ли служебную приставку в аварийном повторе. Потеряется по
// дороге — и повтор либо оставит служебный символ внутри моноширинного блока,
// либо, наоборот, съест данные письма. По ответу API этого не видно, поэтому
// проверяется здесь, на настоящем пути моста: показ уходит, Telegram отвергает
// разметку, и мы смотрим, что ушло вторым запросом.
func TestПризнакПриставокДоезжаетДоЗапроса(t *testing.T) {
	for _, marked := range []bool{true, false} {
		var mu sync.Mutex
		var тексты []string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseMultipartForm(1 << 20)

			mu.Lock()
			тексты = append(тексты, r.FormValue("text"))
			первый := len(тексты) == 1
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			if первый {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: can't parse entities: Unsupported start tag \"x\" at byte offset 1"}`))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":5}}`))
		}))
		defer srv.Close()

		poster := &clientPoster{
			client: tg.New("t", tg.WithBaseURL(srv.URL), tg.WithMinSendGap(0)),
			chatID: "-100",
		}

		post := tg.Post{Text: tg.LineMarker + "строка письма", MarkedLines: marked}
		if _, err := poster.Send(context.Background(), 7, post); err != nil {
			t.Fatalf("отправка не удалась: %v", err)
		}

		mu.Lock()
		повтор := тексты[len(тексты)-1]
		mu.Unlock()

		остался := strings.Contains(повтор, tg.LineMarker)
		if marked && остался {
			t.Errorf("признак стоял, а приставка осталась в аварийном показе: %q", повтор)
		}
		if !marked && !остался {
			t.Errorf("признака не было, а приставка снята — это данные письма: %q", повтор)
		}
	}
}
