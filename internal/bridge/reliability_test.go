package bridge

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// flakyPoster отказывает заданное число раз, а потом работает.
//
// Двойник именно такой, потому что различает две гарантии: «письмо дойдёт,
// когда связь вернётся» и «письмо потеряно навсегда». Двойник, который
// падает всегда, их не различает — при нём молчание витрины выглядит
// одинаково правильно в обоих случаях.
type flakyPoster struct {
	mu sync.Mutex

	failSends  int   // сколько первых Send завалить
	failTopics int   // сколько первых CreateTopic завалить
	sendErr    error // чем именно отказывать
	topicErr   error

	sendCalls  int
	topicCalls int
	posts      []string
	threads    []int
	nextTopic  int
	nextPost   int
}

func (p *flakyPoster) Send(_ context.Context, threadID int, post tg.Post) ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sendCalls++
	if p.sendCalls <= p.failSends {
		return nil, p.sendErr
	}
	p.posts = append(p.posts, post.Text)
	p.threads = append(p.threads, threadID)
	// Идентификатор поста двойник выдаёт свой: настоящий приходит от
	// Telegram, а здесь важно лишь то, что он уникален и возвращается.
	p.nextPost++
	return []int{p.nextPost}, nil
}

func (p *flakyPoster) CreateTopic(_ context.Context, _ string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topicCalls++
	if p.topicCalls <= p.failTopics {
		return 0, p.topicErr
	}
	p.nextTopic++
	return p.nextTopic, nil
}

func (p *flakyPoster) seen() (posts []string, threads []int, sends int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.posts...), append([]int(nil), p.threads...), p.sendCalls
}

// temporary — отказ, который кончится сам: сеть моргнула, у Telegram 5xx.
func temporary() error {
	return &tg.APIError{Method: "sendMessage", Code: 500, Description: "Internal Server Error"}
}

// Письмо, не ушедшее из-за временной беды, обязано дойти после неё.
//
// Это главный тест задачи. На старом коде он красный: отметка о показе
// ставилась ДО отправки, поэтому вернувшееся в поток письмо считалось
// показанным, молча подтверждалось и не доходило до человека никогда.
func TestПисьмоДоходитПослеВременногоОтказаОтправки(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{failSends: 1, sendErr: temporary()}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо после сбоя", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) > 0
	}, "показ письма после восстановления связи")

	posts, _, _ := poster.seen()
	if !strings.Contains(posts[0], "письмо после сбоя") {
		t.Fatalf("показано не то письмо: %q", posts[0])
	}
}

// То же для беды при создании темы.
func TestПисьмоДоходитПослеВременногоОтказаТемы(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{
		failTopics: 1,
		topicErr:   &tg.APIError{Method: "createForumTopic", Code: 500, Description: "Internal Server Error"},
	}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо после сбоя темы", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) > 0
	}, "показ письма после восстановления тем")
}

// Повторные попытки идут с паузой, а не в цикле на полной скорости.
//
// Различающий признак — ЧИСЛО попыток за отрезок времени. Проверять сам факт
// повтора бессмысленно: он был и раньше, беда была в том, что их за секунду
// набегали тысячи.
func TestПовторыИдутСПаузой(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Отказ, который не кончится: письмо будет возвращаться в поток снова
	// и снова, и вот тут важно, КАК часто.
	poster := &flakyPoster{failSends: 1_000_000, sendErr: temporary()}
	show := NewShowcase(conn.JS(), store, poster, "-1001", false)
	go func() { _ = show.Run(ctx) }()

	if _, err := conn.JS().Publish(ctx, bus.MailSubject("m1-codex", "pi-claude"),
		[]byte("{это не письмо")); err != nil {
		t.Fatalf("публикация мусора: %v", err)
	}

	// Ждём заведомо дольше первой паузы, чтобы попытки точно начались.
	time.Sleep(3 * time.Second)

	_, _, sends := poster.seen()
	if sends == 0 {
		t.Fatal("повторов не было вовсе — письмо не возвращается в поток")
	}
	// За три секунды при первой паузе в две секунды их не может быть много.
	if sends > 5 {
		t.Fatalf("%d попыток за три секунды — витрина крутит повторы без паузы", sends)
	}
}

// Недоступный канал останавливает мост, а не порождает вечные повторы.
func TestНедоступныйКаналОстанавливаетВитрину(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{
		failSends: 1_000_000,
		sendErr:   &tg.APIError{Method: "sendMessage", Code: 403, Description: "Forbidden: bot was kicked from the supergroup chat"},
	}
	show := NewShowcase(conn.JS(), store, poster, "-1001", false)

	done := make(chan error, 1)
	go func() { done <- show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "некому показывать", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("витрина завершилась без ошибки — systemd не узнает об отказе")
		}
		if !strings.Contains(err.Error(), "недоступен") {
			t.Fatalf("ошибка не объясняет причину: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("витрина не остановилась: письмо крутится в повторах, хотя канала нет")
	}
}

// Испорченная тема не мешает письму дойти: показываем в общий поток.
func TestИспорченнаяТемаНеОстанавливаетПисьмо(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Тема создаётся, но постинг в неё отвергается: тему закрыли.
	poster := &flakyPoster{
		failSends: 1,
		sendErr:   &tg.APIError{Method: "sendMessage", Code: 400, Description: "Bad Request: TOPIC_CLOSED"},
	}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо в закрытую тему", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) > 0
	}, "показ письма в общий поток")

	_, threads, _ := poster.seen()
	if threads[0] != 0 {
		t.Fatalf("письмо ушло в тему %d, а она закрыта", threads[0])
	}

	// Запись о негодной теме должна исчезнуть, иначе следующее письмо этого
	// разговора споткнётся о неё снова.
	if _, ok, err := store.Get(ctx, m.ThreadID); err != nil {
		t.Fatalf("чтение темы: %v", err)
	} else if ok {
		t.Fatal("запись о закрытой теме осталась в KV")
	}
}

// Копии одного повреждённого письма показываются один раз.
//
// У битого письма нет идентификатора, и раньше отметка о показе для него не
// ставилась вовсе: письмо трём адресатам давало человеку три одинаковых
// поста.
func TestКопииПовреждённогоПисьмаПоказываютсяОдинРаз(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", false)
	go func() { _ = show.Run(ctx) }()

	// Одно и то же тело в три ящика — ровно так лежит в потоке письмо,
	// отправленное троим.
	broken := []byte(`{"id":"обрывок","from":"pi-claude"`)
	for _, recipient := range []string{"node-a", "node-b", "node-c"} {
		if _, err := conn.JS().Publish(ctx, bus.MailSubject(recipient, "pi-claude"), broken); err != nil {
			t.Fatalf("публикация копии: %v", err)
		}
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) > 0
	}, "пост о повреждённом письме")
	// Даём витрине время показать лишнее, если она собиралась.
	time.Sleep(time.Second)

	posts, _, _ := poster.seen()
	if len(posts) != 1 {
		t.Fatalf("человек увидел %d одинаковых постов о повреждённом письме", len(posts))
	}
}

// Разные повреждённые письма от одного отправителя не склеиваются.
//
// Контроль к предыдущему тесту: без него дедупликация «по одному ключу на
// всё» тоже была бы зелёной, а человек не увидел бы второго письма.
func TestРазныеПовреждённыеПисьмаПоказываютсяОба(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", false)
	go func() { _ = show.Run(ctx) }()

	for _, broken := range []string{`{"id":"первый обрывок"`, `{"id":"второй обрывок"`} {
		if _, err := conn.JS().Publish(ctx, bus.MailSubject("m1-codex", "pi-claude"), []byte(broken)); err != nil {
			t.Fatalf("публикация: %v", err)
		}
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) >= 2
	}, "оба повреждённых письма показаны")
}

// Отметка о показе ставится после показа, а не до него.
//
// Проверяется напрямую, без витрины: это и есть то свойство, ради которого
// затевалась задача, и оно должно быть выражено в тесте явно — иначе при
// следующей правке порядок вернут обратно, а тесты выше объяснят падение
// чем-нибудь другим.
func TestОтметкаСтавитсяТолькоПослеПоказа(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	shown, err := store.WasPosted(ctx, "m-abc123")
	if err != nil {
		t.Fatalf("проверка отметки: %v", err)
	}
	if shown {
		t.Fatal("письмо считается показанным до всякого показа")
	}

	if err := store.MarkPosted(ctx, "m-abc123"); err != nil {
		t.Fatalf("отметка: %v", err)
	}

	shown, err = store.WasPosted(ctx, "m-abc123")
	if err != nil {
		t.Fatalf("повторная проверка: %v", err)
	}
	if !shown {
		t.Fatal("отметка не сохранилась")
	}

	// Повторная отметка не ошибка: письмо показано, чего мы и добивались.
	if err := store.MarkPosted(ctx, "m-abc123"); err != nil {
		t.Fatalf("повторная отметка стала ошибкой: %v", err)
	}
}

// watchfulRoutes подглядывает, стояла ли отметка о показе в момент записи
// маршрута, и делегирует настоящему хранилищу.
//
// Двойник нужен именно такой: проверить порядок постфактум нельзя — после
// обработки письма и маршрут, и отметка есть при любой их очерёдности.
// Различает их только взгляд ИЗНУТРИ записи маршрута.
type watchfulRoutes struct {
	store *TopicStore
	// key — отметка того самого письма, за которым следим.
	key string

	mu    sync.Mutex
	calls int
	late  bool
	err   error
}

func (w *watchfulRoutes) PutRoute(ctx context.Context, chatID string, messageID int, route Route) error {
	shown, err := w.store.WasPosted(ctx, w.key)

	w.mu.Lock()
	w.calls++
	if err != nil && w.err == nil {
		w.err = err
	}
	if shown {
		w.late = true
	}
	w.mu.Unlock()

	return w.store.PutRoute(ctx, chatID, messageID, route)
}

func (w *watchfulRoutes) state() (int, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls, w.late, w.err
}

// Маршрут поста записывается ДО отметки о показе.
//
// Порядок объявлен важным в самом коде, но до сих пор ни один тест его не
// различал: перестановка saveRoutes после MarkPosted оставляла всё зелёным.
// Найдено мутацией на ревью.
//
// Важен он вот почему. MarkPosted может упасть — это обращение к хранилищу.
// При нынешнем порядке маршрут уже записан: письмо вернётся в поток, будет
// показано вторым постом, но ответ на ПЕРВЫЙ, уже видимый человеку пост
// дойдёт. При обратном порядке первый пост остаётся без маршрута навсегда, и
// ответ на него получит «разговор не найден».
func TestМаршрутПишетсяДоОтметкиОПоказе(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо", "тело")
	m.Project = "mesh-mail"

	watch := &watchfulRoutes{store: store, key: postedKey("m", m.ID)}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	show.routes = watch
	go func() { _ = show.Run(ctx) }()

	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		shown, err := store.WasPosted(ctx, postedKey("m", m.ID))
		return err == nil && shown
	}, "письмо отмечено показанным")

	calls, late, err := watch.state()
	if err != nil {
		t.Fatalf("проверка отметки изнутри записи маршрута: %v", err)
	}
	// Без этого тест был бы зелёным и в случае, когда маршрут не пишется вовсе.
	if calls == 0 {
		t.Fatal("маршрут не записывался ни разу — проверять порядок было не на чем")
	}
	if late {
		t.Fatal("маршрут записан ПОСЛЕ отметки о показе: упавшая отметка оставит показанный пост без маршрута")
	}

	if _, ok, err := store.Route(ctx, "-1001", 1); err != nil || !ok {
		t.Fatalf("маршрут показанного поста не сохранён (ok=%v, err=%v)", ok, err)
	}
}

// Маршрут переживает перезапуск моста.
//
// Мост перезапускается буднично: обновление, падение, systemd. Человек этого
// не видит — он видит пост в чате и отвечает на него через час. Если маршруты
// живут в памяти процесса, ответ после перезапуска разговора не найдёт, и
// человеку придётся писать заново.
//
// То, что маршруты лежат в KV, — устройство кода; здесь проверяется
// наблюдаемое следствие: новое хранилище и новый приём, поднятые на том же
// JetStream, доставляют ответ участникам того же разговора.
func TestМаршрутПереживаетПерезапускМоста(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}

	// Первый запуск моста: письмо показано человеку.
	showCtx, stopShow := context.WithCancel(ctx)
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(showCtx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "вопрос человеку", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "письмо показано в теме проекта")

	_, threads, _ := poster.snapshot()
	тема := threads[0]

	// Мост умер.
	stopShow()

	// Новый процесс: своё хранилище и свой приём на том же JetStream.
	свежее, err := NewTopicStore(ctx, conn.JS())
	if err != nil {
		t.Fatalf("хранилище после перезапуска: %v", err)
	}

	// Человек отвечает на пост, показанный ДО перезапуска.
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: тема, Text: "отвечаю после перезапуска",
			From: "tester", FromID: 42, ReplyToMessageID: 1, ReplyToBot: true},
	}}
	// В сети четверо, в разговоре двое: веер по живым был бы сразу виден.
	intake := NewIntake(conn.JS(), свежее, updater,
		живойРеестр("pi-claude", "m1-codex", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "ответ дошёл до участника разговора")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if got[0].Message.ThreadID != m.ThreadID {
		t.Fatalf("ответ ушёл в разговор %q, ожидался %q", got[0].Message.ThreadID, m.ThreadID)
	}

	// Даём времени уйти лишнему, если оно собиралось.
	time.Sleep(time.Second)

	for _, посторонний := range []string{"mbp-claude", "pi-codex"} {
		box, err := bus.Inbox(ctx, conn.JS(), посторонний, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", посторонний, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил чужой ответ — маршрут не нашёлся и приём разослал письмо живым", посторонний)
		}
	}
}
