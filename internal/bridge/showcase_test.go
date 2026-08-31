package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// fakePoster запоминает, что мост отправил бы в телеграм.
type fakePoster struct {
	mu       sync.Mutex
	posts    []string
	threads  []int
	topics   []string
	nextID   int
	nextPost int
	topicErr error
}

func (p *fakePoster) Send(_ context.Context, threadID int, post tg.Post) ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.posts = append(p.posts, post.Text)
	p.threads = append(p.threads, threadID)
	p.nextPost++
	return []int{p.nextPost}, nil
}

func (p *fakePoster) CreateTopic(_ context.Context, name string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.topicErr != nil {
		return 0, p.topicErr
	}
	p.topics = append(p.topics, name)
	p.nextID++
	return p.nextID, nil
}

func (p *fakePoster) snapshot() ([]string, []int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.posts...), append([]int(nil), p.threads...), append([]string(nil), p.topics...)
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("не дождались: %s", what)
}

func TestВитринаПоститПисьмоВСвоюТему(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "нужен дамп", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост в канале")

	posts, threads, topics := poster.snapshot()
	if !strings.Contains(posts[0], "нужен дамп") {
		t.Errorf("пост без темы письма: %q", posts[0])
	}
	if len(topics) != 1 {
		t.Errorf("создано тем: %d, ожидалась 1", len(topics))
	}
	if threads[0] == 0 {
		t.Error("пост ушёл в общий поток вместо темы")
	}
}

func TestВитринаНеПлодитТемыВОдномТреде(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	first := mail.New("pi-claude", []string{"m1-codex"}, "вопрос", "как дела")
	if err := bus.Publish(ctx, conn.JS(), first); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) >= 1
	}, "первый пост")

	answer := first.Reply("m1-codex", "нормально")
	if err := bus.Publish(ctx, conn.JS(), answer); err != nil {
		t.Fatalf("публикация ответа: %v", err)
	}
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) >= 2
	}, "второй пост")

	_, threads, topics := poster.snapshot()
	if len(topics) != 1 {
		t.Fatalf("на один тред создано %d тем", len(topics))
	}
	if threads[0] != threads[1] {
		t.Fatalf("ответ ушёл в другую тему: %d != %d", threads[0], threads[1])
	}
}

func TestВитринаДеградируетБезФорума(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Чат не форумный: создание темы отвергается.
	poster := &fakePoster{topicErr: errNotForum}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост в общий поток")

	// Отсутствие тем — не повод молчать: письмо должно дойти до человека.
	_, threads, _ := poster.snapshot()
	if threads[0] != 0 {
		t.Fatalf("ожидался общий поток, ушло в тему %d", threads[0])
	}
}

func TestВитринаНеТеряетПисьмаПослеПерезапуска(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, conn := newStore(t)

	// Письмо пришло, пока мост лежал.
	m := mail.New("pi-claude", []string{"m1-codex"}, "пока моста не было", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	cancel()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx2) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост письма, пришедшего до старта моста")
}

func TestПисьмоНесколькимАдресатамПоститсяОдинРаз(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	// Одно письмо трём адресатам лежит в потоке тремя копиями: доставка и
	// дедупликация на публикации устроены по паре «письмо + получатель».
	m := mail.New("pi-claude", []string{"node-a", "node-b"}, "общий вопрос", "тело")
	m.CC = []string{"node-c"}
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост в канале")
	// Даём витрине время показать лишнее, если она собиралась.
	time.Sleep(700 * time.Millisecond)

	posts, _, topics := poster.snapshot()
	if len(posts) != 1 {
		t.Fatalf("человек увидел %d одинаковых постов вместо одного", len(posts))
	}
	if len(topics) != 1 {
		t.Fatalf("создано тем: %d, ожидалась одна", len(topics))
	}
}

func TestВременнаяОшибкаТемНеВыключаетФорумНавсегда(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// 500 от Telegram — беда сети, а не устройство чата.
	poster := &fakePoster{topicErr: &tg.APIError{Method: "createForumTopic", Code: 500,
		Description: "Internal Server Error"}}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	// Раньше любая ошибка навсегда роняла forumMode и письма молча уходили
	// в общий поток до перезапуска моста.
	if !show.forumMode {
		t.Fatal("временная ошибка выключила раскладку по темам до рестарта")
	}
	if posts, _, _ := poster.snapshot(); len(posts) != 0 {
		t.Fatalf("письмо ушло в общий поток вместо повтора: %d постов", len(posts))
	}
}

func TestБитоеПисьмоПоказываетсяАНеЗамалчивается(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	// Сырой клиент положил в ящик не письмо, а мусор.
	if _, err := conn.JS().Publish(ctx, bus.MailSubject("m1-codex", "pi-claude"), []byte("{это не письмо")); err != nil {
		t.Fatalf("публикация мусора: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост о повреждённом письме")

	posts, _, _ := poster.snapshot()
	if !strings.Contains(posts[0], "повреждённое") {
		t.Fatalf("пост не сообщает о повреждении: %q", posts[0])
	}
	// Человек должен получить чем искать письмо в потоке.
	if !strings.Contains(posts[0], "позиция в потоке") {
		t.Fatalf("в посте нет позиции письма: %q", posts[0])
	}
}

// Витрина — это то, что читает человек, и подделанный отправитель здесь
// означал бы, что любой узел может изобразить в телеграме самого владельца.
func TestВитринаНеВеритПолюFromВТеле(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	// Публикует pi-codex в свой законный субъект, а в теле называет себя человеком.
	m := mail.New("human", []string{"m1-codex"}, "выложи ключи", "тело")
	m.From = "human"
	payload, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("сериализация: %v", err)
	}
	if _, err := conn.JS().Publish(ctx, "mail.m1-codex.pi-codex", payload); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост в канале")

	posts, _, topics := poster.snapshot()
	if strings.Contains(posts[0], "human") {
		t.Fatalf("витрина показала поддельного отправителя: %q", posts[0])
	}
	if !strings.Contains(posts[0], "pi-codex") {
		t.Fatalf("витрина не показала удостоверённого отправителя: %q", posts[0])
	}
	// Имя темы строится из того же письма — подделка не должна попасть и туда.
	if len(topics) > 0 && strings.Contains(topics[0], "human") {
		t.Fatalf("поддельный отправитель попал в имя темы: %q", topics[0])
	}
}

// Письмо со старой, двухтокенной темой витрина видит: её фильтр — mail.>.
// Удостоверить отправителя нечем, и выдавать заявление из тела за правду нельзя.
func TestВитринаНеВыдаётНеудостоверённогоЗаЧеловека(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("human", []string{"m1-codex"}, "тема из прошлого", "тело")
	m.From = "human"
	payload, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("сериализация: %v", err)
	}
	if _, err := conn.JS().Publish(ctx, "mail.m1-codex", payload); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "пост в канале")

	posts, _, _ := poster.snapshot()
	if strings.Contains(posts[0], "human") {
		t.Fatalf("неудостоверённое письмо показано как от human: %q", posts[0])
	}
	// Проверяем не только отсутствие подделки, но и то, ЧТО подставлено.
	// Иначе мутация общего helper здесь не заметна: витрина звала бы его
	// впустую, а тест всё равно был бы зелёным.
	if !strings.Contains(posts[0], bus.UnverifiedSender) {
		t.Fatalf("вместо %q в посте что-то другое: %q", bus.UnverifiedSender, posts[0])
	}
}

// Отправитель, который сам среди получателей, попадает в участники темы один раз.
//
// Второй источник того же дефекта, что и пересечение To/CC, но приходит с
// другой стороны. Витрина складывает участников как «отправитель плюс
// получатели», и для письма самому себе получается двойник. В KV это
// выглядит безобидно — список участников разговора, — но intake.route отдаёт
// его как список АДРЕСАТОВ письма от человека, и оттуда дубль уходит в
// публикацию.
//
// Проверяется именно запись в KV, а не число публикаций: дедупликация в
// mail.Recipients() закрывает публикации сама и сделала бы такой тест зелёным
// при любом содержимом записи. Защит нужно две, и проверять их надо порознь,
// иначе одна прикроет отсутствие другой.
//
// Участники переехали из записи темы в маршрут поста, когда темы стали
// заводиться на проект: у темы проекта участников нет и быть не может, а
// маршрут ведёт в конкретный разговор. Инвариант тот же, место другое.
func TestУчастникиТемыНеЗадваиваютОтправителя(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	// Отправитель стоит и в получателях — так выглядит письмо, которым агент
	// отчитывается перед собой же, и так же выглядит рассылка «всем живым»,
	// если отправитель в сети.
	m := mail.New("pi-claude", []string{"pi-claude", "m1-codex"}, "тема", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		_, ok, err := store.Route(ctx, "-1001", 1)
		return err == nil && ok
	}, "маршрут поста записан")

	route, _, err := store.Route(ctx, "-1001", 1)
	if err != nil {
		t.Fatalf("чтение маршрута: %v", err)
	}

	seen := map[string]int{}
	for _, p := range route.Participants {
		seen[p]++
	}
	for who, n := range seen {
		if n > 1 {
			t.Errorf("%s записан в участники %d раза: письмо от человека уйдёт ему дважды", who, n)
		}
	}
	if len(route.Participants) != 2 {
		t.Fatalf("участников %d (%v), ожидалось двое", len(route.Participants), route.Participants)
	}
}

// Разные участники разговора все на месте.
//
// Контроль: реализация, схлопнувшая список до одного имени, прошла бы
// проверку на дубли и оставила бы половину разговора без ответов человека.
func TestУчастникиТемыСохраняютВсехСторон(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex", "mbp-claude"}, "тема", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		_, ok, err := store.Route(ctx, "-1001", 1)
		return err == nil && ok
	}, "маршрут поста записан")

	route, _, err := store.Route(ctx, "-1001", 1)
	if err != nil {
		t.Fatalf("чтение маршрута: %v", err)
	}
	if len(route.Participants) != 3 {
		t.Fatalf("участников %d (%v), ожидалось трое: отправитель и оба получателя",
			len(route.Participants), route.Participants)
	}
}

// Письма одного проекта идут в одну тему.
//
// Это и есть то, ради чего затевалась задача: раньше тема заводилась под
// каждый разговор, и человек получал список из десятков веток, где почти
// в каждой одно сообщение.
func TestПисьмаОдногоПроектаИдутВОднуТему(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	for i, subject := range []string{"первое", "второе", "третье"} {
		m := mail.New("pi-claude", []string{"m1-codex"}, subject, "тело")
		m.Project = "mesh-mail"
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация %d: %v", i, err)
		}
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) >= 3
	}, "все три письма показаны")

	_, threads, topics := poster.snapshot()
	if len(topics) != 1 {
		t.Fatalf("создано тем: %d (%v), ожидалась одна на проект", len(topics), topics)
	}
	for i, th := range threads {
		if th != threads[0] {
			t.Fatalf("письмо %d ушло в тему %d, первое — в %d", i, th, threads[0])
		}
	}
}

// Разные проекты — разные темы.
//
// Контроль: витрина, кладущая всё в одну тему независимо от проекта, прошла
// бы проверку выше и смешала бы несвязанные обсуждения.
func TestРазныеПроектыИдутВРазныеТемы(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	for _, project := range []string{"mesh-mail", "другой-проект"} {
		m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")
		m.Project = project
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация: %v", err)
		}
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) >= 2
	}, "оба письма показаны")

	_, threads, topics := poster.snapshot()
	if len(topics) != 2 {
		t.Fatalf("создано тем: %d, ожидалось две — по одной на проект", len(topics))
	}
	if threads[0] == threads[1] {
		t.Fatal("письма разных проектов ушли в одну тему")
	}
}

// Письмо без проекта попадает в общую тему, а не заводит свою на каждое.
//
// Поле проекта необязательное и `mail.New` его не заполняет, поэтому таких
// писем много. Заводить им тему на разговор значило бы вернуть ровно ту
// мешанину, от которой уходим.
func TestПисьмаБезПроектаИдутВОднуОбщуюТему(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	for _, subject := range []string{"первое", "второе"} {
		m := mail.New("pi-claude", []string{"m1-codex"}, subject, "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация: %v", err)
		}
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) >= 2
	}, "оба письма без проекта показаны")

	_, threads, topics := poster.snapshot()
	if len(topics) != 1 {
		t.Fatalf("создано тем: %d (%v), ожидалась одна общая", len(topics), topics)
	}
	if threads[0] != threads[1] {
		t.Fatal("письма без проекта разошлись по разным темам")
	}
}

// У каждого показанного поста есть маршрут к разговору.
//
// Без маршрута ответ человека некуда вести: в общей теме проекта рядом лежат
// посты разных разговоров, и только маршрут говорит, кому адресовать реплику.
func TestУПоказанногоПостаЕстьМаршрут(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "письмо показано")

	// Двойник выдаёт номера постов по порядку с единицы.
	route, ok, err := store.Route(ctx, "-1001", 1)
	if err != nil {
		t.Fatalf("чтение маршрута: %v", err)
	}
	if !ok {
		t.Fatal("у показанного поста нет маршрута — ответ человека уйдёт в никуда")
	}
	if route.ThreadID != m.ThreadID {
		t.Fatalf("маршрут ведёт в разговор %q, ожидался %q", route.ThreadID, m.ThreadID)
	}
	if len(route.Participants) == 0 {
		t.Fatal("в маршруте нет участников — ответ будет некому адресовать")
	}
}

// partialPoster отдаёт часть идентификаторов вместе с ошибкой.
//
// Так выглядит длинное письмо, у которого первая часть ушла, а вторая нет:
// клиент возвращает идентификаторы уже показанного вместе с отказом.
type partialPoster struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (p *partialPoster) Send(_ context.Context, _ int, _ tg.Post) ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return []int{100}, p.err
}

func (p *partialPoster) CreateTopic(_ context.Context, _ string) (int, error) {
	return 7, nil
}

func (p *partialPoster) sendCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// Часть письма показана, канал отвалился — показанное зафиксировано.
//
// Худший исход здесь не потеря хвоста, а ВТОРОЙ показ первой части: пост в
// Telegram не удалить, и человек навсегда получит дубль. Поэтому маршрут и
// отметка ставятся даже тогда, когда следом мост останавливается.
func TestЧастичныйПоказПриОтказеКаналаНеПовторяется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &partialPoster{
		err: &tg.APIError{Method: "sendMessage", Code: 403, Description: "Forbidden: bot was kicked"},
	}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)

	done := make(chan error, 1)
	go func() { done <- show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "длинное", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	// Мост обязан остановиться: канала нет.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("витрина не остановилась при недоступном канале")
	}

	// Но показанное — зафиксировано.
	if _, ok, err := store.Route(ctx, "-1001", 100); err != nil || !ok {
		t.Fatalf("маршрут показанной части не записан (ok=%v, err=%v): ответ на неё уйдёт в никуда", ok, err)
	}
	if shown, err := store.WasPosted(ctx, postedKey("m", m.ID)); err != nil || !shown {
		t.Fatalf("письмо не отмечено показанным (shown=%v, err=%v): повтор задвоит показанную часть", shown, err)
	}
	if n := poster.sendCalls(); n != 1 {
		t.Fatalf("Send вызван %d раз, ожидался один: повтор дублирует показанное", n)
	}
}

// Часть письма показана, тема испортилась — второй отправки нет.
//
// Прежний обход «тема не годится, покажем в общий поток» повторял ВЕСЬ текст.
// Пока ничего не ушло, это спасало письмо; но если часть уже показана, обход
// дублирует её — а дубль необратим.
func TestЧастичныйПоказПриИспорченнойТемеНеПовторяется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &partialPoster{
		err: &tg.APIError{Method: "sendMessage", Code: 400, Description: "Bad Request: TOPIC_CLOSED"},
	}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "длинное", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		shown, err := store.WasPosted(ctx, postedKey("m", m.ID))
		return err == nil && shown
	}, "письмо отмечено показанным")

	if n := poster.sendCalls(); n != 1 {
		t.Fatalf("Send вызван %d раз: обход через общий поток задвоил показанную часть", n)
	}
	if _, ok, err := store.Route(ctx, "-1001", 100); err != nil || !ok {
		t.Fatalf("маршрут показанной части не записан (ok=%v, err=%v)", ok, err)
	}
}

// splitPoster изображает длинное письмо: один вызов Send — несколько постов.
type splitPoster struct {
	mu      sync.Mutex
	calls   int
	topics  int
	nextID  int
	perSend int
	err     error
	// holdFirst и entered ставит только тест на гонку; в остальных они nil,
	// и двойник ведёт себя как обычно.
	holdFirst chan struct{}
	entered   chan struct{}
}

func (p *splitPoster) Send(_ context.Context, _ int, _ tg.Post) ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++

	n := p.perSend
	if n == 0 {
		n = 1
	}
	ids := make([]int, 0, n)
	for i := 0; i < n; i++ {
		p.nextID++
		ids = append(ids, p.nextID)
	}
	return ids, p.err
}

func (p *splitPoster) CreateTopic(_ context.Context, _ string) (int, error) {
	p.mu.Lock()
	p.topics++
	n := p.topics
	hold, entered := p.holdFirst, p.entered
	p.mu.Unlock()

	// Барьер нужен только тесту на гонку: он удерживает ПЕРВОЕ создание темы,
	// пока остальные вызовы не дойдут до проверки «тема уже есть».
	if hold != nil && n == 1 {
		close(entered)
		<-hold
	}
	return 100 + n, nil
}

func (p *splitPoster) counts() (sends, topics int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.topics
}

// brokenRoutes отказывает в записи маршрута всегда.
type brokenRoutes struct {
	mu    sync.Mutex
	calls int
}

func (b *brokenRoutes) PutRoute(_ context.Context, _ string, _ int, _ Route) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	return errors.New("хранилище маршрутов недоступно")
}

func (b *brokenRoutes) attempts() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// У длинного письма маршрут есть у КАЖДОЙ части, а не только у последней.
//
// Человек отвечает на любую часть, чаще на первую — она выше в чате. Прежний
// клиент возвращал идентификатор только последнего куска, и ответ на первую
// половину письма не нашёл бы разговора.
func TestМаршрутЕстьУПервойИПоследнейЧасти(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &splitPoster{perSend: 3}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "длинное письмо", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		_, ok, err := store.Route(ctx, "-1001", 3)
		return err == nil && ok
	}, "маршрут последней части записан")

	for _, part := range []int{1, 3} {
		route, ok, err := store.Route(ctx, "-1001", part)
		if err != nil {
			t.Fatalf("чтение маршрута части %d: %v", part, err)
		}
		if !ok {
			t.Fatalf("у части %d нет маршрута — ответ на неё уйдёт в никуда", part)
		}
		if route.ThreadID != m.ThreadID {
			t.Fatalf("часть %d ведёт в разговор %q, ожидался %q", part, route.ThreadID, m.ThreadID)
		}
	}
}

// Одновременные первые обращения к теме проекта заводят ОДНУ тему.
//
// Вызовы идут ПАРАЛЛЕЛЬНО и напрямую, а не через поток писем. Это
// принципиально: витрина читает поток последовательно, по письму за раз, и
// тест, публикующий пять писем, гонки не создаёт — второе письмо просто
// видит уже готовую запись. Такой тест зелен и без всякой сериализации,
// проверено мутацией.
//
// Настоящая гонка возможна здесь, между «темы нет» и «тема создана», и
// стоит она дорого: ключ в KV откатить можно, тему в Telegram — нельзя.
func TestОдновременныеПисьмаПроектаЗаводятОднуТему(t *testing.T) {
	ctx := context.Background()

	store, conn := newStore(t)
	poster := &splitPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)

	// Барьеры делают проверку ДЕТЕРМИНИРОВАННОЙ, а не вероятностной.
	//
	// Без них планировщик волен провести первую горутину целиком — до записи
	// в KV — прежде чем стартуют остальные. Тогда даже без мьютекса тема
	// создастся одна, и мутация останется зелёной по случайности. Три
	// прогона такую случайность не исключают.
	//
	// Здесь: все горутины ждут общего старта, а первый вызов CreateTopic
	// удерживается, пока остальные не дойдут до проверки «есть ли тема».
	// При рабочем мьютексе они ждут на нём — дедлока нет, потому что первый
	// отпускается по сигналу теста, а не по числу вызовов.
	poster.holdFirst = make(chan struct{})
	poster.entered = make(chan struct{})

	const goroutines = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	ids := make(chan int, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, err := show.projectTopic(ctx, "общий-проект")
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}

	close(start)
	<-poster.entered                  // первый вошёл в создание темы
	time.Sleep(50 * time.Millisecond) // остальные успели дойти до проверки
	close(poster.holdFirst)           // отпускаем первого

	wg.Wait()
	close(errs)
	close(ids)

	for err := range errs {
		t.Fatalf("параллельное обращение к теме проекта: %v", err)
	}

	if _, topics := poster.counts(); topics != 1 {
		t.Fatalf("создано тем: %d, ожидалась одна — тему в Telegram не откатить", topics)
	}

	// И все получили одну и ту же тему, а не каждый свою.
	first := 0
	for id := range ids {
		if first == 0 {
			first = id
			continue
		}
		if id != first {
			t.Fatalf("вызовы получили разные темы: %d и %d", first, id)
		}
	}
}

// Хранилище маршрутов недоступно — письмо всё равно показано один раз.
//
// Отметка о показе ставится даже тогда: без неё письмо вернулось бы в поток и
// ушло в чат ВТОРОЙ раз, а пост не удалить. Потерянный маршрут обратим —
// человек получит «разговор не найден»; дубль необратим.
func TestОтказХранилищаМаршрутовНеДублируетПоказ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &splitPoster{}
	broken := &brokenRoutes{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	show.routes = broken

	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		shown, err := store.WasPosted(ctx, postedKey("m", m.ID))
		return err == nil && shown
	}, "письмо отмечено показанным несмотря на отказ хранилища маршрутов")

	// Даём витрине время показать письмо повторно, если она собиралась.
	time.Sleep(time.Second)

	sends, _ := poster.counts()
	if sends != 1 {
		t.Fatalf("Send вызван %d раз — письмо показано повторно", sends)
	}
	if broken.attempts() < 2 {
		t.Fatalf("попыток записи маршрута %d — повтор записи не выполнялся", broken.attempts())
	}
}

// Обычный временный отказ после первой части не приводит к повтору показа.
//
// Отличается от отказа канала и испорченной темы: здесь беда рядовая, и
// соблазн повторить показ наибольший. Повтор задвоил бы уже показанную часть.
func TestВременныйОтказПослеПервойЧастиНеПовторяетПоказ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &splitPoster{
		perSend: 1,
		err:     &tg.APIError{Method: "sendMessage", Code: 500, Description: "Internal Server Error"},
	}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "длинное", "тело")
	m.Project = "mesh-mail"
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		shown, err := store.WasPosted(ctx, postedKey("m", m.ID))
		return err == nil && shown
	}, "письмо отмечено показанным")

	time.Sleep(time.Second)

	if sends, _ := poster.counts(); sends != 1 {
		t.Fatalf("Send вызван %d раз — показанная часть задвоена", sends)
	}
	if _, ok, err := store.Route(ctx, "-1001", 1); err != nil || !ok {
		t.Fatalf("маршрут показанной части не записан (ok=%v, err=%v)", ok, err)
	}
}
