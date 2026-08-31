package bridge

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// Ответ уходит участникам ТОГО разговора, на пост которого ответили.
//
// Главный тест задачи. В общей теме проекта рядом лежат посты разных
// обсуждений; человек отвечает на конкретный, и письмо должно уйти именно его
// участникам. Проверка «до кого-то дошло» здесь бессмысленна: рассылка всем
// живым тоже «дошла бы», заодно показав переписку посторонним.
func TestОтветУходитУчастникамСвоегоРазговора(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)

	// Два разных разговора, чьи посты лежат в одной теме проекта.
	if err := store.PutRoute(ctx, "-1001", 10, Route{
		ThreadID: "разговор-первый", Project: "mesh-mail",
		Participants: []string{"pi-claude"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}
	if err := store.PutRoute(ctx, "-1001", 20, Route{
		ThreadID: "разговор-второй", Project: "mesh-mail",
		Participants: []string{"mbp-claude"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 55, Text: "отвечаю первому",
			From: "tester", FromID: 42, ReplyToMessageID: 10},
	}}
	// В сети четверо, а в разговоре один: рассылка всем сразу видна.
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex", "m1-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло участнику первого разговора")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if got[0].Message.ThreadID != "разговор-первый" {
		t.Fatalf("письмо в разговоре %q, ожидался «разговор-первый»", got[0].Message.ThreadID)
	}

	// Даём времени уйти лишнему, если оно собиралось.
	time.Sleep(time.Second)

	for _, чужой := range []string{"mbp-claude", "pi-codex", "m1-codex"} {
		box, err := bus.Inbox(ctx, conn.JS(), чужой, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", чужой, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил чужой ответ — переписка ушла посторонним", чужой)
		}
	}
}

// Ответ на последнюю часть длинного письма так же точен, как на первую.
func TestОтветНаЛюбуюЧастьВедётВОдинРазговор(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)

	// Одно письмо, показанное двумя постами.
	for _, id := range []int{30, 31} {
		if err := store.PutRoute(ctx, "-1001", id, Route{
			ThreadID: "длинный-разговор", Participants: []string{"pi-claude"},
		}); err != nil {
			t.Fatalf("подготовка: %v", err)
		}
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 55, Text: "отвечаю на хвост",
			From: "tester", FromID: 42, ReplyToMessageID: 31},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "ответ на последнюю часть доставлен")

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "длинный-разговор" {
		t.Fatalf("разговор %q, ожидался «длинный-разговор»", got[0].Message.ThreadID)
	}
}

// Сообщение в тему без ответа на пост не рассылается никому.
//
// Раньше сообщение в неизвестную тему уходило всем живым. В общей теме
// проекта это означало бы, что любая реплика человека — «ага», «понял» —
// рассылается четверым, и каждый обязан решать, к чему она относится.
func TestСообщениеБезОтветаНеРассылаетсяВсем(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 55, Text: "ага, понял", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил подсказку")

	for _, кто := range []string{"pi-claude", "mbp-claude"} {
		box, err := bus.Inbox(ctx, conn.JS(), кто, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение: %v", err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил письмо из сообщения без ответа на пост", кто)
		}
	}
}

// Ответ на пост, которого мост не помнит, тоже никого не будит.
//
// Так выглядит ответ на пост из прежней жизни, на истёкший маршрут или на
// чужое сообщение. Человеку нужен внятный ответ, а не веерная рассылка.
func TestОтветНаНеизвестныйПостНикогоНеБудит(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 55, Text: "что скажете?",
			From: "tester", FromID: 42, ReplyToMessageID: 999},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил объяснение")

	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(box) != 0 {
		t.Fatal("ответ на неизвестный пост стал письмом")
	}
}

// Серия сообщений без ответа даёт ОДНУ подсказку, а не серию.
//
// Человек в живом обсуждении пишет подряд. Отвечать на каждое значит занять
// ограничитель частоты собственными сообщениями: у нас пауза три секунды на
// пост, и десять подсказок — это полминуты, в течение которых витрина не
// покажет ни одного письма.
func TestСерияСообщенийДаётОднуПодсказку(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updates := make([]tg.Update, 0, 5)
	for i := 1; i <= 5; i++ {
		updates = append(updates, tg.Update{
			ID: i, ChatID: "-1001", ThreadID: 55, Text: "реплика", From: "tester", FromID: 42,
		})
	}
	updater := &fakeUpdater{pending: updates}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "подсказка отправлена")

	time.Sleep(time.Second)

	posts, _, _ := poster.snapshot()
	if len(posts) != 1 {
		t.Fatalf("подсказок %d, ожидалась одна на серию: витрина захлебнётся своими же ответами", len(posts))
	}
}

// Сообщение в СТАРУЮ тему разговора работает по-прежнему.
//
// Обсуждения, начатые до перехода, продолжаются там же, и ответ в них не
// требует Reply на конкретный пост: тема сама означает разговор.
func TestСтараяТемаРазговораРаботаетПоПрежнему(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-legacy", Topic{
		MessageThreadID: 77,
		Participants:    []string{"pi-claude", "m1-codex"},
	}); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 77, Text: "продолжаю старое", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло участнику старого разговора")

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "thread-legacy" {
		t.Fatalf("разговор %q, ожидался «thread-legacy»", got[0].Message.ThreadID)
	}
}

// Ответ на СТАРЫЙ пост бота в теме разговора по-прежнему доходит участникам.
//
// Регрессия, которую я допустил и которую не поймал собственный тест: он
// проверял сообщение БЕЗ ответа, а сломался как раз путь с ответом. У постов,
// показанных до перехода на темы проектов, маршрутов нет — они появились
// позже. Значит ответ на такой пост находил «маршрута нет» и превращался в
// подсказку вместо письма.
func TestОтветНаСтарыйПостБотаДоходитУчастникамТемы(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-legacy", Topic{
		MessageThreadID: 77,
		Participants:    []string{"pi-claude", "m1-codex"},
	}); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		// Маршрута для поста 500 нет: он показан до перехода.
		{ID: 1, ChatID: "-1001", ThreadID: 77, Text: "отвечаю на старое",
			From: "tester", FromID: 42, ReplyToMessageID: 500, ReplyToBot: true},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "ответ на старый пост дошёл участнику темы")

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "thread-legacy" {
		t.Fatalf("разговор %q, ожидался «thread-legacy»", got[0].Message.ThreadID)
	}
}

// Ответ на ЧЕЛОВЕЧЕСКОЕ сообщение в той же теме письмом не становится.
//
// Контроль к предыдущему, и без него починка была бы опаснее дефекта:
// разрешив ответ на что угодно в теме разговора, мы превратили бы реплику
// человека самому себе в письмо всем участникам.
func TestОтветНаЧеловеческоеСообщениеНеСтановитсяПисьмом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-legacy", Topic{
		MessageThreadID: 77,
		Participants:    []string{"pi-claude", "m1-codex"},
	}); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 77, Text: "уточню сам себя",
			From: "tester", FromID: 42, ReplyToMessageID: 501, ReplyToBot: false},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил подсказку")

	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(box) != 0 {
		t.Fatal("ответ человека самому себе стал письмом участникам разговора")
	}
}

// Команда `/to` доставляет письмо ровно одному названному агенту.
//
// Смысл задачи — не тревожить остальных, поэтому проверяется не только «дошло
// до адресата», но и «не дошло до других». Первое совместимо и с рассылкой
// всем живым, второе — нет.
func TestКомандаToДоходитТолькоНазванномуАгенту(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to mbp-claude посмотри ветку",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло названному агенту")

	got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика адресата: %v", err)
	}
	if !strings.Contains(got[0].Message.Body, "посмотри ветку") {
		t.Errorf("тело письма %q — команда не отрезана или текст потерян", got[0].Message.Body)
	}
	if strings.Contains(got[0].Message.Body, "/to") {
		t.Errorf("тело письма %q содержит саму команду", got[0].Message.Body)
	}
	if len(got[0].Message.To) != 1 || got[0].Message.To[0] != "mbp-claude" {
		t.Errorf("адресаты письма %v, ожидался ровно один mbp-claude", got[0].Message.To)
	}

	// Даём времени уйти лишнему, если оно собиралось.
	time.Sleep(time.Second)

	for _, посторонний := range []string{"pi-claude", "pi-codex"} {
		box, err := bus.Inbox(ctx, conn.JS(), посторонний, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", посторонний, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил %d писем — адресная команда разбудила посторонних", посторонний, len(box))
		}
	}
}

// Неизвестное имя не превращается в рассылку всем живым.
//
// Это самый опасный отказ: команда просила «только одному», а прежнее
// поведение моста для неузнанного адреса — разослать всем, кто в сети.
func TestКомандаToНеизвестномуАгентуНикогоНеБудит(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to pi-clod опечатка в имени",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	// Человеку обязаны ответить — молчание неотличимо от потери.
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил объяснение")

	posts, _, _ := poster.snapshot()
	if !strings.Contains(posts[0], "pi-claude") {
		t.Errorf("в подсказке нет списка живых агентов: %q", posts[0])
	}
	if strings.Contains(posts[0], "pi-clod") {
		t.Errorf("подсказка возвращает в чат введённое человеком имя: %q", posts[0])
	}

	time.Sleep(time.Second)

	// Проверяются и живые, и сам несуществующий адрес: без второго теста
	// снятие проверки реестра осталось бы незамеченным — письмо ушло бы в
	// ящик, которого никто не читает, а ящики живых так и остались бы пусты.
	for _, агент := range []string{"pi-claude", "mbp-claude", "pi-codex", "pi-clod"} {
		box, err := bus.Inbox(ctx, conn.JS(), агент, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", агент, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил письмо по неизвестному адресату — команда обернулась веером", агент)
		}
	}
}

// `/to` без текста письма не создаёт, но и не молчит.
func TestКомандаToБезТекстаОбъясняетФормат(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to pi-claude",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил формат команды")

	time.Sleep(500 * time.Millisecond)

	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("пустая команда создала %d писем", len(box))
	}
}

// `/to` в ответе на пост остаётся в том же разговоре.
//
// Адресат берётся из команды, а нитка — из маршрута поста: иначе ответ
// человека оторвётся от обсуждения, которое он в этот момент читает.
func TestКомандаToВОтветеНаПостОстаётсяВРазговоре(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.PutRoute(ctx, "-1001", 10, Route{
		ThreadID: "разговор-о-ветке", Project: "mesh-mail",
		Participants: []string{"pi-claude", "pi-codex"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:             "/to mbp-claude глянь этот же коммит",
			Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
			ReplyToMessageID: 10, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "адресное письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "разговор-о-ветке" {
		t.Fatalf("разговор %q, ожидался «разговор-о-ветке» из маршрута поста", got[0].Message.ThreadID)
	}

	// Участники поста, которых человек не назвал, письма не получают:
	// адресная команда сильнее контекста разговора.
	time.Sleep(time.Second)
	for _, участник := range []string{"pi-claude", "pi-codex"} {
		box, err := bus.Inbox(ctx, conn.JS(), участник, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", участник, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил письмо, хотя человек адресовал его одному mbp-claude", участник)
		}
	}
}

// Пустой реестр — это «мост только что поднялся», а не «агента нет».
//
// Реестр наполняется только подпиской на визитки, а они приходят раз в
// минуту: после рестарта моста живая сеть выглядит пустой. Ответ «такого
// агента нет» был бы уверенной неправдой, и человек пошёл бы искать
// несуществующую беду.
func TestКомандаToПриПустомРеестреНеГоворитЧтоАгентаНет(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to pi-codex срочный вопрос",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	// Реестр пуст: визитки ещё не пришли.
	intake := NewIntake(conn.JS(), store, updater, живойРеестр(), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил объяснение")

	posts, _, _ := poster.snapshot()
	if !strings.Contains(posts[0], "минуту") {
		t.Errorf("ответ не объясняет, что визитки ещё не пришли: %q", posts[0])
	}
	if strings.Contains(posts[0], "Не нашёл такого агента") {
		t.Errorf("пустой реестр выдан за отсутствие агента: %q", posts[0])
	}
}

// `/to` вообще без аргументов объясняет формат и не создаёт письма.
func TestКомандаToБезАргументовОбъясняетФормат(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил формат команды")

	posts, _, _ := poster.snapshot()
	if !strings.Contains(posts[0], "/to") {
		t.Errorf("подсказка не показывает формат команды: %q", posts[0])
	}

	time.Sleep(500 * time.Millisecond)
	for _, агент := range []string{"pi-claude", "mbp-claude"} {
		box, err := bus.Inbox(ctx, conn.JS(), агент, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", агент, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил письмо от команды без аргументов", агент)
		}
	}
}

// Команда с суффиксом чужого бота письмом не становится.
//
// MVP принимает только точное `/to`: собственного имени мост здесь не знает,
// а `/to@чужой_бот` адресован не нам.
func TestКомандаСЧужимСуффиксомНеСтановитсяПисьмом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to@другой_бот pi-claude текст",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 13}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	// Имя бота ОБЯЗАТЕЛЬНО задать: без него команда отбрасывается веткой
	// «имени не знаю», и тест был бы зелёным по другой причине, чем та, ради
	// которой написан. В бою Run имя проставляет всегда, и проверяться должно
	// именно сравнение суффикса.
	intake.SetBotUsername("наш_бот")
	go func() { _ = intake.Run(ctx) }()

	time.Sleep(time.Second)

	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("команда чужому боту создала %d писем", len(box))
	}
}

// Пустой суффикс `/to@` командой не считается.
func TestКомандаСПустымСуффиксомНеСтановитсяПисьмом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to@ pi-claude текст",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 4}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	intake.SetBotUsername("наш_бот")
	go func() { _ = intake.Run(ctx) }()

	time.Sleep(time.Second)

	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("команда с пустым суффиксом создала %d писем", len(box))
	}
}

// Чужая команда с аргументами не превращается в адресное письмо.
//
// Контроль к поимённому исключению: `/start` умеет носить параметр (deep
// link), и «пропускать любую команду как адресную» выглядело бы работающим
// на `/start` без аргументов. Здесь аргументы есть, и первый из них — имя
// живого агента, то есть ровно то, что приняла бы ослабленная проверка.
func TestЧужаяКомандаСАргументамиНеСтановитсяПисьмом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/start pi-claude привет",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 6}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	time.Sleep(time.Second)

	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("команда /start с аргументами создала %d писем", len(box))
	}
}

// Команда с суффиксом СВОЕГО бота работает как обычная.
//
// В группах клиент Telegram подставляет суффикс сам, когда команду выбирают
// из подсказки, — это основной способ набора, а не редкость.
func TestКомандаСоСвоимСуффиксомДоставляется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to@наш_бот mbp-claude из подсказки",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 11}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetBotUsername("наш_бот")
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "команда со своим суффиксом доставлена")

	time.Sleep(500 * time.Millisecond)
	box, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("pi-claude получил %d писем — адресность потеряна", len(box))
	}
}

// Имя агента экранируется перед показом человеку.
//
// Ответ уходит с parse_mode HTML. Тема визитки удостоверяет, ЧЬЁ это имя, но
// ничего не говорит о его безопасности в разметке: один `<` превратит
// подсказку в отказ Telegram либо в чужую разметку.
func TestИмяАгентаЭкранируетсяВПодсказке(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:     "/to кого-нибудь текст",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "<b>злой</b>&агент"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил подсказку")

	posts, _, _ := poster.snapshot()
	if strings.Contains(posts[0], "<b>") {
		t.Errorf("имя агента ушло в чат неэкранированным: %q", posts[0])
	}
	if !strings.Contains(posts[0], "&lt;b&gt;") {
		t.Errorf("экранирование не выполнено: %q", posts[0])
	}
	if !strings.Contains(posts[0], "&amp;") {
		t.Errorf("амперсанд не экранирован: %q", posts[0])
	}
}

// Непрочитанный маршрут не начинает новый разговор молча.
//
// Ошибка хранилища уходит наверх, где письмо повторяют. Молчаливый новый
// разговор оторвал бы ответ человека от обсуждения, которое он читает, и
// заметить это в чате было бы нечем.
func TestОшибкаМаршрутаНеРвётРазговорАдресногоПисьма(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}

	// Запись маршрута с чужой версией: чтение обязано вернуть ошибку, а не
	// «маршрута нет». Кладём напрямую в бакет, минуя PutRoute.
	if _, err := store.routes.Put(ctx, routeKey("-1001", 10),
		[]byte(`{"v":2,"mesh_thread_id":"из-будущего"}`)); err != nil {
		t.Fatalf("подготовка испорченного маршрута: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:             "/to mbp-claude продолжаю обсуждение",
			Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
			ReplyToMessageID: 10, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	// Человеку скажут, что не доставлено, — после исчерпания повторов.
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек узнал о неудаче")

	box, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("письмо ушло в обход непрочитанного маршрута: %d шт.", len(box))
	}
}

// `/to` ответом на пост из СТАРОЙ темы остаётся в её разговоре.
//
// У таких постов маршрутов нет — они показаны до перехода на темы проектов.
// Обычный ответ находит разговор по самой теме, и адресный обязан вести себя
// так же: адресата берём из команды, нитку — из темы.
func TestКомандаToВСтаройТемеОстаётсяВРазговоре(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-legacy-77", Topic{
		MessageThreadID: 77,
		Participants:    []string{"pi-claude", "pi-codex"},
	}); err != nil {
		t.Fatalf("подготовка старой темы: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 77, FromID: 42, From: "tester",
			Text:             "/to mbp-claude присоединяйся",
			Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
			ReplyToMessageID: 999, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "адресное письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "thread-legacy-77" {
		t.Fatalf("разговор %q, ожидался thread-legacy-77 из старой темы", got[0].Message.ThreadID)
	}
}

// `/to` без ответа внутри старой темы наследует её разговор.
//
// До перехода на темы проектов обсуждение жило в своей теме, и сообщение в
// ней адресовалось участникам без всякого Reply. Команда меняет адресата, а
// нитку тема даёт по-прежнему — иначе письмо человека оторвётся от разговора,
// который он в этот момент читает.
func TestКомандаToБезОтветаВСтаройТемеНаследуетРазговор(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-legacy-88", Topic{
		MessageThreadID: 88,
		Participants:    []string{"pi-claude", "pi-codex"},
	}); err != nil {
		t.Fatalf("подготовка старой темы: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 88, FromID: 42, From: "tester",
			Text:     "/to mbp-claude без реплая, прямо в тему",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "адресное письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "thread-legacy-88" {
		t.Fatalf("разговор %q, ожидался thread-legacy-88 из темы", got[0].Message.ThreadID)
	}
	if len(got[0].Message.To) != 1 || got[0].Message.To[0] != "mbp-claude" {
		t.Fatalf("адресаты %v, ожидался один mbp-claude", got[0].Message.To)
	}

	// Участники темы, которых человек не назвал, письма не получают.
	time.Sleep(time.Second)
	for _, участник := range []string{"pi-claude", "pi-codex"} {
		box, err := bus.Inbox(ctx, conn.JS(), участник, bus.InboxOptions{})
		if err != nil {
			t.Fatalf("чтение ящика %s: %v", участник, err)
		}
		if len(box) != 0 {
			t.Fatalf("%s получил письмо, хотя человек адресовал одному", участник)
		}
	}
}

// Ответ человека на пост проекта показывается в теме ЭТОГО проекта.
//
// Проверяется сквозь обе половины моста: приём кладёт письму проект из
// маршрута поста, витрина по нему выбирает тему. Раньше проект терялся в
// приёме, и ответ человека — а следом и вся ветка, потому что Reply переносит
// проект дальше, — оседал в «Общее».
func TestОтветЧеловекаПоказываетсяВТемеПроекта(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	go func() { _ = NewShowcase(conn.JS(), store, poster, "-1001", true).Run(ctx) }()

	if err := store.PutRoute(ctx, "-1001", 10, Route{
		ThreadID: "thread-about-routes", Project: "mesh-mail",
		Participants: []string{"pi-claude"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text: "продолжаем по маршрутам", ReplyToMessageID: 10, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		_, _, topics := poster.snapshot()
		return len(topics) > 0
	}, "витрина завела тему для письма человека")

	_, _, topics := poster.snapshot()
	if topics[0] != "mesh-mail" {
		t.Fatalf("тема %q, ожидалась «mesh-mail» — проект не дошёл от маршрута до витрины", topics[0])
	}

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil || len(got) == 0 {
		t.Fatalf("письмо человека не дошло (err=%v)", err)
	}
	if got[0].Message.Project != "mesh-mail" {
		t.Fatalf("проект письма %q, ожидался «mesh-mail»", got[0].Message.Project)
	}
}

// Контроль к предыдущему: маршрут без проекта по-прежнему ведёт в «Общее».
//
// Без этой пары первый тест ничего не различает: «письмо попало в тему»
// одинаково верно и когда проект дошёл, и когда витрина завела тему по
// какой-нибудь другой причине.
func TestОтветПоМаршрутуБезПроектаОстаётсяВОбщем(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	go func() { _ = NewShowcase(conn.JS(), store, poster, "-1001", true).Run(ctx) }()

	if err := store.PutRoute(ctx, "-1001", 20, Route{
		ThreadID: "thread-no-project", Participants: []string{"pi-claude"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text: "тут проекта нет", ReplyToMessageID: 20, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		_, _, topics := poster.snapshot()
		return len(topics) > 0
	}, "витрина завела тему")

	_, _, topics := poster.snapshot()
	if topics[0] != tg.GeneralTopicName {
		t.Fatalf("тема %q, ожидалась «%s»: пустой проект не должен превращаться в проектную тему",
			topics[0], tg.GeneralTopicName)
	}
}

// Адресный `/to` ответом на пост проекта тоже получает проект.
func TestКомандаToВОтветеНаПостНаследуетПроект(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.PutRoute(ctx, "-1001", 30, Route{
		ThreadID: "thread-with-project", Project: "mesh-mail",
		Participants: []string{"pi-claude", "pi-codex"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester",
			Text:             "/to mbp-claude посмотри и ты",
			Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
			ReplyToMessageID: 30, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "адресное письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.Project != "mesh-mail" {
		t.Fatalf("проект адресного письма %q, ожидался «mesh-mail»", got[0].Message.Project)
	}
	if got[0].Message.ThreadID != "thread-with-project" {
		t.Fatalf("разговор %q — нитка потеряна", got[0].Message.ThreadID)
	}
}
