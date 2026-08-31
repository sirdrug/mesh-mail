package bridge

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// fakeUpdater отдаёт заготовленные обновления один раз, потом молчит.
type fakeUpdater struct {
	mu      sync.Mutex
	pending []tg.Update
}

func (u *fakeUpdater) GetUpdates(ctx context.Context, _ int, _ int) ([]tg.Update, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.pending) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		return nil, nil
	}
	out := u.pending
	u.pending = nil
	return out, nil
}

func TestПисьмоИзТемыИдётУчастникамТреда(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-1", Topic{
		MessageThreadID: 7,
		Participants:    []string{"pi-claude", "m1-codex"},
	}); err != nil {
		t.Fatalf("подготовка темы: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 7, Text: "уточните сроки", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо от человека в ящике участника")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if got[0].Message.From != HumanID {
		t.Errorf("отправитель %q, ожидался %q", got[0].Message.From, HumanID)
	}
	if got[0].Message.Body != "уточните сроки" {
		t.Errorf("тело %q", got[0].Message.Body)
	}
	if got[0].Message.ThreadID != "thread-1" {
		t.Errorf("тред %q — ответ человека выпал из разговора", got[0].Message.ThreadID)
	}
}

func TestСообщениеВОбщийПотокУходитВсемЖивым(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	now := time.Now().UTC()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: now})
	reg.Upsert(bus.Card{AgentID: "m1-codex", TTLSeconds: 180, AnnouncedAt: now})
	reg.Upsert(bus.Card{AgentID: "давно-ушедший", TTLSeconds: 60, AnnouncedAt: now.Add(-time.Hour)})

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 0, Text: "кто знает про routes-v2?", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	for _, id := range []string{"pi-claude", "m1-codex"} {
		waitFor(t, func() bool {
			got, err := bus.Inbox(ctx, conn.JS(), id, bus.InboxOptions{})
			return err == nil && len(got) > 0
		}, "вопрос дошёл до "+id)
	}

	// Протухшая визитка означает, что агента нет в сети: письмо ему пролежит
	// без толку, а человек будет ждать ответа.
	gone, err := bus.Inbox(ctx, conn.JS(), "давно-ушедший", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(gone) != 0 {
		t.Fatalf("письмо ушло агенту, которого нет в сети")
	}
}

func TestОтветНаОбщийВопросАдресуетсяЧеловеку(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "общий вопрос", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "вопрос у агента")

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	answer := got[0].Message.Reply("pi-claude", "я знаю")

	// Ответ на broadcast уходит человеку, а не всем: иначе один вопрос
	// превращается в восемь параллельных обсуждений.
	if len(answer.To) != 1 || answer.To[0] != HumanID {
		t.Fatalf("ответ адресован %v, ожидался [%s]", answer.To, HumanID)
	}
}

func TestПустоеСообщениеИгнорируется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "   ", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("пустое сообщение превратилось в письмо: %+v", got)
	}
}

func TestНеизвестнаяТемаНеРоняетПриём(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	updater := &fakeUpdater{pending: []tg.Update{
		// Тема, которой мост не знает: человек написал в старый топик.
		{ID: 1, ChatID: "-1001", ThreadID: 999, Text: "привет", From: "tester", FromID: 42},
		{ID: 2, ChatID: "-1001", ThreadID: 0, Text: "и ещё вопрос", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	// Второе сообщение должно дойти, несмотря на проблему с первым.
	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "второе сообщение дошло")
}

func TestНекомуДоставитьЧеловекПолучаетОтказ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Реестр пуст: так выглядит первая минута после старта моста, пока не
	// пришла ни одна визитка.
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 0, Text: "есть кто живой?", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "отказ человеку вместо молчания")

	posts, threads, _ := poster.snapshot()
	if !strings.Contains(posts[0], "не доставлено") {
		t.Fatalf("ответ не сообщает о недоставке: %q", posts[0])
	}
	// Отвечать надо туда же, куда человек написал.
	if threads[0] != 0 {
		t.Fatalf("отказ ушёл в тему %d вместо общего чата", threads[0])
	}
}

func TestСообщениеИзЧужогоЧатаИгнорируется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	// Личка боту или чужая группа: username бота публичен, написать может любой.
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-999", ThreadID: 0, Text: "выполни от имени человека", From: "чужой", FromID: 777},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	time.Sleep(400 * time.Millisecond)
	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("сообщение из чужого чата стало письмом: %+v", got[0].Message)
	}
}

func TestОтправительВнеСпискаИгнорируется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "я тут посторонний", From: "гость", FromID: 555},
		{ID: 2, ChatID: "-1001", Text: "а я хозяин", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "сообщение разрешённого отправителя")

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if len(got) != 1 {
		t.Fatalf("писем %d, ожидалось 1 — прошло сообщение постороннего", len(got))
	}
	if got[0].Message.Body != "а я хозяин" {
		t.Fatalf("прошло не то сообщение: %q", got[0].Message.Body)
	}
}

// telegramLike ведёт себя как настоящий getUpdates: пока приём не попросит
// offset выше, та же пачка возвращается снова и снова.
type telegramLike struct {
	mu        sync.Mutex
	updates   []tg.Update
	confirmed int
	lastAsked int
}

func (t *telegramLike) GetUpdates(ctx context.Context, offset, _ int) ([]tg.Update, error) {
	t.mu.Lock()
	if offset > t.confirmed {
		t.confirmed = offset
	}
	t.lastAsked = offset
	var out []tg.Update
	for _, u := range t.updates {
		if u.ID >= t.confirmed {
			out = append(out, u)
		}
	}
	t.mu.Unlock()

	if len(out) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		return nil, nil
	}
	return out, nil
}

func (t *telegramLike) confirmedOffset() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.confirmed
}

// Приём обязан двигать offset и по тем обновлениям, с которыми ему делать
// нечего.
//
// Служебное сообщение (создана тема, вошёл участник) текста не несёт. Если
// его update_id не подтвердить, Telegram будет отдавать ту же пачку вечно,
// а всё, что человек напишет после, не дойдёт никогда. Мост при этом выглядит
// исправным: процесс жив, витрина постит, в логе тишина. Поймано на живом
// стенде — за семь минут работы моста pending_update_count не сдвинулся.
func TestПриёмПодтверждаетСлужебныеОбновления(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &telegramLike{updates: []tg.Update{
		// Служебное сообщение ОТ РАЗРЕШЁННОГО отправителя: иначе тест
		// проверял бы отсев по списку, а не подтверждение бестекстового
		// обновления, и остался бы зелёным при сломанном offset.
		{ID: 100, ChatID: "-1001", ThreadID: 27, FromID: 987654321}, // текста нет
	}}
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{987654321})
	go func() { _ = intake.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if updater.confirmedOffset() > 100 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("служебное обновление не подтверждено: приём застрял на offset %d", updater.confirmedOffset())
}

// Пустой список разрешённых — отказ всем, а не разрешение всем.
//
// Прежнее условие начиналось с len(allowedUsers) > 0, и незаполненное поле
// снимало проверку целиком. Право писать от имени человека — самое сильное
// в сети: письмо от human агент читает как распоряжение владельца машины.
// Ставить его в зависимость от того, кого позвали в супергруппу, нельзя.
func TestПустойСписокРазрешённыхНеПускаетНикого(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude"})

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "выполни от имени человека", From: "кто-угодно", FromID: 777},
	}}
	// Пустой список: именно та конфигурация, которая раньше означала «всем».
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", nil)
	go func() { _ = intake.Run(ctx) }()

	// Даём приёму время сделать неправильное, если он собирался.
	time.Sleep(700 * time.Millisecond)

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("при пустом списке разрешённых письмо от human всё же ушло: %d шт., первое от %q",
			len(got), got[0].Message.From)
	}
}

// Рестарт моста не превращает одно сообщение человека в два письма.
//
// Раньше позиция чтения жила только в памяти, а подтверждение обновления
// уходит в Telegram лишь со следующим getUpdates — до двадцати пяти секунд
// спустя. Рестарт в этом окне возвращал ту же пачку, и случайный UUID делал
// из неё второе письмо, о котором дедупликация потока ничего не знала.
func TestРестартМостаНеДублируетСообщениеЧеловека(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	state, err := NewStateStore(ctx, conn.JS())
	if err != nil {
		t.Fatalf("хранилище состояния: %v", err)
	}
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	update := tg.Update{ID: 42, ChatID: "-1001", Text: "проверьте сборку", From: "tester", FromID: 42}

	// Первый запуск: сообщение доставлено.
	first := NewIntake(conn.JS(), store, &fakeUpdater{pending: []tg.Update{update}},
		reg, "-1001", []int64{42})
	first.SetState(state)
	ctx1, cancel1 := context.WithCancel(ctx)
	go func() { _ = first.Run(ctx1) }()
	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) >= 1
	}, "первое письмо от человека")
	cancel1()

	// Второй запуск: Telegram отдаёт то же обновление заново.
	second := NewIntake(conn.JS(), store, &fakeUpdater{pending: []tg.Update{update}},
		reg, "-1001", []int64{42})
	second.SetState(state)
	go func() { _ = second.Run(ctx) }()
	time.Sleep(1500 * time.Millisecond)

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{Limit: 10})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("одно сообщение человека стало %d письмами", len(got))
	}
}

// Позиция восстанавливается: мост не переспрашивает уже разобранное.
func TestПозицияЧтенияПереживаетРестарт(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	state, err := NewStateStore(ctx, conn.JS())
	if err != nil {
		t.Fatalf("хранилище состояния: %v", err)
	}
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	updater := &telegramLike{updates: []tg.Update{
		{ID: 77, ChatID: "-1001", Text: "первое", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	intake.SetState(state)
	ctx1, cancel1 := context.WithCancel(ctx)
	go func() { _ = intake.Run(ctx1) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) >= 1
	}, "письмо дошло")
	cancel1()

	saved, err := state.Offset(ctx)
	if err != nil {
		t.Fatalf("чтение позиции: %v", err)
	}
	if saved != 78 {
		t.Fatalf("сохранена позиция %d, ожидалась 78 (update_id + 1)", saved)
	}
}

// Идентификатор письма от человека выводится из обновления, а не случаен.
//
// Проверяется САМО ПИСЬМО, а не только функция. Прежняя версия теста вызывала
// telegramMessageID напрямую и оставалась зелёной, если перестать вызывать её
// в handle: письмо снова получало случайный идентификатор, а тест, судя по
// имени проверяющий именно письмо, ничего не замечал. Нашёл pi-claude
// мутацией `m.ID = telegramMessageID(update)` → `_ = telegramMessageID(...)`.
func TestИдентификаторПисьмаОтЧеловекаДетерминирован(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	update := tg.Update{ID: 42, ChatID: "-1001", Text: "текст", From: "tester", FromID: 42}
	intake := NewIntake(conn.JS(), store, &fakeUpdater{pending: []tg.Update{update}},
		reg, "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо от человека")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if want := telegramMessageID(update); got[0].Message.ID != want {
		t.Fatalf("письмо получило идентификатор %q вместо выведенного из обновления %q — "+
			"рестарт моста сделает из одного сообщения два письма",
			got[0].Message.ID, want)
	}

	first := telegramMessageID(update)
	if first != telegramMessageID(update) {
		t.Fatal("один и тот же update дал разные идентификаторы")
	}
	// Текст в ключ не входит: Telegram отдаёт повтор дословно, но полагаться
	// на это незачем — уникальна пара «чат + update_id».
	if other := telegramMessageID(tg.Update{ID: 43, ChatID: "-1001"}); other == first {
		t.Fatal("разные обновления получили один идентификатор")
	}
	if other := telegramMessageID(tg.Update{ID: 42, ChatID: "-1002"}); other == first {
		t.Fatal("обновления из разных чатов получили один идентификатор")
	}
}

// Обрыв связи с Telegram не превращается в тугой цикл запросов и логов.
func TestОбрывСвязиНеДаётТугогоЦикла(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	store, conn := newStore(t)
	failing := &alwaysFailing{}
	intake := NewIntake(conn.JS(), store, failing, bus.NewRegistry(), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	<-ctx.Done()

	// Без паузы запросы шли бы тысячами: сетевая ошибка возвращается
	// мгновенно. С растущей паузой их единицы.
	if calls := failing.count(); calls > 5 {
		t.Fatalf("за полторы секунды сделано %d попыток — это тугой цикл", calls)
	}
}

// alwaysFailing изображает недоступный Telegram: ошибка возвращается сразу.
type alwaysFailing struct {
	mu    sync.Mutex
	calls int
}

func (u *alwaysFailing) GetUpdates(_ context.Context, _ int, _ int) ([]tg.Update, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	return nil, errors.New("сети нет")
}

func (u *alwaysFailing) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

// Отметки старого формата в бакете тем не ломают поиск разговора.
//
// До Task A отметки «письмо показано» лежали рядом с темами, в bridge_topics,
// с приставкой posted-. Теперь они в своём бакете, и пропуск таких ключей
// выглядит мёртвым кодом — его предлагалось убрать как legacy.
//
// Убирать нельзя, и это проверено, а не предположено: обратный поиск темы
// читает КАЖДЫЙ ключ бакета и разбирает его как Topic. Значение старой
// отметки — "1", разбор падает, и вся функция возвращает ошибку. Человек,
// написавший в тему, получает «сообщение не доставлено» — при живом мосте
// и существующей теме.
//
// Бакет со старыми отметками — не гипотеза: мост уже поднимался на живом
// стенде с настоящей супергруппой, и отметки там писались именно так.
func TestСтараяОтметкаПоказаНеЛомаетПоискТемы(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	// Ключ ровно в том виде, в каком его оставляла прежняя версия витрины.
	if _, err := store.kv.Put(ctx, postedPrefix+"11111111-2222-3333-4444-555555555555",
		[]byte("1")); err != nil {
		t.Fatalf("подготовка старой отметки: %v", err)
	}
	if err := store.Put(ctx, "thread-1", Topic{
		MessageThreadID: 7,
		Participants:    []string{"pi-claude"},
	}); err != nil {
		t.Fatalf("подготовка темы: %v", err)
	}

	intake := &Intake{store: store}
	_, topic, found, err := intake.findByTopic(ctx, 7)
	if err != nil {
		t.Fatalf("старая отметка сломала поиск темы: %v", err)
	}
	if !found {
		t.Fatal("тема не найдена: старая отметка прервала обход бакета")
	}
	if topic.MessageThreadID != 7 {
		t.Fatalf("найдена тема %d вместо 7", topic.MessageThreadID)
	}
}

// Ответ человека в тему письма, отправленного самому себе, будит агента один раз.
//
// Сквозная проверка обеих защит сразу, от витрины до публикации. Путь такой:
// агент пишет письмо, где он сам среди получателей; витрина заводит под него
// тему и запоминает участников; человек отвечает в эту тему; мост берёт
// участников как список адресатов и публикует письмо от человека.
//
// Раньше на этом пути дубль появлялся дважды — в записи темы и в списке
// получателей, — а считать надо ПУБЛИКАЦИИ, а не письма в ящике: поток
// отбрасывает вторую копию по одинаковому Nats-Msg-Id, поэтому «в ящике одно
// письмо» было бы зелено и до починки. Сторож же подписан на тему напрямую и
// видит всё, что опубликовано.
func TestОтветЧеловекаВСвоюЖеТемуБудитОдинРаз(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)

	m := mail.New("pi-claude", []string{"pi-claude", "m1-codex"}, "отчёт", "тело")

	// 1. Разговор со СВОЕЙ темой — так выглядят обсуждения, начатые до
	// перехода на темы проектов. Проверяем именно этот путь: тест про число
	// пробуждений, а не про то, где заводится тема.
	if err := store.Put(ctx, m.ThreadID, Topic{
		MessageThreadID: 7,
		Participants:    m.Participants(),
	}); err != nil {
		t.Fatalf("подготовка темы разговора: %v", err)
	}

	poster := &flakyPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", true)
	go func() { _ = show.Run(ctx) }()

	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация письма: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) > 0
	}, "витрина показала письмо")

	topic, _, err := store.Get(ctx, m.ThreadID)
	if err != nil {
		t.Fatalf("чтение темы: %v", err)
	}

	// 2. Слушаем ящик так же, как сторож: до потока, а не после него.
	sub, err := conn.NC().SubscribeSync(bus.MailInboxFilter("pi-claude"))
	if err != nil {
		t.Fatalf("подписка: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := conn.NC().Flush(); err != nil {
		t.Fatalf("сброс: %v", err)
	}

	// 3. Человек отвечает в эту тему.
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: topic.MessageThreadID,
			Text: "принято", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	// 4. Считаем пробуждения. Первое обязано прийти и быть от человека.
	first, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("письмо от человека не пришло вовсе: %v", err)
	}
	if sender := bus.SenderFromSubject(first.Subject); sender != HumanID {
		t.Fatalf("отправитель в теме %q, ожидался %q", sender, HumanID)
	}

	// Второго быть не должно. Ждём столько же, сколько ждали первое: короткое
	// окно сделало бы тест зелёным просто потому, что мы не дождались дубля.
	if second, err := sub.NextMsg(5 * time.Second); err == nil {
		t.Fatalf("пришла вторая публикация того же ответа (тема %s) — сторож разбудит дважды",
			second.Subject)
	}
}

// команда — сокращение для обновления с разметкой bot_command.
func команда(id int, text string, offset int) tg.Update {
	return tg.Update{
		ID: id, ChatID: "-1001", Text: text, From: "tester", FromID: 42,
		Entities: []tg.Entity{{Type: "bot_command", Offset: offset, Length: len([]rune(text))}},
	}
}

// живойРеестр — один агент в сети, чтобы общему потоку было куда доставлять.
func живойРеестр(ids ...string) *bus.Registry {
	reg := bus.NewRegistry()
	now := time.Now().UTC()
	for _, id := range ids {
		reg.Upsert(bus.Card{AgentID: id, TTLSeconds: 180, AnnouncedAt: now})
	}
	return reg
}

// Команда боту письмом не становится и никого не будит.
//
// Найдено живой эксплуатацией: человек нажал «Запустить» в супергруппе, и
// `/start@agent_mesh_31115_bot` разошёлся письмом всем троим агентам, разбудив
// каждого и заведя в витрине тему со своим именем. Служебное действие в
// интерфейсе не должно попадать в почту.
//
// Второе обновление в этом же тесте не для полноты: оно доказывает, что
// отсеянная команда не заклинила приём. Отсев, при котором offset не
// двигается, дал бы затор — Telegram отдавал бы ту же пачку снова, и всё
// написанное человеком после неё не дошло бы вовсе. Проверка «письма нет»
// сама по себе зелена и при заторе, и при исправном отсеве.
func TestКомандаБотуНеСтановитсяПисьмом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		команда(1, "/start@agent_mesh_31115_bot", 0),
		{ID: 2, ChatID: "-1001", Text: "а теперь по делу", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "второе сообщение дошло — приём не заклинило")

	got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("писем %d, ожидалось одно: команда не должна была стать письмом", len(got))
	}
	if got[0].Message.Body != "а теперь по делу" {
		t.Fatalf("дошло %q, а команда должна была отсеяться", got[0].Message.Body)
	}
}

// Команда посреди фразы — обычный текст.
//
// «Напиши /start в бота» приходит с той же сущностью, но смещение у неё не
// нулевое. Фильтр «есть bot_command где угодно» съел бы это сообщение,
// пройдя проверку выше.
func TestКомандаПосредиФразыДоставляется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "чтобы поднять, напиши /start боту", From: "tester", FromID: 42,
			Entities: []tg.Entity{{Type: "bot_command", Offset: 21, Length: 6}}},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "сообщение с командой посреди фразы доставлено")
}

// Путь, начинающийся со слэша, — не команда.
//
// Разметки у него нет, и это единственное, чем он отличается от команды.
// Фильтр по первому символу съел бы его молча, а такие строки мы шлём друг
// другу постоянно.
func TestПутьСоСлэшемДоставляется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "/etc/nats/tls/privkey.pem", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "путь доставлен как обычный текст")

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
	if got[0].Message.Body != "/etc/nats/tls/privkey.pem" {
		t.Fatalf("тело %q — путь должен дойти неизменным", got[0].Message.Body)
	}
}

// Разметка другого типа в начале строки командой не является.
//
// Жирный текст, ссылка, упоминание — всё это сущности с нулевым смещением.
// Фильтр, смотрящий только на смещение, съел бы любое сообщение, начатое
// с выделенного слова.
func TestРазметкаДругогоТипаНеФильтруется(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", Text: "срочно: посмотрите логи", From: "tester", FromID: 42,
			Entities: []tg.Entity{{Type: "bold", Offset: 0, Length: 6}}},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "pi-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "сообщение с выделением доставлено")
}

// failingUpdater отдаёт заданную ошибку и считает обращения.
//
// Счётчик здесь главное: «Run вернулся» не отличает выход от паузы, а
// различает их только отсутствие ВТОРОГО запроса обновлений.
type failingUpdater struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (u *failingUpdater) GetUpdates(ctx context.Context, _ int, _ int) ([]tg.Update, error) {
	u.mu.Lock()
	u.calls++
	u.mu.Unlock()
	return nil, u.err
}

func (u *failingUpdater) attempts() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

// Конфликт экземпляров завершает приём, а не уходит в бесконечный повтор.
//
// Два моста с одним токеном вытесняют друг друга по кругу, и каждое
// вытеснение теряет часть сообщений человека: пачку получает тот, кто успел.
// В журнале это выглядит как одна строка про недоступность — то есть
// неотличимо от моргнувшей сети, и процесс при этом жив и «работает».
func TestКонфликтЭкземпляровЗавершаетПриём(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, conn := newStore(t)
	updater := &failingUpdater{
		err: &tg.APIError{
			Method: "getUpdates", Code: 409,
			Description: "Conflict: terminated by other getUpdates request",
		},
	}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})

	err := intake.Run(ctx)
	if !errors.Is(err, ErrPollingConflict) {
		t.Fatalf("Run вернул %v, ожидался ErrPollingConflict", err)
	}
	// Описание Telegram должно остаться видимым: по нему человек поймёт, что
	// произошло, не заглядывая в код.
	if !strings.Contains(err.Error(), "terminated by other getUpdates") {
		t.Errorf("описание Telegram потеряно при обёртке: %v", err)
	}
	// Классификация и диагностика нужны обе сразу: снаружи по sentinel
	// решают, что делать с процессом, а Code и Description объясняют,
	// почему. Обёртка обязана сохранить оба пути.
	var apiErr *tg.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("исходный tg.APIError не достаётся из цепочки: %v", err)
	}
	if apiErr.Code != 409 || apiErr.Method != "getUpdates" {
		t.Errorf("достался не тот APIError: %+v", apiErr)
	}
	if n := updater.attempts(); n != 1 {
		t.Fatalf("обращений к getUpdates %d, ожидалось одно: 409 ушёл в повтор", n)
	}
}

// Обычная беда по-прежнему повторяется.
//
// Контроль к предыдущему тесту: без него «завершается на ошибке» было бы
// верно и для сети, а мост, падающий на каждом пятисотом ответе, — это
// поломка, а не защита.
func TestВременнаяОшибкаПродолжаетПовторы(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	updater := &failingUpdater{
		err: &tg.APIError{Method: "getUpdates", Code: 500, Description: "Internal Server Error"},
	}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})

	done := make(chan error, 1)
	go func() { done <- intake.Run(ctx) }()

	waitFor(t, func() bool { return updater.attempts() >= 2 }, "приём повторил запрос обновлений")

	select {
	case err := <-done:
		t.Fatalf("Run завершился на временной ошибке: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("после отмены Run вернул %v, ожидался nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулся после отмены")
	}
}

// Отмена контекста остаётся штатным завершением, а не конфликтом.
func TestОтменаКонтекстаНеПутаетсяСКонфликтом(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	store, conn := newStore(t)
	updater := &fakeUpdater{}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})

	done := make(chan error, 1)
	go func() { done <- intake.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run вернул %v, ожидался nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулся после отмены")
	}
}

// Код 409 от ДРУГОЙ операции конфликтом опроса не считается.
//
// У отправки сообщения 409 означает совсем иное, и приём из-за него
// останавливаться не должен. Проверяется на настоящем пути: человек пишет в
// тему без разговора, мост отвечает подсказкой, и подсказка получает 409.
func TestЧужой409НеОстанавливаетПриём(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &conflictPoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 1, ChatID: "-1001", ThreadID: 55, FromID: 42, From: "tester", Text: "реплика в тему"},
	}}
	intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)

	done := make(chan error, 1)
	go func() { done <- intake.Run(ctx) }()

	waitFor(t, func() bool { return poster.attempts() > 0 }, "мост попытался ответить человеку")

	// Даём приёму время упасть, если он собирался.
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("приём завершился из-за 409 при отправке: %v", err)
	default:
	}
}

// conflictPoster отвечает 409 на отправку — тем же кодом, что конфликт опроса.
type conflictPoster struct {
	mu    sync.Mutex
	calls int
}

func (p *conflictPoster) Send(_ context.Context, _ int, _ tg.Post) ([]int, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return nil, &tg.APIError{Method: "sendMessage", Code: 409, Description: "Conflict"}
}

func (p *conflictPoster) CreateTopic(_ context.Context, _ string) (int, error) {
	return 0, &tg.APIError{Method: "createForumTopic", Code: 409, Description: "Conflict"}
}

func (p *conflictPoster) attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// toВТеме — короткая заготовка команды `/to` без ответа на пост.
func toВТеме(messageThreadID int, text string) tg.Update {
	return tg.Update{
		ID: 1, ChatID: "-1001", ThreadID: messageThreadID, FromID: 42, From: "tester",
		Text:     text,
		Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
	}
}

// Команда в теме проекта с известным именем наследует это имя.
func TestКомандаВТемеПроектаНаследуетИмя(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.PutProjectTopic(ctx, "mesh-mail", 90); err != nil {
		t.Fatalf("подготовка темы проекта: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{toВТеме(90, "/to mbp-claude что там с веткой")}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "адресное письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.Project != "mesh-mail" {
		t.Fatalf("проект письма %q, ожидался «mesh-mail» из темы", got[0].Message.Project)
	}

	// Имя известно — объяснять нечего.
	time.Sleep(300 * time.Millisecond)
	if posts, _, _ := poster.snapshot(); len(posts) != 0 {
		t.Fatalf("мост объяснился без причины: %q", posts)
	}
}

// Тема проекта БЕЗ записанного имени: письмо без проекта плюс объяснение.
//
// Это состояние старых тем, заведённых до того, как имена стали храниться.
// Молчать здесь нельзя: человек писал в тему проекта, а письмо уйдёт в
// «Общее», и без объяснения он будет искать его там, где его нет.
func TestТемаБезИмениПроектаОбъясняетсяЧеловеку(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Запись прежнего образца: вид и номер темы есть, имени нет.
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		Version: 1, Kind: KindProjectTopic, MessageThreadID: 91,
	}); err != nil {
		t.Fatalf("подготовка старой записи: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{toВТеме(91, "/to mbp-claude вопрос")}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек получил объяснение")

	posts, _, _ := poster.snapshot()
	if !strings.Contains(posts[0], "Общее") {
		t.Errorf("объяснение не говорит, куда уйдёт письмо: %q", posts[0])
	}

	got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if err != nil || len(got) == 0 {
		t.Fatalf("письмо не дошло (err=%v): отказ был бы регрессом", err)
	}
	if got[0].Message.Project != "" {
		t.Fatalf("проект письма %q, ожидался пустой", got[0].Message.Project)
	}
}

// Тема «Общего» — известное пустое имя, а не неизвестное.
//
// Пара к предыдущему тесту: порознь они зелены при реализации, которая
// считает пустое имя всегда известным или всегда неизвестным.
func TestТемаОбщегоНеТребуетОбъяснений(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.PutProjectTopic(ctx, "", 92); err != nil {
		t.Fatalf("подготовка темы «Общего»: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{toВТеме(92, "/to mbp-claude вне проектов")}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло")

	time.Sleep(300 * time.Millisecond)
	if posts, _, _ := poster.snapshot(); len(posts) != 0 {
		t.Fatalf("про известное пустое имя объяснять нечего, а мост сказал: %q", posts)
	}
}

// Тема, не отведённая проекту, объяснений не получает.
//
// Говорить «проект темы неизвестен» про чужую тему бессмысленно: там нет
// никакого проекта, и человеку это ничего не сообщает.
func TestЧужаяТемаНеПолучаетОбъясненийПроПроект(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{toВТеме(93, "/to mbp-claude в неизвестной теме")}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло")

	time.Sleep(300 * time.Millisecond)
	if posts, _, _ := poster.snapshot(); len(posts) != 0 {
		t.Fatalf("мост объяснился про проект чужой темы: %q", posts)
	}
}

// Отказ хранилища при поиске проекта не превращается в пустой проект.
//
// Проверяется не «письмо ушло в Общее», а что письма НЕТ вовсе: тихая
// подмена проекта неотличима от исправной работы, а отсутствие письма видно.
func TestОтказПоискаПроектаНеПубликуетПисьмо(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	// Проектная запись из будущего: чтение обязано вернуть ошибку.
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		Version: 99, Kind: KindProjectTopic, MessageThreadID: 94,
	}); err != nil {
		t.Fatalf("подготовка записи: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{toВТеме(94, "/to mbp-claude вопрос")}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	// Человеку скажут о неудаче после исчерпания повторов.
	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) > 0
	}, "человек узнал о неудаче")

	box, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(box) != 0 {
		t.Fatalf("письмо ушло в обход нечитаемой записи: %d шт.", len(box))
	}
}

// Объяснение про неизвестный проект приходит не чаще раза в час на тему.
func TestОбъяснениеПроПроектНеЧащеРазаВЧас(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		Version: 1, Kind: KindProjectTopic, MessageThreadID: 95,
	}); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		toВТеме(95, "/to mbp-claude раз"),
		{
			ID: 2, ChatID: "-1001", ThreadID: 95, FromID: 42, From: "tester",
			Text:     "/to mbp-claude два",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) >= 2
	}, "оба письма дошли")

	time.Sleep(300 * time.Millisecond)
	posts, _, _ := poster.snapshot()
	if len(posts) != 1 {
		t.Fatalf("объяснений %d, ожидалось одно на тему за час", len(posts))
	}
}

// Счётчик объяснения про проект не общий с подсказкой про ответ.
//
// Иначе два разных объяснения глушат друг друга: человек получит то, чей
// вызов случился раньше, и не получит второго.
func TestДваОбъясненияНеГлушатДругДруга(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		Version: 1, Kind: KindProjectTopic, MessageThreadID: 96,
	}); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	poster := &fakePoster{}
	updater := &fakeUpdater{pending: []tg.Update{
		// Обычная реплика в тему проекта: адресата нет, идёт подсказка guide.
		{ID: 1, ChatID: "-1001", ThreadID: 96, FromID: 42, From: "tester", Text: "просто реплика"},
		// Команда в той же теме: имя проекта неизвестно, идёт своё объяснение.
		{
			ID: 2, ChatID: "-1001", ThreadID: 96, FromID: 42, From: "tester",
			Text:     "/to mbp-claude вопрос",
			Entities: []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	intake.SetPoster(poster)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		posts, _, _ := poster.snapshot()
		return len(posts) >= 2
	}, "пришли оба объяснения")

	posts, _, _ := poster.snapshot()
	var проПроект, проОтвет bool
	for _, p := range posts {
		if strings.Contains(p, "Проект этой темы") {
			проПроект = true
		}
		if strings.Contains(p, "к какому разговору") {
			проОтвет = true
		}
	}
	if !проПроект || !проОтвет {
		t.Fatalf("одно из объяснений проглочено: проект=%v ответ=%v, посты=%q", проПроект, проОтвет, posts)
	}
}

// Ответ на пост без маршрута в теме проекта: письмо получает проект темы.
//
// Так выглядят посты, показанные до появления маршрутов: маршрута нет, а тема
// проектная. Раньше такое письмо уходило в «Общее» молча.
func TestОтветНаСтарыйПостВТемеПроектаБерётЕёПроект(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.PutProjectTopic(ctx, "mesh-mail", 97); err != nil {
		t.Fatalf("подготовка темы проекта: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 97, FromID: 42, From: "tester",
			Text:             "/to mbp-claude по старому посту",
			Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
			ReplyToMessageID: 555, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.Project != "mesh-mail" {
		t.Fatalf("проект письма %q, ожидался «mesh-mail» из темы", got[0].Message.Project)
	}
}

// Тема разговора находится и после промаха проектного поиска.
//
// Контроль к предыдущему тесту. Виды тем взаимоисключающи: один номер темы не
// бывает сразу проектным и разговорным, поэтому речь не о приоритете, а о
// порядке двух поисков. Проектный идёт первым, потому что узкий; разговор
// находится следом, и нитка у него своя.
func TestТемаРазговораНаходитсяПослеПромахаПроекта(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.Put(ctx, "thread-legacy-98", Topic{
		MessageThreadID: 98,
		Participants:    []string{"pi-claude", "pi-codex"},
	}); err != nil {
		t.Fatalf("подготовка старой темы: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{
			ID: 1, ChatID: "-1001", ThreadID: 98, FromID: 42, From: "tester",
			Text:             "/to mbp-claude в старом обсуждении",
			Entities:         []tg.Entity{{Type: "bot_command", Offset: 0, Length: 3}},
			ReplyToMessageID: 556, ReplyToBot: true,
		},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude", "pi-codex"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.ThreadID != "thread-legacy-98" {
		t.Fatalf("разговор %q, ожидался thread-legacy-98: тема разговора не найдена после промаха проекта",
			got[0].Message.ThreadID)
	}
}

// Битая запись чужого разговора не мешает команде в теме проекта.
//
// Ради этого поиск проекта идёт первым: он читает только ключи с приставкой
// `project-`, а поиск разговора перебирает бакет целиком и падает на любой
// повреждённой записи — в том числе не имеющей к теме отношения.
//
// Найдено ревью mbp-claude: изоляция была сделана в хранилище, но на этом
// пути не срабатывала, потому что широкий поиск вызывался первым.
func TestБитаяЗаписьНеМешаетКомандеВТемеПроекта(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if _, err := store.kv.Put(ctx, "thread-broken-xyz", []byte("не json вовсе")); err != nil {
		t.Fatalf("подготовка битой записи: %v", err)
	}
	if err := store.PutProjectTopic(ctx, "mesh-mail", 99); err != nil {
		t.Fatalf("подготовка темы проекта: %v", err)
	}

	updater := &fakeUpdater{pending: []tg.Update{toВТеме(99, "/to mbp-claude вопрос")}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "mbp-claude"), "-1001", []int64{42})
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		got, err := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
		return err == nil && len(got) > 0
	}, "письмо дошло, несмотря на битую запись разговора")

	got, _ := bus.Inbox(ctx, conn.JS(), "mbp-claude", bus.InboxOptions{})
	if got[0].Message.Project != "mesh-mail" {
		t.Fatalf("проект письма %q, ожидался «mesh-mail»", got[0].Message.Project)
	}
}

// наблюдаемаяПозиция — хранилище позиции чтения, замечающее МОМЕНТ записи.
//
// Двойник нужен именно такой: порядок «сначала обработали, потом
// подтвердили» проверить постфактум нельзя. Конечное состояние у правильного
// и у переставленного порядка одинаковое — разница видна только в момент
// самого вызова, и увидеть её может лишь тот, кого вызывают.
type наблюдаемаяПозиция struct {
	mu sync.Mutex
	// вижуПисьмо — доехало ли письмо к моменту очередного сохранения.
	вижуПисьмо []bool
	// отменён — был ли контекст уже отменён в момент сохранения.
	отменён  []bool
	спросить func() bool
	offset   int
}

func (н *наблюдаемаяПозиция) Offset(context.Context) (int, error) {
	н.mu.Lock()
	defer н.mu.Unlock()
	return н.offset, nil
}

func (н *наблюдаемаяПозиция) SetOffset(ctx context.Context, offset int) error {
	есть := false
	if н.спросить != nil {
		есть = н.спросить()
	}

	н.mu.Lock()
	defer н.mu.Unlock()
	н.вижуПисьмо = append(н.вижуПисьмо, есть)
	н.отменён = append(н.отменён, ctx.Err() != nil)
	н.offset = offset
	return nil
}

func (н *наблюдаемаяПозиция) снимок() (вижу, отменён []bool) {
	н.mu.Lock()
	defer н.mu.Unlock()
	return append([]bool(nil), н.вижуПисьмо...), append([]bool(nil), н.отменён...)
}

// Позиция чтения сохраняется ПОСЛЕ доставки, а не до неё.
//
// Это центральная причина, по которой long polling остался нашим: библиотека
// сдвигает позицию до передачи обновления обработчику, и падение в этом окне
// теряет сообщение человека навсегда. Тесты этот порядок не стерегли вовсе —
// перестановка двух строк оставляла весь пакет зелёным.
//
// Разница между «потеряли» и «показали дважды» несимметрична: дубль человек
// заметит и повторит мысль, потерю не заметит никто.
func TestПозицияСохраняетсяПослеДоставки(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	позиция := &наблюдаемаяПозиция{
		спросить: func() bool {
			got, err := bus.Inbox(context.Background(), conn.JS(), "pi-claude", bus.InboxOptions{})
			return err == nil && len(got) > 0
		},
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 42, ChatID: "-1001", Text: "проверьте сборку", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	intake.setState(позиция)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		вижу, _ := позиция.снимок()
		return len(вижу) > 0
	}, "позиция чтения сохранена хотя бы раз")

	вижу, _ := позиция.снимок()
	if !вижу[0] {
		t.Fatal("позиция подтверждена раньше, чем письмо доехало: падение в этом окне потеряет сообщение человека")
	}
}

// Провал доставки не отменяет порядка: сначала обработка, потом позиция.
//
// Ветка провала отдельная и ведёт себя иначе — позиция двигается даже на
// неудаче, иначе одно проблемное сообщение заклинит приём навсегда. Но
// ДВИГАЕТСЯ ОНА ПОСЛЕ: человеку успевают сказать, что письмо не дошло.
//
// Пустой реестр даёт ровно этот путь: доставить некому, попытки исчерпываются,
// человек получает предупреждение.
func TestПриПровалеДоставкиПозицияСохраняетсяПослеОтветаЧеловеку(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &fakePoster{}

	позиция := &наблюдаемаяПозиция{
		спросить: func() bool {
			posts, _, _ := poster.snapshot()
			return len(posts) > 0
		},
	}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 7, ChatID: "-1001", Text: "есть кто живой?", From: "tester", FromID: 42},
	}}
	// Реестр пуст: так выглядит первая минута после старта моста.
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{42})
	intake.SetPoster(poster)
	intake.setState(позиция)
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		вижу, _ := позиция.снимок()
		return len(вижу) > 0
	}, "позиция чтения сохранена после провала доставки")

	вижу, _ := позиция.снимок()
	if !вижу[0] {
		t.Fatal("позиция подтверждена раньше, чем человеку сказали о недоставке")
	}
}

// Отмена во время доставки не даёт сохранению обогнать себя.
//
// Мост останавливают в любой момент, в том числе посреди обработки. Порядок
// должен держаться и здесь: попытка сохранить позицию случается уже после
// выхода из доставки, то есть с контекстом, который к тому времени отменён.
// Иначе остановка моста подтверждала бы обновление, обработку которого сама
// же и прервала.
//
// Что именно доказывает тест, стоит сказать точно: двойник фиксирует ВЫЗОВ
// SetOffset и состояние контекста в этот момент. Настоящий StateStore с
// отменённым контекстом значение, скорее всего, не запишет вовсе — и это
// безопаснее преждевременного подтверждения, потому что непрочитанное
// обновление придёт снова, а подтверждённое не придёт никогда.
//
// Отмена вносится детерминированно — двойник отправителя гасит контекст в
// момент, когда доставка ещё идёт.
func TestОтменаВоВремяДоставкиНеОпережаетСохранение(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	гасящий := &гасящийPoster{}
	ctxRun, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	гасящий.гасить = cancelRun

	позиция := &наблюдаемаяПозиция{}

	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 9, ChatID: "-1001", Text: "есть кто живой?", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{42})
	intake.SetPoster(гасящий)
	intake.setState(позиция)
	go func() { _ = intake.Run(ctxRun) }()

	waitFor(t, func() bool {
		_, отменён := позиция.снимок()
		return len(отменён) > 0
	}, "позиция сохранена после отменённой доставки")

	_, отменён := позиция.снимок()
	if !отменён[0] {
		t.Fatal("позиция сохранена с живым контекстом — сохранение обогнало отмену доставки")
	}
}

// гасящийPoster отменяет контекст прямо во время доставки.
type гасящийPoster struct {
	mu     sync.Mutex
	гасить func()
	раз    int
}

func (г *гасящийPoster) Send(_ context.Context, _ int, _ tg.Post) ([]int, error) {
	г.mu.Lock()
	г.раз++
	гасить := г.гасить
	г.mu.Unlock()
	if гасить != nil {
		гасить()
	}
	return []int{1}, nil
}

func (г *гасящийPoster) CreateTopic(context.Context, string) (int, error) { return 1, nil }

// журналВПамяти — перехват строк журнала для проверки того, что мост
// записывает о своём решении.
type журналВПамяти struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (ж *журналВПамяти) Write(p []byte) (int, error) {
	ж.mu.Lock()
	defer ж.mu.Unlock()
	return ж.buf.Write(p)
}

func (ж *журналВПамяти) строки() string {
	ж.mu.Lock()
	defer ж.mu.Unlock()
	return ж.buf.String()
}

// Мост записывает, КОМУ и ПОЧЕМУ адресовал письмо человека.
//
// Дефект, ради которого тест написан, был не в доставке, а в невозможности
// разобраться: человек написал в общий чат, ожидая всех, письмо ушло двоим,
// и восстановить постфактум, кого мост считал живыми, оказалось нечем.
// Решение живёт в памяти процесса и нигде не сохраняется — ни в журнале, ни
// в KV. Разбор упирался в рассуждение о коде.
func TestЖурналЗаписываетМаршрутИАдресатов(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})
	reg.Upsert(bus.Card{AgentID: "pi-codex", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	журнал := &журналВПамяти{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 42, ChatID: "-1001", Text: "секретное содержимое письма", From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	intake.setLogger(log.New(журнал, "", 0))
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		return strings.Contains(журнал.строки(), "маршрут обновления")
	}, "мост записал маршрут в журнал")

	строки := журнал.строки()
	if !strings.Contains(строки, "источник=alive") {
		t.Errorf("в журнале нет источника выбора адресатов:\n%s", строки)
	}
	// Оба адресата поимённо: строка «адресаты=[pi-claude]» при двух живых
	// узлах — это ровно тот случай, который мы разбирали, и журнал обязан
	// его показывать, а не скрывать за количеством.
	if !strings.Contains(строки, "pi-claude") || !strings.Contains(строки, "pi-codex") {
		t.Errorf("в журнале не все адресаты:\n%s", строки)
	}
}

// Журнал маршрута не тащит содержимое письма.
//
// Строка пишется на каждое сообщение человека, то есть попадает в systemd и
// живёт там дольше самого разговора. Текст пришёл из сети и в журнале не
// отвечает ни на один вопрос: «кому адресовано и почему» решается
// идентификаторами.
//
// Проверка отдельная и обязательная: добавить в строку текст «для удобства
// отладки» — первое, что приходит в голову следующему автору.
func TestЖурналМаршрутаНеНесётТекстаСообщения(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const секрет = "пароль-от-хранилища-и-прочая-тайна"

	store, conn := newStore(t)
	reg := bus.NewRegistry()
	reg.Upsert(bus.Card{AgentID: "pi-claude", TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	журнал := &журналВПамяти{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 7, ChatID: "-1001", Text: секрет, From: "tester", FromID: 42},
	}}
	intake := NewIntake(conn.JS(), store, updater, reg, "-1001", []int64{42})
	intake.setLogger(log.New(журнал, "", 0))
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		return strings.Contains(журнал.строки(), "маршрут обновления")
	}, "мост записал маршрут в журнал")

	строки := журнал.строки()
	if strings.Contains(строки, секрет) {
		t.Errorf("текст сообщения человека попал в журнал:\n%s", строки)
	}
	// Имя автора в телеграме — тоже данные из сети, и в решении о маршруте
	// оно не участвует.
	if strings.Contains(строки, "tester") {
		t.Errorf("имя автора в телеграме попало в журнал:\n%s", строки)
	}
}

// Маршрут записывается и тогда, когда адресатов не нашлось.
//
// Это главный случай ради которого строка заводится: «письмо ушло всем» и
// «письмо не ушло никому» снаружи выглядят одинаково — тишиной. Публикации
// здесь нет вовсе, значит строка обязана появляться ДО неё, а не после;
// иначе в самом интересном случае журнал промолчит.
func TestЖурналЗаписываетМаршрутДажеБезАдресатов(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	журнал := &журналВПамяти{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 9, ChatID: "-1001", Text: "есть кто живой?", From: "tester", FromID: 42},
	}}
	// Реестр пуст: так выглядит первая минута после старта моста и так же
	// выглядит сеть, где ни один узел не объявляет присутствие.
	intake := NewIntake(conn.JS(), store, updater, bus.NewRegistry(), "-1001", []int64{42})
	intake.SetPoster(&fakePoster{})
	intake.setLogger(log.New(журнал, "", 0))
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		return strings.Contains(журнал.строки(), "маршрут обновления")
	}, "мост записал маршрут даже без адресатов")

	строки := журнал.строки()
	if !strings.Contains(строки, "источник=alive") {
		t.Errorf("источник не записан при пустом списке:\n%s", строки)
	}
	if !strings.Contains(строки, "адресаты=[]") {
		t.Errorf("пустой список адресатов не виден в журнале:\n%s", строки)
	}
}

// Источник маршрута различается в журнале: пост, тема, общий чат.
//
// Проверка не косметическая. Строка нужна затем, чтобы отличать «письмо ушло
// участникам разговора» от «письмо ушло всем, кого мост считал живыми»: в
// первом случае отсутствие узла нормально, во втором — дефект. Если источник
// не проставлен в какой-то из веток, журнал будет уверенно врать именно там.
func TestЖурналРазличаетИсточникМаршрута(t *testing.T) {
	t.Run("ответ на пост — источник post", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store, conn := newStore(t)
		if err := store.PutRoute(ctx, "-1001", 555, Route{
			ThreadID:     "thread-post-1",
			Project:      "mesh-mail",
			Participants: []string{"pi-claude"},
		}); err != nil {
			t.Fatalf("подготовка маршрута поста: %v", err)
		}

		журнал := &журналВПамяти{}
		updater := &fakeUpdater{pending: []tg.Update{
			{ID: 11, ChatID: "-1001", ThreadID: 97, FromID: 42, From: "tester",
				Text: "ответ на пост", ReplyToMessageID: 555, ReplyToBot: true},
		}}
		intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
		intake.setLogger(log.New(журнал, "", 0))
		go func() { _ = intake.Run(ctx) }()

		waitFor(t, func() bool {
			return strings.Contains(журнал.строки(), "маршрут обновления")
		}, "мост записал маршрут")

		строки := журнал.строки()
		if !strings.Contains(строки, "источник=post") {
			t.Errorf("ответ на пост записан не как post:\n%s", строки)
		}
		if !strings.Contains(строки, "pi-claude") {
			t.Errorf("адресат из маршрута поста не записан:\n%s", строки)
		}
	})

	// Веток, дающих topic, ДВЕ, и покрывать надо обе.
	//
	// Мутация показала это прямо: снятие источника в первой ветке тест не
	// заметил, потому что ходил по второй. Ветки разные по входу — ответ на
	// пост без маршрута против реплики в теме, — но обе означают «адресаты
	// взяты из записи темы», и врать журнал может в любой из них.
	t.Run("ответ на пост бота без маршрута — тоже topic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store, conn := newStore(t)
		// Тема разговора есть, а маршрута поста нет: так выглядят посты,
		// показанные до появления маршрутов.
		if err := store.Put(ctx, "thread-legacy-3", Topic{
			MessageThreadID: 99,
			Participants:    []string{"pi-claude"},
		}); err != nil {
			t.Fatalf("подготовка темы разговора: %v", err)
		}

		журнал := &журналВПамяти{}
		updater := &fakeUpdater{pending: []tg.Update{
			{ID: 13, ChatID: "-1001", ThreadID: 99, FromID: 42, From: "tester",
				Text: "ответ на старый пост", ReplyToMessageID: 777, ReplyToBot: true},
		}}
		intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-claude"), "-1001", []int64{42})
		intake.setLogger(log.New(журнал, "", 0))
		go func() { _ = intake.Run(ctx) }()

		waitFor(t, func() bool {
			return strings.Contains(журнал.строки(), "маршрут обновления")
		}, "мост записал маршрут")

		строки := журнал.строки()
		if !strings.Contains(строки, "источник=topic") {
			t.Errorf("запасной путь по теме записан не как topic:\n%s", строки)
		}
	})

	t.Run("тема разговора — источник topic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store, conn := newStore(t)
		// Ключ KV латиницей не для красоты: NATS отвергает кириллицу
		// ошибкой «invalid key», и через инструменты сюда всегда попадает
		// UUID. Тест на этом уже споткнулся — оставляю как есть.
		if err := store.Put(ctx, "thread-topic-2", Topic{
			MessageThreadID: 98,
			Participants:    []string{"pi-codex"},
		}); err != nil {
			t.Fatalf("подготовка темы разговора: %v", err)
		}

		журнал := &журналВПамяти{}
		updater := &fakeUpdater{pending: []tg.Update{
			{ID: 12, ChatID: "-1001", ThreadID: 98, FromID: 42, From: "tester",
				Text: "реплика в теме разговора"},
		}}
		intake := NewIntake(conn.JS(), store, updater, живойРеестр("pi-codex"), "-1001", []int64{42})
		intake.setLogger(log.New(журнал, "", 0))
		go func() { _ = intake.Run(ctx) }()

		waitFor(t, func() bool {
			return strings.Contains(журнал.строки(), "маршрут обновления")
		}, "мост записал маршрут")

		строки := журнал.строки()
		if !strings.Contains(строки, "источник=topic") {
			t.Errorf("реплика в теме разговора записана не как topic:\n%s", строки)
		}
		if !strings.Contains(строки, "pi-codex") {
			t.Errorf("адресат из темы не записан:\n%s", строки)
		}
	})
}

// В журнале — тот же список, по которому уйдёт письмо.
//
// Письмо публикуется по `Recipients()`, а тот убирает повторы. Журнал,
// берущий сырой список из маршрута, показал бы адресата дважды там, где
// доставка была одна: читающий решит, что видит дубль доставки, и пойдёт
// искать несуществующую ошибку.
//
// Дубли в участниках — не выдумка ради теста: на них уже ловили двойное
// пробуждение, когда `Recipients` их ещё не убирал.
func TestЖурналПоказываетДедуплицированныйСписок(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	if err := store.PutRoute(ctx, "-1001", 321, Route{
		ThreadID:     "thread-dup",
		Participants: []string{"pi-claude", "pi-claude", "pi-codex"},
	}); err != nil {
		t.Fatalf("подготовка маршрута: %v", err)
	}

	журнал := &журналВПамяти{}
	updater := &fakeUpdater{pending: []tg.Update{
		{ID: 21, ChatID: "-1001", ThreadID: 97, FromID: 42, From: "tester",
			Text: "ответ", ReplyToMessageID: 321, ReplyToBot: true},
	}}
	intake := NewIntake(conn.JS(), store, updater,
		живойРеестр("pi-claude", "pi-codex"), "-1001", []int64{42})
	intake.setLogger(log.New(журнал, "", 0))
	go func() { _ = intake.Run(ctx) }()

	waitFor(t, func() bool {
		return strings.Contains(журнал.строки(), "маршрут обновления")
	}, "мост записал маршрут")

	строка := журнал.строки()
	if n := strings.Count(строка, "pi-claude"); n != 1 {
		t.Errorf("pi-claude встречается %d раза, ожидался один: журнал берёт сырой список\n%s", n, строка)
	}
	if !strings.Contains(строка, "pi-codex") {
		t.Errorf("второй адресат потерян:\n%s", строка)
	}
}
