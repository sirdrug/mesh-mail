package mcpsrv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/bustest"
	"github.com/boreevyuri/mesh-mail/internal/claims"
	"github.com/boreevyuri/mesh-mail/internal/config"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/nats-io/nats.go/jetstream"
)

func nowUTC() time.Time { return time.Now().UTC() }

func setup(t *testing.T) (*handlers, *bus.Conn) {
	t.Helper()
	ctx := context.Background()

	conn, err := bus.Connect(ctx, bus.Options{URLs: []string{bustest.StartTestServer(t)}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	t.Cleanup(conn.Close)
	if err := bus.EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("топология: %v", err)
	}
	// Реестр зон поднимает мост — в тестах его роль играет то же соединение.
	// Раньше бакет заводился побочным действием claims.NewStore, и setup
	// молча на это опирался; теперь агент реестр только открывает.
	if err := claims.EnsureBucket(ctx, conn.JS()); err != nil {
		t.Fatalf("реестр зон: %v", err)
	}

	node := &config.Node{AgentID: "m1-claude", Host: "macbook-m1", Engine: "claude"}
	return &handlers{conn: conn, reg: bus.NewRegistry(), node: node, search: productionSearch()}, conn
}

func TestSendMessageКладётПисьмоВЯщик(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	_, out, err := h.sendMessage(ctx, nil, SendMessageIn{
		To:      []string{"pi-codex"},
		Subject: "привет",
		Body:    "как дела",
	})
	if err != nil {
		t.Fatalf("отправка: %v", err)
	}
	if out.ID == "" {
		t.Fatal("инструмент не вернул идентификатор письма")
	}

	got, err := bus.Inbox(ctx, conn.JS(), "pi-codex", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(got) != 1 || got[0].Message.Subject != "привет" {
		t.Fatalf("в ящике %d писем: %+v", len(got), got)
	}
	if got[0].Message.From != "m1-claude" {
		t.Fatalf("отправитель %q — агент смог соврать про себя", got[0].Message.From)
	}
}

func TestSendMessageНеДаётПодделатьОтправителя(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// В структуре входа поля From нет вовсе: отправитель берётся из конфига
	// узла. Проверяем, что так и есть.
	if _, _, err := h.sendMessage(ctx, nil, SendMessageIn{
		To: []string{"pi-codex"}, Subject: "тема", Body: "тело",
	}); err != nil {
		t.Fatalf("отправка: %v", err)
	}

	got, _ := bus.Inbox(ctx, conn.JS(), "pi-codex", bus.InboxOptions{})
	if got[0].Message.From != "m1-claude" {
		t.Fatalf("отправитель %q", got[0].Message.From)
	}
}

func TestFetchInboxВозвращаетНепрочитанное(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	m := mail.New("pi-codex", []string{"m1-claude"}, "входящее", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	_, out, err := h.fetchInbox(ctx, nil, FetchInboxIn{UnreadOnly: true})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("получено %d писем, ожидалось 1", len(out.Messages))
	}
	if out.Messages[0].Subject != "входящее" {
		t.Fatalf("тема %q", out.Messages[0].Subject)
	}
	if out.Messages[0].Seq == 0 {
		t.Fatal("не отдана позиция письма — нечем отметить прочтение")
	}
}

func TestMarkReadУбираетИзНепрочитанного(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	m := mail.New("pi-codex", []string{"m1-claude"}, "входящее", "тело")
	if err := bus.Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	_, out, err := h.fetchInbox(ctx, nil, FetchInboxIn{UnreadOnly: true})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}

	if _, _, err := h.markRead(ctx, nil, MarkReadIn{Seq: out.Messages[0].Seq}); err != nil {
		t.Fatalf("отметка: %v", err)
	}

	_, after, err := h.fetchInbox(ctx, nil, FetchInboxIn{UnreadOnly: true})
	if err != nil {
		t.Fatalf("повторное чтение: %v", err)
	}
	if len(after.Messages) != 0 {
		t.Fatalf("после отметки осталось %d непрочитанных", len(after.Messages))
	}
}

func TestReplyСохраняетТред(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	orig := mail.New("pi-codex", []string{"m1-claude"}, "вопрос", "как дела")
	if err := bus.Publish(ctx, conn.JS(), orig); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	_, out, err := h.reply(ctx, nil, ReplyIn{MessageID: orig.ID, Body: "нормально"})
	if err != nil {
		t.Fatalf("ответ: %v", err)
	}
	if out.ThreadID != orig.ThreadID {
		t.Fatalf("тред ответа %q != %q", out.ThreadID, orig.ThreadID)
	}

	got, err := bus.Inbox(ctx, conn.JS(), "pi-codex", bus.InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика отправителя: %v", err)
	}
	if len(got) != 1 || got[0].Message.Body != "нормально" {
		t.Fatalf("ответ не дошёл: %+v", got)
	}
}

func TestListAgentsИщетПоПроекту(t *testing.T) {
	ctx := context.Background()
	h, _ := setup(t)

	h.reg.Upsert(bus.Card{AgentID: "pi-codex", Projects: []string{"kumo"}, TTLSeconds: 180, AnnouncedAt: nowUTC()})
	h.reg.Upsert(bus.Card{AgentID: "mbp-claude", Projects: []string{"dns-watcher"}, TTLSeconds: 180, AnnouncedAt: nowUTC()})

	_, out, err := h.listAgents(ctx, nil, ListAgentsIn{Project: "kumo"})
	if err != nil {
		t.Fatalf("поиск: %v", err)
	}
	if len(out.Agents) != 1 || out.Agents[0].AgentID != "pi-codex" {
		t.Fatalf("поиск вернул %+v", out.Agents)
	}
}

func TestReplyНаходитПисьмоПрочитанноеРанее(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Письмо приходит и читается — с этого момента агент его «видел».
	orig := mail.New("pi-codex", []string{"m1-claude"}, "вопрос", "как дела")
	if err := bus.Publish(ctx, conn.JS(), orig); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	if _, _, err := h.fetchInbox(ctx, nil, FetchInboxIn{}); err != nil {
		t.Fatalf("чтение: %v", err)
	}

	// Ящик забивается так, что исходное письмо уходит за предел просмотра.
	for i := 0; i < 5; i++ {
		filler := mail.New("pi-codex", []string{"m1-claude"}, "шум", "тело")
		if err := bus.Publish(ctx, conn.JS(), filler); err != nil {
			t.Fatalf("публикация шума: %v", err)
		}
	}

	_, out, err := h.reply(ctx, nil, ReplyIn{MessageID: orig.ID, Body: "нормально"})
	if err != nil {
		t.Fatalf("ответ на прочитанное письмо не прошёл: %v", err)
	}
	if out.ThreadID != orig.ThreadID {
		t.Fatalf("тред ответа %q != %q", out.ThreadID, orig.ThreadID)
	}
}

// TestОтказГоворитЧтоЯщикПросмотренЦеликом — отказ обязан отличать «письма
// нет» от «я не досмотрел».
//
// Пока поиск брал только первое окно писем, отказ был признанием
// границы. Теперь ящик просматривается от последнего письма до первого, и
// отказ означает настоящее отсутствие — так он и должен читаться. Промолчать
// об этом нельзя: агент решает по тексту, повторять ли попытку.
func TestОтказГоворитЧтоЯщикПросмотренЦеликом(t *testing.T) {
	ctx := context.Background()
	h, _ := setup(t)

	_, _, err := h.reply(ctx, nil, ReplyIn{MessageID: "нет-такого-письма", Body: "ответ"})
	if err == nil {
		t.Fatal("ответ на несуществующее письмо прошёл")
	}
	if !strings.Contains(err.Error(), "просмотрен целиком") {
		t.Fatalf("отказ не говорит, что ящик просмотрен весь: %v", err)
	}
}

func TestClaimZoneОтдаётЗоныИИмяДержателя(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	_, out, err := h.claimZone(ctx, nil, ClaimZoneIn{
		Zones: []string{"internal/claims", "README.md"},
		Note:  "реестр захватов",
	})
	if err != nil {
		t.Fatalf("захват: %v", err)
	}
	if len(out.Taken) != 2 {
		t.Fatalf("взято %d зон: %+v", len(out.Taken), out.Taken)
	}
	if out.Taken[0].AgentID != "m1-claude" {
		t.Fatalf("захват записан не на того агента: %+v", out.Taken[0])
	}

	// Второй агент на том же хабе видит занятое и получает имя держателя.
	other := &handlers{conn: conn, reg: bus.NewRegistry(),
		node: &config.Node{AgentID: "pi-codex", Engine: "codex"}}

	_, denied, err := other.claimZone(ctx, nil, ClaimZoneIn{Zones: []string{"internal/claims/store.go"}})
	if err != nil {
		t.Fatalf("отказ пришёл ошибкой инструмента, а не ответом: %v", err)
	}
	if denied.Held == nil {
		t.Fatalf("вложенная зона захвачена поверх занятой: %+v", denied)
	}
	if denied.Held.AgentID != "m1-claude" {
		t.Fatalf("не назван держатель: %+v", denied.Held)
	}
	// Агенту нужно понять, что делать дальше, а не только что «нельзя».
	if !strings.Contains(denied.Note, "m1-claude") {
		t.Fatalf("в подсказке нет имени держателя: %q", denied.Note)
	}
}

func TestListClaimsОтвечаетПроКонкретныйПуть(t *testing.T) {
	ctx := context.Background()
	h, _ := setup(t)

	_, free, err := h.listClaims(ctx, nil, ListClaimsIn{Zone: "internal/bus"})
	if err != nil {
		t.Fatalf("запрос: %v", err)
	}
	if !free.Free {
		t.Fatalf("свободный путь показан занятым: %+v", free)
	}

	if _, _, err := h.claimZone(ctx, nil, ClaimZoneIn{Zones: []string{"internal/bus"}}); err != nil {
		t.Fatalf("захват: %v", err)
	}

	_, busy, err := h.listClaims(ctx, nil, ListClaimsIn{Zone: "internal/bus/conn.go"})
	if err != nil {
		t.Fatalf("запрос: %v", err)
	}
	if busy.Free || len(busy.Claims) != 1 {
		t.Fatalf("держатель каталога не найден по вложенному файлу: %+v", busy)
	}
}

func TestReleaseZoneОсвобождаетТолькоСвоё(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	if _, _, err := h.claimZone(ctx, nil, ClaimZoneIn{Zones: []string{"nats/hub.conf"}}); err != nil {
		t.Fatalf("захват: %v", err)
	}

	stranger := &handlers{conn: conn, reg: bus.NewRegistry(),
		node: &config.Node{AgentID: "pi-codex", Engine: "codex"}}
	if _, _, err := stranger.releaseZone(ctx, nil, ReleaseZoneIn{Zones: []string{"nats/hub.conf"}}); err == nil {
		t.Fatal("чужой захват снят через MCP")
	}

	if _, out, err := h.releaseZone(ctx, nil, ReleaseZoneIn{Zones: []string{"nats/hub.conf"}}); err != nil {
		t.Fatalf("свой захват не снялся: %v", err)
	} else if len(out.Released) != 1 {
		t.Fatalf("освобождено %d зон", len(out.Released))
	}
}

// TestReplyНаходитСвежееПисьмоВБольшомЯщике — дефект, из-за которого ответ на
// только что пришедшее письмо не проходил.
//
// Поиск исходного письма начинался с НАЧАЛА ящика и брал первые
// одно окно писем — то есть самые старые. Отвечают же почти всегда на
// свежее, и в выросшем ящике оно лежит за этой границей. Снаружи это выглядит
// как «письма не существует», хотя оно пришло минуту назад.
//
// Кэш недавних (recent) дефект маскирует: письмо, только что прочитанное этой
// же сессией, находится без обращения к ящику. Поэтому письма публикуются мимо
// handlers — так же, как в жизни: письмо пришло, а сессия его ещё не читала.
func TestReplyНаходитСвежееПисьмоВБольшомЯщике(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Предел просмотра уменьшен, чтобы не публиковать тысячи писем: дефект
	// зависит от отношения «размер ящика к пределу», а не от самого числа.
	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	// Ящик уже вырос: старые письма занимают всё окно просмотра.
	for i := 0; i < 12; i++ {
		old := mail.New("pi-codex", []string{"m1-claude"}, "старое", "тело")
		if err := bus.Publish(ctx, conn.JS(), old); err != nil {
			t.Fatalf("публикация старого: %v", err)
		}
	}

	// И только теперь приходит письмо, на которое отвечаем.
	fresh := mail.New("pi-codex", []string{"m1-claude"}, "свежий вопрос", "ответь")
	if err := bus.Publish(ctx, conn.JS(), fresh); err != nil {
		t.Fatalf("публикация свежего: %v", err)
	}

	_, out, err := h.reply(ctx, nil, ReplyIn{MessageID: fresh.ID, Body: "отвечаю"})
	if err != nil {
		t.Fatalf("ответ на свежее письмо в большом ящике не прошёл: %v", err)
	}
	if out.ThreadID != fresh.ThreadID {
		t.Fatalf("тред ответа %q != %q", out.ThreadID, fresh.ThreadID)
	}
}

// TestReplyНаходитДавнееПисьмоВБольшомЯщике — проверка, что починка хвоста не
// отняла голову.
//
// Поиск теперь начинается у конца ящика, и это ровно та правка, после которой
// легко потерять обратный случай: ответ на давнее письмо, лежащее за пределом
// просмотра от конца. Такой отказ выглядел бы как «письма не существует» — тот
// же симптом, что чинили, только с другой стороны ящика.
func TestReplyНаходитДавнееПисьмоВБольшомЯщике(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	// Письмо, на которое отвечаем, приходит ПЕРВЫМ.
	old := mail.New("pi-codex", []string{"m1-claude"}, "давний вопрос", "тело")
	if err := bus.Publish(ctx, conn.JS(), old); err != nil {
		t.Fatalf("публикация давнего: %v", err)
	}

	// И уезжает за предел просмотра от конца.
	for i := 0; i < 12; i++ {
		filler := mail.New("pi-codex", []string{"m1-claude"}, "шум", "тело")
		if err := bus.Publish(ctx, conn.JS(), filler); err != nil {
			t.Fatalf("публикация шума: %v", err)
		}
	}

	_, out, err := h.reply(ctx, nil, ReplyIn{MessageID: old.ID, Body: "отвечаю"})
	if err != nil {
		t.Fatalf("ответ на давнее письмо в большом ящике не прошёл: %v", err)
	}
	if out.ThreadID != old.ThreadID {
		t.Fatalf("тред ответа %q != %q", out.ThreadID, old.ThreadID)
	}
}

// TestReplyНеЗависитОтОтметкиПрочтения — ответ не должен зависеть от того,
// докуда сдвинута позиция чтения.
//
// Соблазн «искать только непрочитанное» здесь силён: так окно поиска само
// сужается к концу ящика. Но позиция общая для всех сессий агента, и письмо,
// прочитанное сторожем или другой сессией, оказалось бы недостижимым для
// ответа — при том что человек его видит.
func TestReplyНеЗависитОтОтметкиПрочтения(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	for i := 0; i < 12; i++ {
		filler := mail.New("pi-codex", []string{"m1-claude"}, "шум", "тело")
		if err := bus.Publish(ctx, conn.JS(), filler); err != nil {
			t.Fatalf("публикация шума: %v", err)
		}
	}
	fresh := mail.New("pi-codex", []string{"m1-claude"}, "вопрос", "ответь")
	if err := bus.Publish(ctx, conn.JS(), fresh); err != nil {
		t.Fatalf("публикация свежего: %v", err)
	}

	// Весь ящик отмечен прочитанным — непрочитанного не осталось вовсе.
	envs, err := bus.Inbox(ctx, conn.JS(), "m1-claude", bus.InboxOptions{Limit: 100})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	last := envs[len(envs)-1].Seq
	if _, _, err := h.markRead(ctx, nil, MarkReadIn{Seq: last}); err != nil {
		t.Fatalf("отметка о прочтении: %v", err)
	}

	_, out, err := h.reply(ctx, nil, ReplyIn{MessageID: fresh.ID, Body: "отвечаю"})
	if err != nil {
		t.Fatalf("ответ на прочитанное свежее письмо не прошёл: %v", err)
	}
	if out.ThreadID != fresh.ThreadID {
		t.Fatalf("тред ответа %q != %q", out.ThreadID, fresh.ThreadID)
	}
}

// TestОтказХвостовогоПоискаНеВыдаётсяЗаОтсутствиеПисьма — сломанный поиск не
// должен выглядеть как честное «письма нет».
//
// Хвостовой поиск держится на праве $JS.API.STREAM.INFO.MAIL. Снимут его при
// правке hub.conf — и ответ на свежее письмо снова перестанет проходить, то
// есть вернётся ровно тот дефект, который здесь чинили. Отличить это от
// настоящего отсутствия письма можно только по тексту отказа, поэтому причина
// в него и попадает.
func TestОтказХвостовогоПоискаНеВыдаётсяЗаОтсутствиеПисьма(t *testing.T) {
	ctx := context.Background()
	h, _ := setup(t)

	h.search.lastSeq = func(context.Context, jetstream.JetStream) (uint64, error) {
		return 0, errors.New("нет права STREAM.INFO")
	}

	_, _, err := h.reply(ctx, nil, ReplyIn{MessageID: "какое-то-письмо", Body: "ответ"})
	if err == nil {
		t.Fatal("ответ прошёл при сломанном поиске")
	}
	if !strings.Contains(err.Error(), "не удалось завершить поиск") {
		t.Fatalf("отказ выдаёт поломку за отсутствие письма: %v", err)
	}
	if !strings.Contains(err.Error(), "нет права STREAM.INFO") {
		t.Fatalf("отказ не называет причину: %v", err)
	}
}

// TestПоискПокрываетКаждуюПозициюЯщика — свойство, а не пример.
//
// Одиночная проверка «письмо из середины находится» доказательством не была:
// она берёт ОДНУ позицию, а покрытие — утверждение обо всех сразу. Прежняя
// редакция поиска удваивала отступ, и такая проверка проходила ровно потому,
// что выбранная позиция случайно попала в покрытую полосу; между полосами
// зияли разрывы, из шестидесяти позиций находились двадцать.
//
// Ящик здесь заведомо больше восьми окон: при пределе просмотра 4 это 60
// писем против 32 позиций, которые покрывали восемь шагов прежней редакции.
func TestПоискПокрываетКаждуюПозициюЯщика(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	const total = 60
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		m := mail.New("pi-codex", []string{"m1-claude"}, fmt.Sprintf("письмо %d", i+1), "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация %d: %v", i+1, err)
		}
		ids = append(ids, m.ID)
	}

	// findInInbox, а не reply: reply публикует ответ и двигает конец потока,
	// то есть каждая проверка измеряла бы уже другой ящик.
	var missed []int
	for i, id := range ids {
		if _, _, ok := h.findInInbox(ctx, id); !ok {
			missed = append(missed, i+1)
		}
	}
	if len(missed) > 0 {
		t.Fatalf("не найдены письма на позициях %v из %d", missed, total)
	}
}

// TestПоискПокрываетЯщикСПропускамиПозиций — в потоке лежат письма чужим
// узлам, и позиции нашего ящика идут с пропусками.
//
// Ширина окна считается в позициях ПОТОКА, а предел просмотра — в наших
// письмах. Расходятся эти единицы именно на чередовании: между двумя нашими
// письмами могут лежать чужие, и окно, рассчитанное как «L писем», покрывает
// тогда более широкий кусок потока. Свойство обязано держаться и так.
func TestПоискПокрываетЯщикСПропускамиПозиций(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	// Пятнадцать наших писем при пределе просмотра 4 — это почти четыре окна;
	// больше берут только время, потому что каждый пустой диапазон стоит
	// отдельного запроса с ожиданием.
	ids := make([]string, 0, 15)
	for i := 0; i < 15; i++ {
		// На каждое наше письмо — два чужих: пропуски в ящике втрое.
		for _, to := range []string{"m1-claude", "pi-claude", "pi-codex"} {
			m := mail.New("pi-codex", []string{to}, fmt.Sprintf("письмо %d", i+1), "тело")
			if err := bus.Publish(ctx, conn.JS(), m); err != nil {
				t.Fatalf("публикация: %v", err)
			}
			if to == "m1-claude" {
				ids = append(ids, m.ID)
			}
		}
	}

	var missed []int
	for i, id := range ids {
		if _, _, ok := h.findInInbox(ctx, id); !ok {
			missed = append(missed, i+1)
		}
	}
	if len(missed) > 0 {
		t.Fatalf("не найдены наши письма номер %v из %d", missed, len(ids))
	}
}

// TestПоискПокрываетЯщикСУдалённымиСообщениями — в потоке дыры от удалений.
//
// Номера сообщений в JetStream не переиспользуются: после удаления в потоке
// остаётся пропуск, и последний номер больше числа сообщений. Поиск считает
// окна по номерам, поэтому такой поток для него — не то же самое, что
// плотный: часть окон окажется пустой, и остановиться на них нельзя.
func TestПоискПокрываетЯщикСУдалённымиСообщениями(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	const total = 40
	ids := make([]string, 0, total)
	seqs := make([]uint64, 0, total)
	for i := 0; i < total; i++ {
		m := mail.New("pi-codex", []string{"m1-claude"}, fmt.Sprintf("письмо %d", i+1), "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация %d: %v", i+1, err)
		}
		ids = append(ids, m.ID)
	}

	envs, err := bus.Inbox(ctx, conn.JS(), "m1-claude", bus.InboxOptions{Limit: total})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	for _, env := range envs {
		seqs = append(seqs, env.Seq)
	}

	// Вырезаем сплошной кусок в середине — так дыра шире одного окна.
	stream, err := conn.JS().Stream(ctx, bus.StreamName)
	if err != nil {
		t.Fatalf("поток: %v", err)
	}
	deleted := map[int]bool{}
	for i := 12; i < 24; i++ {
		if err := stream.DeleteMsg(ctx, seqs[i]); err != nil {
			t.Fatalf("удаление сообщения %d: %v", seqs[i], err)
		}
		deleted[i] = true
	}

	var missed []int
	for i, id := range ids {
		_, _, ok := h.findInInbox(ctx, id)
		if deleted[i] {
			if ok {
				t.Fatalf("удалённое письмо %d нашлось", i+1)
			}
			continue
		}
		if !ok {
			missed = append(missed, i+1)
		}
	}
	if len(missed) > 0 {
		t.Fatalf("не найдены уцелевшие письма %v из %d", missed, total)
	}
}

// TestПоискИдётПримыкающимиДиапазонамиБезПовторов — доказательство о самих
// запросах, а не об их результате.
//
// Отсутствие пропусков и повторов — утверждение о ПОСЛЕДОВАТЕЛЬНОСТИ окон.
// Снаружи её не видно: найденное письмо выглядит одинаково и при исправном
// поиске, и при дырявом, — поэтому здесь проверяются сами запросы. Карта по
// позициям (тесты выше) ловит разрывы шире окна, но разрыв РОВНО в окно от
// неё ускользнёт: письма может не оказаться именно там.
func TestПоискИдётПримыкающимиДиапазонамиБезПовторов(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)

	// Окно сужено у ЭТОГО handlers: боевое значение потребовало бы тысяч
	// писем на каждый случай. Пакетные переменные для этого не годятся —
	// тесты идут параллельно, и подмена общего предела была бы гонкой.
	h.search.window = 4

	for i := 0; i < 41; i++ {
		m := mail.New("pi-codex", []string{"m1-claude"}, "письмо", "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация: %v", err)
		}
	}
	last, err := bus.StreamLastSeq(ctx, conn.JS())
	if err != nil {
		t.Fatalf("конец потока: %v", err)
	}

	var starts []uint64
	inner := h.search.scan
	h.search.scan = func(ctx context.Context, js jetstream.JetStream, agentID string,
		opts bus.InboxOptions,
	) ([]bus.Envelope, error) {
		starts = append(starts, opts.StartSeq)
		return inner(ctx, js, agentID, opts)
	}

	if _, _, ok := h.findInInbox(ctx, "такого-письма-нет"); ok {
		t.Fatal("несуществующее письмо нашлось")
	}

	width := h.search.window
	// Запросов ровно столько, сколько диапазонов ширины width укладывается в
	// поток: меньше — значит часть ящика не просмотрена, больше — значит
	// какой-то диапазон запрошен дважды.
	want := int((last + width - 1) / width)
	if len(starts) != want {
		t.Fatalf("запросов %d, а диапазонов ширины %d в потоке из %d позиций — %d: %v",
			len(starts), width, last, want, starts)
	}
	if starts[0] != last-width+1 {
		t.Fatalf("первый запрос с позиции %d, а конец потока %d: хвост не покрыт",
			starts[0], last)
	}
	for i := 1; i < len(starts); i++ {
		expected := uint64(1)
		if starts[i-1] > width {
			expected = starts[i-1] - width
		}
		if starts[i] != expected {
			t.Fatalf("запрос %d начался с %d, а примыкал бы к предыдущему с %d: %v",
				i, starts[i], expected, starts)
		}
	}
	if starts[len(starts)-1] != 1 {
		t.Fatalf("последний запрос с позиции %d, а не с первой: начало ящика не просмотрено",
			starts[len(starts)-1])
	}
}

// TestПоискНеДоходитДоКонцаЯщикаПриШирокомОкне — дефект, найденный ревьюером
// на БОЕВЫХ значениях, которого не видит ни один тест с малым окном.
//
// Чтение ящика ограничено двумя числами сразу: запрошенным Limit и пределом
// bus.ScanCap, и останавливается по первому же из них. Окно шириной больше
// предела читает меньше писем, чем занимает позиций, и до конца своего
// диапазона не дотягивается — а поиск считает, что дотянулось. При окне 2000 и
// пределе 1000 последняя тысяча позиций не просматривалась вовсе, то есть
// ответ на свежее письмо снова не проходил.
//
// Тест держит связь двух чисел, а не одно из них: ширина окна обязана
// оставаться в пределах того, что чтение реально отдаёт.
func TestПоискНеДоходитДоКонцаЯщикаПриШирокомОкне(t *testing.T) {
	if productionSearch().window > bus.ScanCap {
		t.Fatalf("ширина окна %d больше предела чтения %d: между окнами останутся "+
			"незакрытые полосы, а конец ящика не будет просмотрен",
			productionSearch().window, bus.ScanCap)
	}
}

// TestПоискНаБоевыхЗначенияхПроходитЯщикБольшеПредела — ящик, который заведомо
// не помещается в одно окно боевого размера.
//
// Все прочие проверки сужают окно до четырёх писем, и предел bus.ScanCap в них
// не участвует вовсе — именно поэтому расхождение двух пределов ими не
// ловилось. Здесь параметры боевые, а ящик больше предела, так что поиск
// обязан сделать несколько окон по-настоящему.
func TestПоискНаБоевыхЗначенияхПроходитЯщикБольшеПредела(t *testing.T) {
	if testing.Short() {
		t.Skip("тысяча с лишним писем — долго")
	}
	ctx := context.Background()
	h, conn := setup(t)

	total := int(bus.ScanCap) + 251
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		m := mail.New("pi-codex", []string{"m1-claude"}, "письмо", "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация %d: %v", i+1, err)
		}
		ids = append(ids, m.ID)
	}

	// Тест обязан доказать, что многооконный проход СЛУЧИЛСЯ.
	//
	// Без этого он остаётся зелёным и когда весь ящик уместился в одно окно —
	// то есть подтверждает собственную посылку вместо поведения.
	last, err := bus.StreamLastSeq(ctx, conn.JS())
	if err != nil {
		t.Fatalf("конец потока: %v", err)
	}
	if last < uint64(total) {
		t.Fatalf("в потоке %d позиций при %d опубликованных письмах", last, total)
	}
	windows := 0
	inner := h.search.scan
	h.search.scan = func(ctx context.Context, js jetstream.JetStream, agentID string,
		opts bus.InboxOptions,
	) ([]bus.Envelope, error) {
		windows++
		return inner(ctx, js, agentID, opts)
	}

	// Первое, последнее и середина: концы диапазонов и всё между ними.
	for _, probe := range []struct {
		name string
		idx  int
	}{
		{"самое старое", 0},
		{"середина", total / 2},
		{"на границе первого окна", total - int(bus.ScanCap) - 1},
		{"самое свежее", total - 1},
	} {
		windows = 0
		if _, _, ok := h.findInInbox(ctx, ids[probe.idx]); !ok {
			t.Fatalf("%s письмо (позиция %d из %d) не найдено", probe.name, probe.idx+1, total)
		}
		// Самое старое письмо лежит за первым окном: до него поиск обязан
		// сделать несколько запросов, иначе ящик просматривается не весь.
		if probe.idx == 0 && windows < 2 {
			t.Fatalf("самое старое письмо найдено за %d запрос(ов) при ящике из %d писем "+
				"и окне %d: одного окна на такой ящик не хватает",
				windows, total, h.search.window)
		}
	}
}

// TestПоискНаходитПисьмоГоловойКогдаКонецПотокаНедоступен — частичный поиск
// лучше отказа, если он честен.
//
// Право $JS.API.STREAM.INFO.MAIL можно снять правкой hub.conf, и тогда конец
// потока неизвестен. Диапазоны считать не от чего, но первые window писем
// прочитать по-прежнему можно, и если письмо там — ответ верен. Отказываться
// в этом случае значило бы терять работоспособность там, где её ещё хватает.
func TestПоискНаходитПисьмоГоловойКогдаКонецПотокаНедоступен(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)
	h.search.window = 4

	orig := mail.New("pi-codex", []string{"m1-claude"}, "вопрос", "тело")
	if err := bus.Publish(ctx, conn.JS(), orig); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	h.search.lastSeq = func(context.Context, jetstream.JetStream) (uint64, error) {
		return 0, errors.New("нет права STREAM.INFO")
	}

	_, out, err := h.reply(ctx, nil, ReplyIn{MessageID: orig.ID, Body: "отвечаю"})
	if err != nil {
		t.Fatalf("ответ не прошёл, хотя письмо лежит в начале ящика: %v", err)
	}
	if out.ThreadID != orig.ThreadID {
		t.Fatalf("тред ответа %q != %q", out.ThreadID, orig.ThreadID)
	}
}

// TestОтказОкнаНеВыдаётсяЗаОтсутствиеПисьма — сбой чтения обязан выйти наружу.
//
// Раньше ошибка окна проглатывалась и выглядела как «в этом окне не нашлось»:
// поиск шёл дальше, а наружу уходило «письма нет». Отказ по правам, гонка
// консьюмеров и исчерпание попыток при этом неотличимы от честного отсутствия,
// и агент не повторит попытку, потому что ему сказали, что письма не бывает.
func TestОтказОкнаНеВыдаётсяЗаОтсутствиеПисьма(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)
	h.search.window = 4

	for i := 0; i < 12; i++ {
		m := mail.New("pi-codex", []string{"m1-claude"}, "шум", "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация: %v", err)
		}
	}

	calls := 0
	inner := h.search.scan
	h.search.scan = func(ctx context.Context, js jetstream.JetStream, agentID string,
		opts bus.InboxOptions,
	) ([]bus.Envelope, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("отказ по правам")
		}
		return inner(ctx, js, agentID, opts)
	}

	_, _, err := h.reply(ctx, nil, ReplyIn{MessageID: "нет-такого-письма", Body: "ответ"})
	if err == nil {
		t.Fatal("ответ прошёл при сбое чтения ящика")
	}
	if !strings.Contains(err.Error(), "отказ по правам") {
		t.Fatalf("отказ не называет причину сбоя: %v", err)
	}
	// Диапазон в тексте — чтобы было видно, какая часть ящика не просмотрена.
	if !strings.Contains(err.Error(), "позициях") {
		t.Fatalf("отказ не называет непросмотренный диапазон: %v", err)
	}
	if strings.Contains(err.Error(), "просмотрен целиком") {
		t.Fatalf("отказ утверждает полный просмотр при сбое: %v", err)
	}
}

// TestПоискПоПустомуЯщику — вырожденный случай: писем нет вовсе.
func TestПоискПоПустомуЯщику(t *testing.T) {
	ctx := context.Background()
	h, _ := setup(t)
	h.search.window = 4

	calls := 0
	inner := h.search.scan
	h.search.scan = func(ctx context.Context, js jetstream.JetStream, agentID string,
		opts bus.InboxOptions,
	) ([]bus.Envelope, error) {
		calls++
		return inner(ctx, js, agentID, opts)
	}

	m, err, ok := h.findInInbox(ctx, "какое-нибудь-письмо")
	if ok || m != nil {
		t.Fatal("в пустом ящике что-то нашлось")
	}
	if err != nil {
		t.Fatalf("пустой ящик — не ошибка: %v", err)
	}
	// Один запрос, а не бесконечный откат назад от нулевого конца потока.
	if calls != 1 {
		t.Fatalf("запросов по пустому ящику %d, ожидался один", calls)
	}
}

// TestОтветНеУтверждаетОтсутствиеПокаПроверкаНеЗавершена — письмо ЕСТЬ, но
// проверить это нечем.
//
// Случай собран из двух обычных: конец потока недоступен (снятое право
// $JS.API.STREAM.INFO.MAIL), а письмо лежит дальше начальной части ящика.
// Диапазоны считать не от чего, просмотрена только голова — и ответ обязан
// говорить именно это. Прежняя формулировка начиналась со слов «письмо не
// найдено» и лишь потом называла причину: агент читает первую половину как
// факт и попытку не повторяет, хотя письмо на месте и доступно через
// fetch_inbox.
//
// Тест держит и обратное: право утверждать отсутствие остаётся у полного
// прохода, иначе честное «письма нет» превратилось бы в вечное «попробуйте
// ещё раз» (см. TestОтказГоворитЧтоЯщикПросмотренЦеликом).
func TestОтветНеУтверждаетОтсутствиеПокаПроверкаНеЗавершена(t *testing.T) {
	ctx := context.Background()
	h, conn := setup(t)
	h.search.window = 4

	for i := 0; i < 12; i++ {
		m := mail.New("pi-codex", []string{"m1-claude"}, "шум", "тело")
		if err := bus.Publish(ctx, conn.JS(), m); err != nil {
			t.Fatalf("публикация: %v", err)
		}
	}
	fresh := mail.New("pi-codex", []string{"m1-claude"}, "свежее", "ответь")
	if err := bus.Publish(ctx, conn.JS(), fresh); err != nil {
		t.Fatalf("публикация свежего: %v", err)
	}

	h.search.lastSeq = func(context.Context, jetstream.JetStream) (uint64, error) {
		return 0, errors.New("нет права STREAM.INFO")
	}

	_, _, err := h.reply(ctx, nil, ReplyIn{MessageID: fresh.ID, Body: "отвечаю"})
	if err == nil {
		t.Fatal("ответ прошёл, хотя письмо лежит за просмотренной частью ящика")
	}
	if !strings.HasPrefix(err.Error(), "не удалось завершить поиск") {
		t.Fatalf("отказ начинается не с невозможности завершить проверку: %v", err)
	}
	if !strings.Contains(err.Error(), "только начальную часть") &&
		!strings.Contains(err.Error(), "только начальная часть") {
		t.Fatalf("отказ не говорит, что просмотрена лишь часть ящика: %v", err)
	}
	if strings.Contains(err.Error(), "не найдено") {
		t.Fatalf("отказ утверждает отсутствие письма при незавершённой проверке: %v", err)
	}
	if strings.Contains(err.Error(), "просмотрен целиком") {
		t.Fatalf("отказ утверждает полный просмотр: %v", err)
	}
}
