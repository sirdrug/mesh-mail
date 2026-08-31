package bus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/mail"
)

func send(t *testing.T, conn *Conn, from, to, subject string) *mail.Message {
	t.Helper()
	m := mail.New(from, []string{to}, subject, "тело")
	if err := Publish(context.Background(), conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	return m
}

func TestInboxВозвращаетТолькоСвоиПисьма(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	send(t, conn, "pi-claude", "m1-codex", "для m1")
	send(t, conn, "pi-claude", "mbp-claude", "для mbp")

	got, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("получено %d писем, ожидалось 1", len(got))
	}
	if got[0].Message.Subject != "для m1" {
		t.Fatalf("получено письмо %q — прилетело чужое", got[0].Message.Subject)
	}
}

func TestInboxПовторноеЧтениеНеТеряетПисьма(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)
	send(t, conn, "pi-claude", "m1-codex", "письмо")

	first, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("первое чтение: %v", err)
	}
	second, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("второе чтение: %v", err)
	}

	// Ящик, а не очередь: две сессии одного агента читают одно и то же.
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("первое чтение %d писем, второе %d — письма потребляются", len(first), len(second))
	}
}

func TestMarkReadСдвигаетПозицию(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	send(t, conn, "pi-claude", "m1-codex", "первое")
	send(t, conn, "pi-claude", "m1-codex", "второе")

	all, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ожидалось 2 письма, получено %d", len(all))
	}

	if err := MarkRead(ctx, conn.JS(), "m1-codex", all[0].Seq); err != nil {
		t.Fatalf("отметка о прочтении: %v", err)
	}

	unread, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{UnreadOnly: true})
	if err != nil {
		t.Fatalf("чтение непрочитанного: %v", err)
	}
	if len(unread) != 1 {
		t.Fatalf("непрочитанных %d, ожидалось 1", len(unread))
	}
	if unread[0].Message.Subject != "второе" {
		t.Fatalf("непрочитанным осталось %q, ожидалось \"второе\"", unread[0].Message.Subject)
	}
}

func TestПозицияОбщаяДляВсехСессий(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)
	send(t, conn, "pi-claude", "m1-codex", "письмо")

	all, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if err := MarkRead(ctx, conn.JS(), "m1-codex", all[0].Seq); err != nil {
		t.Fatalf("отметка: %v", err)
	}

	// Вторая сессия того же агента — отдельное подключение к хабу.
	other, err := Connect(ctx, Options{URLs: []string{conn.NC().ConnectedUrl()}, Name: "вторая сессия"})
	if err != nil {
		t.Fatalf("второе подключение: %v", err)
	}
	defer other.Close()

	unread, err := Inbox(ctx, other.JS(), "m1-codex", InboxOptions{UnreadOnly: true})
	if err != nil {
		t.Fatalf("чтение из второй сессии: %v", err)
	}
	if len(unread) != 0 {
		t.Fatalf("вторая сессия видит %d непрочитанных — состояние не общее", len(unread))
	}
}

func TestInboxФильтруетПоВажности(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	send(t, conn, "pi-claude", "m1-codex", "обычное")

	urgent := mail.New("pi-claude", []string{"m1-codex"}, "срочное", "тело")
	urgent.Importance = mail.ImportanceUrgent
	if err := Publish(ctx, conn.JS(), urgent); err != nil {
		t.Fatalf("публикация срочного: %v", err)
	}

	got, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{MinImportance: mail.ImportanceUrgent})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(got) != 1 || got[0].Message.Subject != "срочное" {
		t.Fatalf("фильтр по важности вернул %d писем: %+v", len(got), got)
	}
}

func TestInboxПустогоЯщика(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	got, err := Inbox(ctx, conn.JS(), "никому-не-писали", InboxOptions{})
	if err != nil {
		t.Fatalf("чтение пустого ящика вернуло ошибку: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("в пустом ящике %d писем", len(got))
	}
}

// Позицию нельзя увести за конец потока.
//
// Инструмент принимает номер от модели, а та может ошибиться или взять его
// из письма-инъекции. Огромное значение спрятало бы всю будущую почту до тех
// пор, пока поток не дорастёт до этого номера, — и выглядело бы это как
// «писем нет», то есть как исправная работа.
func TestMarkReadОтвергаетПозициюЗаКонцомПотока(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)
	send(t, conn, "pi-claude", "m1-codex", "письмо")

	if err := MarkRead(ctx, conn.JS(), "m1-codex", 1_000_000); err == nil {
		t.Fatal("позиция за концом потока принята — вся будущая почта спрятана")
	}

	unread, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{UnreadOnly: true})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(unread) != 1 {
		t.Fatalf("непрочитанных %d, ожидалось 1 — письмо всё-таки спряталось", len(unread))
	}
}

// Курсор двигается только вперёд даже при одновременных отметках.
//
// Раньше было «прочитать текущее → записать», и две сессии, увидевшие
// одинаковое текущее, затирали друг друга: сохранялась та, что записала
// последней, даже если её позиция меньше.
func TestMarkReadНеОткатываетсяПриКонкуренции(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	for i := 0; i < 6; i++ {
		send(t, conn, "pi-claude", "m1-codex", "письмо")
	}
	all, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(all) != 6 {
		t.Fatalf("получено %d писем, ожидалось 6", len(all))
	}

	high := all[len(all)-1].Seq
	low := all[0].Seq

	var wg sync.WaitGroup
	for _, seq := range []uint64{high, low, high, low} {
		wg.Add(1)
		go func(s uint64) {
			defer wg.Done()
			_ = MarkRead(ctx, conn.JS(), "m1-codex", s)
		}(seq)
	}
	wg.Wait()

	pos, err := ReadPosition(ctx, conn.JS(), "m1-codex")
	if err != nil {
		t.Fatalf("позиция: %v", err)
	}
	if pos != high {
		t.Fatalf("позиция = %d, ожидалась %d — курсор откатился назад", pos, high)
	}
}

// Срочное письмо достижимо, даже если перед ним много обычных.
//
// Фильтр важности применяется после выборки, поэтому при одной выборке на
// limit срочное письмо, перед которым лежит limit обычных, не возвращалось
// никогда — и следующий вызов упирался в те же обычные. Для агента это
// выглядело как «срочных нет», то есть как исправная работа.
func TestСрочноеНеТеряетсяЗаОбычными(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	const ordinary = 60 // заведомо больше одной выборки
	for i := 0; i < ordinary; i++ {
		send(t, conn, "pi-claude", "m1-codex", "обычное")
	}

	urgent := mail.New("pi-claude", []string{"m1-codex"}, "пожар", "срочное дело")
	urgent.Importance = mail.ImportanceUrgent
	if err := Publish(ctx, conn.JS(), urgent); err != nil {
		t.Fatalf("публикация срочного: %v", err)
	}

	got, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{
		MinImportance: mail.ImportanceUrgent,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("срочных найдено %d, ожидалось 1 — письмо недостижимо за обычными", len(got))
	}
	if got[0].Message.Subject != "пожар" {
		t.Fatalf("вернулось не то письмо: %q", got[0].Message.Subject)
	}
}

// Повреждённое письмо не исчезает молча.
//
// Раньше неразобранный JSON просто пропускался, и для агента это было
// неотличимо от «письма не было». Теперь на его месте приходит заглушка
// с номером, по которому письмо можно найти в потоке.
func TestПовреждённоеПисьмоВидноАгенту(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	send(t, conn, "pi-claude", "m1-codex", "нормальное письмо")

	// Кладём в ящик мусор мимо Publish — так делает сломанный отправитель
	// или чужой клиент.
	if _, err := conn.JS().Publish(ctx, MailSubject("m1-codex", "pi-claude"), []byte("{это не JSON")); err != nil {
		t.Fatalf("публикация мусора: %v", err)
	}

	got, err := Inbox(ctx, conn.JS(), "m1-codex", InboxOptions{})
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("получено %d писем, ожидалось 2 (нормальное и заглушка)", len(got))
	}

	var damaged *Envelope
	for i := range got {
		if got[i].Message.From == DamagedSender {
			damaged = &got[i]
		}
	}
	if damaged == nil {
		t.Fatal("повреждённое письмо исчезло молча")
	}
	if damaged.Seq == 0 {
		t.Fatal("у заглушки нет позиции — письмо не найти в потоке")
	}
	if !strings.Contains(damaged.Message.Subject, "не удалось разобрать") {
		t.Fatalf("тема заглушки невнятная: %q", damaged.Message.Subject)
	}
}

// Признак полноты отвечает на вопрос «это всё?».
//
// Срез из limit писем неотличим от полного ящика, а выдача идёт от старых к
// новым — значит взявший одну порцию отвечает по устаревшему состоянию и не
// знает об этом. Проверяется ГРАНИЦА: ровно на лимите ящик может и кончиться,
// и продолжиться, и эти случаи обязаны различаться.
func TestInboxПризнакПолнотыВыдачи(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	const total = 5
	for i := range total {
		send(t, conn, "pi-claude", "m1-codex", fmt.Sprintf("письмо %d", i+1))
	}

	for _, tc := range []struct {
		name     string
		limit    int
		want     int
		wantMore bool
	}{
		{"взяли меньше, чем есть", 2, 2, true},
		{"взяли ровно столько, сколько есть", total, total, false},
		{"попросили больше, чем есть", total + 3, total, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := InboxPage(ctx, conn.JS(), "m1-codex", InboxOptions{Limit: tc.limit})
			if err != nil {
				t.Fatalf("чтение ящика: %v", err)
			}
			if len(page.Envelopes) != tc.want {
				t.Fatalf("писем %d, ожидалось %d", len(page.Envelopes), tc.want)
			}
			if page.HasMore != tc.wantMore {
				t.Fatalf("has_more=%v, ожидалось %v: срез выдан за полный ящик",
					page.HasMore, tc.wantMore)
			}
		})
	}
}

// Дочитывание до конца завершается, и признак гаснет на последней порции.
//
// Без этого «читай, пока has_more» — совет, а не механизм: если признак не
// гаснет, читающий уходит в бесконечный цикл.
func TestInboxДочитываниеЗавершается(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	const total = 7
	for i := range total {
		send(t, conn, "pi-claude", "m1-codex", fmt.Sprintf("письмо %d", i+1))
	}

	seen := 0
	for round := 0; ; round++ {
		if round > total {
			t.Fatalf("дочитывание не сошлось за %d кругов: признак не гаснет", round)
		}
		page, err := InboxPage(ctx, conn.JS(), "m1-codex",
			InboxOptions{Limit: 2, UnreadOnly: true})
		if err != nil {
			t.Fatalf("чтение ящика: %v", err)
		}
		seen += len(page.Envelopes)
		if len(page.Envelopes) > 0 {
			last := page.Envelopes[len(page.Envelopes)-1].Seq
			if err := MarkRead(ctx, conn.JS(), "m1-codex", last); err != nil {
				t.Fatalf("отметка прочитанного: %v", err)
			}
		}
		if !page.HasMore {
			break
		}
	}
	if seen != total {
		t.Fatalf("дочитано %d писем из %d", seen, total)
	}
}

// Упёршись в предел просмотра, выдача обязана заявить о неполноте.
//
// Случай отдельный от «набрали лимит»: писем в ящике много, подходящих мало,
// и цикл останавливает не лимит, а ScanCap. Выдача при этом непустая, поэтому
// отказ «просмотрено N без совпадений» не срабатывает — и без признака агент
// решит, что дочитал ящик, хотя просмотрена лишь его тысяча.
//
// Тест дорогой намеренно: дешевле проверить эту границу нечем, а мутация
// «предел просмотра не считается остатком» иначе проходит незамеченной.
func TestInboxПределПросмотраОзначаетНеполноту(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	// Одно подходящее письмо в начале, дальше — тысяча неподходящих.
	send(t, conn, "pi-claude", "m1-codex", "срочное")
	m := mail.New("pi-claude", []string{"m1-codex"}, "срочное", "тело")
	m.Importance = "urgent"
	if err := Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	for i := range ScanCap + 10 {
		send(t, conn, "pi-claude", "m1-codex", fmt.Sprintf("обычное %d", i))
	}

	page, err := InboxPage(ctx, conn.JS(), "m1-codex", InboxOptions{
		Limit: 50, MinImportance: "urgent",
	})
	if err != nil {
		t.Fatalf("чтение ящика: %v", err)
	}
	if len(page.Envelopes) == 0 {
		t.Fatal("срочное письмо не найдено — проверять неполноту не на чем")
	}
	if !page.HasMore {
		t.Fatal("has_more=false при остановке по пределу просмотра: " +
			"непросмотренный хвост выдан за дочитанный ящик")
	}
}

// Лимит, совпавший с размером ПАЧКИ, тоже обязан давать признак неполноты.
//
// Здесь признак и молчал: defaultInboxLimit и batchSize оба равны 50, и
// письма сверх лимита в пачке физически не было — цикл выходил, не увидев
// хвоста. Прежние тесты этого не ловили, потому что брали лимиты 2, 5 и 8 при
// пачке в 50: «лишнее» письмо всегда лежало в той же выборке. То есть проверки
// были, а самая частая граница осталась непокрытой.
func TestInboxПризнакРаботаетНаГраницеПачки(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	for i := range batchSize + 5 {
		send(t, conn, "pi-claude", "m1-codex", fmt.Sprintf("письмо %d", i+1))
	}

	for _, tc := range []struct {
		name  string
		limit int
	}{
		{"лимит равен размеру пачки", batchSize},
		{"лимит по умолчанию", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := InboxPage(ctx, conn.JS(), "m1-codex", InboxOptions{Limit: tc.limit})
			if err != nil {
				t.Fatalf("чтение ящика: %v", err)
			}
			if len(page.Envelopes) != batchSize {
				t.Fatalf("писем %d, ожидалось %d", len(page.Envelopes), batchSize)
			}
			if !page.HasMore {
				t.Fatal("has_more=false при непустом хвосте: " +
					"лимит совпал с границей пачки, и признак промолчал")
			}
		})
	}
}

// Письмо, доставленное сверх лимита, не теряется.
//
// На этом держится вся схема: добор безопасен ровно потому, что позицию
// прочитанного двигает только MarkRead, а консьюмер эфемерный. Перенесут
// позицию в консьюмер — письмо начнёт пропадать молча, и поймать это будет
// нечем, кроме этого теста.
func TestInboxДоборНеТеряетПисьмо(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	const total = batchSize + 1
	for i := range total {
		send(t, conn, "pi-claude", "m1-codex", fmt.Sprintf("письмо %d", i+1))
	}

	first, err := InboxPage(ctx, conn.JS(), "m1-codex",
		InboxOptions{Limit: batchSize, UnreadOnly: true})
	if err != nil {
		t.Fatalf("первое чтение: %v", err)
	}
	if !first.HasMore {
		t.Fatal("хвост не заявлен")
	}
	last := first.Envelopes[len(first.Envelopes)-1].Seq
	if err := MarkRead(ctx, conn.JS(), "m1-codex", last); err != nil {
		t.Fatalf("отметка прочитанного: %v", err)
	}

	second, err := InboxPage(ctx, conn.JS(), "m1-codex",
		InboxOptions{Limit: batchSize, UnreadOnly: true})
	if err != nil {
		t.Fatalf("второе чтение: %v", err)
	}
	if len(second.Envelopes) != 1 {
		t.Fatalf("во втором чтении %d писем, ожидалось 1: "+
			"добранное сверх лимита письмо пропало", len(second.Envelopes))
	}
	if second.HasMore {
		t.Error("has_more=true на последней порции — дочитывание не сойдётся")
	}
}
