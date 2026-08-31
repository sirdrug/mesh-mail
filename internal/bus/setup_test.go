package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/boreevyuri/mesh-mail/internal/bustest"
)

func TestEnsureTopologyСоздаётПотокИБакет(t *testing.T) {
	ctx := context.Background()
	url := bustest.StartTestServer(t)

	conn, err := Connect(ctx, Options{URLs: []string{url}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer conn.Close()

	if err := EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("создание топологии: %v", err)
	}

	if _, err := conn.JS().Stream(ctx, StreamName); err != nil {
		t.Fatalf("поток %s не создан: %v", StreamName, err)
	}
	if _, err := conn.JS().KeyValue(ctx, StateBucket); err != nil {
		t.Fatalf("бакет %s не создан: %v", StateBucket, err)
	}
}

func TestEnsureTopologyИдемпотентна(t *testing.T) {
	ctx := context.Background()
	url := bustest.StartTestServer(t)

	conn, err := Connect(ctx, Options{URLs: []string{url}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer conn.Close()

	if err := EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("первый вызов: %v", err)
	}
	// Демоны на четырёх машинах стартуют одновременно и все зовут EnsureTopology.
	if err := EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("повторный вызов упал: %v", err)
	}
}

func TestMailSubject(t *testing.T) {
	if got := MailSubject("pi-claude", "m1-codex"); got != "mail.pi-claude.m1-codex" {
		t.Fatalf("MailSubject = %q, ожидалось mail.pi-claude.m1-codex", got)
	}
}

func TestSenderFromSubject(t *testing.T) {
	cases := map[string]string{
		"mail.pi-claude.m1-codex":   "m1-codex",
		"mail.pi-claude.human":      "human",
		"mail.pi-claude":            "",
		"agents.pi-claude.presence": "",
		"mail.a.b.c":                "",
	}
	for subject, want := range cases {
		if got := SenderFromSubject(subject); got != want {
			t.Errorf("SenderFromSubject(%q) = %q, ожидалось %q", subject, got, want)
		}
	}
}

func TestMailInboxFilter(t *testing.T) {
	if got := MailInboxFilter("pi-claude"); got != "mail.pi-claude.*" {
		t.Fatalf("MailInboxFilter = %q, ожидалось mail.pi-claude.*", got)
	}
}

// SenderForDisplay — единственное место, решающее, кого показать
// отправителем. Проверяем сам fallback, а не путь письма: интеграционно
// невалидная тема недостижима, потому что фильтр её не пропускает.
func TestSenderForDisplay(t *testing.T) {
	cases := []struct {
		subject string
		want    string
		почему  string
	}{
		{"mail.pi-claude.m1-codex", "m1-codex", "обычное письмо"},
		{"mail.pi-claude.human", "human", "письмо от человека через мост"},
		{"mail.pi-claude", UnverifiedSender, "старая двухтокенная тема"},
		{"mail.a.b.c", UnverifiedSender, "лишний токен"},
		{"agents.pi-claude.presence", UnverifiedSender, "чужой префикс"},
		{"", UnverifiedSender, "пустая тема"},
	}
	for _, c := range cases {
		if got := SenderForDisplay(c.subject); got != c.want {
			t.Errorf("%s: SenderForDisplay(%q) = %q, ожидалось %q",
				c.почему, c.subject, got, c.want)
		}
	}
}

// Пустую строку возвращать нельзя: пустое поле читается как «не указан»,
// а сказать надо, что отправителю нельзя верить.
func TestSenderForDisplayНикогдаНеПустой(t *testing.T) {
	for _, subject := range []string{"", ".", "mail.", "mail..", "x"} {
		if SenderForDisplay(subject) == "" {
			t.Errorf("SenderForDisplay(%q) вернул пустую строку", subject)
		}
	}
}

// Агентская проверка не создаёт топологию, а внятно отказывает.
//
// Молчаливое создание из агентского процесса невозможно по правам, но до
// разделения функций попытка всё же делалась — и отказ по правам выглядел бы
// как непонятная ошибка при старте узла, а не как «мост ещё не поднят».
func TestCheckTopologyНеСоздаётПотокИОбъясняетПричину(t *testing.T) {
	ctx := context.Background()
	url := bustest.StartTestServer(t)

	conn, err := Connect(ctx, Options{URLs: []string{url}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer conn.Close()

	err = CheckTopology(ctx, conn.JS())
	if err == nil {
		t.Fatal("проверка молча согласилась с отсутствием топологии")
	}
	if !strings.Contains(err.Error(), "мост") {
		t.Fatalf("ошибка не подсказывает, кто поднимает топологию: %v", err)
	}

	// И, главное, ничего не создала: иначе агент на пустом хабе завёл бы
	// поток со своими представлениями о его конфигурации.
	if _, err := conn.JS().Stream(ctx, StreamName); err == nil {
		t.Fatal("агентская проверка создала поток")
	}
}

// Раскатка по одному узлу не должна ронять узлы.
//
// Это тот самый сценарий, ради которого функции и разделены. Поток создан со
// старым окном дедупликации, узел обновлён и ожидает новое. Раньше он в этот
// момент пытался привести конфигурацию, получал отказ по правам и не
// стартовал вовсе — на каждой из восьми машин, пока не обновят мост.
func TestУзелНеПадаетПриСтаромОкнеДедупликации(t *testing.T) {
	ctx := context.Background()
	url := bustest.StartTestServer(t)

	conn, err := Connect(ctx, Options{URLs: []string{url}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer conn.Close()

	// Поток «предыдущей версии»: всё то же, но окно старое.
	old := streamConfig()
	old.Duplicates = 5 * time.Minute
	if _, err := conn.JS().CreateStream(ctx, old); err != nil {
		t.Fatalf("поток старой версии: %v", err)
	}
	if _, err := conn.JS().CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: StateBucket}); err != nil {
		t.Fatalf("бакет: %v", err)
	}

	if err := CheckTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("узел не стартовал из-за расхождения версий: %v", err)
	}

	// Письма при старом окне ходят: хуже только защита от дублей.
	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")
	if err := Publish(ctx, conn.JS(), m); err != nil {
		t.Fatalf("публикация при старом окне: %v", err)
	}
}

// Мост приводит окно дедупликации к ожидаемому.
//
// Без этого поток, созданный до изменения, навсегда остался бы с пятиминутным
// окном: рестарт моста дольше пяти минут превращал бы одно сообщение человека
// в два письма, а детерминированный идентификатор от этого не спасает — за
// пределами окна он не значит ничего.
func TestМостПриводитОкноДедупликацииКОжидаемому(t *testing.T) {
	ctx := context.Background()
	url := bustest.StartTestServer(t)

	conn, err := Connect(ctx, Options{URLs: []string{url}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer conn.Close()

	old := streamConfig()
	old.Duplicates = 5 * time.Minute
	if _, err := conn.JS().CreateStream(ctx, old); err != nil {
		t.Fatalf("поток старой версии: %v", err)
	}

	if err := EnsureBridgeTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("приведение топологии мостом: %v", err)
	}

	stream, err := conn.JS().Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("поток: %v", err)
	}
	if got := stream.CachedInfo().Config.Duplicates; got != dedupWindow {
		t.Fatalf("окно дедупликации %s, ожидалось %s — миграция не сработала", got, dedupWindow)
	}
}

// Окно дедупликации не короче суток.
//
// Тест на само значение, а не на механизм приведения: тот остаётся зелёным
// при любой величине, лишь бы она применялась. А величина здесь смысловая.
//
// Мост даёт письму от человека идентификатор, выведенный из chat_id и
// update_id, чтобы рестарт не превращал одно сообщение в два. За пределами
// окна дедупликации такой идентификатор не значит ничего: сервер о нём уже
// забыл. Прежние пять минут были короче обычного рестарта — обновление,
// перезагрузка VPS, отладка, — то есть защита не работала ровно в том
// случае, ради которого её делали.
//
// Сутки взяты по сроку хранения обновлений в Telegram: за его пределами
// повторять уже нечего.
func TestОкноДедупликацииНеМеньшеСуток(t *testing.T) {
	const need = 24 * time.Hour
	if dedupWindow < need {
		t.Fatalf("окно дедупликации %s короче %s: детерминированный идентификатор "+
			"письма от человека перестанет защищать от дубля при рестарте моста",
			dedupWindow, need)
	}
}
