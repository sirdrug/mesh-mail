package bus

import (
	"context"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/bustest"

	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/nats-io/nats.go/jetstream"
)

func setupBus(t *testing.T) *Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := Connect(ctx, Options{URLs: []string{bustest.StartTestServer(t)}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	t.Cleanup(conn.Close)
	if err := EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("топология: %v", err)
	}
	return conn
}

func countInSubject(t *testing.T, conn *Conn, subject string) int {
	t.Helper()
	ctx := context.Background()
	stream, err := conn.JS().Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("поток: %v", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	batch, err := cons.Fetch(100, jetstream.FetchMaxWait(fetchWait))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	n := 0
	for range batch.Messages() {
		n++
	}
	return n
}

func TestPublishДоставляетКаждомуПолучателю(t *testing.T) {
	conn := setupBus(t)
	m := mail.New("pi-claude", []string{"m1-codex", "mbp-claude"}, "тема", "тело")
	m.CC = []string{"pi-codex"}

	if err := Publish(context.Background(), conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	for _, id := range []string{"m1-codex", "mbp-claude", "pi-codex"} {
		if n := countInSubject(t, conn, MailInboxFilter(id)); n != 1 {
			t.Errorf("в ящике %s писем %d, ожидалось 1", id, n)
		}
	}
}

func TestPublishДедуплицируетПовтор(t *testing.T) {
	conn := setupBus(t)
	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")

	for i := 0; i < 3; i++ {
		if err := Publish(context.Background(), conn.JS(), m); err != nil {
			t.Fatalf("публикация %d: %v", i, err)
		}
	}

	if n := countInSubject(t, conn, MailInboxFilter("m1-codex")); n != 1 {
		t.Fatalf("после трёх публикаций в ящике %d писем, ожидалось 1", n)
	}
}

func TestPublishОтвергаетНевалидное(t *testing.T) {
	conn := setupBus(t)
	m := mail.New("pi-claude", nil, "тема", "тело") // без получателей

	if err := Publish(context.Background(), conn.JS(), m); err == nil {
		t.Fatal("невалидное письмо опубликовано")
	}
}

// Адресат, названный дважды, будит агента один раз.
//
// Считается не содержимое потока, а ЧИСЛО ПУБЛИКАЦИЙ, которые видит core-
// подписчик, — потому что именно так устроен сторож. Разница существенна:
// поток отбрасывает вторую копию по одинаковому Nats-Msg-Id, поэтому проверка
// «в ящике одно письмо» зелена и с дублем, и без него. Она подтверждала бы
// дедупликацию JetStream, а не наше исправление.
//
// Сторож же подписан на тему напрямую и получает обе публикации ДО того, как
// поток решит их судьбу. Отсюда и жалоба, с которой всё началось: два
// уведомления об одном письме.
func TestPublishБудитПолучателяОдинРаз(t *testing.T) {
	conn := setupBus(t)

	sub, err := conn.NC().SubscribeSync(MailInboxFilter("m1-codex"))
	if err != nil {
		t.Fatalf("подписка: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := conn.NC().Flush(); err != nil {
		t.Fatalf("сброс: %v", err)
	}

	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")
	m.CC = []string{"m1-codex"} // тот же адресат вторым полем

	if err := Publish(context.Background(), conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	first, err := sub.NextMsg(fetchWait)
	if err != nil {
		t.Fatalf("первая публикация не пришла вовсе: %v", err)
	}
	if got := SenderFromSubject(first.Subject); got != "pi-claude" {
		t.Fatalf("отправитель в теме %q, ожидался pi-claude", got)
	}

	// Вторая приходить не должна. Ждём столько же, сколько ждали первую:
	// короткое окно сделало бы тест зелёным просто потому, что мы не
	// дождались дубля.
	if second, err := sub.NextMsg(fetchWait); err == nil {
		t.Fatalf("пришла вторая публикация того же письма (тема %s) — сторож разбудит агента дважды",
			second.Subject)
	}
}

// Контроль к предыдущему: разным адресатам по-прежнему уходит по публикации.
//
// Без него «дедупликация», схлопнувшая список до одного получателя, выглядела
// бы почином: одно пробуждение вместо двух, тест зелёный, половина сети без
// писем.
func TestPublishБудитКаждогоИзРазныхПолучателей(t *testing.T) {
	conn := setupBus(t)

	sub, err := conn.NC().SubscribeSync(mailSubjectPrefix + ">")
	if err != nil {
		t.Fatalf("подписка: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := conn.NC().Flush(); err != nil {
		t.Fatalf("сброс: %v", err)
	}

	m := mail.New("pi-claude", []string{"m1-codex", "mbp-claude"}, "тема", "тело")
	m.CC = []string{"pi-codex", "m1-codex"} // один из них повторён

	if err := Publish(context.Background(), conn.JS(), m); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	seen := map[string]int{}
	for {
		msg, err := sub.NextMsg(fetchWait)
		if err != nil {
			break
		}
		seen[msg.Subject]++
	}

	for _, id := range []string{"m1-codex", "mbp-claude", "pi-codex"} {
		subject := MailSubject(id, "pi-claude")
		switch seen[subject] {
		case 1:
		case 0:
			t.Errorf("%s не получил письма вовсе", id)
		default:
			t.Errorf("%s разбужен %d раза одним письмом", id, seen[subject])
		}
	}
}
