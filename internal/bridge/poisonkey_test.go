package bridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
)

// Идентификатор письма приходит из ТЕЛА и ничем не проверяется.
//
// mail.Validate смотрит на отправителя, получателей, тему, размер и hops —
// но не на id. То есть в id лежит то, что написал отправитель, а мы кладём
// его в ключ KV, где допустимы далеко не любые символы.
//
// Различающий случай: письмо с идентификатором, который KV принять не может.
// Если отметка о показе не сохранится, письмо будет возвращаться в поток и
// показываться СНОВА И СНОВА — не потеря, а размножение: человек получит
// один и тот же пост столько раз, сколько выдержит терпение.
func TestПисьмоСНепригоднымИдентификаторомНеРазмножается(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, conn := newStore(t)
	poster := &flakyPoster{}
	show := NewShowcase(conn.JS(), store, poster, "-1001", false)
	go func() { _ = show.Run(ctx) }()

	m := mail.New("pi-claude", []string{"m1-codex"}, "письмо с кривым id", "тело")
	// Ключи KV ограничены набором символов; пробелы и кириллица в них
	// недопустимы. Отправителю ничто не мешает прислать такой id.
	m.ID = "не ключ вовсе"

	payload, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("сериализация: %v", err)
	}
	if _, err := conn.JS().Publish(ctx, bus.MailSubject("m1-codex", "pi-claude"), payload); err != nil {
		t.Fatalf("публикация: %v", err)
	}

	waitFor(t, func() bool {
		posts, _, _ := poster.seen()
		return len(posts) > 0
	}, "показ письма")

	// Ждём дольше первой паузы повтора: если отметка не сохранилась, письмо
	// вернётся и покажется ещё раз.
	time.Sleep(4 * time.Second)

	posts, _, _ := poster.seen()
	if len(posts) != 1 {
		t.Fatalf("письмо показано %d раз: отметка о показе не сохраняется, "+
			"и каждый повтор доставки даёт человеку новый пост", len(posts))
	}
}
