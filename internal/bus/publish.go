package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fetchWait — сколько ждать сообщения при чтении пачки.
const fetchWait = 500 * time.Millisecond

// Publish кладёт письмо в ящик каждому получателю.
//
// Копия на получателя, а не одно сообщение с веером: так ящик агента — это
// ровно одна тема, и права на хабе выражаются одной строкой.
func Publish(ctx context.Context, js jetstream.JetStream, m *mail.Message) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("письмо не прошло валидацию: %w", err)
	}

	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("сериализация письма: %w", err)
	}

	for _, recipient := range m.Recipients() {
		msg := &nats.Msg{
			Subject: MailSubject(recipient, m.From),
			Data:    payload,
			Header: nats.Header{
				// Ключ дедупликации: пара «письмо + получатель». Именно пара, а не
				// один ID письма: иначе копия для второго адресата будет принята
				// за повтор первой и молча пропадёт.
				"Nats-Msg-Id": []string{m.ID + "/" + recipient},
			},
		}
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("публикация в ящик %s: %w", recipient, err)
		}
	}

	return nil
}
