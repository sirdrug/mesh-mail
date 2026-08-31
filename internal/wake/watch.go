// Package wake будит агента, когда пришло письмо.
//
// Ключевой принцип: пробуждение переносит только факт «тебе письмо».
// Само письмо лежит в JetStream и забирается инструментом fetch_inbox.
// Поэтому сигнал имеет право промахнуться — ничего не потеряется.
//
// Из этого же следует, что тело письма в уведомление не попадает: иначе
// недоверенный текст оказался бы в TUI, где агент читает команды человека.
package wake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/nats-io/nats.go"
)

// noticeSubjectLimit — сколько символов темы показываем в уведомлении.
const noticeSubjectLimit = 70

// Notice — однострочное уведомление о письме.
//
// Строго одна строка: Monitor доставляет каждую строку stdout как отдельное
// событие, и многострочный текст превратился бы в несколько пробуждений.
func Notice(m *mail.Message) string {
	subject := m.Subject
	if len([]rune(subject)) > noticeSubjectLimit {
		subject = string([]rune(subject)[:noticeSubjectLimit]) + "…"
	}

	mark := ""
	switch m.Importance {
	case mail.ImportanceUrgent:
		mark = " [срочно]"
	case mail.ImportanceHigh:
		mark = " [важно]"
	}

	return fmt.Sprintf("📬 письмо от %s%s — %s (id=%s)", m.From, mark, subject, m.ID)
}

// Watch печатает уведомление на каждое входящее письмо.
//
// Подписка обычная, а не потребление из потока: иначе сторож «съел» бы письмо
// и fetch_inbox вернул бы пустоту. Позицию прочитанного двигает только агент.
func Watch(ctx context.Context, nc *nats.Conn, agentID string, out io.Writer) error {
	sub, err := nc.Subscribe(bus.MailInboxFilter(agentID), func(msg *nats.Msg) {
		var m mail.Message
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			return // битое письмо не повод шуметь в TUI
		}
		// Отправитель берётся из ТЕМЫ, а не из тела. Тему удостоверил хаб
		// правом publish: mail.*.<свой_id>, а поле from в теле — всего лишь
		// заявление. Без этой строки любой узел писал бы в свой законный
		// субъект, указав в теле `from: human`, и сторож печатал бы человеку
		// «письмо от human» — от самого авторитетного отправителя в сети.
		m.From = bus.SenderForDisplay(msg.Subject)
		// Ошибку записи глотаем сознательно: писать некуда, а письмо уже
		// в ящике — агент увидит его при следующем fetch_inbox.
		_, _ = fmt.Fprintln(out, Notice(&m))
	})
	if err != nil {
		return fmt.Errorf("подписка на ящик %s: %w", agentID, err)
	}

	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}
