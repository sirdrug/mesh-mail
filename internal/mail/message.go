// Package mail описывает письмо — единицу обмена между агентами.
//
// Модель намеренно повторяет почтовую метафору (тема, тело, копия, тред):
// агенты уже умеют ей пользоваться по локальному mcp-agent-mail, а человек
// читает переписку глазами в телеграме.
package mail

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Важность письма. Влияет на то, будить ли простаивающего агента.
const (
	ImportanceNormal = "normal"
	ImportanceHigh   = "high"
	ImportanceUrgent = "urgent"
)

// MaxBodyBytes — предел тела письма.
//
// max_payload на хабе 4 МБ, но письмо на мегабайт — это не письмо, а ошибка
// отправителя. Лучше внятный отказ, чем мусор в ящике у всех получателей.
const MaxBodyBytes = 64 * 1024

// MaxHops — предел длины цепочки ответов.
//
// Два агента, увлечённо отвечающих друг другу, — единственный способ устроить
// в этой системе лавину. Счётчик обрывает её бесплатно.
const MaxHops = 8

// Message — письмо.
type Message struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id"`
	Project  string `json:"project,omitempty"`

	From string   `json:"from"`
	To   []string `json:"to"`
	CC   []string `json:"cc,omitempty"`

	Subject string `json:"subject"`
	Body    string `json:"body"`

	Importance  string `json:"importance"`
	AckRequired bool   `json:"ack_required,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	Hops      int       `json:"hops"`
}

// New создаёт письмо с новым тредом.
func New(from string, to []string, subject, body string) *Message {
	return &Message{
		ID:         uuid.NewString(),
		ThreadID:   uuid.NewString(),
		From:       from,
		To:         to,
		Subject:    subject,
		Body:       body,
		Importance: ImportanceNormal,
		CreatedAt:  time.Now().UTC(),
	}
}

// Reply строит ответ: тред наследуется, hops растёт, идентификатор новый.
//
// Новый ID обязателен — иначе окно дедупликации на потоке примет ответ за
// повтор исходного письма и молча его выбросит.
func (m *Message) Reply(from, body string) *Message {
	// Копия — остальные участники разговора. Автор письма уходит в To, сам
	// отвечающий не копируется себе же, порядок берётся из Participants и
	// потому стабилен.
	//
	// Раньше ответ адресовался РОВНО одному — автору письма, — и остальные
	// о продолжении не узнавали. Стоило это дорого: двое обсуждали правку и
	// меняли ветку, а координатор держал решение по устаревшему хешу, потому
	// что каждый ответ уходил мимо него. Хуже всего, что со стороны всё
	// выглядело исправным: витрина подписана на весь поток и показывала
	// человеку связный разговор, которого участники не видели.
	//
	// Граница у этой памяти одна, и о ней надо знать: состав берётся из
	// ОТВЕЧАЕМОГО письма. Тот, кто вошёл в тред отдельным письмом, в копию
	// не попадёт — Reply о нём просто не знает.
	var cc []string
	for _, p := range m.Participants() {
		if p == m.From || p == from {
			continue
		}
		cc = append(cc, p)
	}

	return &Message{
		ID:         uuid.NewString(),
		ThreadID:   m.ThreadID,
		Project:    m.Project,
		From:       from,
		To:         []string{m.From},
		CC:         cc,
		Subject:    replySubject(m.Subject),
		Body:       body,
		Importance: m.Importance,
		CreatedAt:  time.Now().UTC(),
		Hops:       m.Hops + 1,
	}
}

func replySubject(s string) string {
	if len(s) >= 4 && s[:4] == "Re: " {
		return s
	}
	return "Re: " + s
}

// Recipients возвращает всех, кому письмо должно быть доставлено, — каждого
// по одному разу.
//
// Дедупликация здесь, а не у вызывающих, потому что это единственная точка,
// через которую адресаты идут и в bus.Publish, и в Validate: почини её в
// одном месте — и оба пути становятся честными разом.
//
// Дубли приходят двумя дорогами, и обе настоящие. Человек ставит агента и в
// получатели, и в копию. Мост берёт список участников темы, куда отправитель
// уже добавлен отдельно, и передаёт его как получателей письма от человека.
// Дальше bus.Publish делает публикацию на каждый элемент среза: JetStream
// отбрасывает вторую по одинаковому Nats-Msg-Id, поток остаётся чистым, но
// core-подписчик видит обе публикации ДО того, как поток решит их судьбу, —
// и сторож будит агента дважды на одно письмо.
//
// Порядок сохраняется: сначала To в своём порядке, потом CC. Он виден и в
// теме публикации, и человеку в витрине, поэтому перебирать map здесь нельзя.
func (m *Message) Recipients() []string {
	all := make([]string, 0, len(m.To)+len(m.CC))
	seen := make(map[string]struct{}, len(m.To)+len(m.CC))

	add := func(list []string) {
		for _, r := range list {
			if _, dup := seen[r]; dup {
				continue
			}
			seen[r] = struct{}{}
			all = append(all, r)
		}
	}
	add(m.To)
	add(m.CC)

	return all
}

// Participants — все стороны разговора: отправитель и получатели, каждый по
// разу, отправитель первым.
//
// Нужен витрине, которая заводит по разговору тему в Telegram и хранит в ней
// состав участников. Складывать «From плюс Recipients» на месте вызова она и
// делала — и складывала с дублем, когда отправитель сам оказывался среди
// получателей. Дальше этот список уходил в intake.route как перечень
// АДРЕСАТОВ письма от человека, то есть дубль из записи в KV превращался в
// лишнюю публикацию.
//
// Пока сложение живёт здесь, рядом с Recipients() и её дедупликацией,
// повторить ту ошибку негде.
func (m *Message) Participants() []string {
	recipients := m.Recipients()

	all := make([]string, 0, len(recipients)+1)
	all = append(all, m.From)
	for _, r := range recipients {
		if r != m.From {
			all = append(all, r)
		}
	}
	return all
}

// Validate проверяет письмо перед отправкой.
func (m *Message) Validate() error {
	if m.From == "" {
		return errors.New("не указан отправитель")
	}
	if len(m.Recipients()) == 0 {
		return errors.New("не указан ни один получатель")
	}
	if m.Subject == "" {
		return errors.New("пустая тема")
	}
	if len(m.Body) > MaxBodyBytes {
		return fmt.Errorf("тело письма %d байт при лимите %d", len(m.Body), MaxBodyBytes)
	}
	switch m.Importance {
	case ImportanceNormal, ImportanceHigh, ImportanceUrgent:
	default:
		return fmt.Errorf("неизвестная важность %q", m.Importance)
	}
	if m.Hops >= MaxHops {
		return fmt.Errorf("цепочка достигла предела в %d пересылок", MaxHops)
	}
	return nil
}
