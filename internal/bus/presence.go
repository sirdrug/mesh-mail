package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Card — визитка агента.
//
// Отвечает на вопрос «кто занимается проектом X» — роутинг по возможностям
// полезнее, чем память о том, какой агент на какой машине живёт.
type Card struct {
	AgentID     string    `json:"agent_id"`
	Host        string    `json:"host"`
	Engine      string    `json:"engine"`
	Mode        string    `json:"mode,omitempty"`
	Projects    []string  `json:"projects,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	AnnouncedAt time.Time `json:"announced_at"`
	TTLSeconds  int       `json:"ttl_seconds"`
}

// IsStale — не пропал ли агент.
func (c Card) IsStale(now time.Time) bool {
	if c.TTLSeconds <= 0 {
		return false
	}
	return now.Sub(c.AnnouncedAt) > time.Duration(c.TTLSeconds)*time.Second
}

// PresenceSubject — тема визиток агента.
//
// Публиковать в свою тему может только сам агент: право прописано на хабе
// поимённо, поэтому подделать чужую визитку нельзя.
func PresenceSubject(agentID string) string {
	return "agents." + agentID + ".presence"
}

const presenceWildcard = "agents.*.presence"

// Announce публикует визитку.
//
// Core NATS, а не JetStream: визитка живёт до следующей публикации, хранить
// её историю незачем.
func Announce(ctx context.Context, nc *nats.Conn, card Card) error {
	payload, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("сериализация визитки: %w", err)
	}
	if err := nc.Publish(PresenceSubject(card.AgentID), payload); err != nil {
		return fmt.Errorf("публикация визитки: %w", err)
	}
	return nc.Flush()
}

// PresenceInterval — как часто узел переизлучает визитку.
//
// Отсюда же берётся окно прогрева подписчика: визитки нигде не хранятся, и
// новый процесс узнаёт о соседе только когда тот объявится снова. Значит
// раньше, чем через интервал, отсутствие визитки ничего не означает.
const PresenceInterval = 60 * time.Second

// Registry — известные визитки.
type Registry struct {
	mu    sync.RWMutex
	cards map[string]Card
}

func NewRegistry() *Registry {
	return &Registry{cards: make(map[string]Card)}
}

// Upsert кладёт визитку в реестр.
//
// Возвращает true, если агент новый или изменил содержимое визитки — мосту
// это повод написать в канал, а повторные объявления по таймеру поводом
// не являются.
func (r *Registry) Upsert(card Card) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev, existed := r.cards[card.AgentID]
	r.cards[card.AgentID] = card
	if !existed {
		return true
	}

	// Время объявления меняется каждый раз, содержимое — нет.
	prev.AnnouncedAt = card.AnnouncedAt
	return !reflect.DeepEqual(prev, card)
}

func (r *Registry) Get(agentID string) (Card, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	card, ok := r.cards[agentID]
	return card, ok
}

// Alive — визитки, которые ещё не протухли.
func (r *Registry) Alive() []Card {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now().UTC()
	var out []Card
	for _, card := range r.cards {
		if !card.IsStale(now) {
			out = append(out, card)
		}
	}
	return out
}

// Find ищет живых агентов по проекту и тегу. Пустой фильтр не сужает.
func (r *Registry) Find(project, tag string) []Card {
	var out []Card
	for _, card := range r.Alive() {
		if project != "" && !contains(card.Projects, project) {
			continue
		}
		if tag != "" && !contains(card.Tags, tag) {
			continue
		}
		out = append(out, card)
	}
	return out
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// AgentIDFromPresence достаёт идентификатор агента из темы визитки.
//
// Пустая строка означает, что тема не подходит под шаблон.
func AgentIDFromPresence(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) != 3 || parts[0] != "agents" || parts[2] != "presence" {
		return ""
	}
	return parts[1]
}

// WatchPresence подписывает реестр на визитки соседей.
//
// Идентификатор берётся из ТЕМЫ, а не из тела письма. Публиковать в чужую
// тему сервер не даёт, а вот положить в свою визитку чужой agent_id никто
// не мешает: раньше pi-claude мог отправить в agents.pi-claude.presence
// карточку с agent_id соседа и перезаписать её в реестрах всей сети —
// вместе с проектами, по которым идёт роутинг.
//
// Тема удостоверена правами на хабе, тело — нет. Поэтому тело подчиняется
// теме, а не наоборот.
func WatchPresence(ctx context.Context, nc *nats.Conn, reg *Registry) error {
	sub, err := nc.Subscribe(presenceWildcard, func(msg *nats.Msg) {
		owner := AgentIDFromPresence(msg.Subject)
		if owner == "" {
			return
		}

		var card Card
		if err := json.Unmarshal(msg.Data, &card); err != nil {
			return // битая визитка не повод ронять узел
		}
		card.AgentID = owner

		reg.Upsert(card)
	})
	if err != nil {
		return fmt.Errorf("подписка на визитки: %w", err)
	}

	// Барьер обязателен: Subscribe кладёт запрос в буфер соединения и
	// возвращается, не дожидаясь сервера. Пока сервер о подписке не узнал,
	// визитки проходят мимо — молча, потому что Core NATS ничего не
	// переигрывает. Измерено: без барьера теряется около двух третей визиток,
	// объявленных сразу после подписки.
	//
	// На публикации такой барьер уже стоял — Announce делает Flush после
	// Publish. Здесь его просто забыли.
	if err := nc.Flush(); err != nil {
		return fmt.Errorf("подписка на визитки не подтверждена сервером: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}
