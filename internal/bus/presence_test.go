package bus

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRegistryНаходитПоПроекту(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(Card{AgentID: "m1-claude", Projects: []string{"dns-watcher", "kumo"}, TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})
	reg.Upsert(Card{AgentID: "pi-codex", Projects: []string{"kumo"}, TTLSeconds: 180, AnnouncedAt: time.Now().UTC()})

	got := reg.Find("dns-watcher", "")
	if len(got) != 1 || got[0].AgentID != "m1-claude" {
		t.Fatalf("поиск по проекту вернул %+v", got)
	}
}

func TestRegistryСкрываетПротухшие(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(Card{
		AgentID:     "ушедший",
		TTLSeconds:  60,
		AnnouncedAt: time.Now().UTC().Add(-2 * time.Minute),
	})

	if alive := reg.Alive(); len(alive) != 0 {
		t.Fatalf("протухшая визитка попала в живые: %+v", alive)
	}
}

func TestUpsertСообщаетОбИзменении(t *testing.T) {
	reg := NewRegistry()
	card := Card{AgentID: "m1-claude", Projects: []string{"kumo"}, TTLSeconds: 180, AnnouncedAt: time.Now().UTC()}

	if !reg.Upsert(card) {
		t.Fatal("первая визитка должна считаться изменением")
	}

	card.AnnouncedAt = time.Now().UTC().Add(time.Second)
	if reg.Upsert(card) {
		t.Fatal("повтор той же визитки не должен считаться изменением")
	}

	card.Projects = []string{"kumo", "dns-watcher"}
	if !reg.Upsert(card) {
		t.Fatal("смена списка проектов должна считаться изменением")
	}
}

func TestAnnounceДоходитДоПодписчика(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := setupBus(t)
	reg := NewRegistry()
	if err := WatchPresence(ctx, conn.NC(), reg); err != nil {
		t.Fatalf("подписка на визитки: %v", err)
	}

	card := Card{
		AgentID:     "pi-claude",
		Host:        "raspberrypi",
		Engine:      "claude",
		Projects:    []string{"mesh-mail"},
		TTLSeconds:  180,
		AnnouncedAt: time.Now().UTC(),
	}
	if err := Announce(ctx, conn.NC(), card); err != nil {
		t.Fatalf("публикация визитки: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := reg.Get("pi-claude"); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("визитка не дошла до реестра за 2 секунды")
}

// Агент не должен подменять чужую визитку, публикуя её в свою тему.
//
// Права на хабе стерегут ТЕМУ: писать можно только в agents.<свой>.presence.
// Но тело письма ими не удостоверено, и раньше реестр верил полю agent_id
// оттуда — то есть сосед перезаписывал чужую карточку вместе с проектами,
// по которым идёт роутинг.
func TestВизиткаПринадлежитВладельцуТемы(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := setupBus(t)
	reg := NewRegistry()
	if err := WatchPresence(ctx, conn.NC(), reg); err != nil {
		t.Fatalf("подписка: %v", err)
	}

	// Публикуем в СВОЮ тему карточку, которая выдаёт себя за соседа.
	forged := Card{
		AgentID:     "m1-claude",
		Host:        "машина-злодея",
		Projects:    []string{"подменённый-проект"},
		TTLSeconds:  180,
		AnnouncedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(forged)
	if err != nil {
		t.Fatalf("сериализация: %v", err)
	}
	if err := conn.NC().Publish(PresenceSubject("pi-claude"), payload); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	_ = conn.NC().Flush()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := reg.Get("pi-claude"); ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, ok := reg.Get("m1-claude"); ok {
		t.Fatal("подделка прошла: в реестре появилась визитка m1-claude от чужой темы")
	}
	card, ok := reg.Get("pi-claude")
	if !ok {
		t.Fatal("визитка не попала в реестр вовсе")
	}
	if card.Host != "машина-злодея" {
		t.Fatalf("тело визитки потеряно: host = %q", card.Host)
	}
}

func TestAgentIDFromPresence(t *testing.T) {
	cases := map[string]string{
		"agents.pi-claude.presence": "pi-claude",
		"agents.presence":           "",
		"mail.pi-claude":            "",
		"agents.a.b.presence":       "",
	}
	for subject, want := range cases {
		if got := AgentIDFromPresence(subject); got != want {
			t.Errorf("AgentIDFromPresence(%q) = %q, ожидалось %q", subject, got, want)
		}
	}
}
