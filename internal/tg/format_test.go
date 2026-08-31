package tg

import (
	"strings"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
)

func TestFormatMessageЭкранируетHTML(t *testing.T) {
	m := mail.New("pi-claude", []string{"m1-codex"}, "<b>жирная тема</b>", "текст с <script>alert(1)</script>")

	got := FormatMessage(m).Text

	if strings.Contains(got, "<script>") {
		t.Fatalf("тег из письма не экранирован — сломает разметку канала: %q", got)
	}
	if !strings.Contains(got, "&lt;script&gt;") {
		t.Fatalf("экранирование не сработало: %q", got)
	}
}

func TestFormatMessageПоказываетУчастниковИТему(t *testing.T) {
	m := mail.New("pi-claude", []string{"m1-codex"}, "нужен дамп", "тело письма")

	got := FormatMessage(m).Text

	for _, want := range []string{"pi-claude", "m1-codex", "нужен дамп", "тело письма"} {
		if !strings.Contains(got, want) {
			t.Errorf("в посте нет %q: %q", want, got)
		}
	}
}

func TestFormatMessageОбрезаетДлинноеТело(t *testing.T) {
	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", strings.Repeat("я", BodyLimit*2))

	got := FormatMessage(m).Text

	if len([]rune(got)) > BodyLimit+500 {
		t.Fatalf("пост длиной %d рун — тело не обрезано", len([]rune(got)))
	}
	if !strings.Contains(got, "обрезано") {
		t.Fatal("обрезка не помечена, человек не поймёт, что видит не всё")
	}
}

func TestFormatMessageПомечаетСрочное(t *testing.T) {
	m := mail.New("pi-claude", []string{"m1-codex"}, "тема", "тело")
	m.Importance = mail.ImportanceUrgent

	if !strings.Contains(FormatMessage(m).Text, "срочно") {
		t.Fatal("срочность не видна в канале")
	}
}

func TestFormatCardПоказываетПроекты(t *testing.T) {
	card := bus.Card{
		AgentID: "m1-claude", Host: "macbook-m1", Engine: "claude",
		Projects: []string{"dns-watcher", "kumo"}, AnnouncedAt: time.Now().UTC(), TTLSeconds: 180,
	}

	got := FormatCard(card)

	for _, want := range []string{"m1-claude", "macbook-m1", "dns-watcher", "kumo"} {
		if !strings.Contains(got, want) {
			t.Errorf("в визитке нет %q: %q", want, got)
		}
	}
}

func TestTopicNameКороткоеИСодержательное(t *testing.T) {
	m := mail.New("pi-claude", []string{"m1-codex"}, strings.Repeat("длинная тема ", 20), "тело")

	got := TopicName(m)

	if len([]rune(got)) > 128 {
		t.Fatalf("имя темы длиной %d рун — Telegram обрежет само", len([]rune(got)))
	}
	if !strings.Contains(got, "pi-claude") {
		t.Fatalf("в имени темы нет отправителя: %q", got)
	}
}
