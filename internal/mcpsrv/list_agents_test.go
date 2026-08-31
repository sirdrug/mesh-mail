package mcpsrv

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/bustest"
	"github.com/boreevyuri/mesh-mail/internal/config"
)

// Окно прогрева в тестах — сотые доли секунды вместо минуты. Уменьшать
// безопасно: другого ограничителя на этом пути нет, и малое значение здесь
// ничего не выключает — я проверил, что от длительности зависит только сам
// момент перехода из «прогреваюсь» в «наблюдаю».
const testWarmup = 80 * time.Millisecond

func setupAgents(t *testing.T) *handlers {
	t.Helper()
	h, _ := setup(t)
	h.presence.warmup = testWarmup
	h.presence.since = time.Now()
	return h
}

// TestХолодныйСтартНеВыдаётсяЗаПустуюСеть — главный дефект.
//
// Реестр наполняется только подпиской, а визитки приходят раз в интервал.
// Разовый mesh mcp, поднятый под вызов инструмента, отвечает раньше первой
// визитки — и отвечал пустым списком, неотличимым от «в сети никого нет».
// Замер на живой сети: 0 агентов через 0.3 с, двое через 20 с, пятеро через 45.
func TestХолодныйСтартНеВыдаётсяЗаПустуюСеть(t *testing.T) {
	ctx := context.Background()
	h := setupAgents(t)

	_, out, err := h.listAgents(ctx, nil, ListAgentsIn{})
	if err == nil {
		t.Fatalf("холодный старт вернул список без ошибки: %+v", out)
	}
	if !strings.Contains(err.Error(), "наблюдение") {
		t.Fatalf("отказ не говорит, что наблюдение ещё идёт: %v", err)
	}
	if strings.Contains(err.Error(), "нет агентов") || strings.Contains(err.Error(), "никого") {
		t.Fatalf("отказ утверждает пустоту сети: %v", err)
	}
}

// TestПрогретоеНаблюдениеНеУтверждаетПустотуСети — пустой список после
// прогрева допустим, но означает не то же самое.
//
// Полноты доказать нельзя ничем: визитки идут без хранения и переигрывания,
// потерянное не восстанавливается. Поэтому даже прогретый ответ описывает
// наблюдение, а не сеть.
func TestПрогретоеНаблюдениеНеУтверждаетПустотуСети(t *testing.T) {
	ctx := context.Background()
	h := setupAgents(t)
	time.Sleep(testWarmup * 2)

	_, out, err := h.listAgents(ctx, nil, ListAgentsIn{})
	if err != nil {
		t.Fatalf("прогретое наблюдение вернуло ошибку: %v", err)
	}
	if len(out.Agents) != 0 {
		t.Fatalf("в пустой сети нашлись агенты: %+v", out.Agents)
	}
	if out.Note == "" {
		t.Fatal("ответ не объясняет, чем является пустой список")
	}
	if !strings.Contains(out.Note, "не наблюдал") {
		t.Fatalf("пустой список не назван отсутствием наблюдений: %q", out.Note)
	}
}

// TestНаблюдавшийсяАгентПопадаетВСписок — то, ради чего инструмент есть.
func TestНаблюдавшийсяАгентПопадаетВСписок(t *testing.T) {
	ctx := context.Background()
	h := setupAgents(t)

	h.reg.Upsert(bus.Card{
		AgentID: "pi-codex", Host: "pi", Engine: "codex",
		Projects: []string{"mesh-mail"}, AnnouncedAt: time.Now().UTC(), TTLSeconds: 180,
	})

	_, out, err := h.listAgents(ctx, nil, ListAgentsIn{})
	if err != nil {
		t.Fatalf("список с наблюдавшимся агентом вернул ошибку: %v", err)
	}
	if len(out.Agents) != 1 || out.Agents[0].AgentID != "pi-codex" {
		t.Fatalf("наблюдавшийся агент не попал в список: %+v", out.Agents)
	}
}

// TestДоПрогреваИзвестныеАгентыОтдаютсяАНеСкрываются — отказ не должен
// прятать то, что уже услышано.
//
// Незавершённость наблюдения — причина не утверждать пустоту, а не повод
// молчать о том, кто уже объявился.
func TestДоПрогреваИзвестныеАгентыОтдаютсяАНеСкрываются(t *testing.T) {
	ctx := context.Background()
	h := setupAgents(t)

	h.reg.Upsert(bus.Card{
		AgentID: "pi-claude", Host: "pi", Engine: "claude",
		AnnouncedAt: time.Now().UTC(), TTLSeconds: 180,
	})

	_, out, err := h.listAgents(ctx, nil, ListAgentsIn{})
	if err != nil {
		t.Fatalf("известный агент скрыт за отказом холодного старта: %v", err)
	}
	if len(out.Agents) != 1 {
		t.Fatalf("известный агент не отдан: %+v", out.Agents)
	}
	if !strings.Contains(out.Note, "наблюдение") {
		t.Fatalf("ответ не отмечает, что наблюдение ещё неполно: %q", out.Note)
	}
}

// TestРазрывСвязиНеВыдаётсяЗаПустуюСеть — пока связи нет, визитки не идут
// вовсе, и молчание не значит ничего.
func TestРазрывСвязиНеВыдаётсяЗаПустуюСеть(t *testing.T) {
	ctx := context.Background()
	h := setupAgents(t)
	time.Sleep(testWarmup * 2)

	h.conn.NC().Close() // связь потеряна

	_, _, err := h.listAgents(ctx, nil, ListAgentsIn{})
	if err == nil {
		t.Fatal("при потерянной связи вернулся список")
	}
	if !strings.Contains(err.Error(), "связь") {
		t.Fatalf("отказ не называет причину — разрыв связи: %v", err)
	}
}

// TestПереподключениеНачинаетНаблюдениеЗаново — после разрыва реестр снова
// неполон, сколько бы процесс ни работал до этого.
//
// Подписка разрыв переживает, но визитки, пришедшиеся на окно разрыва,
// теряются безвозвратно: Core NATS не переигрывает, а объявление во время
// разрыва отправитель даже не может доставить — измерено, ему возвращается
// таймаут. Значит отсчитывать наблюдение от старта процесса нельзя: узел,
// объявившийся, пока связи не было, для нас не существует до своего
// следующего тикера.
func TestПереподключениеНачинаетНаблюдениеЗаново(t *testing.T) {
	ctx := context.Background()
	url, stop, start := bustest.StartRestartable(t)

	conn, err := bus.Connect(ctx, bus.Options{URLs: []string{url}, Name: "test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	t.Cleanup(conn.Close)
	if err := bus.EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("топология: %v", err)
	}
	node := &config.Node{AgentID: "m1-claude", Host: "macbook-m1", Engine: "claude"}
	h := &handlers{
		conn: conn, reg: bus.NewRegistry(), node: node, search: productionSearch(),
		presence: presenceWatch{warmup: testWarmup, since: time.Now()},
	}

	// Наблюдение установилось: ответ отдаётся без отказа.
	time.Sleep(testWarmup * 2)
	if _, _, err := h.listAgents(ctx, nil, ListAgentsIn{}); err != nil {
		t.Fatalf("прогретое наблюдение вернуло отказ: %v", err)
	}

	stop()
	time.Sleep(300 * time.Millisecond)
	start()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && conn.NC().Stats().Reconnects == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if conn.NC().Stats().Reconnects == 0 {
		t.Skip("переподключение не состоялось за отведённое время")
	}

	// Сразу после переподключения наблюдение должно начаться заново.
	_, _, err = h.listAgents(ctx, nil, ListAgentsIn{})
	if err == nil {
		t.Fatal("после переподключения ответ выдан так, будто наблюдение не прерывалось")
	}
	if !strings.Contains(err.Error(), "наблюдение за визитками идёт") {
		t.Fatalf("отказ не сообщает о повторном прогреве: %v", err)
	}
}
