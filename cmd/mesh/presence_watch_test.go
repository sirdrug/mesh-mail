package main

// Сторож обязан объявлять присутствие. Проверяется это ТОЛЬКО с другого узла:
// изнутри процесса «я объявил» неотличимо от «я думаю, что объявил», а цена
// ошибки — тихо потерянный участник рассылки.

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/bustest"
	"github.com/boreevyuri/mesh-mail/internal/config"
)

// Сторож появляется в реестре СОСЕДА, а не только в своём коде.
//
// Дефект, ради которого написан тест: из трёх команд визитку излучали две, а
// `mesh watch` — нет. Узел, где работает только сторож, выпадал из адресатов
// общего чата через TTL визитки, и человек, написавший «всем», звал не всех.
// Отказ молчаливый с обеих сторон.
//
// Проверять наличие вызова в коде бессмысленно: это проверка намерения.
// Свойство формулируется снаружи — «сосед видит узел живым», — и меряться
// должно так же.
func TestСторожОбъявляетПрисутствиеСоседям(t *testing.T) {
	url := bustest.StartTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Сосед подписывается ПЕРВЫМ: визитка идёт обычным NATS без хранения, и
	// подписчик, опоздавший к публикации, её не увидит.
	соседний := connectТест(t, url, "сосед")
	реестр := bus.NewRegistry()
	if err := bus.WatchPresence(ctx, соседний.NC(), реестр); err != nil {
		t.Fatalf("подписка соседа на визитки: %v", err)
	}

	сторожевой := connectТест(t, url, "узел-со-сторожем")
	node := &config.Node{
		AgentID:  "узел-со-сторожем",
		Host:     "тестовый-хост",
		Engine:   "claude",
		Projects: []string{"mesh-mail"},
	}
	go func() {
		_ = runWatch(ctx, сторожевой, node, log.New(io.Discard, "", 0))
	}()

	card := ждатьКарточку(t, реестр, "узел-со-сторожем")

	if card.Host != "тестовый-хост" || card.Engine != "claude" {
		t.Errorf("визитка пришла обеднённой: %+v", card)
	}

	// Конечный срок жизни — часть свойства, а не придирка. При TTLSeconds
	// равном нулю IsStale всегда false: узел числился бы живым вечно, в том
	// числе через сутки после выключения ноутбука, и человек ждал бы ответа
	// от того, кого нет.
	if card.TTLSeconds <= 0 {
		t.Errorf("TTLSeconds = %d: визитка никогда не протухнет", card.TTLSeconds)
	}
	// Срок должен переживать пропуск одного-двух объявлений, иначе узел будет
	// мигать между «жив» и «нет» на любой заминке сети.
	if card.TTLSeconds < int(presenceInterval.Seconds())*2 {
		t.Errorf("TTLSeconds = %d при интервале %s: одна пропущенная визитка гасит узел",
			card.TTLSeconds, presenceInterval)
	}
}

// Протухание — вторая половина того же свойства.
//
// Живой узел обязан появляться, ушедший — исчезать. Проверяется на карточке,
// пришедшей ПО СЕТИ от настоящего сторожа: подделанная в тесте карточка
// доказывала бы только то, что мы умеем считать время.
func TestВизиткаСторожаПротухаетПоСвоемуСроку(t *testing.T) {
	url := bustest.StartTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	соседний := connectТест(t, url, "сосед-2")
	реестр := bus.NewRegistry()
	if err := bus.WatchPresence(ctx, соседний.NC(), реестр); err != nil {
		t.Fatalf("подписка соседа на визитки: %v", err)
	}

	сторожевой := connectТест(t, url, "уходящий-узел")
	node := &config.Node{AgentID: "уходящий-узел", Host: "хост", Engine: "codex"}
	go func() {
		_ = runWatch(ctx, сторожевой, node, log.New(io.Discard, "", 0))
	}()

	card := ждатьКарточку(t, реестр, "уходящий-узел")

	// Часы вперёд не переводим и минуты не ждём: IsStale считает от
	// AnnouncedAt, поэтому достаточно спросить его про будущее.
	будущее := card.AnnouncedAt.Add(time.Duration(card.TTLSeconds)*time.Second + time.Second)
	if !card.IsStale(будущее) {
		t.Errorf("визитка не протухла через %d+1 секунд после объявления", card.TTLSeconds)
	}
	if card.IsStale(card.AnnouncedAt.Add(time.Second)) {
		t.Error("визитка протухла через секунду после объявления")
	}
}

// connectТест подключает узел к тестовому серверу тем же путём, что и mesh.
func connectТест(t *testing.T, url, agentID string) *bus.Conn {
	t.Helper()
	conn, err := bus.Connect(context.Background(), bus.Options{
		URLs:    []string{url},
		Name:    "mesh-test-" + agentID,
		AgentID: agentID,
	})
	if err != nil {
		t.Fatalf("подключение %s: %v", agentID, err)
	}
	t.Cleanup(conn.Close)
	return conn
}

// ждатьКарточку опрашивает реестр, пока визитка не появится.
func ждатьКарточку(t *testing.T, реестр *bus.Registry, agentID string) bus.Card {
	t.Helper()
	срок := time.Now().Add(5 * time.Second)
	for time.Now().Before(срок) {
		if card, ok := реестр.Get(agentID); ok {
			return card
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("сосед не увидел визитку %s за 5 секунд: узел объявляет присутствие только на словах", agentID)
	return bus.Card{}
}
