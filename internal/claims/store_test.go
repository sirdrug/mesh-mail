package claims

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/bustest"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()

	conn, err := bus.Connect(ctx, bus.Options{URLs: []string{bustest.StartTestServer(t)}, Name: "claims-test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	t.Cleanup(conn.Close)

	// Реестр поднимает мост, а не агент, — поэтому и в тестах бакет заводится
	// отдельным шагом. Тестовый сервер без прав, так что роль моста здесь
	// играет то же соединение; разделение прав проверяется в topology_test.go,
	// где у учёток разные полномочия.
	if err := EnsureBucket(ctx, conn.JS()); err != nil {
		t.Fatalf("создание реестра: %v", err)
	}

	store, err := NewStore(ctx, conn.JS())
	if err != nil {
		t.Fatalf("реестр: %v", err)
	}
	return store
}

func TestЗахватВидноВсем(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "internal/keygen", "mbp-claude", "генерация ключей"); err != nil {
		t.Fatalf("захват: %v", err)
	}

	held, err := store.List(ctx)
	if err != nil {
		t.Fatalf("список: %v", err)
	}
	if len(held) != 1 {
		t.Fatalf("захватов %d, ожидался один", len(held))
	}
	if held[0].AgentID != "mbp-claude" || held[0].Zone != "internal/keygen" {
		t.Fatalf("захват записан неверно: %+v", held[0])
	}
	if held[0].Note != "генерация ключей" {
		t.Fatalf("пропала причина захвата: %+v", held[0])
	}
}

func TestЗанятуюЗонуВторымНеВзять(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "internal/keygen", "mbp-claude", "пишу keygen"); err != nil {
		t.Fatalf("первый захват: %v", err)
	}

	_, err := store.Take(ctx, "internal/keygen", "pi-claude", "тоже пишу keygen")

	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("второй захват вернул %v, ожидался конфликт", err)
	}
	// Человеку нужно понять, к кому идти договариваться.
	if conflict.Held.AgentID != "mbp-claude" {
		t.Fatalf("в конфликте не указан держатель: %+v", conflict.Held)
	}
	if conflict.Held.Note != "пишу keygen" {
		t.Fatalf("в конфликте нет причины: %+v", conflict.Held)
	}
}

// Ровно наш сегодняшний случай: один держит каталог, другой лезет внутрь.
func TestВложеннуюЗонуТожеНеВзять(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "internal", "mbp-claude", "правлю пакеты"); err != nil {
		t.Fatalf("захват каталога: %v", err)
	}

	if _, err := store.Take(ctx, "internal/keygen", "pi-claude", "а я тут"); err == nil {
		t.Fatal("вложенная зона захвачена поверх занятого каталога")
	}

	// И наоборот: занят файл, кто-то берёт каталог целиком.
	other := newStore(t)
	if _, err := other.Take(ctx, "internal/keygen", "pi-claude", "файл"); err != nil {
		t.Fatalf("захват файла: %v", err)
	}
	if _, err := other.Take(ctx, "internal", "mbp-claude", "каталог"); err == nil {
		t.Fatal("каталог захвачен поверх занятого файла внутри")
	}
}

func TestСоседниеЗоныНеМешают(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "internal/keygen", "mbp-claude", ""); err != nil {
		t.Fatalf("первый: %v", err)
	}
	// Разные каталоги и каталог с похожим именем — работа должна идти парал­лельно.
	if _, err := store.Take(ctx, "internal/claims", "pi-claude", ""); err != nil {
		t.Fatalf("соседний каталог отвергнут: %v", err)
	}
	if _, err := store.Take(ctx, "internal/keygen-old", "pi-codex", ""); err != nil {
		t.Fatalf("каталог с похожим именем отвергнут: %v", err)
	}
}

func TestЧужойЗахватНеСнять(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "nats/hub.conf", "mbp-claude", "права"); err != nil {
		t.Fatalf("захват: %v", err)
	}

	if err := store.Release(ctx, "nats/hub.conf", "pi-claude"); err == nil {
		t.Fatal("чужой захват снят: реестр не защищает ни от чего")
	}

	// Свой — снимается.
	if err := store.Release(ctx, "nats/hub.conf", "mbp-claude"); err != nil {
		t.Fatalf("свой захват не снялся: %v", err)
	}
	if _, err := store.Take(ctx, "nats/hub.conf", "pi-claude", "теперь моё"); err != nil {
		t.Fatalf("освобождённая зона не захватывается: %v", err)
	}
}

func TestПовторныйЗахватСвоегоНеОшибка(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "README.md", "pi-claude", "раздел развёртывания"); err != nil {
		t.Fatalf("первый захват: %v", err)
	}
	// Агент перезапустился и столбит снова — падать тут не за что.
	if _, err := store.Take(ctx, "README.md", "pi-claude", "тот же раздел"); err != nil {
		t.Fatalf("повторный захват своей зоны: %v", err)
	}
}

func TestHolderОтвечаетБезЗахвата(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, ok, err := store.Holder(ctx, "internal/bus"); err != nil || ok {
		t.Fatalf("свободная зона показана занятой: ok=%v err=%v", ok, err)
	}

	if _, err := store.Take(ctx, "internal/bus", "pi-codex", "ревью"); err != nil {
		t.Fatalf("захват: %v", err)
	}

	// Спрашиваем про вложенное — держателя каталога надо увидеть.
	holder, ok, err := store.Holder(ctx, "internal/bus/conn.go")
	if err != nil || !ok {
		t.Fatalf("держатель не найден: ok=%v err=%v", ok, err)
	}
	if holder.AgentID != "pi-codex" {
		t.Fatalf("держатель %+v", holder)
	}
}

// Точное совпадение зоны обязано разрешаться атомарно: двое, попросившие одно
// и то же одновременно, не могут оба уйти с уверенностью, что зона их.
func TestОдновременныйЗахватВыигрываетОдин(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	const racers = 8
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		won   []string
		lost  int
		other int
	)

	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func(n int) {
			defer wg.Done()
			agent := string(rune('a'+n)) + "-agent"

			_, err := store.Take(ctx, "internal/keygen", agent, "гонка")

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won = append(won, agent)
			case errors.As(err, new(*ConflictError)):
				lost++
			default:
				other++
			}
		}(i)
	}
	wg.Wait()

	if len(won) != 1 {
		t.Fatalf("зону выиграли %d агентов (%v), должен был один", len(won), won)
	}
	if other != 0 {
		t.Fatalf("%d попыток упали не конфликтом, а чем-то ещё", other)
	}
	if lost != racers-1 {
		t.Fatalf("проигравших %d, ожидалось %d", lost, racers-1)
	}
}

// Частичный захват хуже отказа: агент не знает, за что он взялся.
func TestГрупповойЗахватЛибоВесьЛибоНикакой(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "nats/hub.conf", "mbp-claude", "права"); err != nil {
		t.Fatalf("чужой захват: %v", err)
	}

	// Просим три зоны, средняя занята.
	_, err := store.TakeAll(ctx, []string{"internal/claims", "nats/hub.conf", "README.md"},
		"pi-claude", "реестр захватов")
	if !errors.As(err, new(*ConflictError)) {
		t.Fatalf("групповой захват вернул %v, ожидался конфликт", err)
	}

	held, err := store.List(ctx)
	if err != nil {
		t.Fatalf("список: %v", err)
	}
	// На диске не должно остаться ничего от неудавшейся попытки.
	for _, h := range held {
		if h.AgentID == "pi-claude" {
			t.Fatalf("после отказа осталась зона %s за pi-claude", h.Zone)
		}
	}
	if len(held) != 1 {
		t.Fatalf("захватов %d, ожидался только чужой", len(held))
	}
}

func TestГрупповойЗахватБеретВсёСразу(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	taken, err := store.TakeAll(ctx, []string{"internal/claims", "README.md"}, "pi-claude", "реестр")
	if err != nil {
		t.Fatalf("групповой захват: %v", err)
	}
	if len(taken) != 2 {
		t.Fatalf("взято %d зон, ожидалось 2", len(taken))
	}
}

// Откат обязан снимать только то, что создал сам этот вызов.
//
// Take идемпотентен: на своей зоне он возвращает существующую запись, ничего
// не создавая. Пока откат этого не различал, неудачная попытка взять пару зон
// стирала старую блокировку — агент терял то, что держал до вызова, и узнать
// об этом мог только по чужой правке в своей зоне.
//
// Отказ здесь обязан прийти ИЗ take, а не из предварительной проверки: та
// отсеивает чужие зоны до первой записи, и на ней откат не выполняется вовсе.
// Поэтому конфликт берётся свой же — попытка расширить зону поверх собственной
// узкой.
func TestОткатНеСнимаетСтарыйЗахват(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	// Две зоны, взятые заранее и к текущей попытке отношения не имеющие.
	if _, err := store.Take(ctx, "internal", "pi-claude", "давно моё"); err != nil {
		t.Fatalf("старый захват каталога: %v", err)
	}
	if _, err := store.Take(ctx, "cmd/mesh", "pi-claude", "и это тоже"); err != nil {
		t.Fatalf("старый узкий захват: %v", err)
	}

	// internal/claims покрыт своим же `internal` — вернётся старая запись.
	// cmd сорвётся: агент держит более узкую cmd/mesh.
	_, err := store.TakeAll(ctx, []string{"internal/claims", "cmd"}, "pi-claude", "попытка")
	if err == nil {
		t.Fatal("расширение зоны поверх своей узкой прошло")
	}

	held, err := store.List(ctx)
	if err != nil {
		t.Fatalf("список: %v", err)
	}
	zones := map[string]string{}
	for _, h := range held {
		zones[h.Zone] = h.Note
	}
	if zones["internal"] != "давно моё" {
		t.Fatalf("откат снёс захват, существовавший до вызова: %+v", held)
	}
	if zones["cmd/mesh"] != "и это тоже" {
		t.Fatalf("откат снёс второй старый захват: %+v", held)
	}
}

// Держащий каталог уже работает внутри: второй ключ не нужен.
func TestСвоёВложенноеНеПлодитКлючи(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "internal", "pi-claude", "правлю пакеты"); err != nil {
		t.Fatalf("захват каталога: %v", err)
	}
	got, err := store.Take(ctx, "internal/claims", "pi-claude", "и тут тоже")
	if err != nil {
		t.Fatalf("своя вложенная зона отвергнута: %v", err)
	}
	// Возвращается объемлющий захват, а не новый.
	if got.Zone != "internal" {
		t.Fatalf("создан лишний ключ на %s", got.Zone)
	}

	held, err := store.List(ctx)
	if err != nil {
		t.Fatalf("список: %v", err)
	}
	if len(held) != 1 {
		t.Fatalf("в реестре %d записей, ожидалась одна: %+v", len(held), held)
	}
}

// Расширять зону молча нельзя: освобождать пришлось бы два ключа, и про
// второй легко забыть — сняв родителя, агент оставил бы ребёнка блокировать
// остальных.
func TestРасширениеЗоныТребуетОсвобождения(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Take(ctx, "internal/claims", "pi-claude", "узкая зона"); err != nil {
		t.Fatalf("захват: %v", err)
	}

	_, err := store.Take(ctx, "internal", "pi-claude", "теперь всё")
	if err == nil {
		t.Fatal("каталог захвачен поверх собственной вложенной зоны")
	}
	// Из ошибки должно быть понятно, что именно освобождать.
	if !strings.Contains(err.Error(), "internal/claims") {
		t.Fatalf("ошибка не называет узкую зону: %v", err)
	}
}
