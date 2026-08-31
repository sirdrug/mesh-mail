package claims

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Bucket — бакет KV с занятыми зонами.
const Bucket = "claims"

// MaxAge — сколько живёт захват без продления.
//
// Захваты обязаны истекать сами. Сессия агента умирает вместе с контекстом,
// падает вместе с машиной и просто закрывается человеком — и ни в одном из
// этих случаев release не вызывается. Без срока реестр за неделю превратится
// в список мёртвых блокировок, которые все обходят, и это надёжнее делает его
// бесполезным, чем полное отсутствие.
//
// Восемь часов — рабочий день: достаточно, чтобы не мешать длинной задаче,
// и мало, чтобы вчерашний мусор не блокировал сегодняшнюю работу.
const MaxAge = 8 * time.Hour

// Claim — кто и зачем занял зону.
type Claim struct {
	Zone    string    `json:"zone"`
	AgentID string    `json:"agent_id"`
	Note    string    `json:"note"`
	Since   time.Time `json:"since"`
}

// ConflictError — зона занята кем-то другим.
//
// Отдельный тип, а не строка: вызывающему нужно показать человеку, КТО держит
// зону и с какого времени, иначе «занято» бесполезно — непонятно, к кому идти.
type ConflictError struct {
	Requested string
	Held      Claim
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("зона %s занята: %s держит %s с %s (%s)",
		e.Requested, e.Held.AgentID, e.Held.Zone,
		e.Held.Since.Format("15:04"), e.Held.Note)
}

// EnsureBucket создаёт реестр зон. Зовёт ТОЛЬКО мост.
//
// Право STREAM.CREATE есть у него одного, и это не формальность: создающий
// бакет задаёт ему TTL и описание, то есть решает за всю сеть, когда чужие
// захваты протухают. Агент, которому позволили бы создать реестр заново,
// молча обнулил бы его настройки.
//
// Зовётся рядом с bus.EnsureBridgeTopology, а не внутри неё: bus не может
// импортировать claims — тесты claims импортируют bus, и получилось бы
// кольцо. Порядок раскатки от этого не меняется: мост поднимает всё, что
// поднимает, одним заходом и до узлов.
func EnsureBucket(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.KeyValue(ctx, Bucket); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return fmt.Errorf("бакет %s: %w", Bucket, err)
	}

	// Гонка: бакет мог появиться между проверкой и созданием.
	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      Bucket,
		Description: "занятые зоны кода: путь -> кто и зачем",
		TTL:         MaxAge,
	})
	if err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return fmt.Errorf("бакет %s не создан (нужна учётка с правом STREAM.CREATE — это мост): %w",
			Bucket, err)
	}
	return nil
}

// Store — реестр захватов поверх KV.
type Store struct {
	kv jetstream.KeyValue
}

// NewStore открывает реестр зон и НИЧЕГО не создаёт.
//
// Раньше пробовал создать, не найдя бакета, — и это давало худший из
// возможных отказов. Право STREAM.CREATE агенту не выдано намеренно, а запрет
// в NATS работает асинхронно: сервер не отвечает отказом на запрос, он просто
// не пропускает публикацию. Клиент ждёт ответа, которого не будет, и через
// пять секунд говорит «context deadline exceeded».
//
// Человек читает это как беду со связью и идёт проверять сеть, хаб и TLS.
// Настоящая причина — «мост ещё не поднимал топологию» — не упомянута нигде,
// а починка ровно одна: запустить мост. Пять секунд при этом тратятся на
// старте каждого узла.
//
// Поэтому здесь только открытие: если бакета нет, мы знаем это сразу и
// говорим то, что человеку нужно сделать. Создаёт реестр EnsureBucket, и
// зовёт её мост.
func NewStore(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	kv, err := js.KeyValue(ctx, Bucket)
	if err == nil {
		return &Store{kv: kv}, nil
	}
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("реестра зон (бакет %s) на хабе нет: его поднимает мост, "+
			"запустите его первым (mesh bridge)", Bucket)
	}
	return nil, fmt.Errorf("открытие бакета %s: %w", Bucket, err)
}

// Take столбит зону за агентом.
//
// Точное совпадение зоны разрешается атомарно: `Create` падает, если ключ уже
// есть, поэтому двое, попросившие одно и то же одновременно, не могут оба
// решить, что они первые. Проверка Get+Put такой гарантии не даёт.
//
// А вот перекрытия (`internal/` против `internal/keygen`) атомарными быть не
// могут: их видно только просмотром всех ключей, и между просмотром и записью
// остаётся окно. Оно узкое и на практике безвредно — мы работаем минутами, а
// не микросекундами, — но об этом надо знать, а не обнаружить. Гарантия здесь
// такая: точную зону реестр защищает всегда, перекрывающуюся — почти всегда.
func (s *Store) Take(ctx context.Context, zone, agentID, note string) (Claim, error) {
	claim, _, err := s.take(ctx, zone, agentID, note)
	return claim, err
}

// take столбит зону и говорит, был ли ключ создан ИМЕННО ЭТИМ вызовом.
//
// Признак нужен откату в TakeAll. Без него откат снимал бы и те захваты,
// которые агент держал ещё до вызова: Take идемпотентен и на своей зоне
// возвращает существующую запись, а откат не отличал бы её от только что
// созданной — неудачная попытка взять пару зон стирала бы старую блокировку
// молча.
func (s *Store) take(ctx context.Context, zone, agentID, note string) (Claim, bool, error) {
	z, err := NormalizeZone(zone)
	if err != nil {
		return Claim{}, false, err
	}
	if agentID == "" {
		return Claim{}, false, errors.New("не указан агент")
	}

	held, err := s.List(ctx)
	if err != nil {
		return Claim{}, false, err
	}
	for _, h := range held {
		if !Overlaps(z, h.Zone) {
			continue
		}
		if h.AgentID != agentID {
			return Claim{}, false, &ConflictError{Requested: z, Held: h}
		}
		// Своё перекрытие. Если уже держим объемлющую зону, второй ключ не
		// нужен: он только засорил бы список и сделал release неочевидным —
		// сняв родителя, агент оставил бы ребёнка блокировать остальных.
		if Covers(h.Zone, z) {
			return h, false, nil
		}
		// А вот обратное — расширение зоны, и делать его молча нельзя:
		// освобождать придётся два ключа, и про второй легко забыть.
		return Claim{}, false, fmt.Errorf(
			"вы уже держите более узкую зону %s: освободите её, чтобы взять %s целиком", h.Zone, z)
	}

	claim := Claim{Zone: z, AgentID: agentID, Note: note, Since: time.Now().UTC()}
	payload, err := json.Marshal(claim)
	if err != nil {
		return Claim{}, false, fmt.Errorf("сериализация захвата: %w", err)
	}

	if _, err := s.kv.Create(ctx, Key(z), payload); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Успел кто-то между нашим просмотром и записью.
			existing, ok, getErr := s.get(ctx, z)
			if getErr != nil {
				return Claim{}, false, getErr
			}
			if ok && existing.AgentID != agentID {
				return Claim{}, false, &ConflictError{Requested: z, Held: existing}
			}
			// Наш собственный захват: повторный вызов не ошибка и не создание.
			return existing, false, nil
		}
		return Claim{}, false, fmt.Errorf("захват зоны %s: %w", z, err)
	}

	return claim, true, nil
}

// TakeAll столбит несколько зон разом: либо все, либо ни одной.
//
// Частичный захват был бы хуже отказа. Агент, попросивший три зоны и
// получивший две, не знает, за что он взялся: одна зона осталась чужой, но
// две уже помечены его именем, и снаружи это выглядит как согласованная
// работа. Ровно на этом обжёгся keygen — оборвавшись посередине, он оставил
// на диске ключи, о которых никто не знал.
//
// Поэтому сначала проверяются все зоны, и лишь потом пишется первая. Если
// гонка всё же перехватила зону между проверкой и записью, уже взятое
// отпускается назад.
func (s *Store) TakeAll(ctx context.Context, zones []string, agentID, note string) ([]Claim, error) {
	if len(zones) == 0 {
		return nil, errors.New("не указано ни одной зоны")
	}

	normalized := make([]string, 0, len(zones))
	for _, z := range zones {
		n, err := NormalizeZone(z)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, n)
	}

	// Проверяем всё до первой записи.
	for _, z := range normalized {
		holder, busy, err := s.Holder(ctx, z)
		if err != nil {
			return nil, err
		}
		if busy && holder.AgentID != agentID {
			return nil, &ConflictError{Requested: z, Held: holder}
		}
	}

	taken := make([]Claim, 0, len(normalized))
	created := make([]string, 0, len(normalized))
	for _, z := range normalized {
		claim, wasCreated, err := s.take(ctx, z, agentID, note)
		if err != nil {
			// Откатываем ТОЛЬКО созданное здесь. Захваты, которые агент держал
			// до вызова, откат не трогает: снимать их из-за чужой занятой зоны
			// значило бы наказывать за попытку.
			for _, zone := range created {
				_ = s.Release(ctx, zone, agentID)
			}
			return nil, err
		}
		taken = append(taken, claim)
		if wasCreated {
			created = append(created, claim.Zone)
		}
	}
	return taken, nil
}

// Release освобождает зону.
//
// Чужие захваты не снимаются: иначе реестр не защищает ни от чего — любой
// снял бы мешающую блокировку вместо того, чтобы договориться. Просроченные
// исчезают сами по TTL, и это единственный способ снять чужое.
func (s *Store) Release(ctx context.Context, zone, agentID string) error {
	z, err := NormalizeZone(zone)
	if err != nil {
		return err
	}

	existing, ok, err := s.get(ctx, z)
	if err != nil {
		return err
	}
	if !ok {
		// Освобождать незанятое — не ошибка: повторный release не должен падать.
		return nil
	}
	if existing.AgentID != agentID {
		return fmt.Errorf("зону %s держит %s, снять чужой захват нельзя", z, existing.AgentID)
	}

	if err := s.kv.Delete(ctx, Key(z)); err != nil {
		return fmt.Errorf("освобождение зоны %s: %w", z, err)
	}
	return nil
}

// List возвращает все живые захваты.
func (s *Store) List(ctx context.Context) ([]Claim, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("список захватов: %w", err)
	}

	claims := make([]Claim, 0, len(keys))
	for _, key := range keys {
		claim, ok, err := s.get(ctx, ZoneFromKey(key))
		if err != nil {
			return nil, err
		}
		if ok {
			claims = append(claims, claim)
		}
	}
	return claims, nil
}

// Holder говорит, кто мешает занять зону. Пусто — зона свободна.
//
// Отдельно от Take, потому что спросить «свободно ли» полезно и без попытки
// захватить: перед началом работы, в отчёте человеку, в проверке перед
// коммитом.
func (s *Store) Holder(ctx context.Context, zone string) (Claim, bool, error) {
	z, err := NormalizeZone(zone)
	if err != nil {
		return Claim{}, false, err
	}

	held, err := s.List(ctx)
	if err != nil {
		return Claim{}, false, err
	}
	for _, h := range held {
		if Overlaps(z, h.Zone) {
			return h, true, nil
		}
	}
	return Claim{}, false, nil
}

func (s *Store) get(ctx context.Context, zone string) (Claim, bool, error) {
	entry, err := s.kv.Get(ctx, Key(zone))
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, fmt.Errorf("чтение захвата %s: %w", zone, err)
	}

	var claim Claim
	if err := json.Unmarshal(entry.Value(), &claim); err != nil {
		return Claim{}, false, fmt.Errorf("разбор захвата %s: %w", zone, err)
	}
	return claim, true, nil
}
