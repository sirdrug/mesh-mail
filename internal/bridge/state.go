package bridge

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// StateBucket — KV с рабочим состоянием моста.
//
// Отдельно от bridge_topics намеренно. В том бакете лежат темы разговоров,
// и обратный поиск темы перебирает его целиком; служебные ключи с другим
// сроком жизни там превращаются в плату за каждое сообщение человека.
const StateBucket = "bridge_state"

// offsetKey — позиция чтения обновлений Telegram.
//
// Ключ один на мост: два моста на одном токене всё равно воруют обновления
// друг у друга (getUpdates отдаёт обновление одному потребителю), и разделять
// позицию между ними бессмысленно.
const offsetKey = "telegram-offset"

// StateStore хранит то, что мост обязан помнить между запусками.
type StateStore struct {
	kv jetstream.KeyValue
}

// NewStateStore открывает бакет состояния, создавая его при необходимости.
func NewStateStore(ctx context.Context, js jetstream.JetStream) (*StateStore, error) {
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      StateBucket,
		Description: "рабочее состояние моста: позиция чтения обновлений Telegram",
	})
	if err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return nil, fmt.Errorf("бакет %s: %w", StateBucket, err)
	}
	if err != nil {
		kv, err = js.KeyValue(ctx, StateBucket)
		if err != nil {
			return nil, fmt.Errorf("открытие бакета %s: %w", StateBucket, err)
		}
	}
	return &StateStore{kv: kv}, nil
}

// Offset возвращает сохранённую позицию. Её отсутствие — не ошибка: так
// выглядит первый запуск моста, и читать с нуля в этом случае правильно.
func (s *StateStore) Offset(ctx context.Context) (int, error) {
	entry, err := s.kv.Get(ctx, offsetKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("чтение позиции чтения обновлений: %w", err)
	}

	offset, err := strconv.Atoi(string(entry.Value()))
	if err != nil {
		// Испорченное значение лечится чтением с нуля: Telegram отдаст
		// накопленное заново, человек увидит дубли — это лучше, чем мост,
		// который не стартует из-за одного битого ключа.
		return 0, nil
	}
	return offset, nil
}

// SetOffset запоминает позицию.
//
// Позиция двигается только вперёд: обновления приходят по возрастанию
// update_id, и запись меньшего значения означала бы, что мост переиграет уже
// разобранные сообщения человека.
func (s *StateStore) SetOffset(ctx context.Context, offset int) error {
	if _, err := s.kv.Put(ctx, offsetKey, []byte(strconv.Itoa(offset))); err != nil {
		return fmt.Errorf("запись позиции чтения обновлений: %w", err)
	}
	return nil
}
