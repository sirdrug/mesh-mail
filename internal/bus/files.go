package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

// Вложения писем живут в ObjectStore, а не в теле письма.
//
// Причина та же, что у всего проекта: письмо — текст ≤64 КБ, а токен бота есть
// только у моста. Значит мост качает файл сам и кладёт БАЙТЫ сюда, в тело идёт
// лишь ключ. Агент достаёт файл отсюда своим NKey (см. права на MAIL_FILES в
// keygen): токен ему не нужен. «Приём есть, получения нет» лечится именно так.
//
// Ключ объекта — UUID, а не имя файла: имена повторяются и приходят из сети
// (недоверенные), а по ключу из чужого письма файл всё равно не достать —
// письма изолированы, чужой ключ агенту взять неоткуда.

// attachmentFilenameKey — под каким ключом в метаданных объекта лежит имя
// файла. Имя нужно, чтобы адресат сохранил файл под человеческим именем, а не
// под UUID; на адресацию оно не влияет.
const attachmentFilenameKey = "filename"

// ensureObjectStore создаёт бакет вложений. Зовёт ТОЛЬКО мост (как и остальную
// топологию): право STREAM.CREATE.OBJ_MAIL_FILES есть у него одного.
func ensureObjectStore(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.ObjectStore(ctx, FilesBucket); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return fmt.Errorf("бакет вложений %s: %w", FilesBucket, err)
	}

	_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      FilesBucket,
		Description: "байты вложений писем, скачанные мостом из Telegram",
		TTL:         filesRetention,
		Storage:     jetstream.FileStorage,
	})
	if err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return fmt.Errorf("бакет вложений %s не создан "+
			"(нужна учётка с правом STREAM.CREATE — это мост): %w", FilesBucket, err)
	}
	return nil
}

// PutAttachment кладёт байты файла в ObjectStore и возвращает ключ объекта.
//
// Зовёт мост, у которого есть право писать в MAIL_FILES. Ключ — свежий UUID,
// его же мост вставляет в тело письма (bridge.attachmentNote). Имя файла едет
// в метаданных объекта, а не в ключе.
func PutAttachment(ctx context.Context, js jetstream.JetStream, filename string, data []byte) (string, error) {
	store, err := js.ObjectStore(ctx, FilesBucket)
	if err != nil {
		return "", fmt.Errorf("бакет вложений %s: %w", FilesBucket, err)
	}

	key := uuid.NewString()
	meta := jetstream.ObjectMeta{Name: key}
	if filename != "" {
		meta.Metadata = map[string]string{attachmentFilenameKey: filename}
	}
	if _, err := store.Put(ctx, meta, bytes.NewReader(data)); err != nil {
		return "", fmt.Errorf("запись вложения %s: %w", key, err)
	}
	return key, nil
}

// FetchAttachment достаёт байты вложения по ключу и его исходное имя.
//
// Зовёт агент через MCP-инструмент fetch_attachment, СВОИМ NKey. Токен бота
// здесь не участвует вовсе. Отсутствие объекта — обычная ошибка (истёк по TTL,
// опечатка в ключе), а не сбой.
func FetchAttachment(ctx context.Context, js jetstream.JetStream, key string) (data []byte, filename string, err error) {
	store, err := js.ObjectStore(ctx, FilesBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, "", fmt.Errorf("вложений на хабе нет: бакет %s не поднят мостом", FilesBucket)
		}
		return nil, "", fmt.Errorf("бакет вложений %s: %w", FilesBucket, err)
	}

	res, err := store.Get(ctx, key)
	if err != nil {
		return nil, "", fmt.Errorf("вложение %s: %w", key, err)
	}
	defer func() { _ = res.Close() }()

	data, err = io.ReadAll(res)
	if err != nil {
		return nil, "", fmt.Errorf("чтение вложения %s: %w", key, err)
	}

	if info, infoErr := res.Info(); infoErr == nil && info.Metadata != nil {
		filename = info.Metadata[attachmentFilenameKey]
	}
	return data, filename, nil
}
