package hubcheck

// Права на бакет вложений MAIL_FILES — на живом хабе из боевого шаблона.
//
// Смысл фичи: мост скачивает файл СВОИМ токеном и кладёт байты в ObjectStore,
// агент достаёт их СВОИМ NKey. Значит проверяем ровно асимметрию: мост пишет,
// агент читает, агент писать не может. Если агент не смог прочитать —
// в шаблоне не хватает права, и это видно здесь, а не на боевой машине.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/nats-io/nats.go/jetstream"
)

// Мост кладёт вложение, агент достаёт его целиком.
func TestВложениеМостПишетАгентЧитает(t *testing.T) {
	url, seeds := liveHub(t)
	ctx := context.Background()

	_, bridgeJS := connect(t, url, "bridge", seeds)
	if err := bus.EnsureBridgeTopology(ctx, bridgeJS); err != nil {
		t.Fatalf("мост поднимает топологию: %v", err)
	}

	payload := bytes.Repeat([]byte("вложение-байты "), 20000) // многочанковое
	key, err := bus.PutAttachment(ctx, bridgeJS, "example.zip", payload)
	if err != nil {
		t.Fatalf("мост кладёт вложение: %v", err)
	}

	_, agentJS := connect(t, url, "pi-claude", seeds)
	got, name, err := bus.FetchAttachment(ctx, agentJS, key)
	if err != nil {
		t.Fatalf("агент достаёт вложение: %v — в шаблоне не хватает права на чтение MAIL_FILES", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("байты не совпали: получено %d, ожидалось %d", len(got), len(payload))
	}
	if name != "example.zip" {
		t.Fatalf("имя файла = %q, ожидалось example.zip", name)
	}
}

// Агент вложения не пишет и бакет не создаёт: запись — только у моста.
func TestАгентНеПишетВложения(t *testing.T) {
	url, seeds := liveHub(t)
	ctx := context.Background()

	_, bridgeJS := connect(t, url, "bridge", seeds)
	if err := bus.EnsureBridgeTopology(ctx, bridgeJS); err != nil {
		t.Fatalf("топология: %v", err)
	}

	_, agentJS := connect(t, url, "pi-claude", seeds)
	// Короткий срок: отклонённая публикация иначе висит до общего таймаута,
	// а нам нужен сам факт отказа, а не ожидание.
	denyCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := bus.PutAttachment(denyCtx, agentJS, "чужое.zip", []byte("не должно записаться")); err == nil {
		t.Fatal("агент записал вложение — права на запись MAIL_FILES у него быть не должно")
	}
	if _, err := agentJS.CreateObjectStore(denyCtx, jetstream.ObjectStoreConfig{Bucket: "AGENT_TRIES"}); err == nil {
		t.Fatal("агент создал ObjectStore — права STREAM.CREATE у него быть не должно")
	}
}
