package bus

import (
	"bytes"
	"context"
	"testing"
)

// Файл кладётся мостом и достаётся адресатом целиком, включая имя.
//
// Тело намеренно многочанковое (> 128 КБ, размер чанка по умолчанию): так
// проверяется и сборка из чанков, и сверка SHA-256, которую делает Get.
func TestВложениеКладётсяИДостаётсяЦеликом(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)

	data := bytes.Repeat([]byte("вложение-байты "), 40000) // ~600 КБ
	key, err := PutAttachment(ctx, conn.JS(), "пример.zip", data)
	if err != nil {
		t.Fatalf("запись вложения: %v", err)
	}
	if key == "" {
		t.Fatal("ключ объекта пуст")
	}

	got, name, err := FetchAttachment(ctx, conn.JS(), key)
	if err != nil {
		t.Fatalf("чтение вложения: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("байты не совпали: получено %d, ожидалось %d", len(got), len(data))
	}
	if name != "пример.zip" {
		t.Fatalf("имя файла = %q, ожидалось пример.zip", name)
	}
}

// Несуществующий ключ — обычная ошибка, а не паника: истёкший TTL или опечатка.
func TestFetchНесуществующегоВложенияОшибка(t *testing.T) {
	ctx := context.Background()
	conn := setupBus(t)
	if _, _, err := FetchAttachment(ctx, conn.JS(), "нет-такого-ключа"); err == nil {
		t.Fatal("ожидалась ошибка на несуществующий ключ вложения")
	}
}
