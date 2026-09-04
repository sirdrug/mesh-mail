package bridge

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// fakeFetcher отдаёт заготовленные байты вместо скачивания из Telegram.
type fakeFetcher struct {
	data []byte
	err  error
}

func (f *fakeFetcher) FetchFile(_ context.Context, _ string) ([]byte, error) {
	return f.data, f.err
}

// Мост скачивает файл и кладёт БАЙТЫ в ObjectStore; адресат достаёт их по ключу.
func TestВложениеСкачиваетсяИКладётсяВObjectStore(t *testing.T) {
	ctx := context.Background()
	store, conn := newStore(t)
	intake := NewIntake(conn.JS(), store, &fakeUpdater{}, bus.NewRegistry(), "-1001", []int64{42})

	payload := bytes.Repeat([]byte("байты-файла "), 3000)
	intake.SetFileFetcher(&fakeFetcher{data: payload})
	doc := &tg.Attachment{FileID: "tg-file-id-xyz", FileName: "example.zip", FileSize: int64(len(payload)), MimeType: "application/zip"}

	key, err := intake.storeAttachment(ctx, doc)
	if err != nil {
		t.Fatalf("storeAttachment: %v", err)
	}
	got, name, err := bus.FetchAttachment(ctx, conn.JS(), key)
	if err != nil {
		t.Fatalf("FetchAttachment: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("байты не совпали: %d против %d", len(got), len(payload))
	}
	if name != "example.zip" {
		t.Fatalf("имя файла = %q, ожидалось example.zip", name)
	}
}

// Тело письма несёт КЛЮЧ ОБЪЕКТА и инструмент fetch_attachment, но НЕ file_id,
// НЕ labfile и НЕ токен: получение — из сети, своим NKey.
func TestТелоВложенияНесётКлючНеТокен(t *testing.T) {
	ctx := context.Background()
	store, conn := newStore(t)
	intake := NewIntake(conn.JS(), store, &fakeUpdater{}, bus.NewRegistry(), "-1001", []int64{42})
	intake.SetFileFetcher(&fakeFetcher{data: []byte("содержимое")})

	doc := &tg.Attachment{FileID: "tg-file-id-xyz", FileName: "example.zip"}
	body := intake.bodyForMessage(ctx, "вот структура репо", doc)

	if !strings.Contains(body, "вот структура репо") {
		t.Errorf("подпись человека потеряна: %q", body)
	}
	if !strings.Contains(body, "fetch_attachment") {
		t.Errorf("нет инструмента получения: %q", body)
	}
	if !strings.Contains(body, "объект:") {
		t.Errorf("нет ключа объекта: %q", body)
	}
	for _, bad := range []string{"file_id", "labfile", "tg-file-id-xyz"} {
		if strings.Contains(body, bad) {
			t.Errorf("в теле осталось запретное %q: %q", bad, body)
		}
	}
}

// Без загрузчика или при отказе скачивания сообщение доходит, но файл помечен
// как неполученный — терять само письмо из-за файла нельзя.
func TestБезЗагрузчикаВложениеПомеченоКакНеполученное(t *testing.T) {
	ctx := context.Background()
	store, conn := newStore(t)
	intake := NewIntake(conn.JS(), store, &fakeUpdater{}, bus.NewRegistry(), "-1001", []int64{42})
	// FileFetcher намеренно не задан.

	body := intake.bodyForMessage(ctx, "", &tg.Attachment{FileName: "example.zip"})
	if !strings.Contains(body, "не удалось") {
		t.Errorf("нет пометки о неудаче получения: %q", body)
	}
	if !strings.Contains(body, "example.zip") {
		t.Errorf("нет имени файла в пометке: %q", body)
	}
}

// Тема файла без подписи берётся из имени, а не «сообщение от человека».
func TestТемаФайлаБезПодписиИзИмени(t *testing.T) {
	doc := &tg.Attachment{FileID: "x", FileName: "repo-skeleton.tar.gz"}

	if got := subjectForMessage("", doc); !strings.Contains(got, "repo-skeleton.tar.gz") {
		t.Errorf("тема файла без подписи = %q, ожидалось имя файла", got)
	}
	if got := subjectForMessage("посмотри вложение", doc); got != "посмотри вложение" {
		t.Errorf("тема при наличии подписи должна быть из подписи: %q", got)
	}
}
