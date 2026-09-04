package mcpsrv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fetch_attachment — забрать файл, приложенный к письму.
//
// Мост скачивает файл из Telegram СВОИМ токеном и кладёт байты в ObjectStore,
// а в тело письма ставит ключ объекта. Этот инструмент достаёт байты СВОИМ
// NKey агента и сохраняет их файлом. Токен бота агенту не нужен и не выдаётся —
// в этом и смысл: «приём» у моста, «получение» у адресата, инвариант «токен
// только у моста» цел.

type FetchAttachmentIn struct {
	Object string `json:"object" jsonschema:"ключ вложения из тела письма (строка после «объект:» в блоке ВЛОЖЕНИЕ)"`
	Dest   string `json:"dest,omitempty" jsonschema:"куда сохранить: путь файла или каталог; по умолчанию — имя файла в текущем каталоге"`
}

type FetchAttachmentOut struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	Size     int    `json:"size"`
}

func (h *handlers) fetchAttachment(ctx context.Context, _ *mcp.CallToolRequest, in FetchAttachmentIn) (*mcp.CallToolResult, FetchAttachmentOut, error) {
	object := strings.TrimSpace(in.Object)
	if object == "" {
		return nil, FetchAttachmentOut{}, fmt.Errorf("нужен ключ вложения (object) из тела письма")
	}

	data, filename, err := bus.FetchAttachment(ctx, h.conn.JS(), object)
	if err != nil {
		return nil, FetchAttachmentOut{}, err
	}

	path := attachmentDest(in.Dest, filename, object)
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, FetchAttachmentOut{}, fmt.Errorf("каталог %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, FetchAttachmentOut{}, fmt.Errorf("сохранение %s: %w", path, err)
	}
	if abs, absErr := filepath.Abs(path); absErr == nil {
		path = abs
	}
	return nil, FetchAttachmentOut{Path: path, Filename: filename, Size: len(data)}, nil
}

// attachmentDest — куда положить файл.
//
// dest пуст — имя файла в текущем каталоге. dest — существующий каталог или
// оканчивается на слэш — файл внутрь него. Иначе dest считается путём файла.
// Имя из ObjectStore используется как запасное, когда его нет — ключ.
func attachmentDest(dest, filename, object string) string {
	name := filename
	if name == "" {
		name = object
	}
	if dest == "" {
		return name
	}
	if info, err := os.Stat(dest); err == nil && info.IsDir() {
		return filepath.Join(dest, name)
	}
	if strings.HasSuffix(dest, "/") || strings.HasSuffix(dest, string(os.PathSeparator)) {
		return filepath.Join(dest, name)
	}
	return dest
}
