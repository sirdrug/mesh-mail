package tg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

// readRequestFields приводит тело запроса к общему виду.
//
// Двойник понимает оба вида тела ПОСТОЯННО, а не на время миграции:
// исходящие методы идут через библиотеку и приходят multipart даже без
// файлов, а наш getUpdates остаётся на своём пути и шлёт JSON — его durable
// offset библиотеке не отдан. Ветку JSON нельзя убрать как отработавшую.
//
// Числа из multipart возвращаются как float64 — так же, как их отдаёт
// encoding/json. Иначе утверждения тестов пришлось бы писать по-разному для
// двух видов тела, то есть проверять не поведение, а способ передачи.
//
// Приведение идёт ПО ИМЕНИ поля, а не по виду значения. Иначе текст «123»
// стал бы числом только потому, что состоит из цифр, — и тест, сравнивающий
// текст сообщения, показал бы ложное расхождение между JSON и multipart.
func readRequestFields(r *http.Request) (map[string]any, error) {
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil && contentType != "" {
		return nil, fmt.Errorf("тип содержимого %q: %w", contentType, err)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		if params["boundary"] == "" {
			return nil, errors.New("multipart без границы")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			// Пустое тело — обычное дело: у getMe параметров нет вовсе.
			if errors.Is(err, io.EOF) {
				return map[string]any{}, nil
			}
			return nil, fmt.Errorf("разбор multipart: %w", err)
		}
		out := make(map[string]any, len(r.MultipartForm.Value))
		for name, values := range r.MultipartForm.Value {
			if len(values) == 0 {
				continue
			}
			out[name] = normalizeField(name, values[0])
		}
		return out, nil
	}

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		if errors.Is(err, io.EOF) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("разбор JSON: %w", err)
	}
	if body == nil {
		body = map[string]any{}
	}
	return body, nil
}

// numericFields — поля Bot API, которые в JSON приходят числами.
//
// Список поимённый и короткий намеренно: гадать по виду значения нельзя, а
// молча приводить всё подряд — тем более. Незнакомое поле остаётся строкой, и
// это заметно в тесте сразу, в отличие от неверно угаданного типа.
var numericFields = map[string]bool{
	"message_thread_id":   true,
	"offset":              true,
	"timeout":             true,
	"message_id":          true,
	"reply_to_message_id": true,
}

// boolFields — поля, которые в JSON приходят булевыми.
var boolFields = map[string]bool{
	"disable_web_page_preview": true,
	"disable_notification":     true,
}

// normalizeField приводит значение из multipart к тому виду, в каком его
// отдал бы encoding/json — по ИМЕНИ поля, а не по содержимому.
func normalizeField(name, value string) any {
	if numericFields[name] {
		if number, err := strconv.ParseFloat(value, 64); err == nil {
			return number
		}
		return value
	}
	if boolFields[name] {
		switch value {
		case "true":
			return true
		case "false":
			return false
		}
	}
	return value
}
