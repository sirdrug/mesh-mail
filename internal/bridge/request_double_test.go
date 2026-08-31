package bridge

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

// botRequest — параметры вызова Bot API, как их увидел двойник.
//
// Двойник понимает ОБА вида тела, и это ПОСТОЯННОЕ устройство, а не
// совместимость на время миграции.
//
// Исходящие методы идут через библиотеку, а она всегда собирает multipart —
// независимо от того, есть ли в запросе файлы. Наш собственный getUpdates
// остаётся на своём пути и шлёт JSON: durable offset и порядок обработки
// библиотеке не отданы и отданы не будут. Значит оба формата будут ходить
// одновременно и дальше, а ветка JSON — не отработавший костыль, который
// можно убрать при уборке.
type botRequest struct {
	ChatID   string
	Text     string
	ThreadID int
	Name     string
	Offset   int
}

// readBotRequest разбирает тело запроса к Bot API.
//
// Ошибка возвращается, а не проглатывается: молчаливый разбор превращает
// сломанный двойник в «параметры пустые», и тест начинает проверять пустоту
// вместо поведения.
func readBotRequest(r *http.Request) (botRequest, error) {
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil && contentType != "" {
		return botRequest{}, fmt.Errorf("тип содержимого %q: %w", contentType, err)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		return readMultipart(r, params["boundary"])
	}
	return readJSON(r)
}

func readJSON(r *http.Request) (botRequest, error) {
	var body struct {
		ChatID   any    `json:"chat_id"`
		Text     string `json:"text"`
		ThreadID int    `json:"message_thread_id"`
		Name     string `json:"name"`
		Offset   int    `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		// Пустое тело у методов без параметров — не ошибка.
		if errors.Is(err, io.EOF) {
			return botRequest{}, nil
		}
		return botRequest{}, fmt.Errorf("разбор JSON: %w", err)
	}
	out := botRequest{Text: body.Text, ThreadID: body.ThreadID, Name: body.Name, Offset: body.Offset}
	// chat_id приходит и строкой, и числом — Bot API принимает оба вида.
	switch v := body.ChatID.(type) {
	case string:
		out.ChatID = v
	case float64:
		out.ChatID = strconv.FormatInt(int64(v), 10)
	}
	return out, nil
}

func readMultipart(r *http.Request, boundary string) (botRequest, error) {
	if boundary == "" {
		return botRequest{}, fmt.Errorf("multipart без границы")
	}
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		// Пустое тело — обычное дело: у getMe параметров нет вовсе, и
		// библиотека шлёт multipart без единой части. Это не поломка.
		if errors.Is(err, io.EOF) {
			return botRequest{}, nil
		}
		return botRequest{}, fmt.Errorf("разбор multipart: %w", err)
	}

	out := botRequest{
		ChatID: r.FormValue("chat_id"),
		Text:   r.FormValue("text"),
		Name:   r.FormValue("name"),
	}
	// Числа приходят строками: пустое поле — это ноль, а не ошибка.
	for _, field := range []struct {
		name string
		dst  *int
	}{
		{"message_thread_id", &out.ThreadID},
		{"offset", &out.Offset},
	} {
		raw := r.FormValue(field.name)
		if raw == "" {
			continue
		}
		value, err := strconv.Atoi(raw)
		if err != nil {
			return botRequest{}, fmt.Errorf("поле %s = %q: %w", field.name, raw, err)
		}
		*field.dst = value
	}
	return out, nil
}
