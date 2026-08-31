package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/go-telegram/bot"
)

// Ответ Telegram разбирается НИЖЕ библиотеки, а не по тексту её ошибки.
//
// Библиотека отображает отказ в набор своих значений и вклеивает описание в
// строку: `fmt.Errorf("%w, %s", ErrorBadRequest, description)`. Числа кода
// там не остаётся вовсе, а для кодов вне её списка — например 5xx — нет и
// значения. Разбирать отформатированную строку значило бы поставить
// классификацию отказов в зависимость от чужого формата сообщений.
//
// Поэтому перехватываем сам ответ: обёртка вокруг HTTP-клиента читает тело,
// возвращает его библиотеке нетронутым и запоминает то, что нам нужно, —
// код ответа, описание и retry_after.
type apiFailureCapture struct {
	mu sync.Mutex
	// failure — последний отказ Telegram в этом вызове.
	failure *rawFailure
}

// rawFailure — то, что Telegram сказал об отказе.
type rawFailure struct {
	// status — код HTTP-ответа. Именно он попадает в APIError.Code, как и в
	// прежнем клиенте: тело несёт свой error_code, и он может отличаться.
	status      int
	description string
	retryAfter  int
}

type captureKeyType struct{}

var captureKey captureKeyType

// withCapture привязывает накопитель к контексту одного вызова.
//
// К контексту, а не к клиенту: клиент один на все запросы, и общий изменяемый
// накопитель смешал бы отказы двух одновременных вызовов. Библиотека доносит
// наш контекст до http.Request (`raw_request.go`, NewRequestWithContext), так
// что обёртка достаёт из запроса именно тот накопитель, что завёл вызывающий.
func withCapture(ctx context.Context) (context.Context, *apiFailureCapture) {
	c := &apiFailureCapture{}
	return context.WithValue(ctx, captureKey, c), c
}

func captureFrom(ctx context.Context) *apiFailureCapture {
	c, _ := ctx.Value(captureKey).(*apiFailureCapture)
	return c
}

func (c *apiFailureCapture) remember(f rawFailure) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failure = &f
}

func (c *apiFailureCapture) taken() (rawFailure, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure == nil {
		return rawFailure{}, false
	}
	return *c.failure, true
}

// capturingClient — HTTP-клиент, запоминающий отказ Telegram по дороге.
type capturingClient struct {
	inner *http.Client
}

func (c *capturingClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.inner.Do(req)
	if err != nil || resp == nil {
		return resp, err
	}

	capture := captureFrom(req.Context())
	if capture == nil {
		return resp, nil
	}

	// Тело читается целиком и возвращается на место: библиотека разбирает его
	// следом, и подменять поток нельзя.
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if readErr != nil {
		return resp, nil
	}

	var envelope struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.OK {
		// Не разобралось или отказа нет — запоминать нечего. Ошибку разбора
		// сообщит сама библиотека, и выдумывать APIError на ровном месте
		// нельзя: не всякая беда — отказ Telegram.
		return resp, nil
	}

	capture.remember(rawFailure{
		status:      resp.StatusCode,
		description: envelope.Description,
		retryAfter:  envelope.Parameters.RetryAfter,
	})
	return resp, nil
}

// asAPIError переводит ошибку библиотеки в нашу.
//
// Первым делом смотрит перехваченный ответ: там код и описание такие же, как
// у прежнего клиента. Если перехвата почему-то нет — отказ пришёл не от
// Telegram либо тело не разобралось, — остаётся отображение известных
// значений библиотеки в коды. Текст ошибки не разбирается ни в каком случае.
func asAPIError(method string, capture *apiFailureCapture, err error) error {
	if err == nil {
		return nil
	}

	// Накопителя может не быть, если вызов забыли завести через withCapture.
	// Тогда лучше вернуть обычный отказ, чем уронить процесс: витрина живёт в
	// том же процессе, и паника внутри одного вызова остановила бы мост.
	if capture != nil {
		if failure, ok := capture.taken(); ok {
			return &APIError{Method: method, Code: failure.status, Description: failure.description}
		}
	}

	if code, ok := codeOfSentinel(err); ok {
		return &APIError{Method: method, Code: code, Description: err.Error()}
	}
	return fmt.Errorf("вызов %s: %w", method, err)
}

// codeOfSentinel — запасное отображение значений библиотеки в коды.
func codeOfSentinel(err error) (int, bool) {
	var tooMany *bot.TooManyRequestsError
	if errors.As(err, &tooMany) {
		return http.StatusTooManyRequests, true
	}
	switch {
	case errors.Is(err, bot.ErrorForbidden):
		return http.StatusForbidden, true
	case errors.Is(err, bot.ErrorUnauthorized):
		return http.StatusUnauthorized, true
	case errors.Is(err, bot.ErrorBadRequest):
		return http.StatusBadRequest, true
	case errors.Is(err, bot.ErrorNotFound):
		return http.StatusNotFound, true
	case errors.Is(err, bot.ErrorConflict):
		return http.StatusConflict, true
	}
	return 0, false
}

// retryAfterOf — пауза, которую просил Telegram. Ноль, если не просил.
func retryAfterOf(capture *apiFailureCapture, err error) int {
	if capture != nil {
		if failure, ok := capture.taken(); ok && failure.retryAfter > 0 {
			return failure.retryAfter
		}
	}
	var tooMany *bot.TooManyRequestsError
	if errors.As(err, &tooMany) {
		return tooMany.RetryAfter
	}
	return 0
}
