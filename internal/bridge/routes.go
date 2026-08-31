package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// RouteBucket — KV с соответствием «пост в чате → разговор».
//
// Отдельный бакет, а не пространство ключей внутри bridge_topics, и причина
// та же, по которой отдельно живут отметки о показе: TTL в JetStream задаётся
// НА БАКЕТ ЦЕЛИКОМ. Держать вместе вечные записи о темах и временные маршруты
// означало бы либо вечные маршруты, либо исчезающие темы. Разные пространства
// ключей от этого не спасают: они различают вид записи, а не её срок жизни.
const RouteBucket = "bridge_routes"

// RouteTTL — сколько живёт маршрут ответа.
//
// Ровно столько же, сколько письмо, к которому он ведёт: письма хранятся
// девяносто дней. Меньше — человек ответит на видимый пост, а маршрут уже
// истёк. Больше — маршрут переживёт письмо и приведёт в никуда. Для человека
// обе ошибки выглядят одинаково: «ответил, ничего не произошло».
const RouteTTL = 90 * 24 * time.Hour

// routeVersion — версия формата записи.
//
// Есть с самого начала, потому что добавить её потом нельзя: старые записи
// уже лежат без неё, и отличить «версия 1» от «версии не было» будет не по
// чему. Один байт сейчас против неразличимости навсегда.
const routeVersion = 1

// Route — куда вести ответ человека на конкретный пост.
type Route struct {
	Version      int       `json:"v"`
	ThreadID     string    `json:"mesh_thread_id"`
	Project      string    `json:"project"`
	Participants []string  `json:"participants"`
	PostedAt     time.Time `json:"posted_at"`
}

// routeKey — ключ маршрута: хеш от пары «чат + сообщение».
//
// Хеш, а не «<чат>/<номер>», по той же причине, что и у отметок о показе:
// ключи KV принимают ограниченный набор символов, идентификатор чата
// начинается с минуса, и полагаться на то, что сегодня это проходит, значит
// однажды получить отказ при записи — уже после того, как пост показан.
//
// Чат входит в ключ обязательно: номера сообщений уникальны в пределах чата,
// а не глобально, и без него две супергруппы склеили бы разговоры.
func routeKey(chatID string, messageID int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", chatID, messageID)))
	return fmt.Sprintf("post-%x", sum)
}

// PutRoute запоминает, какому разговору принадлежит пост.
func (s *TopicStore) PutRoute(ctx context.Context, chatID string, messageID int, route Route) error {
	route.Version = routeVersion
	if route.PostedAt.IsZero() {
		route.PostedAt = time.Now().UTC()
	}

	payload, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("сериализация маршрута: %w", err)
	}
	if _, err := s.routes.Put(ctx, routeKey(chatID, messageID), payload); err != nil {
		return fmt.Errorf("запись маршрута поста %d: %w", messageID, err)
	}
	return nil
}

// Route возвращает разговор, которому принадлежит пост. Отсутствие — не ошибка.
//
// Отсутствие означает ровно одно из трёх: пост показан до перехода на общую
// тему, маршрут истёк по TTL, или человек ответил не на пост бота. Все три
// случая вызывающий обязан объяснить человеку, а не превратить в отказ.
func (s *TopicStore) Route(ctx context.Context, chatID string, messageID int) (Route, bool, error) {
	entry, err := s.routes.Get(ctx, routeKey(chatID, messageID))
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
		return Route{}, false, nil
	}
	if err != nil {
		return Route{}, false, fmt.Errorf("чтение маршрута поста %d: %w", messageID, err)
	}

	var route Route
	if err := json.Unmarshal(entry.Value(), &route); err != nil {
		return Route{}, false, fmt.Errorf("разбор маршрута поста %d: %w", messageID, err)
	}

	// Принимается РОВНО известная версия, а не «не больше нашей».
	//
	// Ноль тут особенно важен: это не «первая версия», а «версии нет».
	// Маршруты появились сразу с полем версии, поэтому запись без неё
	// сделана не нами — читать её как свою значит отправить ответ по данным,
	// которых не понимаешь. Человеку это обойдётся письмом не тем людям.
	if route.Version != routeVersion {
		return Route{}, false, fmt.Errorf(
			"маршрут поста %d записан версией %d, эта сборка понимает только %d",
			messageID, route.Version, routeVersion)
	}
	return route, true, nil
}
