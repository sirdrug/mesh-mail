// Package bus отвечает за всё общение с хабом NATS: подключение, публикацию
// писем, чтение ящика и визитки.
//
// Единственное место в проекте, которое знает про NATS. Остальной код видит
// только методы этого пакета.
package bus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Options — параметры подключения к хабу.
type Options struct {
	URLs []string
	Name string // видно в /varz на хабе, помогает разбирать соединения

	// AgentID задаёт персональный namespace ответов. Обязателен для узлов;
	// пустой допустим только там, где прав по темам нет вовсе (тесты).
	AgentID string

	NKeySeedFile string
	CAFile       string // нужен, только если сертификат хаба не от публичного CA

	// Logger — куда писать о разрыве и восстановлении связи.
	//
	// Необязателен: пустой означает журнал по умолчанию. Отдельным полем, а не
	// глобальной переменной, потому что тесту нужно увидеть эти строки, не
	// перехватывая вывод всего процесса.
	Logger *log.Logger
}

// InboxPrefix — префикс тем, куда сервер шлёт ответы этому агенту.
//
// Персональный, а не общий `_INBOX`, и это не косметика. JetStream отдаёт
// письмо в reply-subject запроса; при общем префиксе любой агент с правом
// `subscribe: _INBOX.>` подписывался бы на чужие ответы и читал чужую почту
// в обход всех прав на консьюмеры. Проверено эксплойтом, см. perms_test.go.
//
// Точка в префиксе не используется: `_INBOX_<agent>` даёт отдельное поддерево,
// которое нельзя захватить шаблоном `_INBOX.>`.
func InboxPrefix(agentID string) string {
	return "_INBOX_" + agentID
}

// Conn — соединение с хабом вместе с контекстом JetStream.
type Conn struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func (c *Conn) NC() *nats.Conn          { return c.nc }
func (c *Conn) JS() jetstream.JetStream { return c.js }
func (c *Conn) Close()                  { c.nc.Close() }

// Connect подключается к хабу.
//
// Соединение исходящее: узел сам идёт на VPS и авторизуется своим NKey.
// Входящих портов на машине открывать не требуется — это и позволяет держать
// узел за корпоративным периметром.
func Connect(ctx context.Context, opts Options) (*Conn, error) {
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}

	natsOpts := []nats.Option{
		nats.Name(opts.Name),
		// Мобильный MacBook уезжает из сети и возвращается — переподключаемся вечно.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),

		// Разрыв и восстановление записываются отдельно от всего остального.
		//
		// Без этих двух строк переподключение происходит молча, и единственный
		// след разрыва — случайная ошибка публикации, попавшая в то же окно.
		// За день на маке набралось восемь строк «не смог объявить визитку», и
		// по ним нельзя было отличить одну потерю связи от восьми: событие в
		// журнале отсутствовало, была видна только его тень.
		//
		// Различие существенное. Визитка живёт три интервала, поэтому узел
		// исчезает из сети ровно после трёх неудач подряд; две подряд — уже
		// предаварийный сигнал, а по прежнему журналу они выглядели как два
		// независимых происшествия.
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Printf("bus: связь с хабом потеряна (узел %s): %v", opts.Name, err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Printf("bus: связь с хабом восстановлена (узел %s), хаб %s, переподключений %d",
				opts.Name, safeHost(nc.ConnectedUrl()), nc.Reconnects)
		}),
	}

	// Ответы сервера уходят в персональное поддерево: без этого сосед
	// подписывается на общий _INBOX.> и читает чужие письма.
	if opts.AgentID != "" {
		natsOpts = append(natsOpts, nats.CustomInboxPrefix(InboxPrefix(opts.AgentID)))
	}

	if opts.NKeySeedFile != "" {
		opt, err := nats.NkeyOptionFromSeed(opts.NKeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("NKey из %s: %w", opts.NKeySeedFile, err)
		}
		natsOpts = append(natsOpts, opt)
	}

	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("чтение CA %s: %w", opts.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("в %s нет ни одного сертификата", opts.CAFile)
		}
		natsOpts = append(natsOpts, nats.Secure(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	}

	nc, err := nats.Connect(strings.Join(opts.URLs, ","), natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("подключение к хабу: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("контекст JetStream: %w", err)
	}

	return &Conn{nc: nc, js: js}, nil
}

// safeHost оставляет от адреса только хост и порт.
//
// В URL может стоять `nats://user:pass@host`, и целиком он в журнале не нужен:
// адрес хаба помогает понять, куда переподключились, а учётные данные —
// секрет, которому в systemd не место. Неразобранный адрес не печатается
// вовсе: лучше пустое поле, чем случайно выведенный пароль.
func safeHost(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}

// jetstreamFor — контекст JetStream поверх готового соединения.
// Нужен тестам прав, которые подключаются со своими учётными данными.
func jetstreamFor(nc *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(nc)
}
