// Package hubcheck проверяет, что шаблон конфигурации хаба вообще разбирается.
//
// Пакет состоит из одного теста и намеренно не содержит кода: `nats-server`
// разрешён только в тестах и не должен попадать в продакшн-граф — иначе он
// утяжеляет бинарник на каждой машине сети, включая Pi.
package hubcheck

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// templatePath — шаблон, который лежит в репозитории.
const templatePath = "../../nats/hub.conf"

// expectedSlots — сколько учёток в шаблоне.
//
// Число зашито намеренно. Если узел добавили или убрали, тест обязан упасть
// и потребовать осознанной правки: права на хабе — самое опасное место
// проекта, и «вдруг стало восемь» не должно проходить молча.
const expectedSlots = 9

// nkeyRe достаёт значение ключа из строки вида `{ nkey: U..., permissions: {`.
var nkeyRe = regexp.MustCompile(`nkey:\s*([A-Z0-9]+)`)

// Шаблон обязан разбираться настоящим разборщиком nats-server.
//
// Проверять его напрямую нельзя: в репозитории лежат заведомо невалидные
// заглушки вместо ключей, и `nats-server -t -c nats/hub.conf` на них падает
// сообщением «Not a valid public nkey for a user». Заглушки — это фича, а не
// недоделка: они не дают случайно поднять хаб по шаблону. Но из-за них
// документированная команда `make hub-check` не могла пройти никогда, и её
// отказ читался как «у вас сломан конфиг».
//
// Поэтому проверяется ВРЕМЕННАЯ копия, где слоты заполнены эфемерными
// ключами. Сам шаблон при этом не меняется — это проверяется отдельно, по
// контрольной сумме.
func TestШаблонХабаРазбираетсяСЭфемернымиКлючами(t *testing.T) {
	original, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("чтение шаблона: %v", err)
	}
	sumBefore := sha256.Sum256(original)

	slots := nkeyRe.FindAllStringSubmatch(string(original), -1)
	if len(slots) != expectedSlots {
		t.Fatalf("в шаблоне %d учёток, ожидалось %d: если узел добавлен или убран, "+
			"поправьте expectedSlots осознанно — права на хабе не должны меняться незаметно",
			len(slots), expectedSlots)
	}

	config := string(original)
	for _, slot := range slots {
		placeholder := slot[1]

		// Заглушка обязана быть невалидной. Настоящий ключ в шаблоне означал
		// бы, что кто-то может поднять хаб по нему, ничего не подставляя, —
		// а заодно что чей-то ключ уехал в git.
		if _, err := nkeys.FromPublicKey(placeholder); err == nil {
			t.Fatalf("в шаблоне лежит ВАЛИДНЫЙ ключ %s…: это не заглушка, "+
				"по такому конфигу можно случайно поднять хаб", placeholder[:12])
		}

		pair, err := nkeys.CreateUser()
		if err != nil {
			t.Fatalf("создание эфемерного ключа: %v", err)
		}
		public, err := pair.PublicKey()
		if err != nil {
			t.Fatalf("публичный ключ: %v", err)
		}
		// Приватная половина не нужна вовсе: разборщику достаточно публичной.
		// Затираем её сразу, чтобы она не жила в памяти дольше необходимого.
		pair.Wipe()

		before := config
		config = strings.Replace(config, placeholder, public, 1)
		if config == before {
			t.Fatalf("заглушка %s… не найдена при подстановке", placeholder[:12])
		}
	}

	// Шаблон ссылается на копию сертификата в /etc/nats/tls, которой вне VPS
	// нет, и без подмены разборщик падает на ней, а не на конфигурации. Кладём
	// эфемерную самоподписанную пару: так проверяется и сам блок tls,
	// а не только всё вокруг него.
	dir := t.TempDir()
	certFile, keyFile := ephemeralTLS(t, dir)
	config = strings.Replace(config,
		`"/etc/nats/tls/fullchain.pem"`, strconv.Quote(certFile), 1)
	config = strings.Replace(config,
		`"/etc/nats/tls/privkey.pem"`, strconv.Quote(keyFile), 1)
	// Сторож проверяет ИМЕННО закавыченные значения, а не подстроку пути:
	// путь упоминается ещё и в комментарии рядом с блоком tls, и поиск по
	// подстроке краснел бы на нём, хотя обе подмены прошли.
	for _, quoted := range []string{`"/etc/nats/tls/fullchain.pem"`, `"/etc/nats/tls/privkey.pem"`} {
		if strings.Contains(config, quoted) {
			t.Fatalf("путь %s не подменён — подмена промахнулась", quoted)
		}
	}

	temp := filepath.Join(dir, "hub.conf")
	if err := os.WriteFile(temp, []byte(config), 0o600); err != nil {
		t.Fatalf("временная копия: %v", err)
	}

	opts, err := natsserver.ProcessConfigFile(temp)
	if err != nil {
		t.Fatalf("шаблон не разбирается: %v", err)
	}
	if len(opts.Nkeys) != expectedSlots {
		t.Fatalf("разборщик увидел %d учёток вместо %d — часть прав потерялась",
			len(opts.Nkeys), expectedSlots)
	}

	// Шаблон должен остаться нетронутым: подстановка живёт только в копии.
	after, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("повторное чтение шаблона: %v", err)
	}
	if sha256.Sum256(after) != sumBefore {
		t.Fatal("шаблон изменился во время проверки")
	}
}

// ephemeralTLS выписывает самоподписанную пару на время теста.
//
// Живёт в каталоге теста и удаляется вместе с ним; в репозиторий не попадает
// ничего. Нужна только чтобы разборщик nats-server смог прочитать блок tls —
// проверяется структура конфигурации, а не доверие к сертификату.
func ephemeralTLS(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ключ сертификата: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "mesh.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"mesh.example.com"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("создание сертификата: %v", err)
	}

	certFile = filepath.Join(dir, "cert.pem")
	certPEM, err := os.Create(certFile)
	if err != nil {
		t.Fatalf("файл сертификата: %v", err)
	}
	if err := pem.Encode(certPEM, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("запись сертификата: %v", err)
	}
	if err := certPEM.Close(); err != nil {
		t.Fatalf("закрытие сертификата: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("сериализация ключа: %v", err)
	}
	keyFile = filepath.Join(dir, "key.pem")
	keyPEM, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("файл ключа: %v", err)
	}
	if err := pem.Encode(keyPEM, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("запись ключа: %v", err)
	}
	// Закрываем явно, а не в defer: разборщик читает файл сразу после
	// возврата, и незакрытый буфер дал бы пустой сертификат.
	if err := keyPEM.Close(); err != nil {
		t.Fatalf("закрытие ключа: %v", err)
	}

	return certFile, keyFile
}
