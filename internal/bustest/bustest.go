// Package bustest поднимает настоящий nats-server для тестов.
//
// Отдельный пакет, а не файл внутри bus: любой импорт bus тянул бы за собой
// весь nats-server, и он уезжал бы в продакшн-бинарник на каждой машине,
// включая Pi. Здесь его импортируют только тесты, поэтому в mesh он не
// попадает.
package bustest

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// StartTestServer поднимает настоящий nats-server с JetStream в памяти теста.
//
// Настоящий сервер, а не заглушка: проверять надо именно поведение JetStream
// и прав по темам, а заглушка воспроизводит наши ожидания, а не реальность.
func StartTestServer(t *testing.T) string {
	t.Helper()

	opts := &natsserver.Options{
		Port:      -1, // произвольный свободный порт
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("не удалось создать тестовый сервер: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("тестовый сервер не поднялся за 5 секунд")
	}
	t.Cleanup(ns.Shutdown)

	return ns.ClientURL()
}

// StartTestServerWithConf поднимает сервер из куска конфигурации.
//
// Нужен там, где проверяются права: конфигурация — единственное место, где
// они по-настоящему живут, и никакая заглушка не воспроизведёт того, что
// сделает настоящий сервер с запретом. Порт, каталог и тишина в логе
// переопределяются здесь, чтобы тесту не пришлось повторять это в каждом
// конфиге.
func StartTestServerWithConf(t *testing.T, conf string) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "hub-*.conf")
	if err != nil {
		t.Fatalf("временный конфиг: %v", err)
	}
	if _, err := file.WriteString(conf); err != nil {
		t.Fatalf("запись конфига: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("закрытие конфига: %v", err)
	}

	opts, err := natsserver.ProcessConfigFile(file.Name())
	if err != nil {
		t.Fatalf("разбор конфига: %v", err)
	}
	opts.Port = -1
	opts.NoLog = true
	opts.NoSigs = true
	opts.StoreDir = t.TempDir()

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("сервер: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("сервер не поднялся")
	}
	t.Cleanup(ns.Shutdown)

	return ns.ClientURL()
}

// StartRestartable поднимает сервер, который можно остановить и поднять снова
// на ТОМ ЖЕ порту.
//
// Нужен там, где проверяется поведение клиента при разрыве: остановить и
// поднять обычный тестовый сервер нельзя — порт ему выдаётся случайный, и
// после перезапуска клиент искал бы старый адрес.
//
// Возвращает адрес и две функции: остановить и поднять. Останавливать дважды
// безопасно.
func StartRestartable(t *testing.T) (url string, stop func(), start func()) {
	t.Helper()

	// Свободный порт выбирает ядро: слушаем на нулевом, запоминаем выданный и
	// сразу освобождаем. Щель между освобождением и стартом теоретически
	// занимается чужим процессом, но фиксированный номер в исходнике ломался
	// бы при параллельном прогоне пакетов — а это случается каждый день.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("выбор свободного порта: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatalf("освобождение порта: %v", err)
	}

	dir := t.TempDir()
	var current *natsserver.Server

	start = func() {
		opts := &natsserver.Options{
			Host:      "127.0.0.1",
			Port:      port,
			JetStream: true,
			StoreDir:  dir,
			NoLog:     true,
		}
		srv, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatalf("создание сервера на порту %d: %v", port, err)
		}
		go srv.Start()
		if !srv.ReadyForConnections(5 * time.Second) {
			t.Fatalf("сервер на порту %d не поднялся за 5 секунд", port)
		}
		current = srv
	}

	stop = func() {
		if current == nil {
			return
		}
		current.Shutdown()
		current.WaitForShutdown()
		current = nil
	}

	start()
	t.Cleanup(stop)

	return fmt.Sprintf("nats://127.0.0.1:%d", port), stop, start
}
