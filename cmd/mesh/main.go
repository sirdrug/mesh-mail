// Команда mesh — единый бинарник узла агентской сети.
//
//	mesh mcp     MCP-сервер почты для агента (stdio)
//	mesh watch   сторож для Claude Code Monitor: печатает уведомления
//	mesh daemon  будильник для Codex: тычок в tmux на входящее
//	mesh bridge  телеграм-мост: витрина и обратная связь (на VPS)
//	mesh keygen  NKey-пары узлам и готовый блок прав для hub.conf
//
// Один бинарник вместо трёх, потому что раскладывать по четырём машинам
// (включая arm-Pi) проще один файл.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bridge"
	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/claims"
	"github.com/boreevyuri/mesh-mail/internal/config"
	"github.com/boreevyuri/mesh-mail/internal/keygen"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/boreevyuri/mesh-mail/internal/mcpsrv"
	"github.com/boreevyuri/mesh-mail/internal/wake"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go"
)

const version = "0.1.0"

const defaultConfigPath = "config/node.yaml"

// presenceInterval — как часто узел переизлучает визитку.
//
// Значение живёт в bus: от него считается не только тикер, но и окно, за
// которое подписчик вправе считать своё наблюдение установившимся. Два числа
// в разных пакетах разъехались бы молча.
const presenceInterval = bus.PresenceInterval

type options struct {
	command    string
	configPath string
}

func parseArgs(argv []string) (options, error) {
	if len(argv) < 2 {
		return options{}, errors.New("нужна подкоманда: mcp, watch, daemon, bridge, keygen или version")
	}

	opts := options{command: argv[1], configPath: defaultConfigPath}
	switch opts.command {
	case "mcp", "watch", "daemon", "bridge", "keygen", "version":
	default:
		return options{}, fmt.Errorf("неизвестная подкоманда %q", opts.command)
	}

	fs := flag.NewFlagSet(opts.command, flag.ContinueOnError)
	fs.StringVar(&opts.configPath, "config", defaultConfigPath, "путь к конфигурации узла")
	if err := fs.Parse(argv[2:]); err != nil {
		return options{}, err
	}

	return opts, nil
}

// earlyCommands обрабатывает подкоманды, которым не нужны ни конфигурация
// узла, ни подключение к хабу.
//
// Вынесено из main отдельной функцией не ради красоты: в main эти ветки
// зажаты между os.Args и os.Exit, и проверить их было нечем — а именно они
// решают, полезет ли команда за конфигом и в сеть. Теперь тест вызывает их
// напрямую с заведомо несуществующим путём конфига: успех означает, что
// ни того, ни другого не потребовалось.
//
// Вывод идёт в переданный writer, а не в глобальный stdout, чтобы тест
// читал его, ничего не перехватывая.
func earlyCommands(argv []string, out io.Writer) (handled bool, err error) {
	if len(argv) < 2 {
		return false, nil
	}

	switch argv[1] {
	case "version":
		_, err := fmt.Fprintln(out, "mesh", version)
		return true, err
	case "keygen":
		// Своих флагов у keygen хватает, поэтому общий парсер её не разбирает.
		return true, runKeygen(argv[2:], out)
	}
	return false, nil
}

func main() {
	if handled, err := earlyCommands(os.Args, os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, "ошибка:", err)
			os.Exit(1)
		}
		return
	}

	opts, err := parseArgs(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		os.Exit(2)
	}

	// Логи в stderr: stdout у mcp занят протоколом, у watch — уведомлениями.
	logger := log.New(os.Stderr, "mesh: ", log.LstdFlags)

	if err := run(opts, logger); err != nil {
		logger.Fatal(err)
	}
}

// validateForCommand проверяет то, что зависит от подкоманды, ДО подключения.
//
// Порядок важен. Раньше wake_target разбирался внутри runDaemon, то есть уже
// после bus.Connect: оператор с опечаткой в цели видел «подключение к хабу»,
// а про настоящую причину не узнавал вовсе — а при недоступном хабе не узнал
// бы никогда. Конфигурационная ошибка обязана всплывать раньше сетевой.
//
// Разобранная цель возвращается наружу, чтобы runDaemon не разбирал строку
// второй раз: wake.ParseTarget остаётся единственным местом, знающим
// синтаксис.
func validateForCommand(command string, node *config.Node) (wake.Target, error) {
	if command != "daemon" {
		return wake.Target{}, nil
	}

	if node.WakeTarget == "" {
		return wake.Target{}, errors.New("для daemon нужен wake_target в конфигурации узла, " +
			"например screen:codex или tmux:pi-codex:0")
	}
	target, err := wake.ParseTarget(node.WakeTarget)
	if err != nil {
		return wake.Target{}, fmt.Errorf("wake_target: %w", err)
	}
	return target, nil
}

func run(opts options, logger *log.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	node, err := config.Load(opts.configPath)
	if err != nil {
		return fmt.Errorf("конфигурация: %w", err)
	}

	// До сети: ошибку в конфигурации незачем узнавать через таймаут хаба.
	target, err := validateForCommand(opts.command, node)
	if err != nil {
		return err
	}

	conn, err := bus.Connect(ctx, bus.Options{
		URLs:         node.NATS.URLs,
		Name:         "mesh-" + opts.command + "-" + node.AgentID,
		AgentID:      node.AgentID,
		NKeySeedFile: node.NATS.NKeySeedFile,
		CAFile:       node.NATS.CAFile,
	})
	if err != nil {
		return fmt.Errorf("подключение к хабу: %w", err)
	}
	defer conn.Close()

	switch opts.command {
	case "mcp":
		return runMCP(ctx, conn, node, logger)
	case "watch":
		return runWatch(ctx, conn, node, logger)
	case "daemon":
		return runDaemon(ctx, conn, node, target, logger)
	case "bridge":
		return runBridge(ctx, conn, node, logger)
	}
	return nil
}

// runMCP обслуживает агента по stdio.
func runMCP(ctx context.Context, conn *bus.Conn, node *config.Node, logger *log.Logger) error {
	// Проверка, а не приведение: у агентской учётки нет права менять поток,
	// и попытка это сделать не дала бы узлу стартовать вовсе.
	if err := bus.CheckTopology(ctx, conn.JS()); err != nil {
		return fmt.Errorf("топология: %w", err)
	}

	reg := bus.NewRegistry()
	if err := bus.WatchPresence(ctx, conn.NC(), reg); err != nil {
		return fmt.Errorf("подписка на визитки: %w", err)
	}
	go announceLoop(ctx, conn, node, logger)

	logger.Printf("MCP-сервер поднят для %s (проекты: %v)", node.AgentID, node.Projects)
	return mcpsrv.New(conn, reg, node).Run(ctx, &mcp.StdioTransport{})
}

// runWatch печатает уведомления в stdout — их читает Monitor.
func runWatch(ctx context.Context, conn *bus.Conn, node *config.Node, logger *log.Logger) error {
	if err := wake.Watch(ctx, conn.NC(), node.AgentID, os.Stdout); err != nil {
		return err
	}

	// Сторож объявляет присутствие наравне с mcp и daemon, и это не
	// симметрия ради симметрии.
	//
	// Письмо человека в общий чат адресуется живым узлам — тем, чья визитка
	// не протухла. Узел, у которого из трёх команд работает только сторож,
	// без этой строки выпадает из адресатов через TTL визитки, и человек,
	// написавший «всем», зовёт не всех. Отказ молчаливый с обеих сторон:
	// письмо уходит успешно, а тот, кого не позвали, ничего не узнаёт.
	//
	// Именно так и случилось: на маке живёт `mesh watch`, а `mesh mcp`
	// поднимается разово под вызов инструмента, и между вызовами узел для
	// сети не существовал.
	go announceLoop(ctx, conn, node, logger)

	logger.Printf("сторож слушает ящик %s", node.AgentID)
	<-ctx.Done()
	return nil
}

// runDaemon будит сессию Codex через мультиплексор.
func runDaemon(ctx context.Context, conn *bus.Conn, node *config.Node,
	target wake.Target, logger *log.Logger,
) error {
	// Цель уже разобрана в validateForCommand, до подключения к хабу.

	// Отказываемся стартовать, если будить некого. Промах цели сам по себе
	// выглядит как исправная работа: демон принимает письма и «будит» пустоту,
	// а мультиплексор отвечает на несуществующую сессию быстро и тихо.
	if err := wake.Alive(ctx, target); err != nil {
		return fmt.Errorf("будить некого: %w", err)
	}

	sub, err := conn.NC().Subscribe(bus.MailInboxFilter(node.AgentID), func(msg *nats.Msg) {
		var m mail.Message
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			return
		}
		// В сессию уходит фиксированная строка, а не Notice: и tmux, и screen
		// вводят текст как набранный человеком, и тема письма — недоверенные
		// данные из сети — стала бы там запросом к агенту.
		// Промах тычка не теряет письмо: оно уже в ящике.
		if err := wake.Poke(ctx, target); err != nil {
			logger.Printf("не смог разбудить сессию: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("подписка: %w", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // при остановке процесса это неважно

	go announceLoop(ctx, conn, node, logger)

	logger.Printf("демон будит %s через %s", node.AgentID, target)
	<-ctx.Done()
	return nil
}

// announceLoop переизлучает визитку, пока процесс жив.
func announceLoop(ctx context.Context, conn *bus.Conn, node *config.Node, logger *log.Logger) {
	ticker := time.NewTicker(presenceInterval)
	defer ticker.Stop()

	for {
		card := bus.Card{
			AgentID:     node.AgentID,
			Host:        node.Host,
			Engine:      node.Engine,
			Projects:    node.Projects,
			Tags:        node.Tags,
			AnnouncedAt: time.Now().UTC(),
			TTLSeconds:  int(presenceInterval.Seconds()) * 3,
		}
		if err := bus.Announce(ctx, conn.NC(), card); err != nil {
			logger.Printf("не смог объявить визитку: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runBridge поднимает телеграм-мост. Работает на VPS рядом с хабом.
func runBridge(ctx context.Context, conn *bus.Conn, node *config.Node, logger *log.Logger) error {
	// Мост — единственный, кто вправе создавать топологию и приводить её
	// конфигурацию к ожидаемой.
	if err := bus.EnsureBridgeTopology(ctx, conn.JS()); err != nil {
		return fmt.Errorf("топология: %w", err)
	}
	// Реестр зон — часть той же топологии, и поднимать его должен тот же, кто
	// поднимает остальное. Раньше не поднимал никто: мост о нём не знал, а
	// агент пытался создать сам и упирался в запрет по правам. Отдельным
	// вызовом, а не внутри EnsureBridgeTopology, потому что bus не может
	// импортировать claims — тесты claims импортируют bus, вышло бы кольцо.
	if err := claims.EnsureBucket(ctx, conn.JS()); err != nil {
		return fmt.Errorf("реестр зон: %w", err)
	}

	tokenEnv := node.Telegram.TokenEnv
	if tokenEnv == "" {
		tokenEnv = "TELEGRAM_TOKEN"
	}

	logger.Printf("поднимаю мост в чат %s", node.Telegram.ChatID)
	return bridge.Run(ctx, conn, bridge.Config{
		ChatID:         node.Telegram.ChatID,
		Token:          os.Getenv(tokenEnv),
		ForumTopics:    node.Telegram.ForumTopics,
		AllowedUserIDs: node.Telegram.AllowedUserIDs,
	})
}

// runKeygen выдаёт ключи узлам и печатает блок прав.
//
// Отдельно от остальных подкоманд: конфиг узла и подключение к хабу ей не
// нужны — её запускают один раз до развёртывания, на машине владельца.
func runKeygen(argv []string, out io.Writer) error {
	fs := flag.NewFlagSet("keygen", flag.ContinueOnError)
	outDir := fs.String("out", "secrets", "куда положить приватные seed-файлы")
	show := fs.String("show", "", "напечатать публичный ключ из seed-файла и выйти")
	if err := fs.Parse(argv); err != nil {
		return err
	}

	// Публичный ключ по seed-файлу: вывод генерации давно закрыт вместе с
	// терминалом, а hub.conf правят и через полгода. Ключи при этом не
	// выдаются — файл только читается.
	if *show != "" {
		public, err := keygen.PublicFromSeedFile(*show)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out, public); err != nil {
			return fmt.Errorf("вывод публичного ключа: %w", err)
		}
		return nil
	}

	agents := fs.Args()
	if len(agents) == 0 {
		return errors.New("перечислите узлы: mesh keygen pi-claude pi-codex ... bridge")
	}
	for _, id := range agents {
		if err := config.ValidateAgentID(id); err != nil {
			return err
		}
	}

	pairs, err := keygen.Generate(agents)
	if err != nil {
		return err
	}
	if err := keygen.WriteSeeds(pairs, *outDir); err != nil {
		return err
	}

	// Собираем целиком и пишем одним вызовом: так проверка ошибки записи
	// одна, а не пять, и вывод не может уехать наполовину.
	var report strings.Builder
	fmt.Fprintf(&report, "# Seed-файлы: %s/*.nk — каждому узлу увезти ТОЛЬКО его файл.\n", *outDir)
	fmt.Fprintf(&report, "# На VPS попадает единственный seed — %s.nk\n", keygen.BridgeAgentID)
	report.WriteString("# Блок ниже заменяет секцию users в nats/hub.conf целиком.\n\n")
	report.WriteString(keygen.HubBlock(pairs))

	if _, err := io.WriteString(out, report.String()); err != nil {
		return fmt.Errorf("вывод блока прав: %w", err)
	}
	return nil
}
