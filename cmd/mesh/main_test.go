package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"regexp"

	"github.com/boreevyuri/mesh-mail/internal/config"
	"github.com/boreevyuri/mesh-mail/internal/wake"
)

func TestParseArgsТребуетПодкоманду(t *testing.T) {
	if _, err := parseArgs([]string{"mesh"}); err == nil {
		t.Fatal("запуск без подкоманды не дал ошибки")
	}
}

func TestParseArgsОтвергаетНеизвестную(t *testing.T) {
	if _, err := parseArgs([]string{"mesh", "летать"}); err == nil {
		t.Fatal("неизвестная подкоманда принята")
	}
}

func TestParseArgsЧитаетКонфиг(t *testing.T) {
	opts, err := parseArgs([]string{"mesh", "mcp", "--config", "/etc/mesh/node.yaml"})
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if opts.command != "mcp" {
		t.Fatalf("команда %q", opts.command)
	}
	if opts.configPath != "/etc/mesh/node.yaml" {
		t.Fatalf("конфиг %q", opts.configPath)
	}
}

func TestParseArgsКонфигПоУмолчанию(t *testing.T) {
	opts, err := parseArgs([]string{"mesh", "watch"})
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if opts.configPath == "" {
		t.Fatal("путь к конфигу по умолчанию пуст")
	}
}

// --- ранние команды ---------------------------------------------------------

// version не должен требовать ни конфига, ни хаба.
//
// Путь конфига намеренно указан несуществующий: если бы команда за ним
// полезла, тест бы это увидел.
// seedLike — форма приватного ключа NATS: начинается с SU и состоит из
// base32 длиной 58 символов.
var seedLike = regexp.MustCompile(`\bSU[A-Z2-7]{54,}\b`)

func TestVersionНеТребуетНиКонфигаНиСети(t *testing.T) {
	var out bytes.Buffer

	handled, err := earlyCommands([]string{"mesh", "version", "-config", "/нет/такого/файла.yaml"}, &out)

	if !handled {
		t.Fatal("version не обработана как ранняя команда")
	}
	if err != nil {
		t.Fatalf("version вернула ошибку: %v", err)
	}
	if !strings.Contains(out.String(), version) {
		t.Fatalf("версия не напечатана: %q", out.String())
	}
}

// keygen работает до развёртывания: конфига узла ещё нет, хаба тоже.
func TestKeygenНеТребуетНиКонфигаНиСети(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer

	handled, err := earlyCommands(
		[]string{"mesh", "keygen", "-out", filepath.Join(dir, "secrets"), "pi-claude", "bridge"}, &out)

	if !handled {
		t.Fatal("keygen не обработана как ранняя команда")
	}
	if err != nil {
		t.Fatalf("keygen вернула ошибку: %v", err)
	}
	// Успех сам по себе и есть доказательство: конфига узла в каталоге нет,
	// подключаться некуда, а команда отработала.
	if !strings.Contains(out.String(), "users = [") {
		t.Fatalf("блок для hub.conf не напечатан: %q", out.String())
	}
	for _, name := range []string{"pi-claude.nk", "bridge.nk"} {
		if _, err := os.Stat(filepath.Join(dir, "secrets", name)); err != nil {
			t.Fatalf("ключ %s не создан: %v", name, err)
		}
	}
	// Приватные ключи в вывод попадать не должны никогда. Ищем именно форму
	// seed — отдельным словом, а не подстрокой: «SU» встречается внутри
	// CONSUMER, и наивная проверка ловила бы права вместо секретов.
	if seedLike.MatchString(out.String()) {
		t.Fatal("в выводе keygen похоже на приватный seed")
	}
}

func TestОбычныеКомандыНеРанние(t *testing.T) {
	for _, cmd := range []string{"mcp", "watch", "daemon", "bridge"} {
		var out bytes.Buffer
		handled, err := earlyCommands([]string{"mesh", cmd}, &out)
		if handled || err != nil {
			t.Errorf("%s обработана как ранняя: handled=%v err=%v", cmd, handled, err)
		}
		if out.Len() != 0 {
			t.Errorf("%s что-то напечатала: %q", cmd, out.String())
		}
	}
}

// --- проверки по подкомандам ------------------------------------------------

func TestValidateForCommand(t *testing.T) {
	cases := []struct {
		name    string
		command string
		target  string
		wantErr string
		wantTgt wake.Target
	}{
		{
			name: "демон без цели", command: "daemon", target: "",
			wantErr: "wake_target",
		},
		{
			name: "демон с непонятной целью", command: "daemon", target: "codex",
			wantErr: "wake_target",
		},
		{
			name: "демон с неизвестным мультиплексором", command: "daemon", target: "kitty:codex",
			wantErr: "wake_target",
		},
		{
			name: "демон со screen", command: "daemon", target: "screen:codex",
			wantTgt: wake.Target{Kind: wake.KindScreen, Name: "codex"},
		},
		{
			name: "демон с tmux и окном", command: "daemon", target: "tmux:pi-codex:0",
			wantTgt: wake.Target{Kind: wake.KindTmux, Name: "pi-codex", Window: "0"},
		},
		// Остальным подкомандам цель не нужна и её отсутствие не ошибка.
		{name: "mcp без цели", command: "mcp"},
		{name: "watch без цели", command: "watch"},
		{name: "bridge без цели", command: "bridge"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := &config.Node{AgentID: "pi-codex", Engine: "codex", WakeTarget: c.target}

			got, err := validateForCommand(c.command, node)

			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("ошибки нет, ожидалась про %s", c.wantErr)
				}
				// Проверяем именно текст: «какая-то ошибка» не подсказывает
				// оператору, что чинить.
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("ошибка не про %s: %v", c.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if got != c.wantTgt {
				t.Fatalf("цель разобрана как %+v, ожидалось %+v", got, c.wantTgt)
			}
		})
	}
}

// --- проводка run ------------------------------------------------------------

// Плохой конфиг обязан останавливать команду ДО всякой сети.
//
// Адрес хаба в конфиге заведомо недостижим: если бы run добрался до
// подключения, тест ждал бы таймаут и получил другую ошибку.
func TestRunПадаетНаКонфигеДоСети(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "мост с чужим agent_id",
			body:    "agent_id: mesh-bridge\nhost: vps\nengine: bridge\nnats:\n  urls: [\"nats://192.0.2.1:4222\"]\n",
			wantErr: "конфигурация",
		},
		{
			name:    "неизвестный движок",
			body:    "agent_id: pi-claude\nhost: pi\nengine: копилот\nnats:\n  urls: [\"nats://192.0.2.1:4222\"]\n",
			wantErr: "конфигурация",
		},
		{
			name:    "старое имя поля",
			body:    "agent_id: pi-codex\nhost: pi\nengine: codex\ntmux_target: pi-codex:0\nnats:\n  urls: [\"nats://192.0.2.1:4222\"]\n",
			wantErr: "конфигурация",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.yaml")
			if err := os.WriteFile(path, []byte(c.body), 0o600); err != nil {
				t.Fatal(err)
			}

			done := make(chan error, 1)
			go func() {
				done <- run(options{command: "bridge", configPath: path},
					log.New(io.Discard, "", 0))
			}()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("плохой конфиг принят")
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("ошибка не про конфигурацию: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("run ушёл в сеть вместо отказа по конфигу")
			}
		})
	}
}

// Демон с негодной целью обязан отказать до подключения к хабу.
func TestRunОтвергаетЦельДоПодключения(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yaml")
	body := "agent_id: pi-codex\nhost: pi\nengine: codex\n" +
		"wake_target: кто-то-там\nnats:\n  urls: [\"nats://192.0.2.1:4222\"]\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- run(options{command: "daemon", configPath: path}, log.New(io.Discard, "", 0))
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("демон принял негодную цель")
		}
		if !strings.Contains(err.Error(), "wake_target") {
			t.Fatalf("ошибка не про цель пробуждения: %v", err)
		}
		// Если бы проверка стояла после Connect, сюда пришла бы сетевая ошибка.
		if strings.Contains(err.Error(), "подключение к хабу") {
			t.Fatalf("отказ пришёл после попытки подключения: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run полез в сеть вместо отказа по цели")
	}
}

// keygen --show печатает публичный ключ по seed-файлу.
//
// Флаг существует потому, что его обещает runbook: публичные ключи нужны при
// правке hub.conf, а вывод генерации к тому времени давно закрыт вместе с
// терминалом. До этой правки команда из документации отвечала «flag provided
// but not defined» — то есть инструкция была невыполнима буквально.
func TestKeygenShowПечатаетПубличныйКлюч(t *testing.T) {
	dir := t.TempDir()
	var generated bytes.Buffer
	if err := runKeygen([]string{"--out", dir, "pi-claude"}, &generated); err != nil {
		t.Fatalf("генерация: %v", err)
	}

	var shown bytes.Buffer
	if err := runKeygen([]string{"--show", filepath.Join(dir, "pi-claude.nk")}, &shown); err != nil {
		t.Fatalf("keygen --show: %v", err)
	}

	public := strings.TrimSpace(shown.String())
	if public == "" {
		t.Fatal("ключ не напечатан")
	}
	// Тот же ключ, что попал в блок прав: иначе оператор вставит в hub.conf
	// ключ, которым узел не подключается, и отказ будет молчаливым.
	if !strings.Contains(generated.String(), public) {
		t.Fatalf("напечатан ключ %q, которого нет в выданном блоке прав", public)
	}
	// Публичный ключ пользователя NATS начинается с U.
	if !strings.HasPrefix(public, "U") {
		t.Fatalf("это не похоже на публичный ключ: %q", public)
	}
}

// Показ ключа не выдаёт новых и не трогает каталог.
func TestKeygenShowНеВыдаётКлючей(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	if err := runKeygen([]string{"--out", dir, "bridge"}, &buf); err != nil {
		t.Fatalf("генерация: %v", err)
	}
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("чтение каталога: %v", err)
	}

	buf.Reset()
	if err := runKeygen([]string{"--show", filepath.Join(dir, "bridge.nk")}, &buf); err != nil {
		t.Fatalf("keygen --show: %v", err)
	}

	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("повторное чтение каталога: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("после --show в каталоге %d файлов вместо %d — команда выдала ключи",
			len(after), len(before))
	}
	// И не печатает блок прав: вывод должен быть пригоден для подстановки.
	if strings.Contains(buf.String(), "permissions") {
		t.Fatalf("--show напечатал блок прав вместо одного ключа: %q", buf.String())
	}
}

// Невнятный файл — внятная ошибка, а не паника и не пустой вывод.
func TestKeygenShowОтвергаетНеSeed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "не-ключ.nk")
	if err := os.WriteFile(path, []byte("это не seed"), 0o600); err != nil {
		t.Fatalf("подготовка файла: %v", err)
	}

	var buf bytes.Buffer
	err := runKeygen([]string{"--show", path}, &buf)
	if err == nil {
		t.Fatal("мусор принят за seed")
	}
	if !strings.Contains(err.Error(), "seed") {
		t.Fatalf("ошибка не объясняет, что не так с файлом: %v", err)
	}
}
