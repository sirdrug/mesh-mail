package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("запись конфига: %v", err)
	}
	return path
}

func TestLoadЧитаетУзел(t *testing.T) {
	path := write(t, `
agent_id: pi-claude
host: raspberrypi
engine: claude
projects: [mesh-mail, dotfiles]
wake_target: screen:claude
nats:
  urls: ["tls://mesh.example.com:4222"]
  nkey_seed_file: secrets/pi-claude.nk
`)

	node, err := Load(path)
	if err != nil {
		t.Fatalf("загрузка: %v", err)
	}
	if node.AgentID != "pi-claude" {
		t.Fatalf("agent_id = %q", node.AgentID)
	}
	if len(node.Projects) != 2 {
		t.Fatalf("проектов %d, ожидалось 2", len(node.Projects))
	}
	if node.NATS.URLs[0] != "tls://mesh.example.com:4222" {
		t.Fatalf("url = %q", node.NATS.URLs[0])
	}
	if node.WakeTarget != "screen:claude" {
		t.Fatalf("wake_target = %q", node.WakeTarget)
	}
}

// Старое имя поля обязано быть ошибкой, а не тишиной.
//
// yaml молча игнорирует незнакомые ключи. Оставшийся в конфиге `tmux_target`
// не вызвал бы ни ошибки, ни предупреждения — демон просто не будил бы никого,
// и понять это можно было бы только по тому, что агент перестал отвечать.
func TestLoadОтвергаетСтароеИмяПоля(t *testing.T) {
	path := write(t, `
agent_id: pi-codex
host: raspberrypi
engine: codex
tmux_target: pi-codex:0
nats:
  urls: ["nats://127.0.0.1:4222"]
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("конфиг со старым tmux_target загрузился молча")
	}
	// Из текста должно быть понятно, как чинить.
	if !strings.Contains(err.Error(), "wake_target") {
		t.Fatalf("ошибка не подсказывает новое имя поля: %v", err)
	}
}

func TestLoadПодставляетПеременныеОкружения(t *testing.T) {
	t.Setenv("MESH_SEED", "/etc/mesh/pi.nk")
	path := write(t, `
agent_id: pi-claude
host: raspberrypi
engine: claude
nats:
  urls: ["nats://127.0.0.1:4222"]
  nkey_seed_file: ${MESH_SEED}
`)

	node, err := Load(path)
	if err != nil {
		t.Fatalf("загрузка: %v", err)
	}
	if node.NATS.NKeySeedFile != "/etc/mesh/pi.nk" {
		t.Fatalf("seed = %q, переменная не подставилась", node.NATS.NKeySeedFile)
	}
}

func TestLoadТребуетИдентификатор(t *testing.T) {
	path := write(t, `
host: raspberrypi
engine: claude
nats:
  urls: ["nats://127.0.0.1:4222"]
`)

	if _, err := Load(path); err == nil {
		t.Fatal("конфиг без agent_id загрузился")
	}
}

func TestLoadТребуетИзвестныйДвижок(t *testing.T) {
	path := write(t, `
agent_id: pi-claude
host: raspberrypi
engine: копилот
nats:
  urls: ["nats://127.0.0.1:4222"]
`)

	if _, err := Load(path); err == nil {
		t.Fatal("конфиг с неизвестным движком загрузился")
	}
}

// Мост — полноправный узел сети, но не Claude и не Codex.
//
// Раньше ему приходилось ставить заглушку engine: claude, и конфигурация
// врала о том, что это за узел.
func TestLoadПринимаетМостКакДвижок(t *testing.T) {
	path := write(t, `
agent_id: bridge
host: vps
engine: bridge
nats:
  urls: ["nats://127.0.0.1:4222"]
telegram:
  chat_id: "-1001234567890"
  token_env: TELEGRAM_TOKEN
  forum_topics: true
`)

	node, err := Load(path)
	if err != nil {
		t.Fatalf("конфиг моста не загрузился: %v", err)
	}
	if node.Engine != EngineBridge {
		t.Fatalf("engine = %q, ожидался %q", node.Engine, EngineBridge)
	}
	if node.Telegram.ChatID != "-1001234567890" {
		t.Fatalf("chat_id = %q", node.Telegram.ChatID)
	}
}

// Мост с чужим именем не должен запускаться вовсе.
//
// Имя определяет префикс, куда хаб шлёт ответы, а права выданы поимённо под
// `_INBOX_bridge.>`. С любым другим agent_id мост не получает ни одного
// ответа и висит на таймауте — молча, без внятной ошибки. Именно так и
// выглядела эта фикстура до правки: `mesh-bridge` читается совершенно
// правдоподобно и ломает развёртывание целиком.
func TestLoadОтвергаетМостСЧужимИменем(t *testing.T) {
	path := write(t, `
agent_id: mesh-bridge
host: vps
engine: bridge
nats:
  urls: ["nats://127.0.0.1:4222"]
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("конфиг моста с agent_id mesh-bridge загрузился")
	}
	// Оператору нужно из текста понять, что именно поставить.
	if !strings.Contains(err.Error(), BridgeAgentID) {
		t.Fatalf("в ошибке нет требуемого имени: %v", err)
	}
}

// Точка в идентификаторе ломает адресацию, потому что он попадает в тему.
func TestLoadОтвергаетТочкуВИдентификаторе(t *testing.T) {
	path := write(t, `
agent_id: pi.claude
host: raspberrypi
engine: claude
nats:
  urls: ["nats://127.0.0.1:4222"]
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("agent_id с точкой принят: тема письма разъедется с правами")
	}
	if !strings.Contains(err.Error(), "тем") {
		t.Fatalf("ошибка не объясняет причину: %v", err)
	}
}

func TestLoadПринимаетОбычныеИдентификаторы(t *testing.T) {
	for _, id := range []string{"pi-claude", "m1_codex", "bridge", "studio2"} {
		path := write(t, `
agent_id: `+id+`
host: h
engine: claude
nats:
  urls: ["nats://127.0.0.1:4222"]
`)
		if _, err := Load(path); err != nil {
			t.Errorf("идентификатор %q отвергнут: %v", id, err)
		}
	}
}

// Опечатка в имени поля обязана быть ошибкой, а не тишиной.
//
// Тот же класс, что и tmux_target выше, но ловушки на каждое возможное имя не
// напишешь. Особенно дорого молчание стоит в allowed_user_ids: пустой список
// означает, что мост не стартует, а вот НЕПРОЧИТАННЫЙ из-за опечатки выглядит
// в файле настроенным — оператор видит строку с идентификаторами и уверен,
// что ограничение работает.
func TestLoadОтвергаетНеизвестноеПоле(t *testing.T) {
	path := write(t, `
agent_id: bridge
host: vps
engine: bridge
nats:
  urls: ["nats://127.0.0.1:4222"]
telegram:
  chat_id: "-1001"
  token_env: TELEGRAM_TOKEN
  allowed_users_ids: [987654321]
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("конфиг с опечаткой allowed_users_ids загрузился молча — "+
			"ограничение на право писать от имени человека оказалось бы снято",
		)
	}
	// Имя виноватого поля обязано быть в тексте: без него оператор ищет
	// опечатку глазами по всему файлу.
	if !strings.Contains(err.Error(), "allowed_users_ids") {
		t.Fatalf("ошибка не называет поле: %v", err)
	}
}

// Незнакомое поле верхнего уровня — тот же случай.
func TestLoadОтвергаетНеизвестноеПолеВерхнегоУровня(t *testing.T) {
	path := write(t, `
agent_id: pi-claude
host: raspberrypi
engine: claude
wake_targets: screen:claude
nats:
  urls: ["nats://127.0.0.1:4222"]
`)

	if _, err := Load(path); err == nil {
		t.Fatal("незнакомое поле верхнего уровня принято молча")
	}
}
