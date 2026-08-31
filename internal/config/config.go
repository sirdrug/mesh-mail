// Package config читает описание узла.
//
// Секретов в YAML нет: путь к NKey подставляется из окружения через ${VAR}.
// Файл коммитится, секреты — нет.
package config

import (
	"bytes"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// NATSConfig — как достучаться до хаба.
type NATSConfig struct {
	URLs         []string `yaml:"urls"`
	NKeySeedFile string   `yaml:"nkey_seed_file"`
	CAFile       string   `yaml:"ca_file"`
}

// TelegramConfig — настройки витрины. Нужны только узлу-мосту.
type TelegramConfig struct {
	ChatID string `yaml:"chat_id"`
	// TokenEnv — имя переменной окружения с токеном, а не сам токен:
	// конфиг коммитится, секреты нет.
	TokenEnv    string `yaml:"token_env"`
	ForumTopics bool   `yaml:"forum_topics"`
	// AllowedUserIDs — числовые id тех, кому позволено писать агентам.
	// Username сюда не годится: владелец меняет его когда угодно.
	AllowedUserIDs []int64 `yaml:"allowed_user_ids"`
}

// Движок узла: чем именно он занят в сети.
//
// Мост не является ни Claude, ни Codex — за ним не стоит агента вообще, он
// только зеркалит переписку в телеграм. Раньше приходилось ставить ему
// заглушку `engine: claude`, и конфигурация врала о том, что это за узел.
const (
	EngineClaude = "claude"
	EngineCodex  = "codex"
	EngineBridge = "bridge"
)

// BridgeAgentID — единственно допустимый agent_id узла-моста.
//
// Ровно это имя, и выбирать его оператор не может. Права в nats/hub.conf
// выданы учётке моста поимённо, и в них есть `subscribe: _INBOX_bridge.>`.
// Префикс, куда хаб шлёт ответы, строится из agent_id узла (bus.InboxPrefix),
// а не из имени учётки, — поэтому любой другой agent_id уводит ответы
// в поддерево, на которое мост не подписан.
//
// Отказ при этом молчаливый и особенно мерзкий: запросы уходят и исполняются,
// а ответы не возвращаются. Мост висит на таймауте каждого обращения к
// JetStream, в логе — только асинхронное «Permissions Violation for
// Subscription», ничего не говорящее о причине. Стережёт
// TestМостСЧужимИменемНеПолучаетОтветов на живом сервере.
const BridgeAgentID = "bridge"

// validAgentID — что допустимо в идентификаторе узла.
//
// Точка запрещена не из вкусовщины: идентификатор попадает в тему письма,
// а в NATS точка разделяет токены. `pi.claude` дал бы четырёхтокенную тему,
// права `mail.*.<self>` перестали бы совпадать, и узел молча остался бы без
// писем.
var validAgentID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// ValidateAgentID проверяет идентификатор узла.
//
// Отдельная функция, а не проверка внутри Load: тем же правилам обязан
// подчиняться keygen, иначе ключи выдадут узлу, который потом не сможет
// загрузить конфиг.
func ValidateAgentID(id string) error {
	if id == "" {
		return fmt.Errorf("пустой agent_id")
	}
	if !validAgentID.MatchString(id) {
		return fmt.Errorf("agent_id %q недопустим: разрешены латиница, цифры, "+
			"дефис и подчёркивание. Точка ломает адресацию — идентификатор "+
			"становится частью темы письма (mail.<получатель>.<отправитель>), "+
			"и лишний токен разъедет права на хабе", id)
	}
	return nil
}

// Node — описание одного узла сети.
type Node struct {
	AgentID  string   `yaml:"agent_id"`
	Host     string   `yaml:"host"`
	Engine   string   `yaml:"engine"`
	Projects []string `yaml:"projects"`
	Tags     []string `yaml:"tags"`

	// WakeTarget — куда слать уведомление на Codex-узлах: способ и сессия,
	// например "screen:codex" или "tmux:pi-codex". Для Claude-узлов не
	// нужен — там будит Monitor.
	//
	// Способ пишется явно, потому что мультиплексор мы человеку не диктуем:
	// на Pi, скажем, все агенты живут в screen, и демон, знающий только tmux,
	// не разбудил бы никого.
	WakeTarget string `yaml:"wake_target"`

	// LegacyTmuxTarget существует только чтобы поймать старое имя поля.
	//
	// yaml молча игнорирует незнакомые ключи, поэтому оставшийся в конфиге
	// `tmux_target` не вызвал бы ни ошибки, ни предупреждения: демон просто
	// не будил бы никого и не сообщал бы об этом. Такой отказ мы сегодня уже
	// разбирали в мосте.
	LegacyTmuxTarget string `yaml:"tmux_target"`

	NATS     NATSConfig     `yaml:"nats"`
	Telegram TelegramConfig `yaml:"telegram"`
}

// Load читает и проверяет конфигурацию узла.
func Load(path string) (*Node, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("чтение конфига: %w", err)
	}

	expanded := os.ExpandEnv(string(raw))

	// KnownFields, а не Unmarshal: yaml по умолчанию молча пропускает поля,
	// которых нет в структуре. Опечатка в имени ключа тогда не отвергается —
	// она просто ничего не делает, и конфигурация врёт о том, что настроено.
	//
	// Цена этого молчания несимметрична. `allowed_users_ids` вместо
	// `allowed_user_ids` оставляет список получателей пустым, а пустой список
	// снимает ограничение на то, кто вправе писать от имени человека. Оператор
	// при этом видит в файле строку, которая, как ему кажется, работает.
	//
	// Ловушка на одно забытое поле у нас уже есть (LegacyTmuxTarget ниже) и
	// она подтверждает диагноз: перечислять поимённо каждую возможную опечатку
	// — заведомо проигранная гонка. Отвергать надо всё незнакомое.
	decoder := yaml.NewDecoder(bytes.NewReader([]byte(expanded)))
	decoder.KnownFields(true)

	var node Node
	if err := decoder.Decode(&node); err != nil {
		return nil, fmt.Errorf("разбор конфига: %w", err)
	}

	if node.AgentID == "" {
		return nil, fmt.Errorf("в конфиге не задан agent_id")
	}
	if err := ValidateAgentID(node.AgentID); err != nil {
		return nil, err
	}
	switch node.Engine {
	case EngineClaude, EngineCodex, EngineBridge:
	default:
		return nil, fmt.Errorf("неизвестный движок %q, ожидался один из: %s, %s, %s",
			node.Engine, EngineClaude, EngineCodex, EngineBridge)
	}
	// Ловим здесь, при загрузке, а не таймаутом получаса спустя: имя моста
	// определяет префикс ответов, и ошибка в нём не проявляется ничем, кроме
	// зависания.
	if node.Engine == EngineBridge && node.AgentID != BridgeAgentID {
		return nil, fmt.Errorf(
			"узел-мост обязан иметь agent_id %q, а не %q: права в hub.conf выданы поимённо, "+
				"и с другим именем ответы хаба уходят мимо моста",
			BridgeAgentID, node.AgentID)
	}
	if node.LegacyTmuxTarget != "" {
		return nil, fmt.Errorf(
			"поле tmux_target больше не читается, переименуйте его в wake_target "+
				"и укажите способ: wake_target: %q или %q",
			"tmux:"+node.LegacyTmuxTarget, "screen:<сессия>")
	}
	if len(node.NATS.URLs) == 0 {
		return nil, fmt.Errorf("не задан ни один адрес хаба")
	}

	return &node, nil
}
