// Package keygen выдаёт NKey-пары узлам и печатает готовый блок прав.
//
// Права руками не пишут. Их двенадцать строк на агента, каждая привязана к
// его идентификатору, и любая опечатка даёт молчаливый отказ: узел
// подключится, но не получит писем. Поэтому блок для hub.conf генерируется
// целиком, а человек его только вставляет.
package keygen

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nats-io/nkeys"
)

// BridgeAgentID — идентификатор узла-моста.
//
// У моста права шире: он видит всю переписку и создаёт топологию. Держим
// имя здесь же, чтобы генератор и валидация конфига не разъехались.
const BridgeAgentID = "bridge"

// Pair — выданная агенту пара ключей.
type Pair struct {
	AgentID string
	Public  string
	seed    []byte
}

// Generate создаёт пару для каждого агента.
func Generate(agentIDs []string) ([]Pair, error) {
	pairs := make([]Pair, 0, len(agentIDs))
	for _, id := range agentIDs {
		kp, err := nkeys.CreateUser()
		if err != nil {
			return nil, fmt.Errorf("создание ключа для %s: %w", id, err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("публичный ключ %s: %w", id, err)
		}
		seed, err := kp.Seed()
		if err != nil {
			return nil, fmt.Errorf("seed %s: %w", id, err)
		}
		pairs = append(pairs, Pair{AgentID: id, Public: pub, seed: seed})
	}
	return pairs, nil
}

// WriteSeeds раскладывает приватные половины по файлам.
//
// Права 0600 и отдельный файл на узел: seed — единственное в проекте, что
// нельзя вернуть после утечки. Каждому узлу увозится только его файл.
//
// Либо записываются все ключи, либо ни одного. Раньше цикл обрывался на
// первом занятом файле, а записанное до него оставалось на диске — и это
// было хуже, чем кажется: публичные ключи сирот не напечатаны (блок для
// hub.conf выводится ПОСЛЕ записи), повторный запуск падает уже на них,
// а от рабочего ключа развёрнутого узла они внешне неотличимы. Оператору
// оставалось удалять файлы наугад — ровно то разрушительное действие,
// от которого проверка и защищает.
func WriteSeeds(pairs []Pair, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("каталог %s: %w", dir, err)
	}

	// Сперва убеждаемся, что свободны ВСЕ пути, и только потом пишем.
	for _, p := range pairs {
		path := filepath.Join(dir, p.AgentID+".nk")
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("файл %s уже есть: перезапись стёрла бы рабочий ключ "+
				"узла %s. Ничего не записано, диск не тронут", path, p.AgentID)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("проверка %s: %w", path, err)
		}
	}

	for _, p := range pairs {
		path := filepath.Join(dir, p.AgentID+".nk")
		// O_EXCL, а не WriteFile: между проверкой выше и записью есть окно,
		// и создание с флагом исключительности закрывает его на уровне ядра.
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("создание %s: %w", path, err)
		}
		_, writeErr := f.Write(p.seed)
		closeErr := f.Close()
		if writeErr != nil {
			return fmt.Errorf("запись %s: %w", path, writeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("закрытие %s: %w", path, closeErr)
		}
	}
	return nil
}

// PublicFromSeedFile восстанавливает публичный ключ из приватного seed.
//
// Нужен, когда вывод генерации давно закрыт вместе с терминалом, а публичный
// ключ понадобился снова — например, чтобы поправить hub.conf через полгода.
// Он же выручает, если от прошлого запуска остались файлы без напечатанных
// ключей.
func PublicFromSeedFile(path string) (string, error) {
	seed, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("чтение %s: %w", path, err)
	}

	kp, err := nkeys.FromSeed(bytes.TrimSpace(seed))
	if err != nil {
		return "", fmt.Errorf("%s не похож на seed NKey: %w", path, err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return "", fmt.Errorf("публичный ключ из %s: %w", path, err)
	}
	return pub, nil
}

// HubBlock печатает блок authorization.users для hub.conf.
func HubBlock(pairs []Pair) string {
	var b strings.Builder
	b.WriteString("  users = [\n")
	for i, p := range pairs {
		if i > 0 {
			b.WriteString("\n")
		}
		if p.AgentID == BridgeAgentID {
			b.WriteString(bridgeUser(p))
			continue
		}
		b.WriteString(agentUser(p))
	}
	b.WriteString("  ]\n")
	return b.String()
}

// agentUser — права обычного узла.
//
// Ни одного шаблона в CONSUMER.*: изоляция ящиков держится на точном имени
// консьюмера, см. комментарий в hub.conf. Отправитель зашит в тему, поэтому
// publish разрешён только на mail.*.<свой_id>.
func agentUser(p Pair) string {
	id := p.AgentID
	return fmt.Sprintf(`    # %s
    { nkey: %s, permissions: {
        publish: { allow: [
          "mail.*.%s",
          "agents.%s.presence",
          "$JS.API.INFO",
          "$JS.API.STREAM.INFO.MAIL",
          "$JS.API.CONSUMER.CREATE.MAIL.inbox-%s.mail.%s.*",
          "$JS.API.CONSUMER.INFO.MAIL.inbox-%s",
          "$JS.API.CONSUMER.MSG.NEXT.MAIL.inbox-%s",
          "$JS.API.CONSUMER.DELETE.MAIL.inbox-%s",
          "$JS.ACK.MAIL.inbox-%s.>",
          "$KV.mail_state.%s",
          "$JS.API.STREAM.INFO.KV_mail_state",
          "$JS.API.DIRECT.GET.KV_mail_state.$KV.mail_state.%s",
          "$KV.claims.>",
          "$JS.API.STREAM.INFO.KV_claims",
          "$JS.API.DIRECT.GET.KV_claims.>",
          "$JS.API.CONSUMER.CREATE.KV_claims.>",
          "$JS.API.CONSUMER.MSG.NEXT.KV_claims.>",
          "$JS.API.CONSUMER.DELETE.KV_claims.>",
          "$JS.API.STREAM.INFO.OBJ_MAIL_FILES",
          "$JS.API.DIRECT.GET.OBJ_MAIL_FILES.>",
          "$JS.API.CONSUMER.CREATE.OBJ_MAIL_FILES.>",
          "$JS.API.CONSUMER.INFO.OBJ_MAIL_FILES.>",
          "$JS.API.CONSUMER.MSG.NEXT.OBJ_MAIL_FILES.>",
          "$JS.API.CONSUMER.DELETE.OBJ_MAIL_FILES.>"
        ] }
        subscribe: { allow: ["mail.%s.*", "agents.*.presence", "_INBOX_%s.>"] }
      }
    }
`, id, p.Public, id, id, id, id, id, id, id, id, id, id, id, id)
}

// bridgeUser — права витрины.
//
// Единственная учётка, которая видит переписку целиком и вправе писать от
// имени человека. Поэтому мост живёт на самом VPS и ходит по петле: его seed
// не уезжает на клиентские машины.
func bridgeUser(p Pair) string {
	return fmt.Sprintf(`    # %s — витрина: видит всё, пишет от имени человека
    { nkey: %s, permissions: {
        publish: { allow: [
          "mail.*.human",
          "mail.*.bridge",
          "agents.human.presence",
          "$JS.API.INFO",
          "$JS.API.STREAM.CREATE.MAIL", "$JS.API.STREAM.UPDATE.MAIL",
          "$JS.API.STREAM.INFO.MAIL",
          "$JS.API.CONSUMER.CREATE.MAIL.>",
          "$JS.API.CONSUMER.INFO.MAIL.>",
          "$JS.API.CONSUMER.MSG.NEXT.MAIL.>",
          "$JS.API.CONSUMER.DELETE.MAIL.>",
          "$JS.ACK.MAIL.>",
          "$JS.API.STREAM.CREATE.KV_mail_state", "$JS.API.STREAM.INFO.KV_mail_state",
          "$KV.bridge_topics.>",
          "$JS.API.STREAM.CREATE.KV_bridge_topics",
          "$JS.API.STREAM.INFO.KV_bridge_topics",
          "$JS.API.DIRECT.GET.KV_bridge_topics.>",
          "$JS.API.CONSUMER.CREATE.KV_bridge_topics.>",
          "$JS.API.CONSUMER.MSG.NEXT.KV_bridge_topics.>",
          "$JS.API.CONSUMER.DELETE.KV_bridge_topics.>",
          "$KV.bridge_posted.>",
          "$JS.API.STREAM.CREATE.KV_bridge_posted",
          "$JS.API.STREAM.INFO.KV_bridge_posted",
          "$JS.API.DIRECT.GET.KV_bridge_posted.>",
          "$KV.bridge_state.>",
          "$JS.API.STREAM.CREATE.KV_bridge_state",
          "$JS.API.STREAM.INFO.KV_bridge_state",
          "$JS.API.DIRECT.GET.KV_bridge_state.>",
          "$KV.bridge_routes.>",
          "$JS.API.STREAM.CREATE.KV_bridge_routes",
          "$JS.API.STREAM.INFO.KV_bridge_routes",
          "$JS.API.DIRECT.GET.KV_bridge_routes.>",
          "$JS.API.STREAM.CREATE.KV_claims",
          "$JS.API.STREAM.INFO.KV_claims",
          "$KV.claims.>",
          "$JS.API.DIRECT.GET.KV_claims.>",
          "$JS.API.CONSUMER.CREATE.KV_claims.>",
          "$JS.API.CONSUMER.MSG.NEXT.KV_claims.>",
          "$JS.API.CONSUMER.DELETE.KV_claims.>",
          "$O.MAIL_FILES.>",
          "$JS.API.STREAM.CREATE.OBJ_MAIL_FILES",
          "$JS.API.STREAM.UPDATE.OBJ_MAIL_FILES",
          "$JS.API.STREAM.INFO.OBJ_MAIL_FILES",
          "$JS.API.DIRECT.GET.OBJ_MAIL_FILES.>"
        ] }
        subscribe: { allow: ["mail.>", "agents.*.presence", "_INBOX_%s.>"] }
      }
    }
`, BridgeAgentID, p.Public, BridgeAgentID)
}
