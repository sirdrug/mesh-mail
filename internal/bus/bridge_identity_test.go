package bus

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// bridgeConf — учётка моста с правами из nats/hub.conf дословно, включая
// подписку только на `_INBOX_bridge.>`.
//
// В permsConf мост оставлен вовсе без ограничений: там он лишь поднимает
// топологию для чужих проверок. Из-за этого расхождение имени всплыть не
// могло — тесты соединялись как connectAs(url, "bridge") и сами задавали
// префикс `_INBOX_bridge`, а боевой mesh берёт имя из agent_id конфига.
const bridgeConf = `
jetstream: enabled
authorization {
  users = [
    { user: "bridge", password: "p", permissions: {
        publish: { allow: [
          "mail.*", "agents.human.presence",
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
          "$JS.API.CONSUMER.DELETE.KV_bridge_topics.>"
        ] }
        subscribe: { allow: ["mail.>", "agents.*.presence", "_INBOX_bridge.>"] }
      }
    }
  ]
}
`

// connectBridgeAs подключается учёткой моста, но с префиксом ответов от
// произвольного agent_id — ровно так, как это делает bus.Connect в mesh.
func connectBridgeAs(t *testing.T, url, agentID string) jetstream.JetStream {
	t.Helper()

	nc, err := nats.Connect(url, nats.UserInfo("bridge", "p"), nats.Name("mesh-bridge-"+agentID),
		nats.CustomInboxPrefix(InboxPrefix(agentID)))
	if err != nil {
		t.Fatalf("подключение моста как %s: %v", agentID, err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream для %s: %v", agentID, err)
	}
	return js
}

// Мост обязан зваться ровно `bridge`, иначе он не получает ни одного ответа.
//
// Права выданы учётке поимённо: `subscribe: _INBOX_bridge.>`. Префикс ответов
// строится из agent_id узла, а не из имени учётки, — значит любой другой
// agent_id уводит ответы в поддерево, на которое мост не подписан. Запросы
// уходят и исполняются, ответы не возвращаются: мост молча висит на таймауте
// вместо внятного отказа. Диагноз по логам почти невозможен, поэтому имя
// закреплено в config.Load, а не оставлено на внимательность оператора.
func TestМостСЧужимИменемНеПолучаетОтветов(t *testing.T) {
	url := startServerWithConf(t, bridgeConf)

	// Так выглядела фикстура конфига моста: правдоподобное имя, ломающее всё.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := EnsureTopology(ctx, connectBridgeAs(t, url, "mesh-bridge")); err == nil {
		t.Fatal("мост с agent_id mesh-bridge поднял топологию, хотя подписан только на _INBOX_bridge.>")
	}

	ctxOK, cancelOK := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelOK()
	if err := EnsureTopology(ctxOK, connectBridgeAs(t, url, "bridge")); err != nil {
		t.Fatalf("мост с правильным именем не поднял топологию: %v", err)
	}
}
