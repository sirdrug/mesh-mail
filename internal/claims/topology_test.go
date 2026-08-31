package claims

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/bustest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// permsConf — те же права на реестр зон, что уходят в nats/hub.conf.
//
// Ключевое здесь — чего у агента НЕТ: права $JS.API.STREAM.CREATE.KV_claims.
// Оно и не должно выдаваться: создав бакет, агент задал бы ему TTL и
// описание, то есть переконфигурировал общий реестр под себя. Именно из-за
// этого запрета и появился дефект, который чинится ниже: попытка создать
// бакет без права не отвергается сразу, а уходит в никуда, и клиент ждёт до
// собственного таймаута.
const permsConf = `
jetstream: enabled
authorization {
  users = [
    { user: "pi-claude", password: "p", permissions: {
        publish:   { allow: ["$JS.API.INFO",
                             "$KV.claims.>",
                             "$JS.API.STREAM.INFO.KV_claims",
                             "$JS.API.DIRECT.GET.KV_claims.>",
                             "$JS.API.CONSUMER.CREATE.KV_claims.>",
                             "$JS.API.CONSUMER.MSG.NEXT.KV_claims.>",
                             "$JS.API.CONSUMER.DELETE.KV_claims.>"] }
        subscribe: { allow: ["_INBOX_pi-claude.>"] }
      }
    }
    # Мост: единственная учётка, которой позволено поднимать топологию.
    { user: "bridge", password: "p" }
  ]
}
`

func connectAs(t *testing.T, url, user string) jetstream.JetStream {
	t.Helper()

	nc, err := nats.Connect(url, nats.UserInfo(user, "p"), nats.Name(user),
		nats.CustomInboxPrefix(bus.InboxPrefix(user)))
	if err != nil {
		t.Fatalf("подключение %s: %v", user, err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream для %s: %v", user, err)
	}
	return js
}

// Агент, пришедший на хаб без реестра, узнаёт об этом сразу и по делу.
//
// Проверяются ДВЕ вещи разом, и обе нужны.
//
// Первая — что ответ вообще про дело: в тексте должен быть мост, потому что
// починка ровно одна — запустить его первым. Прежняя ошибка говорила
// «context deadline exceeded», из чего человек делал вывод про сеть или
// тормозящий хаб и шёл чинить не то.
//
// Вторая — что ответ приходит БЫСТРО. Без этого проверка бессмысленна:
// внятный текст можно вернуть и через пять секунд ожидания, и тест на
// подстроку такую починку принял бы. Пять секунд на старте узла — это узел,
// который «не запускается», а не узел, который объяснил причину.
func TestОтсутствиеРеестраОбъясняетсяСразу(t *testing.T) {
	url := bustest.StartTestServerWithConf(t, permsConf)
	js := connectAs(t, url, "pi-claude")

	// Мост не поднимали: бакета нет, как на свежем хабе.
	start := time.Now()
	_, err := NewStore(context.Background(), js)
	spent := time.Since(start)

	if err == nil {
		t.Fatal("реестр открылся там, где бакета нет: агент решит, что все зоны свободны")
	}
	if spent > 2*time.Second {
		t.Errorf("ответ через %s — это не отказ, а зависание на старте узла", spent.Round(time.Millisecond))
	}
	if !strings.Contains(err.Error(), "мост") {
		t.Errorf("ошибка не подсказывает починку: %v", err)
	}
}

// Мост поднял реестр — агент им пользуется.
//
// Проверка порядка раскатки целиком: мост первым, узлы после. Без неё
// «внятная ошибка» могла бы оказаться единственным, что умеет код, и реестр
// не заработал бы вовсе — тест выше остался бы зелёным.
func TestМостСоздаётРеестрИАгентИмПользуется(t *testing.T) {
	ctx := context.Background()
	url := bustest.StartTestServerWithConf(t, permsConf)

	if err := EnsureBucket(ctx, connectAs(t, url, "bridge")); err != nil {
		t.Fatalf("мост не создал реестр: %v", err)
	}

	store, err := NewStore(ctx, connectAs(t, url, "pi-claude"))
	if err != nil {
		t.Fatalf("агент не открыл созданный мостом реестр: %v", err)
	}

	if _, err := store.Take(ctx, "internal/mail", "pi-claude", "дедупликация адресатов"); err != nil {
		t.Fatalf("захват: %v", err)
	}
	held, err := store.List(ctx)
	if err != nil {
		t.Fatalf("список: %v", err)
	}
	if len(held) != 1 || held[0].AgentID != "pi-claude" {
		t.Fatalf("реестр вернул %v, ожидался один захват pi-claude", held)
	}
}

// Агенту по-прежнему нельзя создавать бакет.
//
// Инвариант прав, а не поведения кода: даже если завтра кто-то вернёт вызов
// CreateKeyValue в агентский путь, хаб обязан ему отказать. Тест стережёт
// саму конфигурацию — ту, что уезжает на VPS.
func TestАгентНеСоздаётРеестрДажеНапрямую(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	url := bustest.StartTestServerWithConf(t, permsConf)
	js := connectAs(t, url, "pi-claude")

	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: Bucket}); err == nil {
		t.Fatal("агент создал бакет claims — право STREAM.CREATE выдано по ошибке")
	}
}
