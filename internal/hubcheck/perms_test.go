package hubcheck

// Права проверяются на живом сервере, поднятом из БОЕВОГО nats/hub.conf.
//
// Тесты в internal/bus проверяют ту же логику на своей копии прав, записанной
// через пароли. Копия удобна, но она отвечает на вопрос «правильно ли мы
// рассуждаем о темах», а не «что написано в файле, который уедет на VPS».
// Разъехаться эти два ответа могут молча: шаблон правит человек руками, и
// пропущенная строка обнаружится отсутствующим бакетом на живой машине.
//
// Здесь поднимается сам шаблон. Подменяются ровно три вещи, ни одна из
// которых к правам не относится: заглушки ключей — на эфемерные пары, блок
// tls — на ничего (сертификата Let's Encrypt вне VPS нет), пути хранения —
// на временный каталог.

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// slotOrder — порядок учёток в шаблоне.
//
// Заглушки ключей неотличимы друг от друга, поэтому единственный способ
// понять, кому какая пара досталась, — порядок. Он зашит здесь и обязан
// совпадать с файлом; расхождение поймает проверка на выданный ключ моста.
var slotOrder = []string{
	"pi-claude", "pi-codex",
	"m1-claude", "m1-codex",
	"mbp-claude", "mbp-codex",
	"studio-claude", "studio-codex",
	"bridge",
}

// liveHub поднимает хаб из шаблона и возвращает адрес и seed-файлы учёток.
func liveHub(t *testing.T) (url string, seeds map[string]string) {
	t.Helper()

	raw, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("чтение шаблона: %v", err)
	}

	dir := t.TempDir()
	seeds = make(map[string]string, len(slotOrder))

	slot := 0
	config := nkeyRe.ReplaceAllStringFunc(string(raw), func(match string) string {
		if slot >= len(slotOrder) {
			t.Fatalf("в шаблоне больше учёток, чем в slotOrder (%d)", len(slotOrder))
		}
		name := slotOrder[slot]
		slot++

		pair, err := nkeys.CreateUser()
		if err != nil {
			t.Fatalf("ключ для %s: %v", name, err)
		}
		public, err := pair.PublicKey()
		if err != nil {
			t.Fatalf("публичный ключ %s: %v", name, err)
		}
		seed, err := pair.Seed()
		if err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}

		path := filepath.Join(dir, name+".nk")
		if err := os.WriteFile(path, seed, 0o600); err != nil {
			t.Fatalf("файл ключа %s: %v", name, err)
		}
		seeds[name] = path

		// Сохраняем форму строки: подменяется только сам ключ.
		return strings.Replace(match, nkeyRe.FindStringSubmatch(match)[1], public, 1)
	})
	if slot != len(slotOrder) {
		t.Fatalf("в шаблоне %d учёток, в slotOrder %d", slot, len(slotOrder))
	}

	// TLS и пути хранения к правам отношения не имеют.
	config = regexp.MustCompile(`(?s)\ntls \{.*?\n\}\n`).ReplaceAllString(config, "\n")
	config = strings.ReplaceAll(config, `log_file: "/var/log/nats/nats-server.log"`, "")
	config = strings.ReplaceAll(config, `store_dir: "/var/lib/nats/jetstream"`,
		`store_dir: `+strconvQuote(filepath.Join(dir, "js")))

	confPath := filepath.Join(dir, "hub.conf")
	if err := os.WriteFile(confPath, []byte(config), 0o600); err != nil {
		t.Fatalf("временный конфиг: %v", err)
	}

	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatalf("шаблон не разбирается: %v", err)
	}
	opts.Port = -1 // свободный порт: тесты бегают параллельно
	opts.Host = "127.0.0.1"
	opts.NoSigs = true
	opts.NoLog = true

	server, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("создание сервера: %v", err)
	}
	go server.Start()
	if !server.ReadyForConnections(10 * time.Second) {
		t.Fatal("хаб не поднялся")
	}
	t.Cleanup(server.Shutdown)

	return server.ClientURL(), seeds
}

// strconvQuote — кавычки без импорта strconv ради одной строки.
func strconvQuote(s string) string { return `"` + s + `"` }

// connect подключается учёткой из шаблона её собственным seed-файлом.
func connect(t *testing.T, url, agent string, seeds map[string]string) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	opt, err := nats.NkeyOptionFromSeed(seeds[agent])
	if err != nil {
		t.Fatalf("nkey %s: %v", agent, err)
	}
	// Префикс ответов личный у каждого: общий _INBOX.> когда-то давал читать
	// чужую почту целиком, и права в шаблоне выданы именно поимённо.
	nc, err := nats.Connect(url, opt, nats.Name(agent),
		nats.CustomInboxPrefix("_INBOX_"+agent))
	if err != nil {
		t.Fatalf("подключение %s: %v", agent, err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream %s: %v", agent, err)
	}
	return nc, js
}

// bridgeBuckets — служебные бакеты витрины и то, что мост обязан в них уметь.
//
// bridge_posted хранит отметки «письмо показано», bridge_state — позицию
// чтения обновлений Telegram. Оба нужны, чтобы мост не терял письма и не
// показывал их повторно после рестарта.
var bridgeBuckets = []string{"bridge_posted", "bridge_state", "bridge_routes"}

// Мосту хватает прав вести свои бакеты.
//
// Проверка именно живая: не хватит одной строки в шаблоне — мост на VPS
// не создаст бакет и упадёт при старте, а до тех пор всё выглядит исправным.
func TestМостВедётСвоиБакеты(t *testing.T) {
	ctx := context.Background()
	url, seeds := liveHub(t)
	_, js := connect(t, url, "bridge", seeds)

	for _, bucket := range bridgeBuckets {
		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: bucket,
			// TTL нужен маркерам показа: они копятся на каждое письмо и
			// иначе живут вечно. Для bridge_state он безвреден — ключ там
			// перезаписывается чаще любого разумного срока.
			TTL: time.Hour,
		})
		if err != nil {
			t.Fatalf("мост не создал бакет %s: %v", bucket, err)
		}

		if _, err := kv.Create(ctx, "probe", []byte("1")); err != nil {
			t.Fatalf("мост не записал в %s: %v", bucket, err)
		}
		entry, err := kv.Get(ctx, "probe")
		if err != nil {
			t.Fatalf("мост не прочитал из %s: %v", bucket, err)
		}
		if string(entry.Value()) != "1" {
			t.Fatalf("из %s вернулось %q", bucket, entry.Value())
		}
		// Перезапись по ревизии: так Intake двигает позицию чтения.
		if _, err := kv.Update(ctx, "probe", []byte("2"), entry.Revision()); err != nil {
			t.Fatalf("мост не обновил ключ в %s: %v", bucket, err)
		}

		// Рестарт: бакет уже есть, и мост его ОТКРЫВАЕТ, а не создаёт.
		// Путь другой и прав требует других — без него нехватка STREAM.INFO
		// не проявится ни разу до первого перезапуска на живой машине,
		// то есть ровно там, где чинить дороже всего.
		reopened, err := js.KeyValue(ctx, bucket)
		if err != nil {
			t.Fatalf("мост не открыл существующий %s после рестарта: %v", bucket, err)
		}
		again, err := reopened.Get(ctx, "probe")
		if err != nil {
			t.Fatalf("мост не прочитал %s после рестарта: %v", bucket, err)
		}
		if string(again.Value()) != "2" {
			t.Fatalf("после рестарта в %s значение %q вместо %q", bucket, again.Value(), "2")
		}
	}
}

// Обычный агент не должен ни писать в служебные бакеты моста, ни читать их.
//
// Позиция чтения обновлений и отметки о показанном — это управление витриной.
// Агент, дотянувшийся до bridge_state, заставит мост переиграть переписку
// человека заново либо пропустить её; дотянувшийся до bridge_posted — скроет
// от человека любое письмо, отметив его показанным заранее.
func TestАгентНеЛезетВБакетыМоста(t *testing.T) {
	ctx := context.Background()
	url, seeds := liveHub(t)

	// Бакеты заводит мост: у агента нет и права их создать.
	_, bridgeJS := connect(t, url, "bridge", seeds)
	for _, bucket := range bridgeBuckets {
		kv, err := bridgeJS.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, TTL: time.Hour})
		if err != nil {
			t.Fatalf("подготовка бакета %s: %v", bucket, err)
		}
		if _, err := kv.Put(ctx, "foreign", []byte("значение моста")); err != nil {
			t.Fatalf("подготовка ключа в %s: %v", bucket, err)
		}
	}

	nc, js := connect(t, url, "mbp-claude", seeds)

	for _, bucket := range bridgeBuckets {
		// Через KV-обёртку агент не пройдёт уже на открытии бакета: у него
		// нет STREAM.INFO на чужой поток. Это первый рубеж.
		if _, err := js.KeyValue(ctx, bucket); err == nil {
			t.Errorf("агент открыл служебный бакет моста %s", bucket)
		}

		// Второй рубеж, и он главный: сырая публикация в тему бакета мимо
		// всякой библиотеки. Настоящий злоумышленник обёрткой не пользуется.
		//
		// Отказ по правам в core NATS приходит асинхронно, поэтому проверяем
		// не код возврата Publish (он всегда nil), а то, что значение в
		// бакете не изменилось.
		if err := nc.Publish("$KV."+bucket+".foreign", []byte("подделка")); err != nil {
			t.Fatalf("публикация: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	// Даём серверу время применить запись, если бы она прошла.
	time.Sleep(300 * time.Millisecond)

	for _, bucket := range bridgeBuckets {
		kv, err := bridgeJS.KeyValue(ctx, bucket)
		if err != nil {
			t.Fatalf("открытие %s мостом: %v", bucket, err)
		}
		entry, err := kv.Get(ctx, "foreign")
		if err != nil {
			t.Fatalf("чтение %s мостом: %v", bucket, err)
		}
		if string(entry.Value()) != "значение моста" {
			t.Errorf("агент переписал ключ в %s: там %q", bucket, entry.Value())
		}
	}
}
