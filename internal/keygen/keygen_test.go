package keygen

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestGenerateДаётРазныеПары(t *testing.T) {
	pairs, err := Generate([]string{"pi-claude", "m1-codex"})
	if err != nil {
		t.Fatalf("генерация: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("пар %d, ожидалось 2", len(pairs))
	}
	if pairs[0].Public == pairs[1].Public {
		t.Fatal("у разных агентов одинаковый ключ")
	}
	for _, p := range pairs {
		if !strings.HasPrefix(p.Public, "U") {
			t.Errorf("публичный ключ %s не похож на пользовательский: %q", p.AgentID, p.Public)
		}
		if !strings.HasPrefix(string(p.seed), "SU") {
			t.Errorf("seed %s не похож на пользовательский", p.AgentID)
		}
	}
}

// Приватная половина — единственное в проекте, что нельзя вернуть после
// утечки, поэтому файл читается только владельцем.
func TestWriteSeedsКладётФайлыПодЗамок(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "secrets")
	pairs, _ := Generate([]string{"pi-claude"})

	if err := WriteSeeds(pairs, dir); err != nil {
		t.Fatalf("запись: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "pi-claude.nk"))
	if err != nil {
		t.Fatalf("файл не создан: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("права на seed %o, ожидались 600", perm)
	}
}

// Перезапись стёрла бы рабочий ключ узла, который уже развёрнут.
func TestWriteSeedsНеЗатираетСуществующий(t *testing.T) {
	dir := t.TempDir()
	pairs, _ := Generate([]string{"pi-claude"})
	if err := WriteSeeds(pairs, dir); err != nil {
		t.Fatalf("первая запись: %v", err)
	}

	again, _ := Generate([]string{"pi-claude"})
	if err := WriteSeeds(again, dir); err == nil {
		t.Fatal("повторная запись затёрла существующий ключ")
	}
}

// В правах агента не должно быть ни одного шаблона на консьюмерах: именно
// на точном имени держится изоляция ящиков.
func TestHubBlockБезШаблоновУАгента(t *testing.T) {
	pairs, _ := Generate([]string{"pi-claude"})
	block := HubBlock(pairs)

	for _, forbidden := range []string{
		"CONSUMER.CREATE.MAIL.>", "CONSUMER.MSG.NEXT.MAIL.>",
		"CONSUMER.DELETE.MAIL.>", "STREAM.MSG.GET", "_INBOX.>",
		"$JS.API.STREAM.CREATE.MAIL",
	} {
		if strings.Contains(block, forbidden) {
			t.Errorf("в правах агента появился %q — это открывает чужую переписку", forbidden)
		}
	}
	for _, required := range []string{
		"mail.*.pi-claude",
		"CONSUMER.CREATE.MAIL.inbox-pi-claude.mail.pi-claude.*",
		"CONSUMER.MSG.NEXT.MAIL.inbox-pi-claude",
		"_INBOX_pi-claude.>",
		"mail.pi-claude.*",
	} {
		if !strings.Contains(block, required) {
			t.Errorf("в правах агента нет обязательного %q", required)
		}
	}
}

// Мост — единственный, кому позволено писать от имени человека.
func TestHubBlockМостуПозволеноБольше(t *testing.T) {
	pairs, _ := Generate([]string{"pi-claude", BridgeAgentID})
	block := HubBlock(pairs)

	if !strings.Contains(block, `"mail.*.human"`) {
		t.Error("мост не может писать от имени человека")
	}
	if !strings.Contains(block, `"_INBOX_bridge.>"`) {
		t.Error("у моста нет своего namespace ответов")
	}

	agentPart := block[:strings.Index(block, "витрина")]
	if strings.Contains(agentPart, "mail.*.human") {
		t.Error("обычный агент может писать от имени человека — это дыра")
	}
}

// Либо все ключи, либо ни одного.
//
// Раньше цикл обрывался на первом занятом файле, а записанное до него
// оставалось: публичные ключи сирот нигде не напечатаны, повторный запуск
// падает уже на них, а от рабочих ключей они неотличимы.
func TestWriteSeedsНеОставляетСирот(t *testing.T) {
	dir := t.TempDir()

	// В каталоге уже развёрнут узел c.
	existing, _ := Generate([]string{"c"})
	if err := WriteSeeds(existing, dir); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	pairs, _ := Generate([]string{"a", "b", "c", "d"})
	if err := WriteSeeds(pairs, dir); err == nil {
		t.Fatal("запись поверх существующего ключа прошла")
	}

	for _, orphan := range []string{"a", "b", "d"} {
		if _, err := os.Stat(filepath.Join(dir, orphan+".nk")); err == nil {
			t.Errorf("остался ключ-сирота %s.nk: его публичный ключ нигде не напечатан", orphan)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "c.nk")); err != nil {
		t.Error("существующий ключ пострадал")
	}
}

// Публичный ключ восстанавливается из seed: вывод генерации закрывается
// вместе с терминалом, а hub.conf правят и через полгода.
func TestPublicFromSeedFile(t *testing.T) {
	dir := t.TempDir()
	pairs, _ := Generate([]string{"pi-claude"})
	if err := WriteSeeds(pairs, dir); err != nil {
		t.Fatalf("запись: %v", err)
	}

	got, err := PublicFromSeedFile(filepath.Join(dir, "pi-claude.nk"))
	if err != nil {
		t.Fatalf("восстановление: %v", err)
	}
	if got != pairs[0].Public {
		t.Fatalf("восстановлен %q, ожидался %q", got, pairs[0].Public)
	}
}

func TestPublicFromSeedFileОтвергаетМусор(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-key.nk")
	if err := os.WriteFile(path, []byte("это не ключ"), 0o600); err != nil {
		t.Fatalf("подготовка: %v", err)
	}
	if _, err := PublicFromSeedFile(path); err == nil {
		t.Fatal("мусор принят за seed")
	}
}

// Записанный seed обязан читаться тем же кодом, что читает его в бою.
//
// Тесты выше проверяют права и содержимое, но не то, что nats.go примет
// файл: формат мог бы разойтись незаметно, а живой прогон руками никто
// не повторяет при каждом изменении.
func TestSeedЧитаетсяКлиентомNATS(t *testing.T) {
	dir := t.TempDir()
	pairs, _ := Generate([]string{"pi-claude"})
	if err := WriteSeeds(pairs, dir); err != nil {
		t.Fatalf("запись: %v", err)
	}

	if _, err := nats.NkeyOptionFromSeed(filepath.Join(dir, "pi-claude.nk")); err != nil {
		t.Fatalf("nats.go не принял наш seed-файл: %v", err)
	}
}

// Блок из keygen обязан совпадать с hub.conf по составу прав.
//
// Разъедутся — развёртывание с нуля даст сеть, где чего-то нет, и выяснится
// это молчаливым отказом на живой машине.
func TestHubBlockДаётПраваНаРеестрЗон(t *testing.T) {
	pairs, _ := Generate([]string{"pi-claude", BridgeAgentID})
	block := HubBlock(pairs)

	for _, required := range []string{
		`"$KV.claims.>"`,
		`"$JS.API.CONSUMER.CREATE.KV_claims.>"`,
	} {
		if strings.Count(block, required) < 2 {
			t.Errorf("право %s есть не у всех учёток", required)
		}
	}
	// Создавать бакеты вправе только мост — у агентов STREAM.CREATE нет.
	if strings.Count(block, `"$JS.API.STREAM.CREATE.KV_claims"`) != 1 {
		t.Error("создание бакета claims должно быть только у моста")
	}
}

// templatePath — шаблон конфигурации хаба, лежащий в репозитории.
const templatePath = "../../nats/hub.conf"

// permsRe достаёт строки прав: всё, что в кавычках.
var permsRe = regexp.MustCompile(`"([^"]+)"`)

// accountPerms вырезает права одной учётки из куска конфигурации.
//
// Комментарии выбрасываются целиком: в них встречаются те же темы, что и в
// правах, и без этого «право» находилось бы там, где его сняли, оставив
// объяснение почему.
func accountPerms(block string) (publish, subscribe map[string]bool) {
	var clean strings.Builder
	for line := range strings.SplitSeq(block, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		clean.WriteString(line)
		clean.WriteString("\n")
	}
	text := clean.String()

	section := func(keyword string) map[string]bool {
		out := map[string]bool{}
		start := strings.Index(text, keyword)
		if start < 0 {
			return out
		}
		rest := text[start:]
		end := strings.Index(rest, "] }")
		if end < 0 {
			end = len(rest)
		}
		for _, match := range permsRe.FindAllStringSubmatch(rest[:end], -1) {
			out[match[1]] = true
		}
		return out
	}
	return section("publish:"), section("subscribe:")
}

// splitAccounts режет конфигурацию на учётки по строке с nkey.
func splitAccounts(text string) []string {
	parts := strings.Split(text, "{ nkey:")
	if len(parts) <= 1 {
		return nil
	}
	return parts[1:]
}

// Шаблон hub.conf и вывод keygen обязаны давать ОДИН И ТОТ ЖЕ состав прав.
//
// Это два независимых источника одной истины: по шаблону поднимают хаб при
// развёртывании с нуля, а блок из keygen оператор вставляет туда же руками.
// Разъезжаются они молча — расхождение вскрывается на живой машине тем, что
// мост не может создать бакет или узел не получает писем. Ни один из
// существующих тестов этой сверки не делал: keygen_test смотрел только внутрь
// собственного вывода, hubcheck — только внутрь шаблона.
func TestШаблонИГенераторНеРазъезжаются(t *testing.T) {
	raw, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("чтение шаблона: %v", err)
	}

	accounts := splitAccounts(string(raw))
	if len(accounts) == 0 {
		t.Fatal("в шаблоне не нашлось ни одной учётки — разбор промахнулся")
	}

	// Первая учётка в шаблоне — pi-claude, последняя — мост. Порядок задан
	// самим файлом и проверяется тем, что права совпадут: у моста и агента
	// они разные настолько, что перепутать невозможно.
	tmplAgentPub, tmplAgentSub := accountPerms(accounts[0])
	tmplBridgePub, tmplBridgeSub := accountPerms(accounts[len(accounts)-1])

	pairs, err := Generate([]string{"pi-claude", BridgeAgentID})
	if err != nil {
		t.Fatalf("генерация пар: %v", err)
	}
	generated := splitAccounts(HubBlock(pairs))
	if len(generated) != 2 {
		t.Fatalf("keygen выдал %d учёток вместо двух", len(generated))
	}
	genAgentPub, genAgentSub := accountPerms(generated[0])
	genBridgePub, genBridgeSub := accountPerms(generated[1])

	compare(t, "агент/publish", tmplAgentPub, genAgentPub)
	compare(t, "агент/subscribe", tmplAgentSub, genAgentSub)
	compare(t, "мост/publish", tmplBridgePub, genBridgePub)
	compare(t, "мост/subscribe", tmplBridgeSub, genBridgeSub)
}

// compare показывает расхождение поимённо: «множества не равны» заставило бы
// сличать по сорок строк глазами.
func compare(t *testing.T, what string, template, generated map[string]bool) {
	t.Helper()

	for right := range template {
		if !generated[right] {
			t.Errorf("%s: право %q есть в nats/hub.conf, но keygen его не выдаёт — "+
				"развёртывание с нуля даст сеть без него", what, right)
		}
	}
	for right := range generated {
		if !template[right] {
			t.Errorf("%s: keygen выдаёт право %q, которого нет в nats/hub.conf — "+
				"поднятый по шаблону хаб окажется строже, чем ожидает узел", what, right)
		}
	}
}
