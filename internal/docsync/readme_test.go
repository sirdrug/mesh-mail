package docsync_test

// Сверка README с кодом.
//
// Оба теста написаны после того, как README дважды за день разошёлся с
// реальностью, причём во второй раз — в описании изоляции ящиков, то есть
// там, где ошибка стоит чужой переписки. Расхождение находилось глазами,
// и оба раза случайно: сверять таблицу с исходником никто не обязан.
//
// Проверка сравнивает МНОЖЕСТВА в обе стороны. Односторонняя («каждое поле
// кода упомянуто в README») пропускает лишнее в документе — а именно так
// выглядит устаревшая строка: поле давно переименовано, а запись о нём
// осталась и читается как действующая.

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/bridge"
	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/claims"
	"github.com/boreevyuri/mesh-mail/internal/mail"
)

// readme читает README.md из корня репозитория.
func readme(t *testing.T) string {
	t.Helper()
	// Тест исполняется в каталоге своего пакета; корень на два уровня выше.
	path := filepath.Join("..", "..", "README.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение %s: %v", path, err)
	}
	return string(data)
}

// section вырезает раздел README по его заголовку — до следующего заголовка
// того же или более высокого уровня.
func section(t *testing.T, text, heading string) string {
	t.Helper()
	start := strings.Index(text, heading)
	if start < 0 {
		t.Fatalf("в README нет раздела %q — переименовали или удалили", heading)
	}
	rest := text[start+len(heading):]
	// Следующий заголовок любого уровня заканчивает раздел.
	if end := regexp.MustCompile(`(?m)^#{1,3} `).FindStringIndex(rest); end != nil {
		return rest[:end[0]]
	}
	return rest
}

// Таблица полей письма в README совпадает со структурой mail.Message.
//
// Ошибка здесь тихая и живучая: читатель берёт имя поля из документа и
// пишет код, который молча не находит его в JSON.
func TestТаблицаПолейПисьмаСовпадаетСоСтруктурой(t *testing.T) {
	вДокументе := полиИзТаблицы(t, section(t, readme(t), "### Письмо"))
	вКоде := полиСтруктуры(mail.Message{})

	if !reflect.DeepEqual(вДокументе, вКоде) {
		t.Errorf("таблица полей письма разошлась со структурой Message.\n"+
			"в README:    %v\nв коде:      %v\n"+
			"лишнее в README: %v\nне описано:      %v",
			вДокументе, вКоде,
			лишние(вДокументе, вКоде), лишние(вКоде, вДокументе))
	}
}

// Схема хранилищ в README совпадает с именами в коде.
//
// Хранилищ семь, и каждое заводится своей константой. Забытое в README
// хранилище означает, что при восстановлении из резервной копии его никто
// не хватится, — а это ровно тот случай, когда узнают поздно.
func TestСхемаХранилищСовпадаетСКодом(t *testing.T) {
	вДокументе := хранилищаИзСхемы(t, readme(t))
	вКоде := []string{
		bus.StreamName,
		bus.StateBucket,
		bridge.TopicBucket,
		bridge.RouteBucket,
		bridge.StateBucket,
		bridge.PostedBucket,
		claims.Bucket,
	}
	sort.Strings(вКоде)

	if !reflect.DeepEqual(вДокументе, вКоде) {
		t.Errorf("схема хранилищ разошлась с кодом.\n"+
			"в README: %v\nв коде:   %v\n"+
			"лишнее в README: %v\nне описано:      %v",
			вДокументе, вКоде,
			лишние(вДокументе, вКоде), лишние(вКоде, вДокументе))
	}
}

// Контроль оракула: разбор README вообще что-то находит.
//
// Без него оба теста выше остаются зелёными, если разметка изменится и
// парсер начнёт возвращать пустоту: пустое множество совпадёт само с собой
// только при пустом коде, но регрессия парсера на глаз неотличима от
// «всё сошлось». Числа посчитаны руками по текущему README.
func TestРазборREADMEНеПустой(t *testing.T) {
	text := readme(t)

	поля := полиИзТаблицы(t, section(t, text, "### Письмо"))
	if len(поля) < 8 {
		t.Errorf("из таблицы полей разобрано %d имён (%v) — парсер сломался", len(поля), поля)
	}

	хранилища := хранилищаИзСхемы(t, text)
	if len(хранилища) < 7 {
		t.Errorf("из схемы разобрано %d хранилищ (%v) — парсер сломался", len(хранилища), хранилища)
	}
}

// полиИзТаблицы достаёт имена полей из первой колонки markdown-таблицы.
//
// Имена берутся только из обратных кавычек: в той же колонке встречается
// пояснительный текст, и брать её целиком значило бы ловить слова.
func полиИзТаблицы(t *testing.T, раздел string) []string {
	t.Helper()

	вКавычках := regexp.MustCompile("`([^`]+)`")
	var out []string
	seen := make(map[string]struct{})

	for _, line := range strings.Split(раздел, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || strings.HasPrefix(line, "|---") {
			continue
		}
		колонки := strings.Split(strings.Trim(line, "|"), "|")
		if len(колонки) == 0 {
			continue
		}
		for _, m := range вКавычках.FindAllStringSubmatch(колонки[0], -1) {
			// `to[]` в документе — то же поле, что `to` в структуре.
			имя := strings.TrimSuffix(strings.TrimSpace(m[1]), "[]")
			if имя == "" || имя == "Поле" {
				continue
			}
			if _, dup := seen[имя]; dup {
				continue
			}
			seen[имя] = struct{}{}
			out = append(out, имя)
		}
	}
	sort.Strings(out)
	return out
}

// полиСтруктуры возвращает имена json-тегов структуры.
func полиСтруктуры(v any) []string {
	typ := reflect.TypeOf(v)
	var out []string
	for i := range typ.NumField() {
		tag := typ.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		out = append(out, strings.Split(tag, ",")[0])
	}
	sort.Strings(out)
	return out
}

// хранилищаИзСхемы достаёт имена потоков и бакетов из блок-схемы README.
func хранилищаИзСхемы(t *testing.T, text string) []string {
	t.Helper()

	строка := regexp.MustCompile(`(?m)^\s*(?:поток|KV)\s+(\S+)`)
	var out []string
	seen := make(map[string]struct{})
	for _, m := range строка.FindAllStringSubmatch(text, -1) {
		if _, dup := seen[m[1]]; dup {
			continue
		}
		seen[m[1]] = struct{}{}
		out = append(out, m[1])
	}
	sort.Strings(out)
	return out
}

// лишние возвращает элементы a, которых нет в b.
func лишние(a, b []string) []string {
	есть := make(map[string]struct{}, len(b))
	for _, s := range b {
		есть[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := есть[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
