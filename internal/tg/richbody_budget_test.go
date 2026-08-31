package tg

import (
	"regexp"
	"strings"
	"testing"
)

// tagPattern — независимый разбор тегов для проверки баланса.
//
// Написан здесь, а не взят из рендерера: тест обязан судить о выводе сам, а
// не спрашивать у проверяемого кода, что он считает тегом.
var tagPattern = regexp.MustCompile(`<(/?)([a-zA-Z]+)[^>]*>`)

// requireBalanced — теги открыты и закрыты правильно и в правильном порядке.
//
// Несбалансированную разметку Telegram отвергает целиком, то есть письмо
// пропадает — а пропадает оно ровно тогда, когда тело длинное и обрезано,
// когда проверить глазами труднее всего.
func requireBalanced(t *testing.T, html string) {
	t.Helper()

	var stack []string
	for _, m := range tagPattern.FindAllStringSubmatch(html, -1) {
		closing, name := m[1] == "/", m[2]
		if !closing {
			stack = append(stack, name)
			continue
		}
		if len(stack) == 0 {
			t.Fatalf("закрытие </%s> без открытия: %q", name, html)
		}
		if last := stack[len(stack)-1]; last != name {
			t.Fatalf("закрыт <%s>, а открыт был <%s>: %q", name, last, html)
		}
		stack = stack[:len(stack)-1]
	}
	if len(stack) != 0 {
		t.Fatalf("остались открытыми %v: %q", stack, html)
	}
}

// requireWithinBudget — предел соблюдён по рунам ГОТОВОЙ разметки.
func requireWithinBudget(t *testing.T, html string, budget int) {
	t.Helper()

	if got := len([]rune(html)); got > budget {
		t.Fatalf("вывод %d рун при пределе %d: %q", got, budget, html)
	}
}

// видимоеТело — то, что человек прочтёт: без тегов, без экранирования и без
// знака обрезки.
//
// Служебные приставки — маркер строки, «> », буллет, отступ — намеренно НЕ
// снимаются. Снимать их значит каждый раз угадывать, что служебное, а что
// письмо; за день такое угадывание врало трижды и каждый раз по-новому:
// сперва «> » пропадало из фразы «c > d», потом «│ » исчезало из таблицы
// внутри блока кода, потом попытка различать блоки съела приставку цитаты у
// всех профилей сразу.
//
// Восстановить канонический текст письма из готовой разметки нельзя в
// принципе: в общем блоке структурная приставка и буквальное начало строки
// кода неразличимы. Поэтому свойства этот helper НЕ используют — они
// опираются на счётчик покрытия, а он удостоверен числами, посчитанными
// вручную.
//
// Остался он для одного: адресных проверок, что символы письма пережили
// снятие тегов и разэкранирование (TestОракулНеСъедаетСимволыПисьма).
func видимоеТело(html string) string {
	return strings.TrimSuffix(visibleText(html), TruncationMark)
}

// budgetBodies — тела, по-разному расходующие предел.
var budgetBodies = map[string]string{
	"обычный текст":   strings.Repeat("обычная строка письма. ", 200),
	"жирный":          "**" + strings.Repeat("очень жирный текст ", 200) + "**",
	"спецсимволы":     strings.Repeat("a<b>&\"'</b>c ", 200),
	"эмодзи":          strings.Repeat("✉️🔑👀 ", 200),
	"длинный адрес":   "смотри https://example.com/" + strings.Repeat("a", 3000),
	"блок кода":       "```\n" + strings.Repeat("func main() {}\n", 200) + "```",
	"список":          strings.Repeat("- пункт списка\n", 200),
	"кривая разметка": "**[a](" + strings.Repeat("*_`~", 300) + "\n> ```\n**",
	"много абзацев":   strings.Repeat("абзац\n\n", 300),
}

// Предел не нарушается ни на одном теле и ни на одном размере.
func TestПределСоблюдаетсяВсегда(t *testing.T) {
	for имя, body := range budgetBodies {
		t.Run(имя, func(t *testing.T) {
			for budget := 1; budget <= 200; budget++ {
				html, _ := renderRich(body, budget)
				requireWithinBudget(t, html, budget)
				requireBalanced(t, html)
			}
			for _, budget := range []int{512, 1024, 4096, 60000} {
				html, _ := renderRich(body, budget)
				requireWithinBudget(t, html, budget)
				requireBalanced(t, html)
			}
		})
	}
}

// Тело на 60k укладывается в предел поста и говорит об обрезке.
func TestОченьДлинноеТелоОбрезается(t *testing.T) {
	body := strings.Repeat("строка письма, довольно длинная. ", 2000)

	html, truncated := renderRich(body, 4096)

	if !truncated {
		t.Error("обрезка не отмечена")
	}
	requireWithinBudget(t, html, 4096)
	requireBalanced(t, html)
	if n := strings.Count(html, TruncationMark); n != 1 {
		t.Errorf("знаков обрезки %d, а должен быть один", n)
	}
}

// Ровно помещающееся тело не обрезается, а на руну меньший предел — обрезает.
func TestГраницаПределаТочная(t *testing.T) {
	body := "первый абзац письма\n\n**второй абзац** и https://example.com/x"

	full, truncated := renderRich(body, 60000)
	if truncated {
		t.Fatalf("просторный предел обрезал тело: %q", full)
	}
	exact := len([]rune(full))

	same, truncated := renderRich(body, exact)
	if truncated || same != full {
		t.Errorf("предел ровно по размеру изменил вывод:\nбыло:  %q\nстало: %q (обрезка=%v)", full, same, truncated)
	}

	// На руну меньше показ обязан ИЗМЕНИТЬСЯ, но не обязан обрезаться:
	// беднее оформленный профиль может вместить то же письмо целиком. Это и
	// есть содержимое важнее оформления.
	shorter, _ := renderRich(body, exact-1)
	if shorter == full {
		t.Errorf("предел на руну меньше ничего не изменил: %q", shorter)
	}
	requireWithinBudget(t, shorter, exact-1)
	requireBalanced(t, shorter)

	// А вот на пределе, куда не влезает ни один профиль, обрезка обязана быть.
	tiny, truncated := renderRich(body, runes(LineMarker)+runes(TruncationMark)+1)
	if !truncated {
		t.Errorf("тесный предел не вызвал обрезки: %q", tiny)
	}
}

// Длинный абзац режется внутри, а не пропадает целиком.
func TestДлинныйАбзацРежетсяВнутри(t *testing.T) {
	body := "начало абзаца, " + strings.Repeat("продолжение, ", 500) + "конец"

	html, truncated := renderRich(body, 100)

	if !truncated {
		t.Fatal("обрезка не отмечена")
	}
	if !strings.Contains(html, "начало абзаца") {
		t.Errorf("абзац пропал целиком вместо обрезки: %q", html)
	}
}

// Пустое тело не даёт ни разметки, ни паники.
func TestПустоеТелоНеЛомается(t *testing.T) {
	for _, budget := range []int{0, 1, 2, 10} {
		html, _ := renderRich("", budget)
		requireWithinBudget(t, html, budget)
		requireBalanced(t, html)
	}
}

// Вход ограничен BodyLimit до разбора, и обрезка входа видна человеку.
func TestВходОграниченДоРазбора(t *testing.T) {
	const хвост = "ХВОСТ-КОТОРОГО-БЫТЬ-НЕ-ДОЛЖНО"
	body := strings.Repeat("а", BodyLimit) + хвост

	out := RenderBody(body, 60000)

	if strings.Contains(out.HTML, хвост) {
		t.Error("тело за пределом BodyLimit попало в показ")
	}
	if !strings.HasSuffix(out.HTML, TruncationMark) {
		t.Error("обрезка входа не показана человеку")
	}
	if !out.Truncated {
		t.Error("обрезка входа не отмечена в результате")
	}
}

// Тело ровно по BodyLimit проходит целиком.
func TestТелоРовноПоПределуВходаЦело(t *testing.T) {
	out := RenderBody(strings.Repeat("а", BodyLimit), 60000)

	if out.Truncated {
		t.Error("тело ровно по пределу входа обрезано")
	}
	if strings.Contains(out.HTML, TruncationMark) {
		t.Error("знак обрезки поставлен зря")
	}
}

// Урезанный на входе текст помечается всегда, даже если урезанное влезло.
func TestОбрезкаВходаПомечаетсяВсегда(t *testing.T) {
	body := strings.Repeat("а", BodyLimit) + "ХВОСТ"

	for _, budget := range []int{500, 4000, 10000, 60000} {
		out := RenderBody(body, budget)

		if !out.Truncated {
			t.Errorf("предел %d: обрезка входа не отмечена", budget)
		}
		if !strings.HasSuffix(out.HTML, TruncationMark) {
			t.Errorf("предел %d: обрезка входа не показана человеку", budget)
		}
	}
}
