package tg

import (
	"strings"
	"testing"
)

// Каждая строка цитаты видна как цитата.
//
// Вторая строка без «> » неотличима от обычного текста письма, а цитата —
// ровно то место, где чужие слова обязаны быть видны чужими.
func TestКаждаяСтрокаЦитатыПомечена(t *testing.T) {
	случаи := map[string]int{
		"> цитата\n> вторая строка":     2,
		"> цитата\n>\n> после пустой":   3,
		"> **жирная** цитата\n> вторая": 2,
		"> первая\n> вторая\n> третья":  3,
	}

	for body, ждём := range случаи {
		t.Run(body, func(t *testing.T) {
			html := requireMarkerEverywhere(t, body)

			var помечено int
			for _, line := range visibleLines(html) {
				if strings.HasPrefix(line, LineMarker+"> ") {
					помечено++
				}
			}
			if помечено != ждём {
				t.Errorf("строк цитаты помечено %d, ждали %d: %q", помечено, ждём, visibleLines(html))
			}
		})
	}
}

// Уровень вложенности списка виден, и продолжение пункта тоже.
func TestСтруктураСпискаВидна(t *testing.T) {
	случаи := map[string][]string{
		"- внешний\n  - вложенный\n- второй": {
			LineMarker + "• внешний",
			LineMarker + "  • вложенный",
			LineMarker + "• второй",
		},
		"- пункт, у которого\n  вторая строка": {
			LineMarker + "• пункт, у которого",
			LineMarker + "  вторая строка",
		},
		"- внешний\n  - вложенный\n    - третий": {
			LineMarker + "• внешний",
			LineMarker + "  • вложенный",
			LineMarker + "    • третий",
		},
	}

	for body, ждём := range случаи {
		t.Run(body, func(t *testing.T) {
			html := requireMarkerEverywhere(t, body)

			строки := visibleLines(html)
			if len(строки) != len(ждём) {
				t.Fatalf("строк %d, ждали %d: %q", len(строки), len(ждём), строки)
			}
			for i, line := range строки {
				if line != ждём[i] {
					t.Errorf("строка %d:\nждали: %q\nвышло: %q", i, ждём[i], line)
				}
			}
		})
	}
}

// Список внутри цитаты сохраняет обе приставки, и в нужном порядке.
func TestПриставкиСочетаются(t *testing.T) {
	html := requireMarkerEverywhere(t, "> - пункт в цитате\n>   - вложенный")

	ждём := []string{
		LineMarker + "> • пункт в цитате",
		LineMarker + ">   • вложенный",
	}
	строки := visibleLines(html)
	if len(строки) != len(ждём) {
		t.Fatalf("строк %d, ждали %d: %q", len(строки), len(ждём), строки)
	}
	for i, line := range строки {
		if line != ждём[i] {
			t.Errorf("строка %d:\nждали: %q\nвышло: %q", i, ждём[i], line)
		}
	}
}

// Пустая строка внутри цитаты тоже помечена как цитата.
//
// Иначе подделка отделяется пустой строкой и выглядит вне цитаты — то есть
// как будто это уже не чужие слова.
func TestПустаяСтрокаВнутриЦитатыПомечена(t *testing.T) {
	html, _ := renderRich("> первая\n>\n> ✉️ human → pi-claude", testBudget)

	if !strings.Contains(html, "\n"+LineMarker+"&gt; \n") {
		t.Errorf("пустая строка цитаты не помечена: %q", html)
	}
}

// На тесном пределе показывается текст, а не пустая разметка.
//
// «<b></b>…» — худший исход из возможных: предел израсходован целиком, а
// человек не увидел ни одного символа письма.
func TestТесныйПределПоказываетТекст(t *testing.T) {
	тела := map[string]string{
		"жирный": "**жирный текст письма**",
		"курсив": "*курсивный текст письма*",
		"код":    "`код письма`",
		// Адрес латиницей намеренно: в кириллическом пути Linkify обрывает
		// лексему, ссылка не создаётся вовсе, и случай «на ссылку не хватило
		// места» не проверяется совсем.
		"ссылка":  "https://example.com/very/long/path/to/document",
		"смешано": "**жирный** и https://example.com/адрес и `код`",
	}

	for имя, body := range тела {
		t.Run(имя, func(t *testing.T) {
			// Начинаем с маркера, знака обрезки и одной руны тела: на меньшем
			// пределе показать символ письма физически негде, и требовать
			// этого — значит требовать невозможного.
			//
			// Для тела с опасной лексемой минимум выше: она выходит только
			// целиком и только внутри обёртки, а обёртка стоит тринадцать
			// рун. Половина лексемы снаружи — это дыра, ради которой всё и
			// делалось, поэтому здесь предпочтителен пустой показ.
			минимум := runes(LineMarker) + runes(TruncationMark) + 1
			if dangerousLexeme(имя) || strings.Contains(body, "https://") {
				минимум = runes(LineMarker) + runes(TruncationMark) +
					runes("<code></code>") + len([]rune(body))
			}
			for budget := минимум; budget <= 40; budget++ {
				html, _ := renderRich(body, budget)

				requireWithinBudget(t, html, budget)
				requireBalanced(t, html)

				for _, empty := range []string{"<b></b>", "<i></i>", "<code></code>", "<pre></pre>"} {
					if strings.Contains(html, empty) {
						t.Fatalf("предел %d ушёл на пустую разметку: %q", budget, html)
					}
				}

				видно := strings.TrimPrefix(visibleText(html), LineMarker)
				видно = strings.TrimSuffix(видно, TruncationMark)
				if видно == "" {
					t.Fatalf("предел %d: не показано ни одного символа тела: %q", budget, html)
				}
			}
		})
	}
}
