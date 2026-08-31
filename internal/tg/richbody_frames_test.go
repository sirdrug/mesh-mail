package tg

import (
	"strings"
	"testing"
)

// styleOf — какие теги охватывают каждый видимый символ строки.
//
// Оракул независимый: разбирает вывод сам, а не спрашивает рендерер. Нужен
// потому, что проверки баланса тут бессильны — при потере владения фреймом
// вывод остаётся сбалансированным, а стиль уезжает с чужого текста.
func styleOf(line string) []struct {
	ch   rune
	tags []string
} {
	var открыты []string
	var результат []struct {
		ch   rune
		tags []string
	}

	for i := 0; i < len(line); {
		if line[i] == '<' {
			конец := strings.IndexByte(line[i:], '>')
			if конец < 0 {
				break
			}
			tag := line[i+1 : i+конец]
			switch {
			case strings.HasPrefix(tag, "/"):
				if len(открыты) > 0 {
					открыты = открыты[:len(открыты)-1]
				}
			default:
				name := tag
				if пробел := strings.IndexByte(name, ' '); пробел >= 0 {
					name = name[:пробел]
				}
				открыты = append(открыты, name)
			}
			i += конец + 1
			continue
		}

		r, size := decodeRune(line[i:])
		i += size

		снимок := make([]string, len(открыты))
		copy(снимок, открыты)
		результат = append(результат, struct {
			ch   rune
			tags []string
		}{r, снимок})
	}
	return результат
}

func decodeRune(s string) (rune, int) {
	for i, r := range s {
		if i == 0 {
			return r, len(string(r))
		}
	}
	return 0, 1
}

func hasTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

// Неудача внутреннего стиля не закрывает внешний.
//
// Дефект, который эта проверка держит: узел, которому не хватило места на свой
// тег, звал закрытие — и снимал чужой, открытый выше. Вывод оставался
// сбалансированным, поэтому ни проверка баланса, ни проверка пустых тегов
// его не видели: «*A **B** C*» на пределе 14 давал «<i>A B</i> C», где «C»
// вышло без курсива, который в этот момент был открыт.
func TestВнутреннийСтильНеЗакрываетВнешний(t *testing.T) {
	тела := map[string]string{
		"курсив вокруг жирного": "*A **B** C*",
		"жирный вокруг курсива": "**A *B* C**",
		"курсив вокруг кода":    "*A `B` C*",
		"жирный вокруг кода":    "**A `B` C**",
		"одинаковые имена":      "*A *B* C*",
		"одинаковые имена, жир": "**A **B** C**",
		"три уровня":            "*A **B *C* D** E*",
	}

	for имя, body := range тела {
		t.Run(имя, func(t *testing.T) {
			for budget := runes(LineMarker) + runes(TruncationMark) + 1; budget <= 60; budget++ {
				html, _ := renderRich(body, budget)

				requireWithinBudget(t, html, budget)
				requireBalanced(t, html)

				for _, line := range strings.Split(html, "\n") {
					символы := styleOf(line)

					// Внешний стиль открылся — значит он обязан покрывать всё
					// содержимое строки до конца: обрыв стиля посреди строки
					// и есть потеря владения.
					// Внешний стиль — тот, что охватывает ПЕРВЫЙ видимый
					// символ строки. Если первый символ без тегов, внешнего
					// стиля нет вовсе, и проверять нечего: профиль мог
					// отказаться от оформления целиком, это законно.
					var внешний string
					for i, симв := range символы {
						if i == 0 {
							if len(симв.tags) == 0 {
								break
							}
							внешний = симв.tags[0]
							continue
						}
						if симв.ch == []rune(TruncationMark)[0] {
							continue
						}
						if !hasTag(симв.tags, внешний) {
							t.Fatalf("предел %d: символ %q вышел из-под <%s>: %q",
								budget, string(симв.ch), внешний, html)
						}
					}
				}
			}
		})
	}
}

// Перенос строки внутри вложенных стилей владения не путает.
func TestПереносВнутриВложенныхСтилей(t *testing.T) {
	тела := []string{
		"*A **B\nвторая строка** C*",
		"**A *B\nвторая строка* C**",
		"*A **B  \nжёсткий перенос** C*",
	}

	for _, body := range тела {
		t.Run(body, func(t *testing.T) {
			for budget := 10; budget <= 80; budget++ {
				html, _ := renderRich(body, budget)

				requireWithinBudget(t, html, budget)
				requireBalanced(t, html)

				for i, line := range strings.Split(withoutCodeBlocks(html), "\n") {
					if !strings.HasPrefix(line, LineMarker) {
						t.Fatalf("предел %d: строка %d без маркера: %q", budget, i, html)
					}
				}
			}
		})
	}
}

// Закрыть можно только свой фрейм, и различается он самоличностью.
//
// Проверка прямая, а не через показ: вложенность одинаковых тегов бывает
// («*A *B* C*» даёт «<i>A <i>B</i> C</i>»), но случай, когда закрытие приходит
// дважды или не по порядку, через обход дерева не получить — обход строг. А
// защита от него нужна: именно потерянное владение и было дефектом.
func TestЗакрываетсяТолькоСвойФрейм(t *testing.T) {
	r := newRenderer(200, profiles[0])

	внешний := r.openTag("i")
	внутренний := r.openTag("i")
	if внешний == внутренний {
		t.Fatal("два фрейма с одной самоличностью — различить их нечем")
	}

	r.closeFrame(внутренний)
	if !r.live(внешний) {
		t.Fatal("закрытие внутреннего фрейма сняло внешний")
	}

	// Повторное закрытие уже закрытого — та самая ошибка, от которой защищает
	// самоличность: по имени и по вершине стека здесь снялся бы внешний.
	r.closeFrame(внутренний)
	if !r.live(внешний) {
		t.Error("повторное закрытие внутреннего фрейма сняло внешний")
	}

	r.closeFrame(внешний)
	if len(r.frames) != 0 {
		t.Errorf("после закрытия внешнего осталось %d фреймов", len(r.frames))
	}
}

// Блок кода, для которого не хватило места, показывается другим профилем.
//
// Раньше деградация происходила внутри одного прохода: тег не влез — пишем
// строки как есть. Теперь так нельзя, и это сознательно: решение внутри
// прохода принималось по остатку предела и вытесняло текст. Профиль, где код
// идёт обычными строками, отдельный, и выбирается он по покрытию.
//
// Проверяется главное свойство деградации: строки кода, ставшие обычным
// текстом, получают маркер. Внутри <pre> маркера нет намеренно, и текст,
// выпавший из тега, оказался бы строками без границы доверия.
func TestКодБезТегаПоказываетсяСМаркером(t *testing.T) {
	const body = "```\nкод один\nкод два\n```"

	for budget := runes(LineMarker) + runes(TruncationMark) + 1; budget <= 40; budget++ {
		out := RenderBody(body, budget)

		requireWithinBudget(t, out.HTML, budget)
		requireBalanced(t, out.HTML)

		if strings.Contains(out.HTML, "<pre>") {
			// Тег влез — это другой случай, он проверен отдельно.
			continue
		}
		for i, line := range strings.Split(out.HTML, "\n") {
			if !strings.HasPrefix(line, LineMarker) {
				t.Fatalf("предел %d, профиль %s: строка %d кода без маркера: %q",
					budget, out.Profile, i, out.HTML)
			}
		}
	}
}
