package tg

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yuin/goldmark/ast"
)

// опасныеЛексемы — по одной на каждый класс, который Telegram активирует сам.
//
// Список снят живым прогоном 23.08.2026, а не взят из документации. Он же
// служит матрицей для стенда: если Telegram заведёт новый класс, тест об этом
// не узнает — узнает только повторный прогон пробника.
var опасныеЛексемы = map[string]string{
	"команда":         "/to",
	"команда с ботом": "/start@agent_mesh_bot",
	"короткий путь":   "/tmp",
	"длинный путь":    "/etc/nats/tls.pem",
	"упоминание":      "@agent_mesh_bot",
	// Без точек и подчёркиваний: иначе лексему прикрывают другие признаки —
	// точка внутри слова или разрыв узла на подчёркивании, — и пропуск
	// «собаки» остался бы незамеченным.
	"упоминание без знаков": "@agentmesh",
	"метка":             "#срочно",
	"тикер":             "$USD",
	"почта":             "mail@example.com",
	"почта кириллицей":  "кир@пример.рф",
	"телефон":           "+79991234567",
	"телефон с дефисом": "+7-999-123-45-67",
	"голый домен":       "example.com",
	"домен с путём":     "example.com/doc",
	"адрес с портом":    "example.com:8080/x",
	"IP":                "192.168.1.1",
	"чужая схема":       "tg://resolve?domain=x",
	"имя файла":         "README.md",
	"скрипт":            "run.sh",
	"домен без границы": "текстexample.com",
}

// codeРегион — куски вывода внутри code или pre.
var codeРегион = regexp.MustCompile(`(?s)<(code|pre)>.*?</(code|pre)>`)

// вне возвращает то, что осталось снаружи защищённых зон и ссылок.
//
// Оракул независимый: он не спрашивает у рендерера, что тот считал опасным, а
// просто вырезает всё, что Telegram точно не распознаёт, и смотрит на остаток.
func вне(html string) string {
	без := codeРегион.ReplaceAllString(html, " ")
	без = regexp.MustCompile(`(?s)<a href="[^"]*">.*?</a>`).ReplaceAllString(без, " ")
	return visibleText(без)
}

// Опасная лексема не остаётся снаружи защищённой зоны — в любом месте строки.
func TestОпаснаяЛексемаНеВыходитГолой(t *testing.T) {
	обрамления := map[string]string{
		"одна в строке": "%s",
		"в начале":      "%s и дальше текст",
		"в середине":    "текст %s и дальше",
		"в конце":       "текст и потом %s",
		"перед запятой": "текст %s, дальше",
		"в скобках":     "(%s)",
		"в цитате":      "> цитата %s тут",
		"в списке":      "- пункт %s тут",
		"в выделении":   "**жирный %s хвост**",
		"в заголовке":   "# заголовок %s",
		"две подряд":    "%s %s",
	}

	for имяЛексемы, лексема := range опасныеЛексемы {
		for имяМеста, шаблон := range обрамления {
			t.Run(имяЛексемы+"/"+имяМеста, func(t *testing.T) {
				body := strings.ReplaceAll(шаблон, "%s", лексема)

				out := RenderBody(body, 4000)

				requireBalanced(t, out.HTML)
				requireNoOverlap(t, out.HTML)

				if strings.Contains(вне(out.HTML), лексема) {
					t.Errorf("лексема осталась снаружи защищённой зоны:\nтело:  %q\nвывод: %q\nснаружи: %q",
						body, out.HTML, вне(out.HTML))
				}
			})
		}
	}
}

// Видимый текст письма не меняется ни на руну.
//
// Обёртка обязана быть невидимой для читателя: ни вставок, ни замен, ни
// потерь. Невидимые разделители мы отвергли именно поэтому — они уезжают в
// буфер при копировании.
func TestПодавлениеНеМеняетВидимыйТекст(t *testing.T) {
	for имя, лексема := range опасныеЛексемы {
		t.Run(имя, func(t *testing.T) {
			body := "текст " + лексема + " хвост"

			out := RenderBody(body, 4000)

			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("видимый текст изменился:\nбыло:  %q\nстало: %q\nвывод: %q", body, got, out.HTML)
			}
		})
	}
}

// Обычный текст обёртки не получает.
//
// Пара к предыдущим: без неё зелен и подавитель, который заворачивает в код
// всё подряд, — он тоже никогда не оставит лексему голой.
func TestБезопасныйТекстОстаётсяБезОбёртки(t *testing.T) {
	безопасные := []string{
		"обычная строка письма",
		"числа 42 и 3 в тексте",
		"main.go", // расширение не совпадает с доменной зоной, но точка есть — обернём
		"дефис-через-дефис и тире — вот",
		"имя │ значение и рамка ┌───┐",
	}

	for _, body := range безопасные {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			// Псевдографика и обычные слова обёртки не требуют; для main.go
			// требуют — это осознанная цена правила по форме.
			if body == "main.go" {
				if !strings.Contains(out.HTML, "<code>main.go</code>") {
					t.Errorf("слово с точкой не обёрнуто: %q", out.HTML)
				}
				return
			}
			if strings.Contains(out.HTML, "<code>") {
				t.Errorf("безопасный текст обёрнут зря: %q", out.HTML)
			}
		})
	}
}

// Авторский код вложенной обёртки не получает.
func TestАвторскийКодНеПолучаетВложенности(t *testing.T) {
	тела := []string{
		"вот `/to pi-codex` в коде",
		"```\n/to pi-codex\n/etc/nats\n```",
		"`@agent_bot` и `#метка`",
	}

	for _, body := range тела {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if strings.Contains(out.HTML, "<code><code>") || strings.Contains(out.HTML, "<pre><code>") {
				t.Errorf("вложенный код: %q", out.HTML)
			}
			if strings.Contains(вне(out.HTML), "/to") || strings.Contains(вне(out.HTML), "@agent_bot") {
				t.Errorf("лексема вышла из-под авторского кода: %q", out.HTML)
			}
		})
	}
}

// Разрешённый адрес остаётся ссылкой и обёртки не получает.
func TestРазрешённыйАдресОстаётсяСсылкой(t *testing.T) {
	out := RenderBody("смотри https://example.com/doc дальше", 4000)

	if !strings.Contains(out.HTML, `<a href="https://example.com/doc">https://example.com/doc</a>`) {
		t.Errorf("разрешённый адрес не стал ссылкой: %q", out.HTML)
	}
	if strings.Contains(out.HTML, "<code>") {
		t.Errorf("разрешённый адрес обёрнут в код: %q", out.HTML)
	}
}

// Лексема, разорванная разбором, обезврежена целиком.
//
// Telegram смотрит на готовую строку, а не на наши узлы: «@agent_bot»
// приходит двумя узлами из-за подчёркивания, и обёртка только первого
// оставила бы «bot» голым — упоминание распозналось бы целиком.
func TestРазорваннаяЛексемаОбезвреженаЦеликом(t *testing.T) {
	тела := map[string]string{
		"подчёркивание": "спроси @agent_bot и всё",
		"жирное внутри": "путь /etc/**жирный**/x",
		"курсив стыком": "текст *курсив*@evil.example хвост",
		"файл с чертой": "файл __init__.py рядом",
	}

	for имя, body := range тела {
		t.Run(имя, func(t *testing.T) {
			out := RenderBody(body, 4000)

			снаружи := вне(out.HTML)
			for _, опасный := range []string{"@agent_", "bot", "/etc/", "@evil.example", ".py"} {
				if !strings.Contains(body, strings.TrimPrefix(опасный, "@")) && !strings.Contains(body, опасный) {
					continue
				}
				if strings.Contains(снаружи, опасный) {
					t.Errorf("кусок опасной лексемы снаружи: %q\nвывод: %q", опасный, out.HTML)
				}
			}
		})
	}
}

// Обёртка атомарна: либо целиком, либо показ обрывается перед лексемой.
func TestОбёрткаАтомарна(t *testing.T) {
	const лексема = "/etc/nats/tls.pem"
	body := "путь " + лексема + " рядом"

	// Проверяется КАНДИДАТ, а не итог выбора: на тесном пределе побеждает
	// общий блок, где обёрток нет вовсе, и неатомарная обёртка до итога не
	// доходит — а дефект при этом жив. Тот же урок был со ссылкой.
	for budget := 1; budget <= 80; budget++ {
		got := показПрофиля(body, budget, profiles[0])

		requireWithinBudget(t, got.html, budget)
		requireBalanced(t, got.html)

		if strings.Contains(got.html, "<code>") {
			if !strings.Contains(got.html, "<code>"+лексема+"</code>") {
				t.Fatalf("предел %d: лексема обёрнута не целиком: %q", budget, got.html)
			}
		}
		if strings.Contains(вне(got.html), "/etc") {
			t.Fatalf("предел %d: кусок лексемы снаружи: %q", budget, got.html)
		}
	}
}

// Внутри общего блока обёрток нет: подавление даёт сам блок.
func TestОбщийБлокОбёртокНеТребует(t *testing.T) {
	source, cutInput := boundInput("/to pi-codex и @agent_bot и #метка")
	got := (&renderSession{doc: parseSource(source), source: source}).
		finalize(4000, profiles[len(profiles)-1], cutInput)

	if strings.Contains(got.html, "<code>") {
		t.Errorf("в общем блоке появилась обёртка: %q", got.html)
	}
	if !strings.HasPrefix(got.html, "<pre>") || !strings.HasSuffix(got.html, "</pre>") {
		t.Errorf("общий блок собран неверно: %q", got.html)
	}
}

// Обёртка никогда не оказывается внутри ссылки.
//
// Живая проверка показала, что Telegram принимает такую вложенность и МОЛЧА
// теряет одну из сущностей: при полном перекрытии исчезает ссылка, при
// частичном — сама обёртка, и оба раза ответ успешный. Значит проверять успех
// отправки бесполезно; полагаться можно только на то, что мы такой разметки
// не порождаем вовсе.
func TestОбёрткаНеПопадаетВнутрьСсылки(t *testing.T) {
	тела := []string{
		"смотри https://example.com/x/to дальше",
		"https://example.com/x и рядом /to",
		"/to рядом с https://example.com/x",
		"смотри https://example.com/doc?a=/to тут",
	}

	for _, body := range тела {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)
			requireNoOverlap(t, out.HTML)
		})
	}
}

// requireNoOverlap — ссылка и защищённые зоны нигде не вложены друг в друга.
//
// Проверка СТРУКТУРНАЯ, разбором стека тегов, а не поиском образца. Образец
// «<a…><code>» пропускает случай, где между ними стоит ещё один тег:
// «<a…><b>…<code>» — вложенность та же, а строка другая.
//
// Цена ошибки здесь высокая: живая проверка показала, что Telegram принимает
// такую разметку и МОЛЧА теряет одну из сущностей, отвечая успехом. Значит
// заметить это по ответу API нельзя — только не допускать в выводе.
func requireNoOverlap(t *testing.T, html string) {
	t.Helper()

	var stack []string
	for _, m := range tagPattern.FindAllStringSubmatch(html, -1) {
		closing, name := m[1] == "/", m[2]
		if closing {
			if len(stack) == 0 {
				t.Fatalf("закрытие </%s> без открытия: %q", name, html)
			}
			stack = stack[:len(stack)-1]
			continue
		}

		защита := name == "code" || name == "pre"
		for _, открыт := range stack {
			внешняяСсылка := открыт == "a"
			внешняяЗащита := открыт == "code" || открыт == "pre"

			if защита && внешняяСсылка {
				t.Fatalf("<%s> внутри ссылки: %q", name, html)
			}
			if name == "a" && внешняяЗащита {
				t.Fatalf("ссылка внутри <%s>: %q", открыт, html)
			}
			if защита && внешняяЗащита {
				t.Fatalf("<%s> внутри <%s>: %q", name, открыт, html)
			}
		}
		stack = append(stack, name)
	}
	if len(stack) != 0 {
		t.Fatalf("остались открытыми %v: %q", stack, html)
	}
}

// Обёрнутая лексема внутри выделения — вид меняется, и это ожидаемо.
//
// Telegram разрывает выделение вокруг code на три сущности: жирный, код,
// жирный. Закрепляем это тестом, чтобы не обнаружить на первом же письме и не
// принять за дефект: разметка наша валидна, а поведение — их.
func TestОбёрткаВнутриВыделенияРазрываетЕго(t *testing.T) {
	out := RenderBody("**жирный /to хвост**", 4000)

	if !strings.Contains(out.HTML, "<b>жирный <code>/to</code> хвост</b>") {
		t.Errorf("обёртка внутри выделения собрана иначе: %q", out.HTML)
	}
	if !strings.Contains(visibleText(out.HTML), "жирный /to хвост") {
		t.Errorf("видимый текст изменился: %q", visibleText(out.HTML))
	}
}

// Псевдографика письма обёрток не получает и не портится.
func TestПсевдографикаНеСтрадает(t *testing.T) {
	body := "```\n┌───┐\n│ a │\n└───┘\n```\n\nи таблица без кода:\n\nимя │ значение"

	out := RenderBody(body, 4000)

	for _, кусок := range []string{"┌───┐", "│ a │", "└───┘", "имя │ значение"} {
		if !strings.Contains(visibleText(out.HTML), кусок) {
			t.Errorf("псевдографика испорчена (%q): %q", кусок, visibleText(out.HTML))
		}
	}
}

// Обёртка не режется, даже когда строка начинается заново.
//
// Прямая проверка пути рендерера: маркер строки, приставки цитаты и списка и
// переоткрытие стилей занимают место ПЕРЕД лексемой. Считая стоимость обёртки
// до них, мы получали бы запас, которого нет, — и лексема выходила бы
// обрезанной внутри code, то есть половиной.
func TestОбёрткаЦелаНаНовойСтроке(t *testing.T) {
	тела := map[string]string{
		"после переноса":      "**жирная строка\n/etc/nats/tls.pem**",
		"в цитате":            "> первая строка\n> /etc/nats/tls.pem",
		"во вложенном списке": "- пункт\n  - /etc/nats/tls.pem",
		"в цитате с жирным":   "> **жирная\n> /etc/nats/tls.pem**",
	}

	for имя, body := range тела {
		t.Run(имя, func(t *testing.T) {
			for budget := 1; budget <= 90; budget++ {
				for _, p := range profiles {
					got := показПрофиля(body, budget, p)

					requireWithinBudget(t, got.html, budget)
					requireBalanced(t, got.html)
					requireNoOverlap(t, got.html)

					if !strings.Contains(got.html, "<code>") {
						continue
					}
					if !strings.Contains(got.html, "<code>/etc/nats/tls.pem</code>") {
						t.Fatalf("профиль %s, предел %d: лексема обрезана внутри обёртки: %q",
							p.name, budget, got.html)
					}
				}
			}
		})
	}
}

// Сырой HTML и лексема за его границей.
//
// Узел сырого HTML идёт тем же путём, что и обычный текст: лексема может
// начаться в нём и продолжиться в соседнем узле. Молчаливой ветки «здесь
// граница всегда разрыв» быть не должно — это ровно то предположение, которое
// уже один раз оказалось неверным на подчёркивании.
//
// Что проверяется здесь — ЕДИНЫЙ КОНТРАКТ границы, а не доказанная угроза:
// сырой HTML виден буквально, со скобками, и адреса на склейке не даёт.
// Доказательство самой угрозы — в тесте на соседних строковых узлах ниже,
// где видимый текст действительно склеивается в адрес.
func TestЛексемаЧерезГраницуСырогоHTML(t *testing.T) {
	тела := []string{
		"текст <b>@evil.example тут",
		"текст <b>/to</b> хвост",
		"<i>@agentmesh</i> и дальше",
		// Сырой HTML показывается БУКВАЛЬНО — «example&lt;b&gt;.com» на
		// экране, — поэтому опасного адреса тут не возникает: перед точкой
		// стоит «>», а не буква. Фикстура фиксирует не опасность, а
		// консервативность: узел сырого HTML идёт тем же путём, что и
		// обычный текст, и лексема на его границе обезвреживается заодно.
		//
		// Ложное подавление дешевле пропуска, но выдавать его за доказанную
		// угрозу нельзя — я это сделал в первой версии и был поправлен.
		"example<b>.com</b>",
		"site<i>.io</i> тут",
	}

	for _, body := range тела {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			requireBalanced(t, out.HTML)
			requireNoOverlap(t, out.HTML)

			// Снаружи защищённых зон не должно остаться ничего, кроме
			// служебного и заведомо безопасных слов.
			снаружи := strings.TrimSpace(strings.ReplaceAll(вне(out.HTML), LineMarker, ""))
			for _, безопасное := range []string{"текст", "хвост", "и дальше", "тут"} {
				снаружи = strings.TrimSpace(strings.ReplaceAll(снаружи, безопасное, ""))
			}
			if снаружи != "" {
				t.Errorf("снаружи обёртки осталось %q: %q", снаружи, out.HTML)
			}
		})
	}
}

// Ни одной голой руны опасной лексемы — на любом пределе.
//
// Лексему может разорвать разбор: «@agent_bot» приходит двумя узлами, и на
// тесном пределе показаться успевает только начало. Это допустимо — скрытой
// цели у обёртки нет, и непоказанный хвост никому не вредит. Недопустимо
// другое: чтобы хоть одна показанная руна оказалась СНАРУЖИ целой обёртки.
//
// Тело здесь состоит из одной лексемы, поэтому проверка прямая: снаружи
// защищённых зон не должно остаться ничего, кроме служебного.
func TestНиОднойГолойРуныЛексемы(t *testing.T) {
	тела := map[string]string{
		"два узла":         "@agent_bot",
		"три узла":         "@agent_mesh_bot",
		"путь с жирным":    "/etc/**жирный**/x",
		"адрес с курсивом": "https://зло.example/*x*/y",
	}

	for имя, body := range тела {
		t.Run(имя, func(t *testing.T) {
			for budget := 1; budget <= 90; budget++ {
				for _, p := range profiles {
					got := показПрофиля(body, budget, p)

					requireWithinBudget(t, got.html, budget)
					requireBalanced(t, got.html)
					requireNoOverlap(t, got.html)

					if p.fullPre {
						continue // там подавление даёт сам блок
					}

					снаружи := вне(got.html)
					снаружи = strings.ReplaceAll(снаружи, LineMarker, "")
					снаружи = strings.ReplaceAll(снаружи, TruncationMark, "")
					снаружи = strings.TrimSpace(снаружи)

					if снаружи != "" {
						t.Fatalf("профиль %s, предел %d: снаружи обёртки осталось %q: %q",
							p.name, budget, снаружи, got.html)
					}
				}
			}
		})
	}
}

// Соседние строковые узлы склеиваются в адрес — и он обезврежен.
//
// Дерево строится руками: через разбор такой случай не получить, а ветка
// существует и должна работать. Здесь видимый текст ДЕЙСТВИТЕЛЬНО становится
// «example.com» — в отличие от сырого HTML, который виден буквально, со
// скобками, и опасного адреса не даёт.
//
// Это и есть доказательство контракта границы: снятие учёта границы у
// строковых узлов красит именно этот тест.
func TestСоседниеСтроковыеУзлыСклеиваютсяВАдрес(t *testing.T) {
	doc := ast.NewDocument()
	para := ast.NewParagraph()
	para.AppendChild(para, ast.NewString([]byte("example")))
	para.AppendChild(para, ast.NewString([]byte(".com")))
	doc.AppendChild(doc, para)

	сессия := &renderSession{doc: doc, source: nil}
	got := сессия.finalize(4000, profiles[0], false)

	if видно := visibleText(got.html); !strings.Contains(видно, "example.com") {
		t.Fatalf("узлы не склеились в адрес — проверка ничего не проверяет: %q", видно)
	}
	requireBalanced(t, got.html)
	requireNoOverlap(t, got.html)

	снаружи := strings.TrimSpace(strings.ReplaceAll(вне(got.html), LineMarker, ""))
	if снаружи != "" {
		t.Errorf("часть адреса осталась снаружи обёртки: %q\nвывод: %q", снаружи, got.html)
	}
}

// Обычные слова со знаками препинания моноширинными не становятся.
//
// Разбор дробит текст чаще, чем кажется: «Привет!» приходит двумя узлами,
// потому что восклицательный знак в markdown начинает изображение. Считая
// слитным любой непробельный символ, мы заворачивали в код обычные слова —
// заметная порча вида на ровном месте, найденная на живом показе письма.
func TestОбычныеСловаНеСтановятсяКодом(t *testing.T) {
	тела := []string{
		"Привет!",
		"Готово. Спасибо!",
		"Вопрос? Ответ, наверное.",
		"Скобка (в тексте) и кавычки «ёлочки»",
		"Двоеточие: вот; точка с запятой",
	}

	for _, body := range тела {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if strings.Contains(out.HTML, "<code>") {
				t.Errorf("обычный текст стал моноширинным: %q", out.HTML)
			}
			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("текст искажён:\nбыло:  %q\nстало: %q", body, got)
			}
		})
	}
}

// Продолжение обезвреживается по опасности склейки, а не по соседству.
//
// Пара к предыдущему тесту: без неё зелен и подавитель, который вообще не
// учитывает продолжение, — а тогда «example» + «.com» разъедется на голые
// куски. Здесь же проверяется обратная сторона: безобидная склейка обёртки не
// получает.
//
// Правило первого выпуска спрашивало про один соседний символ и на «курсив» +
// «;» отвечало «продолжает» — обычный текст уходил в моноширинный вид, а
// вместе с ним пропадал стиль. Спрашивать надо про склейку целиком.
func TestОбезвреживаниеПоОпасностиСклейки(t *testing.T) {
	опасные := []struct{ кусок, хвост string }{
		{"https", "://example.com"},
		{"example", ".com"},
		{"README", ".md"},
		{"tls", ".pem"},
		{"pi", "@example.com"},
		{"путь", "/внутри"},
	}
	for _, п := range опасные {
		if !joinedDangerous(п.кусок, boundary{joined: true, tail: п.хвост}) {
			t.Errorf("склейка %q + %q не считается опасной — часть выйдет голой",
				п.кусок, п.хвост)
		}
	}

	безобидные := []struct{ кусок, хвост string }{
		{"курсив", ";"},
		{"жирный", ":"},
		{"важно", ","},
		{"Итог", "."},
		{"Привет", "!"},
		{"слово", ")"},
		{"вопрос", "?"},
	}
	for _, п := range безобидные {
		if joinedDangerous(п.кусок, boundary{joined: true, tail: п.хвост}) {
			t.Errorf("склейка %q + %q обезврежена без причины — пропадёт стиль",
				п.кусок, п.хвост)
		}
	}

	// Непрозрачная граница решается в безопасную сторону: не увидев
	// продолжение целиком, обезвреживаем.
	if !joinedDangerous("слово", boundary{joined: true, opaque: true}) {
		t.Error("неизвестное продолжение не обезврежено")
	}
}

// Выделение перед знаком препинания остаётся выделением.
//
// Дефект первого выпуска показа, найденный глазами на боевом узле: владелец
// посмотрел пост и сказал «кажется курсив не курсив». Так и было — Telegram не
// накладывает стиль на code, поэтому лишняя обёртка съедала не только вид
// моноширинным шрифтом, но и сам курсив.
//
// Оборот частый: «это **важно**, потому что…». Проверяется здесь показом, а не
// вызовом функции: ошибка была видна именно на экране, и стеречь её должно то,
// что смотрит туда же.
func TestВыделениеПередПунктуациейНеОбёрнуто(t *testing.T) {
	случаи := map[string]string{
		"двоеточие":       "**жирный**: дальше",
		"запятая":         "**жирный**, дальше",
		"точка с запятой": "*курсив*; дальше",
		"точка":           "**Итог**. дальше",
		"внутри фразы":    "это **важно**, потому что так пишут",
		"скобка":          "(**жирный**) дальше",
	}

	for имя, body := range случаи {
		t.Run(имя, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if strings.Contains(out.HTML, "<code>") {
				t.Errorf("выделение перед знаком препинания обёрнуто — стиль пропадёт: %q", out.HTML)
			}
			if !strings.Contains(out.HTML, "<b>") && !strings.Contains(out.HTML, "<i>") {
				t.Errorf("выделение потеряно вовсе: %q", out.HTML)
			}
			if got := visibleText(out.HTML); !strings.Contains(got, "дальше") &&
				!strings.Contains(got, "так пишут") {
				t.Errorf("текст искажён: %q", got)
			}
		})
	}
}

// Опасное слово дальше по строке выделение не задевает.
//
// Хвост склейки кончается на пробеле, и это стережётся здесь. Мутация
// «не останавливаться на пробеле» иначе проходит незамеченной: она портит
// только те тела, где за выделением ЧЕРЕЗ ПРОБЕЛ стоит опасная лексема, а
// таких среди прежних фикстур не было ни одной. Ошибка безвредная по
// последствиям — лишняя обёртка, — но невидимая проверками, а значит
// свободно доживающая до следующей правки.
func TestОпасноеСловоДальшеПоСтрокеВыделениеНеЗадевает(t *testing.T) {
	out := RenderBody("это **важно**, потому что example.com рядом", 4000)

	if !strings.Contains(out.HTML, "<b>важно</b>") {
		t.Errorf("выделение обёрнуто из-за опасного слова через пробел: %q", out.HTML)
	}
	if strings.Contains(вне(out.HTML), "example.com") {
		t.Errorf("опасная лексема осталась голой: %q", out.HTML)
	}
}

// Предел хвоста считается в рунах, а не в байтах.
//
// Расхождение единиц измерения здесь стоит дороже, чем кажется: предел,
// посчитанный по байтам, наступает для кириллицы вдвое раньше, чем для
// латиницы, и русское письмо получает лишние обёртки там, где английское не
// получает. Порча вида, зависящая от языка отправителя, — тот же класс
// дефекта, ради которого писан этот коммит, только незаметный на английских
// фикстурах.
//
// Семантика предела: 256 рун ещё известны, 257-я делает границу непрозрачной.
// Проверяются обе стороны — и что известное известно, и что превышение
// действительно обрывает.
func TestПределХвостаСчитаетсяВРунах(t *testing.T) {
	хвостЗаУзлом := func(текст string) (string, bool) {
		doc := ast.NewDocument()
		para := ast.NewParagraph()
		первый := ast.NewString([]byte("x"))
		para.AppendChild(para, первый)
		para.AppendChild(para, ast.NewString([]byte(текст)))
		doc.AppendChild(doc, para)
		return nextVisibleLexeme(первый, nil)
	}

	алфавиты := map[string]string{
		"латиница":  "a", // одна руна — один байт
		"кириллица": "я", // одна руна — два байта
	}

	for имя, буква := range алфавиты {
		t.Run(имя, func(t *testing.T) {
			for _, длина := range []int{255, joinedTailLimit} {
				tail, known := хвостЗаУзлом(strings.Repeat(буква, длина) + " конец")
				if !known {
					t.Errorf("хвост в %d рун объявлен неизвестным", длина)
				}
				if got := utf8.RuneCountInString(tail); got != длина {
					t.Errorf("хвост в %d рун собран как %d рун", длина, got)
				}
			}

			if _, known := хвостЗаУзлом(strings.Repeat(буква, joinedTailLimit+1) + " конец"); known {
				t.Errorf("хвост длиннее предела объявлен известным — склейка проверена не целиком")
			}
		})
	}

	// Главное утверждение: два алфавита ведут себя одинаково при равном числе
	// рун. Без этой пары каждый из подтестов выше зелен и при счёте по
	// байтам — просто с разными порогами.
	латинский, _ := хвостЗаУзлом(strings.Repeat("a", joinedTailLimit) + " конец")
	кириллический, _ := хвостЗаУзлом(strings.Repeat("я", joinedTailLimit) + " конец")
	if utf8.RuneCountInString(латинский) != utf8.RuneCountInString(кириллический) {
		t.Errorf("предел зависит от алфавита: латиница %d рун, кириллица %d рун",
			utf8.RuneCountInString(латинский), utf8.RuneCountInString(кириллический))
	}
}

// Опасная склейка через границу выделения защищена целиком.
//
// Пара к предыдущему: она стережёт ровно ту сторону, в которую легко
// перегнуть, чиня вид. «**example**.com» и «**https**://example.com» разорваны
// разбором так же, как «**жирный**:», и внешне отличаются только тем, что
// склейка образует адрес.
func TestОпаснаяСклейкаЧерезВыделениеЗащищена(t *testing.T) {
	тела := []string{
		"**example**.com дальше",
		"**https**://example.com дальше",
		"*README*.md дальше",
		"**tls**.pem дальше",
	}

	for _, body := range тела {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			// Фикстура обязана задеть проверяемое место: разрыв лексемы
			// границей ВЫДЕЛЕНИЯ. Без этой проверки тест зеленел бы и на
			// теле, где выделения не возникает вовсе, — «**pi**@example.com»
			// разбором не выделяется, звёздочки остаются текстом, и лексема
			// приходит одним куском. Проверялась бы другая ветка под видом
			// этой.
			if !strings.Contains(out.HTML, "<b>") && !strings.Contains(out.HTML, "<i>") {
				t.Fatalf("фикстура не образует выделения — проверяется не та ветка: %q", out.HTML)
			}

			голое := вне(out.HTML)
			for _, кусок := range []string{"example", "https", "README", "tls", ".com", ".md", ".pem"} {
				if strings.Contains(body, кусок) && strings.Contains(голое, кусок) {
					t.Errorf("часть опасной склейки %q вне обёртки: %q", кусок, out.HTML)
				}
			}
		})
	}
}

// Схема адреса, разорванная на узлы, защищена целиком.
//
// «https» + «://example.com» — случай, на котором сломалась первая версия
// списка терминаторов: двоеточие считалось завершающей пунктуацией, и «https»
// выходило голым перед защищённым хвостом. Голая часть опасной лексемы — ровно
// та дыра, ради которой подавитель и написан.
func TestСхемаАдресаЧерезУзлыЗащищена(t *testing.T) {
	doc := ast.NewDocument()
	para := ast.NewParagraph()
	para.AppendChild(para, ast.NewString([]byte("https")))
	para.AppendChild(para, ast.NewString([]byte("://example.com")))
	doc.AppendChild(doc, para)

	got := (&renderSession{doc: doc, source: nil}).finalize(4000, profiles[0], false)

	if видно := visibleText(got.html); !strings.Contains(видно, "https://example.com") {
		t.Fatalf("узлы не склеились в адрес — проверка ничего не проверяет: %q", видно)
	}
	снаружи := strings.TrimSpace(strings.ReplaceAll(вне(got.html), LineMarker, ""))
	if снаружи != "" {
		t.Errorf("часть адреса осталась снаружи обёртки: %q\nвывод: %q", снаружи, got.html)
	}
}

// Адрес показывается дословно, как написан в письме.
//
// Дефект нашла живая матрица: «www.example.com» выходило как
// «http://www.example.com» — Linkify достраивает схему, а мы печатали её.
// Отправитель написал одно, человек читал другое; тесты этого не видели,
// потому что сверяли безопасность, а не дословность.
//
// Заодно исчезла несогласованность вида: «example.com», «www.example.com» и
// «WWW.EXAMPLE.COM» теперь ведут себя одинаково — видны дословно и не
// кликабельны. Раньше средний был ссылкой только потому, что его успел
// разобрать Linkify.
func TestАдресПоказываетсяДословно(t *testing.T) {
	случаи := map[string]string{
		"www":            "www.example.com",
		"www в тексте":   "смотри www.example.com тут",
		"www заглавными": "WWW.EXAMPLE.COM",
		"голый домен":    "example.com",
		"домен с путём":  "example.com/doc",
	}

	for имя, body := range случаи {
		t.Run(имя, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("текст письма изменён:\nбыло:  %q\nстало: %q", body, got)
			}
			if strings.Contains(out.HTML, "<a ") {
				t.Errorf("адрес без явной схемы стал ссылкой: %q", out.HTML)
			}
			if strings.Contains(вне(out.HTML), strings.TrimPrefix(body, "смотри ")) {
				t.Errorf("адрес остался голым: %q", out.HTML)
			}
		})
	}
}

// Явно написанный разрешённый адрес остаётся ссылкой и виден дословно.
//
// Пара к предыдущему: без неё зелена и реализация, которая не делает ссылок
// вовсе.
func TestЯвныйАдресОстаётсяСсылкой(t *testing.T) {
	случаи := []string{
		"https://example.com/doc",
		"http://example.com",
		"смотри https://example.com/a/b?x=1 дальше",
	}

	for _, body := range случаи {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if !strings.Contains(out.HTML, "<a href=") {
				t.Fatalf("явный адрес не стал ссылкой: %q", out.HTML)
			}
			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("текст изменён:\nбыло:  %q\nстало: %q", body, got)
			}
			// Видимый текст ссылки обязан совпадать с целью: иначе цель
			// скрыта от читателя.
			адрес := body
			if i := strings.Index(body, "http"); i >= 0 {
				адрес = body[i:]
			}
			if j := strings.Index(адрес, " "); j >= 0 {
				адрес = адрес[:j]
			}
			if !strings.Contains(out.HTML, `<a href="`+адрес+`">`+адрес+`</a>`) {
				t.Errorf("видимый текст ссылки не равен цели: %q", out.HTML)
			}
		})
	}
}

// Явный, но неразрешённый адрес остаётся дословным защищённым текстом.
//
// Эти случаи goldmark не превращает в AutoLink. Они нужны как контроль к
// правкам границы: новый путь отказа ссылки не должен ослабить уже работавший
// обычный подавитель.
func TestНеразрешённыйЯвныйАдресОстаётсяТекстом(t *testing.T) {
	for _, body := range []string{
		"https://пример.рф/страница",
		"HTTPS://EXAMPLE.COM/DOC",
	} {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("текст изменён:\nбыло:  %q\nстало: %q", body, got)
			}
			if strings.Contains(out.HTML, "<a ") {
				t.Errorf("неразрешённый адрес стал ссылкой: %q", out.HTML)
			}
			if strings.Contains(вне(out.HTML), body) {
				t.Errorf("неразрешённый адрес остался голым: %q", out.HTML)
			}
		})
	}
}

// Хвост адреса, не вошедший в разбор, обезврежен вместе с адресом.
//
// Разбор берёт в адрес только ASCII: «https://example.com/путь» приходит двумя
// узлами, и кириллический хвост остаётся обычным текстом. Ссылочный узел
// обязан передать продолжение дальше — иначе «путь» выходит голым рядом с
// обёрнутым адресом, а Telegram видит строку целиком.
//
// Нашла это живая матрица: тесты сверяли безопасность самой лексемы и не
// смотрели, что стоит сразу за ней.
func TestХвостАдресаОбезвреженВместеСАдресом(t *testing.T) {
	случаи := []string{
		"https://example.com/путь",
		"смотри https://example.com/путь тут",
		"https://example.com/док?q=да",
		"http://example.com/раздел/подраздел",
	}

	for _, body := range случаи {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			requireNoOverlap(t, out.HTML)
			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("текст изменён:\nбыло:  %q\nстало: %q", body, got)
			}

			снаружи := strings.TrimSpace(strings.ReplaceAll(вне(out.HTML), LineMarker, ""))
			for _, слово := range []string{"смотри", "тут"} {
				снаружи = strings.TrimSpace(strings.ReplaceAll(снаружи, слово, ""))
			}
			if снаружи != "" {
				t.Errorf("часть адреса осталась голой: %q\nвывод: %q", снаружи, out.HTML)
			}
		})
	}
}

// Хвост после опасной части защищён, даже если начинается с восклицания.
//
// Исключение для «!» действует только там, где текущий кусок безопасен:
// «Привет» + «!» — конец слова. Но «https://example.com/a» + «!b» — хвост
// опасной лексемы, и глобальное исключение оставляло бы «!b» голым.
//
// Дерево строится руками: разбор такой пары не даёт, а инвариант варианта B
// требует, чтобы ни одна показанная руна лексемы не оказалась снаружи.
func TestХвостПослеОпаснойЧастиЗащищён(t *testing.T) {
	случаи := []struct {
		имя    string
		первый string
		второй string
	}{
		{"адрес и восклицание", "https://example.com/a", "!b"},
		{"команда и восклицание", "/to", "!x"},
		{"домен и восклицание", "example.com", "!x"},
	}

	for _, случай := range случаи {
		t.Run(случай.имя, func(t *testing.T) {
			doc := ast.NewDocument()
			para := ast.NewParagraph()
			para.AppendChild(para, ast.NewString([]byte(случай.первый)))
			para.AppendChild(para, ast.NewString([]byte(случай.второй)))
			doc.AppendChild(doc, para)

			got := (&renderSession{doc: doc, source: nil}).finalize(4000, profiles[0], false)

			if видно := visibleText(got.html); !strings.Contains(видно, случай.первый+случай.второй) {
				t.Fatalf("узлы не склеились — проверка ничего не проверяет: %q", видно)
			}
			снаружи := strings.TrimSpace(strings.ReplaceAll(вне(got.html), LineMarker, ""))
			if снаружи != "" {
				t.Errorf("хвост опасной лексемы остался голым: %q\nвывод: %q", снаружи, got.html)
			}
		})
	}
}

// Восклицательный знак внутри адреса с путём не разрывает защиту.
//
// Живая проверка: «https://example.com/a!b» Telegram берёт в адрес целиком, а
// «/to!b» обрывает команду на знаке. То есть у символа нет собственного права
// разрывать лексему — решает то, что стоит перед ним.
func TestВосклицаниеВнутриАдресаЗащищено(t *testing.T) {
	случаи := []string{
		"https://example.com/a!b",
		"example.com/a!b",
		"смотри https://example.com/a!b тут",
	}

	for _, body := range случаи {
		t.Run(body, func(t *testing.T) {
			out := RenderBody(body, 4000)

			if got := visibleText(out.HTML); !strings.Contains(got, body) {
				t.Errorf("текст изменён:\nбыло:  %q\nстало: %q", body, got)
			}

			снаружи := strings.TrimSpace(strings.ReplaceAll(вне(out.HTML), LineMarker, ""))
			for _, слово := range []string{"смотри", "тут"} {
				снаружи = strings.TrimSpace(strings.ReplaceAll(снаружи, слово, ""))
			}
			if снаружи != "" {
				t.Errorf("часть адреса осталась голой: %q\nвывод: %q", снаружи, out.HTML)
			}
		})
	}
}
