package tg

import (
	"strings"
	"testing"

	"github.com/yuin/goldmark/ast"
)

// Таблица адресов целиком: что становится ссылкой, а что остаётся текстом.
//
// Проверка сквозная, через RenderBody, а не через allowedURL: на `3bc21c4`
// безопасность давала не проверка адреса, а посторонние совпадения — Linkify
// сам отрезал userinfo, а эвристика оборачивала остаток в код. Проверка
// одного allowedURL этого не различала бы.
func TestТаблицаАдресовСквозная(t *testing.T) {
	недопустимые := map[string]string{
		"логин впереди":   "https://зло.example@github.com",
		"логин позади":    "https://github.com@зло.example",
		"логин с паролем": "https://user:pass@зло.example/path",
		"собака в начале": "смотри @github.com/boreevyuri/mesh-mail дальше",
		"без схемы":       "//host.example/путь",
		"чужая схема":     "tg://resolve?domain=x",
		"скрипт":          "javascript:alert(1)",
		"не-ASCII хост":   "https://аpple.com/doc",
		"только порт":     "http://:80/x",
		"порт без хоста":  "https://:8080/x",
	}

	for имя, body := range недопустимые {
		t.Run(имя, func(t *testing.T) {
			html, _ := renderRich(body, testBudget)
			if strings.Contains(html, "<a ") {
				t.Errorf("адрес стал ссылкой: %q", html)
			}
			if got := visibleText(html); !strings.Contains(got, body) {
				t.Errorf("видимый текст не равен исходному:\nбыло:  %q\nстало: %q", body, got)
			}
		})
	}

	допустимые := map[string]string{
		"обычный":           "https://example.com/doc",
		"с портом":          "https://example.com:8080/doc",
		"с собакой в query": "https://example.com/doc?q=a@b",
		"с якорем":          "https://example.com/a/b#anchor",
	}

	for имя, адрес := range допустимые {
		t.Run(имя, func(t *testing.T) {
			html, _ := renderRich("смотри "+адрес+" дальше", testBudget)
			if !strings.Contains(html, `<a href="`+адрес+`">`) {
				t.Errorf("адрес не стал ссылкой: %q", html)
			}
			if !strings.Contains(html, `">`+адрес+`</a>`) {
				t.Errorf("видимый текст ссылки не равен её цели: %q", html)
			}
		})
	}
}

// Граница лексемы считается по порядку документа, а не по родству узлов.
//
// Вопрос, на который отвечает проверка, — «что человек увидит сразу за
// ссылкой». Родство узлов на него не отвечает: в одних случаях хвост лежит
// соседом, в других — снаружи выделения, внутри которого оказалась ссылка.
//
// Случаи подписаны намеренно: сегодня их различает только расположение
// разметки относительно адреса, и через месяц это будет неочевидно.
func TestГраницаЛексемыПоПорядкуДокумента(t *testing.T) {
	случаи := map[string]struct {
		body    string
		видно   string
		почему  string
		anchors int
	}{
		"выделение обёртывает ссылку, курсив": {
			body: "*https://github.com*@evil.example", видно: "https://github.com@evil.example",
			почему: "регрессия: ссылка — последний ребёнок выделения, соседа у неё нет вовсе",
		},
		"выделение обёртывает ссылку, жирный": {
			body: "**https://github.com**@evil.example", видно: "https://github.com@evil.example",
			почему: "регрессия: то же внутри жирного",
		},
		"кодовый хвост": {
			body: "https://github.com`@evil.example`", видно: "https://github.com@evil.example",
			почему: "регрессия: обратные кавычки — разметка, на экране адрес читается слитно",
		},
		"сырой HTML хвостом": {
			body: "https://github.com<b>@evil.example</b>", видно: "https://github.com<b>@evil.example</b>",
			почему: "регрессия: сырой HTML мы печатаем текстом, и он прилегает к адресу",
		},
		"выделение после ссылки": {
			body: "https://github.com**@evil.example**", видно: "https://github.com**@evil.example**",
			почему: "контроль: выделение тут не образуется вовсе, ссылки нет и без проверки границы",
		},
		"ссылка с текстом": {
			body: "[текст](https://example.com)@evil.example", видно: "текст (https://example.com)@evil.example",
			почему: "контроль: ссылка с подменённым текстом и так не становится ссылкой",
		},
	}

	for имя, случай := range случаи {
		t.Run(имя, func(t *testing.T) {
			html, _ := renderRich(случай.body, testBudget)
			if n := strings.Count(html, "<a "); n != случай.anchors {
				t.Errorf("ссылок %d, ждали %d (%s): %q", n, случай.anchors, случай.почему, html)
			}
			if got := visibleText(html); !strings.Contains(got, случай.видно) {
				t.Errorf("видимый текст разорван (%s):\nждали: %q\nвышло: %q", случай.почему, случай.видно, got)
			}
		})
	}
}

// Ссылка в конце текста остаётся ссылкой, даже если она внутри выделения.
//
// Цена правила названа вслух: у конца документа следующего видимого символа
// нет, и без явного разрешения «конец текста» ссылка внутри курсива перестала
// бы работать без всякой причины. Тест держит это разрешение на месте.
func TestСсылкаВКонцеТекстаОстаётсяСсылкой(t *testing.T) {
	случаи := map[string]string{
		"внутри курсива": "*https://github.com*",
		"внутри жирного": "**https://github.com**",
		"голая":          "смотри https://github.com",
	}

	for имя, body := range случаи {
		t.Run(имя, func(t *testing.T) {
			html, _ := renderRich(body, testBudget)
			if !strings.Contains(html, `<a href="https://github.com">`) {
				t.Errorf("ссылка в конце текста потеряна: %q", html)
			}
		})
	}
}

// Пробел после адреса ссылку не отменяет.
//
// Парный к предыдущему и обязательный: правило «сосед не текст — не делаем
// ссылку» без него зелено и у реализации, которая не делает ссылок никогда.
// Пробел живёт отдельным текстовым узлом, поэтому обычная строка с адресом и
// выделением рядом должна остаться ссылкой.
func TestРазметкаЧерезПробелСсылкуНеОтменяет(t *testing.T) {
	случаи := map[string]string{
		"жирное следом":  "https://example.com/doc **жирный**",
		"код следом":     "https://example.com/doc `код`",
		"перенос следом": "https://example.com/doc\n@evil.example",
		"конец тела":     "смотри https://example.com/doc",
		"запятая следом": "смотри https://example.com/doc, дальше",
		"точка следом":   "смотри https://example.com/doc.",
		"скобка следом":  "(смотри https://example.com/doc)",
		"новый абзац":    "https://example.com/doc\n\n@evil.example",
	}

	for имя, body := range случаи {
		t.Run(имя, func(t *testing.T) {
			html, _ := renderRich(body, testBudget)
			if !strings.Contains(html, `<a href="https://example.com/doc">`) {
				t.Errorf("адрес не стал ссылкой: %q", html)
			}
		})
	}
}

// Хост из одного порта адресом не считается.
//
// Отдельно от сквозной таблицы: Linkify такого узла не создаёт, поэтому
// сквозная проверка зелена и при неверном правиле — она ничего не различает.
func TestТолькоПортНеАдрес(t *testing.T) {
	for _, raw := range []string{"http://:80", "https://:8080/x", "http://:80/path"} {
		if allowedURL(raw) {
			t.Errorf("адрес без хоста признан допустимым: %q", raw)
		}
	}
	if !allowedURL("https://example.com:8080/doc") {
		t.Error("адрес с портом и хостом должен быть допустим")
	}
}

// Узел, о котором мы не знаем, что он покажет, считается видимым.
//
// Проверяется напрямую: сегодня таких узлов в нашей сборке goldmark не
// возникает — расширений включено одно, — и через рендерер этот случай
// недостижим. Тем нужнее прямая проверка: ветка существует ровно на случай,
// когда список узлов пополнится, и молчаливое «ничего не видно» открыло бы
// ссылку там, где на экране стоит хвост.
func TestНеизвестныйУзелСчитаетсяВидимым(t *testing.T) {
	// Inline-узел без детей и не из знакомых: что он выведет — неизвестно.
	неизвестный := ast.NewEmphasis(1)

	r, ok := firstVisibleRune(неизвестный, nil)

	if !ok {
		t.Fatal("узел признан невидимым, значит ссылка перед ним будет разрешена")
	}
	if isLexemeTerminator(r) {
		t.Errorf("символ %q сочтён разрывом лексемы", r)
	}
}
