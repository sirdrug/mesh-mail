package tg

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/mail"
)

// Пост с кодом внутри укладывается в одно сообщение и не рвёт разметку.
//
// Различающий случай — обычный фрагмент кода на Go. Угловые скобки и
// амперсанды в нём после экранирования вырастают вчетверо: тело, влезавшее
// в предел по символам, вылезало за предел сообщения уже разметкой, делилось
// пополам и первая половина уходила с незакрытым <pre>. Telegram отвечал на
// неё отказом, письмо возвращалось в поток и не доходило вовсе.
func TestПостСКодомНеРвётРазметку(t *testing.T) {
	line := "if a < b && c > d { m[k] = v } // <-- сравнение\n"
	body := strings.Repeat(line, BodyLimit/len([]rune(line)))

	m := mail.New("pi-claude", []string{"m1-codex"}, "большой фрагмент", body)
	post := FormatMessage(m).Text

	if got := len([]rune(post)); got > MaxMessageRunes {
		t.Fatalf("пост %d рун при пределе %d — его придётся делить, а деление рвёт теги",
			got, MaxMessageRunes)
	}

	parts := Split(post)
	if len(parts) != 1 {
		t.Fatalf("пост разделился на %d частей", len(parts))
	}
	if opened, closed := strings.Count(post, "<pre>"), strings.Count(post, "</pre>"); opened != closed {
		t.Fatalf("разметка не сбалансирована: %d открывающих, %d закрывающих", opened, closed)
	}
	if !strings.Contains(post, "обрезано") {
		t.Fatal("тело урезано молча — человек не поймёт, что видит не всё")
	}
}

// Даже письмо из одних угловых скобок укладывается в предел.
//
// Худший случай для экранирования: каждый символ вырастает вчетверо.
func TestПостИзОднихСкобокУкладываетсяВПредел(t *testing.T) {
	m := mail.New("pi-claude", []string{"m1-codex"}, "скобки", strings.Repeat("<", BodyLimit*2))

	if got := len([]rune(FormatMessage(m).Text)); got > MaxMessageRunes {
		t.Fatalf("пост %d рун при пределе %d", got, MaxMessageRunes)
	}
}

// Длинная тема и длинный список получателей тоже не выталкивают пост за предел.
func TestДлиннаяОбвязкаНеВыталкиваетПостЗаПредел(t *testing.T) {
	recipients := make([]string, 40)
	for i := range recipients {
		recipients[i] = "очень-длинное-имя-узла-номер-" + strings.Repeat("x", 20)
	}

	m := mail.New("pi-claude", recipients, strings.Repeat("тема ", 200), strings.Repeat("тело ", 2000))
	m.Project = strings.Repeat("проект-", 50)
	m.AckRequired = true

	if got := len([]rune(FormatMessage(m).Text)); got > MaxMessageRunes {
		t.Fatalf("пост %d рун при пределе %d", got, MaxMessageRunes)
	}
}

func TestStripMarkupВозвращаетЧитаемыйТекст(t *testing.T) {
	got := StripMarkup("✉️ <b>pi-claude</b>\n<pre>if a &lt; b &amp;&amp; c &gt; d</pre>")

	for _, unwanted := range []string{"<b>", "</b>", "<pre>", "&lt;", "&amp;"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("в тексте осталось %q: %q", unwanted, got)
		}
	}
	if !strings.Contains(got, "if a < b && c > d") {
		t.Errorf("содержание потерялось: %q", got)
	}
}

// Отказ из-за разметки лечится безопасным <pre>, а не голым текстом.
func TestОтказРазметкиПревращаетсяВБезопасныйПоказ(t *testing.T) {
	var mu sync.Mutex
	var attempts []map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := readRequestFields(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"description":"двойник не разобрал запрос"}`))
			return
		}

		mu.Lock()
		attempts = append(attempts, payload)
		first := len(attempts) == 1
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if first {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: can't parse entities: Unsupported start tag \"3\" at byte offset 42"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
	}))
	defer srv.Close()

	c := New("t", WithBaseURL(srv.URL), WithMinSendGap(0))
	ids, err := c.SendMessage(context.Background(), SendRequest{
		ChatID: "-100", Text: "<b>тема</b>\n<pre>/to pi-codex\nif a &lt; 3</pre>",
	})
	if err != nil {
		t.Fatalf("сообщение не доставлено вовсе: %v", err)
	}
	if len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("идентификаторы %v, ожидался [7]", ids)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 {
		t.Fatalf("попыток %d, ожидались две: исходная и безопасная", len(attempts))
	}
	if got := attempts[1]["parse_mode"]; got != "HTML" {
		t.Errorf("безопасный повтор ушёл без HTML parse_mode: %v", got)
	}
	text, _ := attempts[1]["text"].(string)
	if !strings.HasPrefix(text, "<pre>") || !strings.HasSuffix(text, "</pre>") {
		t.Fatalf("повтор не защищён единым pre: %q", text)
	}
	if strings.Contains(text, "<b>") {
		t.Errorf("сложная разметка пережила упрощение: %q", text)
	}
	if !strings.Contains(text, "/to pi-codex") {
		t.Errorf("команда потерялась в безопасном показе: %q", text)
	}
	if !strings.Contains(text, "if a &lt; 3") {
		t.Errorf("содержание не переэкранировано: %q", text)
	}
}

func TestБезопасныйПоказУкладываетсяВЛимитПослеЭкранирования(t *testing.T) {
	got := safeFallbackMarkup(strings.Repeat("<", MaxMessageRunes), false)

	if n := len([]rune(got)); n > MaxMessageRunes {
		t.Fatalf("запасной показ %d рун при пределе %d", n, MaxMessageRunes)
	}
	if !strings.HasPrefix(got, "<pre>") || !strings.HasSuffix(got, "</pre>") {
		t.Fatalf("запасной показ вышел из pre: %q", got)
	}
	if !strings.Contains(got, "&lt;") || !strings.Contains(got, "…") {
		t.Fatalf("нет экранированного текста или знака обрезки: %q", got)
	}
}

// Отказ, не связанный с разметкой, повтором без неё не лечится.
//
// Контроль: иначе клиент отправлял бы всё подряд по два раза, а тест выше
// был бы зелёным и при такой ошибке.
func TestОбычныйОтказНеПовторяетсяБезРазметки(t *testing.T) {
	var mu sync.Mutex
	calls := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: chat not found"}`))
	}))
	defer srv.Close()

	c := New("t", WithBaseURL(srv.URL), WithMinSendGap(0))
	if _, err := c.SendMessage(context.Background(), SendRequest{ChatID: "-100", Text: "текст"}); err == nil {
		t.Fatal("ожидался отказ")
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("запросов %d, ожидался один: повтор без разметки тут ничего не чинит", calls)
	}
}

// Частота отправки ограничена.
//
// Telegram разрешает около двадцати сообщений в минуту на чат; раньше мост
// выкладывал их подряд и упирался в 429 уже постфактум.
func TestОтправкаОграниченаПоЧастоте(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer srv.Close()

	const gap = 150 * time.Millisecond
	c := New("t", WithBaseURL(srv.URL), WithMinSendGap(gap))

	start := time.Now()
	for range 3 {
		if _, err := c.SendMessage(context.Background(), SendRequest{ChatID: "-100", Text: "текст"}); err != nil {
			t.Fatalf("отправка: %v", err)
		}
	}
	elapsed := time.Since(start)

	// Три сообщения — это два промежутка между ними.
	if elapsed < 2*gap {
		t.Fatalf("три сообщения ушли за %v при паузе %v — ограничение не работает", elapsed, gap)
	}
}

// Ожидание очереди прерывается вместе с контекстом, а не держит мост.
func TestОжиданиеОчередиПрерываетсяКонтекстом(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer srv.Close()

	c := New("t", WithBaseURL(srv.URL), WithMinSendGap(time.Hour))
	if _, err := c.SendMessage(context.Background(), SendRequest{ChatID: "-100", Text: "первое"}); err != nil {
		t.Fatalf("первая отправка: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := c.SendMessage(ctx, SendRequest{ChatID: "-100", Text: "второе"}); err == nil {
		t.Fatal("ожидание не прервалось: мост завис бы на час")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("ожидание прервалось только через %v", elapsed)
	}
}

// Маркер строки не уезжает в аварийный показ.
//
// StripMarkup снимает теги, а маркер — текст, и он остался бы внутри блока:
// человек скопировал бы команду вместе со служебной приставкой. Ровно из-за
// буфера обмена блок кода и не метится маркером.
func TestАварийныйПоказСнимаетМаркерыСтрок(t *testing.T) {
	показ := LineMarker + "смотри код:\n" +
		LineMarker + "\n" +
		LineMarker + "<pre>rm -rf /tmp/x\ncd /etc</pre>\n" +
		LineMarker + "и путь /var/log"

	got := safeFallbackMarkup(показ, true)

	if strings.Contains(got, LineMarker) {
		t.Errorf("маркер строки попал в аварийный показ: %q", got)
	}
	for _, кусок := range []string{"смотри код:", "rm -rf /tmp/x", "cd /etc", "и путь /var/log"} {
		if !strings.Contains(got, кусок) {
			t.Errorf("текст потерян (%q): %q", кусок, got)
		}
	}
	if !strings.HasPrefix(got, "<pre>") || !strings.HasSuffix(got, "</pre>") {
		t.Errorf("аварийный показ не в одном блоке: %q", got)
	}
}

// Вертикальная черта внутри строки — обычный символ письма.
//
// Тело вправе рисовать псевдографикой рамку или таблицу. Вырезать оттуда
// черту значит портить текст — а это уже было однажды в тестовом оракуле,
// который снимал приставки отовсюду и съедал «> » из середины предложения.
func TestАварийныйПоказСохраняетЧертуВнутриСтроки(t *testing.T) {
	// Боковая грань таблицы, а НЕ угол: угол «┌───┬───┐» не начинается с
	// черты и до проверяемого места не доходит вовсе — тест на нём зелен по
	// неправильной причине. Средняя строка таблицы начинается ровно со
	// служебной приставки, и вот она под удар и попадает.
	показ := LineMarker + "│ a │ b │\n" +
		LineMarker + "имя │ значение\n" +
		LineMarker + "│ так тело написало свой маркер"

	got := safeFallbackMarkup(показ, true)

	if !strings.Contains(got, "│ a │ b │") {
		t.Errorf("боковая грань таблицы развалилась: %q", got)
	}
	if !strings.Contains(got, "имя │ значение") {
		t.Errorf("черта внутри строки вырезана: %q", got)
	}
	if !strings.Contains(got, "│ так тело написало свой маркер") {
		t.Errorf("вторая приставка подряд снята — пропало, что маркер написало тело: %q", got)
	}
}

// Строки многострочного блока кода приходят без маркера, и это не меняется.
//
// Маркер стоял перед <pre>, а внутри его нет: если снимать приставку не в
// начале строки, первая строка кода осталась бы помеченной, вторая — нет, и
// в одном блоке вышел бы разнобой.
func TestАварийныйПоказНеРазличаетСтрокиБлока(t *testing.T) {
	показ := LineMarker + "<pre>первая строка\nвторая строка</pre>"

	got := safeFallbackMarkup(показ, true)

	if !strings.Contains(got, "первая строка\nвторая строка") {
		t.Errorf("строки блока разошлись: %q", got)
	}
}

// Без явного признака аварийный показ не трогает ни одной строки.
//
// Тело письма вправе начинаться с вертикальной черты — например, внутри блока
// кода лежит нарисованная псевдографикой таблица. Отличить её от служебной
// приставки по виду нельзя, и догадка здесь стоит потери данных.
func TestАварийныйПоказПоУмолчаниюНичегоНеСнимает(t *testing.T) {
	показ := "<pre>│ пользовательские данные\n│ вторая строка</pre>"

	got := safeFallbackMarkup(показ, false)

	if !strings.Contains(got, "│ пользовательские данные") {
		t.Errorf("данные письма испорчены: %q", got)
	}
	if !strings.Contains(got, "│ вторая строка") {
		t.Errorf("данные письма испорчены: %q", got)
	}
}

// Тот же текст со снятием отличается ровно приставками.
//
// Пара к предыдущему: порознь каждый тест зелен при реализации, которая
// снимает всегда или не снимает никогда.
func TestСнятиеПриставокВключаетсяПризнаком(t *testing.T) {
	показ := LineMarker + "первая\n" + LineMarker + "вторая"

	без := safeFallbackMarkup(показ, false)
	со := safeFallbackMarkup(показ, true)

	if !strings.Contains(без, LineMarker+"первая") {
		t.Errorf("без признака приставка снята: %q", без)
	}
	if strings.Contains(со, LineMarker) {
		t.Errorf("с признаком приставка осталась: %q", со)
	}
}

// Без признака аварийный показ совпадает с прежним поведением ДОСЛОВНО.
//
// Не «содержит данные» и не «ничего не потерялось», а точное равенство: за
// день мы дважды находили дыры именно в том, что обязано было НЕ измениться, и
// оба раза проверка была слабее равенства. Эталон собирается здесь же,
// независимо от проверяемого кода.
func TestБезПризнакаПоказСовпадаетСПрежним(t *testing.T) {
	случаи := []string{
		"обычное письмо",
		"│ пользовательские данные",
		"┌───┬───┐\n│ a │ b │\n└───┴───┘",
		"<pre>│ внутри блока</pre>",
		"текст с <b>тегами</b> и &lt;экранированным&gt;",
		LineMarker + "строка со служебной приставкой",
	}

	for _, показ := range случаи {
		t.Run(показ, func(t *testing.T) {
			эталон := "<pre>" + esc(StripMarkup(показ)) + "</pre>"

			if got := safeFallbackMarkup(показ, false); got != эталон {
				t.Errorf("поведение изменилось:\nждали: %q\nвышло: %q", эталон, got)
			}
		})
	}
}

// Со снятием остаётся ровно то, что написало тело.
func TestСоСнятиемОстаётсяПользовательскаяЧерта(t *testing.T) {
	показ := LineMarker + "│ a │ b │"

	got := safeFallbackMarkup(показ, true)

	if got != "<pre>│ a │ b │</pre>" {
		t.Errorf("снята не ровно одна приставка: %q", got)
	}
}

// Признак доезжает от запроса до аварийного показа.
//
// Проверяется через двойник API: без него можно поменять поле в запросе и не
// заметить, что до показа оно не доходит — мутация «звать со снятием всегда»
// не покраснела бы ни на одном тесте самой функции.
func TestПризнакДоезжаетДоАварийногоПоказа(t *testing.T) {
	для := func(marked bool) string {
		var mu sync.Mutex
		var second string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			payload, err := readRequestFields(r)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"ok":false,"description":"двойник не разобрал запрос"}`))
				return
			}

			mu.Lock()
			first := second == "" && len(payload) > 0
			if !first {
				second, _ = payload["text"].(string)
			}
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			if first {
				text, _ := payload["text"].(string)
				mu.Lock()
				second = text
				mu.Unlock()
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: can't parse entities: Unsupported start tag \"3\" at byte offset 42"}`))
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
		}))
		defer srv.Close()

		c := New("t", WithBaseURL(srv.URL), WithMinSendGap(0))
		_, err := c.SendMessage(context.Background(), SendRequest{
			ChatID:      "-100",
			Text:        LineMarker + "строка письма",
			MarkedLines: marked,
		})
		if err != nil {
			t.Fatalf("отправка не удалась: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		return second
	}

	if got := для(false); !strings.Contains(got, LineMarker) {
		t.Errorf("без признака приставка снята: %q", got)
	}
	if got := для(true); strings.Contains(got, LineMarker) {
		t.Errorf("с признаком приставка осталась: %q", got)
	}
}

// Внутри блока кода черта принадлежит письму и остаётся.
//
// Приставок внутри блока мы не ставим — они уехали бы в буфер при копировании,
// — значит строка кода, начинающаяся с черты, написана телом. После снятия
// тегов эту разницу уже не восстановить: строки выглядят одинаково, поэтому
// приставки снимаются по РАЗМЕТКЕ, до StripMarkup.
func TestСнятиеНеТрогаетСтрокиВнутриБлокаКода(t *testing.T) {
	показ := LineMarker + "<pre>первая\n│ a │ b │\nтретья</pre>\n" + LineMarker + "хвост"

	got := safeFallbackMarkup(показ, true)

	if !strings.Contains(got, "│ a │ b │") {
		t.Errorf("боковая грань таблицы внутри блока стёрта: %q", got)
	}
	if strings.Contains(got, LineMarker+"хвост") {
		t.Errorf("служебная приставка вне блока осталась: %q", got)
	}
	if strings.Contains(got, LineMarker+"первая") {
		t.Errorf("служебная приставка перед блоком осталась: %q", got)
	}
}

// Пользовательская черта сразу после открытия блока остаётся.
func TestСнятиеОставляетЧертуПослеОткрытияБлока(t *testing.T) {
	показ := LineMarker + "<pre>│ данные\nвторая</pre>"

	got := safeFallbackMarkup(показ, true)

	if got != "<pre>│ данные\nвторая</pre>" {
		t.Errorf("снята не ровно служебная приставка: %q", got)
	}
}

// На сломанной разметке снятие прекращается: данные дороже вида.
func TestСломаннаяРазметкаОстанавливаетСнятие(t *testing.T) {
	случаи := map[string]string{
		"незакрытый блок":     LineMarker + "<pre>код\n" + LineMarker + "хвост",
		"закрытие без начала": LineMarker + "первая</pre>\n" + LineMarker + "вторая",
	}

	for имя, показ := range случаи {
		t.Run(имя, func(t *testing.T) {
			got := safeFallbackMarkup(показ, true)

			if !strings.Contains(got, "хвост") && !strings.Contains(got, "вторая") {
				t.Fatalf("текст письма потерян: %q", got)
			}
			if !strings.Contains(got, LineMarker) {
				t.Errorf("на сломанной разметке приставки сняты, хотя разбирать её нельзя: %q", got)
			}
		})
	}
}

// Экранированный «<pre>» из тела письма тегом не считается.
//
// Иначе тело, где человек написал про блок кода словами, выключало бы снятие
// приставок у всего остатка письма.
func TestЭкранированныйБлокНеСчитаетсяТегом(t *testing.T) {
	показ := LineMarker + "в письме написано &lt;pre&gt;\n" + LineMarker + "и вторая строка"

	got := safeFallbackMarkup(показ, true)

	if strings.Contains(got, LineMarker) {
		t.Errorf("экранированный текст принят за тег, приставки не сняты: %q", got)
	}
	if !strings.Contains(got, "<pre>") {
		t.Errorf("текст письма испорчен: %q", got)
	}
}

// Одно тело со всеми зонами маркера сразу.
//
// Таблица внутри блока кода, обрамлённая текстом: строки вне блока помечены,
// строки внутри — нет, а боковая грань таблицы выглядит ровно как приставка.
// Одна фикстура закрывает первую строку блока, середину и хвост после него.
func TestВсеЗоныМаркераВОдномТеле(t *testing.T) {
	показ := LineMarker + "текст\n" +
		LineMarker + "\n" +
		LineMarker + "<pre>┌───┐\n" +
		"│ a │\n" +
		"└───┘</pre>\n" +
		LineMarker + "\n" +
		LineMarker + "хвост"

	got := safeFallbackMarkup(показ, true)

	// Проверяется НАЧАЛО строк, а не наличие подстроки: «│ » встречается и в
	// самой таблице, и поиск по всему выводу нашёл бы её там. Ровно этой
	// ошибкой — искать вхождение вместо позиции — сегодня уже был испорчен
	// один оракул.
	ждём := []string{"<pre>текст", "", "┌───┐", "│ a │", "└───┘", "", "хвост</pre>"}
	строки := strings.Split(got, "\n")

	if len(строки) != len(ждём) {
		t.Fatalf("строк %d, ждали %d: %q", len(строки), len(ждём), got)
	}
	for i, line := range строки {
		if line != ждём[i] {
			t.Errorf("строка %d:\nждали: %q\nвышло: %q", i, ждём[i], line)
		}
	}
	for _, кусок := range []string{"текст", "┌───┐", "└───┘", "хвост"} {
		if !strings.Contains(got, кусок) {
			t.Errorf("потерян кусок письма %q: %q", кусок, got)
		}
	}
}

// Строка, начинающаяся с инлайн-кода, доказывает порядок работы.
//
// Служебная приставка стоит перед тегом, пользовательская — внутри него. По
// сериализованному виду они различаются тегом между ними; после снятия тегов
// строка выглядела бы «│ │ грань дальше», и правило «ровно один префикс»
// сработало бы верно по совпадению, а не по устройству.
func TestСтрокаНачинающаясяСИнлайнКода(t *testing.T) {
	показ := LineMarker + "<code>│ грань</code> дальше"

	got := safeFallbackMarkup(показ, true)

	if got != "<pre>│ грань дальше</pre>" {
		t.Errorf("снята не ровно служебная приставка: %q", got)
	}
}
