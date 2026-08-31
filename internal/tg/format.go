package tg

import (
	"fmt"
	"html"
	"regexp"
	"strings"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/mail"
)

// BodyLimit — сколько символов тела показываем в канале.
//
// Канал — витрина, а не архив: полное письмо всегда доступно агенту через
// fetch_inbox. Длинные простыни в телеграме читать всё равно невозможно.
const BodyLimit = 3000

func esc(s string) string { return html.EscapeString(s) }

// fitEscaped обрезает текст так, чтобы ЭКРАНИРОВАННЫЙ он уложился в бюджет.
//
// Считать бюджет по исходному тексту нельзя, и это не мелочь. Экранирование
// раздувает строку вчетверо на каждой угловой скобке: обычный фрагмент кода
// вроде `if a < b && c > d` из трёх тысяч символов превращался в четыре с
// лишним тысячи, вылезал за предел сообщения и разрезался пополам — прямо
// посреди <pre>. Telegram отвечал на такую половину отказом разметки, письмо
// возвращалось в поток и не доходило вовсе.
//
// Возвращает вторым значением признак обрезки: молча укорачивать письмо
// нельзя, человек должен видеть, что оно длиннее показанного.
func fitEscaped(text string, budget int) (string, bool) {
	if budget <= 0 {
		return "", text != ""
	}
	if len([]rune(esc(text))) <= budget {
		return text, false
	}

	// По руне, накапливая длину в экранированном виде: у разных символов она
	// разная, и одной арифметикой её не получить.
	var out strings.Builder
	used := 0
	for _, r := range text {
		cost := len([]rune(esc(string(r))))
		if used+cost > budget {
			break
		}
		out.WriteRune(r)
		used += cost
	}
	return out.String(), true
}

// truncationReserve — место под строку «обрезано».
//
// С запасом: строка содержит число, а сколько в нём цифр, заранее неизвестно.
const truncationReserve = 80

// StripMarkup убирает разметку, оставляя текст.
//
// Нужен запасному показу: если Telegram не принял разметку, содержание письма
// всё равно надо донести. Сущности возвращаются в обычные символы — иначе
// человек прочитает `&lt;` вместо `<`.
func StripMarkup(text string) string {
	return html.UnescapeString(tagRe.ReplaceAllString(text, ""))
}

// safeFallbackMarkup сводит отвергнутую разметку к одному безопасному <pre>.
//
// Повтор без parse_mode небезопасен: Telegram сам превращает команды, адреса
// и другие лексемы в активные сущности. Внутри <pre> это распознавание
// подавлено. Текст сначала возвращается к видимому виду, затем экранируется
// заново; так отказ сложной разметки не переносится в запасной показ.
// dropMarkers говорит, что строки пришли со служебной приставкой и её надо
// снять. Отдельным признаком, а не догадкой по виду текста: та же черта в
// начале строки бывает и обычным текстом письма, и тогда снятие — потеря
// данных.
func safeFallbackMarkup(text string, dropMarkers bool) string {
	const (
		open       = "<pre>"
		close      = "</pre>"
		truncation = "…"
	)

	if dropMarkers {
		// До снятия тегов: только так видно, какие строки лежат внутри блока
		// кода, где приставок мы не ставили.
		text = dropLineMarkers(text)
	}
	visible := StripMarkup(text)
	budget := MaxMessageRunes - len([]rune(open)) - len([]rune(close))
	body, cut := fitEscaped(visible, budget)
	if cut {
		body, _ = fitEscaped(visible, budget-len([]rune(truncation)))
		body += truncation
	}
	return open + esc(body) + close
}

// dropLineMarkers убирает служебную приставку строки — но только там, где мы
// её ставили.
//
// Зовётся ТОЛЬКО по явному признаку от того, кто эти приставки поставил.
// Догадаться по тексту нельзя: `│ пользовательские данные` в теле письма — это
// данные, а не приставка, и разница видна лишь тому, кто текст собирал.
//
// Работает по РАЗМЕЧЕННОМУ тексту, до снятия тегов, и в этом всё дело. Внутри
// блока кода приставок нет намеренно — они уехали бы в буфер при копировании,
// — значит черта в начале строки кода принадлежит письму. После снятия тегов
// эту разницу уже не восстановить: строки выглядят одинаково.
//
// Снимается РОВНО ОДНА приставка и только в начале строки. Черта внутри
// строки — обычный символ: тело вправе рисовать псевдографикой таблицу, и её
// боковая грань выглядит ровно как приставка. Вторая приставка подряд тоже
// остаётся: «│ │ …» означает, что маркер написало само тело, и это видно.
//
// На сломанной разметке — незакрытый блок, закрытие без открытия — снятие
// прекращается вовсе. Лишняя приставка на экране хуже, чем стёртая строка
// письма, и выбор здесь всегда в пользу данных.
func dropLineMarkers(markup string) string {
	lines := strings.Split(markup, "\n")

	var depth int
	var broken bool
	for i, line := range lines {
		if !broken && depth == 0 {
			lines[i] = strings.TrimPrefix(line, LineMarker)
		}

		// Экранированный `&lt;pre&gt;` тегом не считается: угловых скобок в
		// нём нет, и Count его не увидит — это ровно то, что нужно.
		depth += strings.Count(line, "<pre>") - strings.Count(line, "</pre>")
		if depth < 0 {
			broken = true
			depth = 0
		}
	}
	return strings.Join(lines, "\n")
}

// tagRe находит теги разметки.
//
// Экранированные скобки под неё не попадают: в тексте они уже выглядят как
// `&lt;`, то есть угловой скобки в них нет.
var tagRe = regexp.MustCompile(`</?[a-zA-Z][^>]*>`)

// Post — готовый пост и то, что отправителю нужно о нём знать.
//
// Признак приставок нельзя восстановить из текста: та же вертикальная черта
// в начале строки бывает и данными письма. Знает о ней только тот, кто её
// поставил, — поэтому она едет вместе с текстом, а не угадывается по нему.
type Post struct {
	Text string
	// MarkedLines — обычные строки поста начинаются со служебной приставки.
	MarkedLines bool
}

// FormatMessage превращает письмо в пост канала.
//
// Тело показывается размеченным: markdown отрисован, ссылки кликабельны, а
// лексемы, которые Telegram превращает в действия, обезврежены моноширинным
// видом. Обвязка — заголовок, тема, проект, пометки — остаётся нашей и в
// разметку тела не попадает: подделать её из письма нельзя, потому что каждая
// строка тела начинается со служебной приставки, а строки обвязки — нет.
func FormatMessage(m *mail.Message) Post {
	mark := ""
	switch m.Importance {
	case mail.ImportanceUrgent:
		mark = " ⚠️ срочно"
	case mail.ImportanceHigh:
		mark = " ❗️важно"
	}

	recipients := strings.Join(m.Recipients(), ", ")

	// Сперва обвязка: заголовок, тема, проект, пометки. Её длину надо знать,
	// чтобы понять, сколько места остаётся телу.
	var head strings.Builder
	fmt.Fprintf(&head, "✉️ <b>%s</b> → <b>%s</b>%s\n", esc(m.From), esc(recipients), mark)
	fmt.Fprintf(&head, "<b>%s</b>\n", esc(m.Subject))
	if m.Project != "" {
		fmt.Fprintf(&head, "<i>проект: %s</i>\n", esc(m.Project))
	}

	tail := ""
	if m.AckRequired {
		tail = "\n<i>требуется подтверждение</i>"
	}

	// Пост обязан уложиться в одно сообщение целиком. Резать его потом на
	// части нельзя: границы частей рвут разметку, а Telegram отвергает
	// половину тега целым сообщением.
	budget := MaxMessageRunes - len([]rune(head.String())) - len([]rune(tail)) -
		truncationReserve
	if budget > BodyLimit {
		budget = BodyLimit
	}

	shown := RenderBody(m.Body, budget)

	var b strings.Builder
	b.WriteString(head.String())
	b.WriteString(shown.HTML)
	if shown.Truncated {
		fmt.Fprintf(&b, "\n<i>…обрезано, всего в письме %d символов</i>", len([]rune(m.Body)))
	}
	b.WriteString(tail)

	return Post{
		Text: b.String(),
		// Приставки ставят все показы, кроме общего блока: там границу
		// держит сам блок, и снимать нечего.
		MarkedLines: shown.Profile != profiles[len(profiles)-1].name,
	}
}

// FormatCorrupted — пост о письме, которое не удалось разобрать.
//
// Показываем, а не замалчиваем: молчание неотличимо от «писем не было».
// Номер в потоке позволяет найти письмо, а сырое начало тела — понять,
// кто и чем его испортил. Тело экранируется, как и у обычного письма:
// оно пришло из сети и вполне может содержать разметку.
func FormatCorrupted(seq uint64, raw string) string {
	return fmt.Sprintf(
		"⚠️ <b>повреждённое письмо</b>\nне удалось разобрать, позиция в потоке: <code>%d</code>\n<pre>%s</pre>",
		seq, esc(raw),
	)
}

// FormatCard — пост о появлении агента в сети.
func FormatCard(c bus.Card) string {
	projects := strings.Join(c.Projects, ", ")
	if projects == "" {
		projects = "—"
	}

	return fmt.Sprintf(
		"🪪 <b>%s</b> на связи\nхост: <code>%s</code> · движок: <code>%s</code>\nпроекты: %s",
		esc(c.AgentID), esc(c.Host), esc(c.Engine), esc(projects),
	)
}

// topicNameLimit — предел Telegram на имя темы.
const topicNameLimit = 128

// TopicName — имя форумной темы под разговор.
// GeneralTopicName — имя темы для писем без проекта.
//
// Поле проекта необязательное, и `mail.New` его не заполняет, поэтому таких
// писем много. Им нужна одна общая тема, а не тема на каждое: иначе
// возвращается ровно та мешанина, ради которой всё затевалось.
const GeneralTopicName = "Общее"

// ProjectTopicName — имя темы под проект.
//
// Обрезается по тому же пределу, что и остальные имена тем: Telegram длиннее
// не принимает, а отказ пришёл бы уже после того, как письмо показано.
func ProjectTopicName(project string) string {
	if project == "" {
		return GeneralTopicName
	}

	runes := []rune(project)
	if len(runes) > topicNameLimit {
		return string(runes[:topicNameLimit-1]) + "…"
	}
	return project
}

func TopicName(m *mail.Message) string {
	name := fmt.Sprintf("%s → %s: %s", m.From, strings.Join(m.To, ","), m.Subject)

	runes := []rune(name)
	if len(runes) > topicNameLimit {
		name = string(runes[:topicNameLimit-1]) + "…"
	}
	return name
}
