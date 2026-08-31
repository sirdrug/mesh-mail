package tg

import (
	"strings"
	"unicode"
)

// Подавление опасных лексем.
//
// Живая проверка 23.08.2026 показала: вне code и pre Telegram сам превращает в
// действия команды, упоминания, метки, тикеры, почту, телефоны и адреса —
// включая tg://, голые домены и имена файлов вроде README.md. Внутри code и
// pre не распознаётся НИЧЕГО, и это единственный работающий способ подавления;
// невидимые разделители отвергнуты отдельно — они уезжают в буфер обмена.
//
// Опасна тут одна вещь: строка чужого письма становится действием человека,
// нажавшего на неё. Остальное — навигация, но по цене одного и того же приёма.

// dangerousLexeme — может ли Telegram сделать эту лексему активной.
//
// Правило по ФОРМЕ, а не по списку классов, и это принципиально. Список того,
// что распознаёт Telegram, ведём не мы: он пополняется молча, и наш список
// устарел бы в сторону ПРОПУСКА — то есть в сторону кликабельной команды в
// чате владельца. Форма же зависит только от нас.
//
// Отсюда и направление ошибки: сомнительная лексема уходит в моноширинный
// вид. Лишняя моноширинность — цена, которую платит вид письма; пропущенная
// лексема — цена, которую платит человек.
//
// Ровно поэтому здесь нет ни списка доменных зон, ни точного повторения
// правил Telegram для команд. Живая матрица показала, что зоны решают всё
// (`run.sh` — ссылка, `main.go` — нет), а зоны ведёт IANA, не Telegram и не мы.
func dangerousLexeme(lexeme string) bool {
	// Пунктуация вокруг лексемы Telegram в сущность не берёт, но и распознавать
	// не мешает: «(/to)» — команда, «+79991234567,» — телефон. Значит и решать
	// надо по ядру, а оборачивать — всю лексему целиком: обёртка вокруг ядра
	// оставила бы скобку снаружи, а лишняя скобка в моноширинном виде дешевле
	// разбирательств, где именно Telegram проводит границу.
	lexeme = lexemeCore(lexeme)
	if lexeme == "" {
		return false
	}

	// Слеш: команда «/to», путь «/etc», схема «https://». Короткие пути
	// Telegram делает командами — проверено живьём на «/tmp» и «/etc».
	if strings.ContainsRune(lexeme, '/') {
		return true
	}

	// Собака: упоминание и почта.
	if strings.ContainsRune(lexeme, '@') {
		return true
	}

	// Метка и тикер. Проверяется наличие знака где угодно в лексеме, а не
	// только в начале: где именно Telegram считает границу — его дело, а
	// ошибиться в сторону лишней моноширинности дешевле.
	if strings.ContainsAny(lexeme, "#$") {
		return true
	}

	if hasInnerDot(lexeme) {
		return true
	}

	return looksLikePhone(lexeme)
}

// hasInnerDot — точка между буквенно-цифровыми знаками.
//
// Так выглядят и домен, и имя файла: «example.com», «README.md», «run.sh».
// Различать их значило бы держать список зон; здесь оба уходят в
// моноширинный вид, и это осознанная цена.
//
// Точка в конце предложения под правило не попадает: после неё нет знака.
func hasInnerDot(lexeme string) bool {
	runes := []rune(lexeme)
	for i := 1; i < len(runes)-1; i++ {
		if runes[i] != '.' {
			continue
		}
		if isAlnum(runes[i-1]) && isAlnum(runes[i+1]) {
			return true
		}
	}
	return false
}

// looksLikePhone — цифры, из которых Telegram может собрать номер.
//
// Живая матрица: «+79991234567» и «+7-999-123-45-67» распознаются, «+7 999 …»
// с пробелами — нет. Пробел разбивает лексему сам, поэтому здесь достаточно
// смотреть на одну лексему целиком.
func looksLikePhone(lexeme string) bool {
	var digits int
	for _, r := range lexeme {
		switch {
		case unicode.IsDigit(r):
			digits++
		case strings.ContainsRune("+-()", r):
		default:
			return false
		}
	}
	return digits >= 5
}

// lexemeCore — лексема без обрамляющей пунктуации.
//
// Слева снимаются открывающие скобки и кавычки, справа — завершающие знаки.
// Внутренняя пунктуация не трогается: она часть адреса или пути.
func lexemeCore(lexeme string) string {
	lexeme = strings.TrimLeft(lexeme, `([{«"'`)
	return strings.TrimRight(lexeme, `)]}»"',.!?;:`)
}

func isAlnum(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// lexemes режет строку на куски: пробельные и непробельные вперемешку.
//
// Границей служит пробел, и только он. Telegram распознаёт лексему целиком —
// «текстexample.com» становится ссылкой вместе с приклеенным словом, — значит
// и оборачивать надо целиком, иначе часть останется голой.
func lexemes(s string) []string {
	if s == "" {
		return nil
	}

	var out []string
	var current strings.Builder
	space := unicode.IsSpace([]rune(s)[0])

	for _, r := range s {
		if unicode.IsSpace(r) == space {
			current.WriteRune(r)
			continue
		}
		out = append(out, current.String())
		current.Reset()
		current.WriteRune(r)
		space = !space
	}
	if current.Len() > 0 {
		out = append(out, current.String())
	}
	return out
}
