package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// StreamName — поток, в котором лежат все письма сети.
	StreamName = "MAIL"
	// StateBucket — KV с позицией чтения каждого агента.
	StateBucket = "mail_state"

	mailSubjectPrefix = "mail."
	// Письма живут три месяца: этого хватает, чтобы поднять старый тред,
	// и не превращает VPS в бесконечный архив.
	mailRetention = 90 * 24 * time.Hour
	// Окно дедупликации. Повтор публикации в его пределах не создаёт письмо.
	//
	// Сутки, а не пять минут, как было. Мост даёт письму от человека
	// детерминированный идентификатор из chat_id и update_id, чтобы рестарт
	// не превращал одно сообщение в два. Но за пределами окна такой ключ
	// не значит ничего, а рестарт дольше пяти минут — это норма: обновление,
	// перезагрузка VPS, отладка. Telegram хранит недоставленные обновления
	// около суток, поэтому и окно суточное — за его пределами повторять
	// уже нечего.
	//
	// Плата за это — таблица дедупликации в памяти сервера. В ней лежат
	// только идентификаторы, а писем у нас сотни в сутки, не миллионы.
	dedupWindow = 24 * time.Hour
)

// MailSubject — тема, в которую кладут письмо для агента.
//
// Отправитель — часть темы, и это не украшение: хаб проверяет право
// `publish: mail.*.<свой_id>`, поэтому соврать про себя нельзя. Раньше
// отправитель лежал только в теле письма, где его никто не удостоверял:
// любой клиент с любым агентским ключом мог представиться `human` — самым
// авторитетным отправителем в сети, — и получатель верил.
//
// Токенов ровно три, поэтому идентификаторы не должны содержать точку;
// это проверяет config.Load.
func MailSubject(recipient, sender string) string {
	return mailSubjectPrefix + recipient + "." + sender
}

// MailInboxFilter — что слушает и читает сам агент: письма от кого угодно.
func MailInboxFilter(recipient string) string {
	return mailSubjectPrefix + recipient + ".*"
}

// UnverifiedSender — отправитель письма, за которое хаб не поручился.
//
// Пустую строку сюда ставить нельзя: пустое поле читается как «не указан»,
// а показать надо именно недоверие. Значение общее для инбокса, сторожа и
// витрины — человек должен видеть одно и то же во всех трёх местах.
const UnverifiedSender = "неизвестный"

// SenderFromSubject достаёт удостоверённого отправителя из темы письма.
//
// Пустая строка означает, что тема не подходит под шаблон, — такое письмо
// доверия не заслуживает.
func SenderFromSubject(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) != 3 || parts[0] != "mail" {
		return ""
	}
	return parts[2]
}

// SenderForDisplay — кого показать человеку или агенту как отправителя.
//
// Единственная функция на все места, где отправитель попадает наружу:
// инбокс, строка сторожа, пост в канале. Раньше каждое из них решало само,
// и три одинаковые константы жили в трёх пакетах — комментарий обещал
// «общее значение», а по факту копии могли разойтись молча.
//
// Возвращает либо того, за кого поручился хаб, либо UnverifiedSender.
// Пустой строки не возвращает никогда: пустое поле читается как «отправитель
// не указан», а сказать надо, что ему нельзя верить.
func SenderForDisplay(subject string) string {
	if sender := SenderFromSubject(subject); sender != "" {
		return sender
	}
	return UnverifiedSender
}

// EnsureTopology поднимает топологию целиком.
//
// Оставлена ради тестов и вызовов, которым безразлично разделение прав.
// В продакшн-путях зовут EnsureBridgeTopology (мост) либо CheckTopology
// (агент) — см. комментарии к ним.
func EnsureTopology(ctx context.Context, js jetstream.JetStream) error {
	return EnsureBridgeTopology(ctx, js)
}

// EnsureBridgeTopology создаёт топологию и приводит её к ожидаемой.
//
// Зовёт ТОЛЬКО мост. Право менять поток есть у него одного: с ним можно
// убрать дедупликацию или сменить retention на work-queue, после чего письма
// начнут исчезать при первом же чтении.
//
// Приведение конфигурации нужно потому, что параметры со временем меняются —
// окно дедупликации выросло с пяти минут до суток, — а поток, созданный
// раньше, сам об этом не узнает.
func EnsureBridgeTopology(ctx context.Context, js jetstream.JetStream) error {
	if err := ensureStream(ctx, js); err != nil {
		return err
	}
	return ensureBucket(ctx, js)
}

// CheckTopology убеждается, что топология на месте, и НИЧЕГО не меняет.
//
// Зовут агенты. Разделение не косметическое: агентской учётке право
// STREAM.UPDATE не выдано намеренно, и попытка привести конфигурацию из
// агентского процесса кончилась бы отказом по правам ещё до первого письма.
//
// Проявилось бы это на раскатке. Узлы обновляются по одному; узел с новой
// версией ожидает новое окно дедупликации, поток на хабе пока со старым,
// конфигурации разошлись — и узел не стартует вовсе. Восемь машин, на каждой
// «mesh mcp не запускается» без видимой связи с потоком.
//
// Поэтому расхождение версий агента не касается: письма ходят при любом окне,
// хуже только защита от дублей, а приведёт конфигурацию мост при своём старте.
func CheckTopology(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.Stream(ctx, StreamName); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("потока %s на хабе нет: топологию поднимает мост, "+
				"запустите его первым (mesh bridge)", StreamName)
		}
		return fmt.Errorf("поток %s: %w", StreamName, err)
	}
	if _, err := js.KeyValue(ctx, StateBucket); err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("бакета %s на хабе нет: топологию поднимает мост, "+
				"запустите его первым (mesh bridge)", StateBucket)
		}
		return fmt.Errorf("бакет %s: %w", StateBucket, err)
	}
	return nil
}

// streamConfig — какой поток мы ожидаем видеть на хабе.
func streamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      StreamName,
		Subjects:  []string{mailSubjectPrefix + ">"},
		Retention: jetstream.LimitsPolicy, // ящик, а не очередь: письма не исчезают при чтении
		MaxAge:    mailRetention,
		// Дедупликация по Nats-Msg-Id: JetStream доставляет at-least-once,
		// и без окна агент однажды ответит на одно письмо дважды.
		Duplicates: dedupWindow,
	}
}

func ensureStream(ctx context.Context, js jetstream.JetStream) error {
	stream, err := js.Stream(ctx, StreamName)
	if err == nil {
		// Поток есть — сверяем то, что со временем менялось.
		if stream.CachedInfo().Config.Duplicates == dedupWindow {
			return nil
		}
		if _, err := js.UpdateStream(ctx, streamConfig()); err != nil {
			return fmt.Errorf("окно дедупликации потока %s не приведено к %s "+
				"(нужна учётка с правом STREAM.UPDATE — это мост): %w",
				StreamName, dedupWindow, err)
		}
		return nil
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("поток %s: %w", StreamName, err)
	}

	// Гонка: топологию мог создать другой процесс между проверкой и созданием.
	if _, err := js.CreateStream(ctx, streamConfig()); err != nil &&
		!errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("поток %s не создан (нужна учётка с правом STREAM.CREATE): %w",
			StreamName, err)
	}
	return nil
}

func ensureBucket(ctx context.Context, js jetstream.JetStream) error {
	if _, err := js.KeyValue(ctx, StateBucket); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return fmt.Errorf("бакет %s: %w", StateBucket, err)
	}

	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      StateBucket,
		Description: "позиция чтения ящика по agent_id",
	})
	if err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return fmt.Errorf("бакет %s не создан (нужна учётка с правом STREAM.CREATE): %w",
			StateBucket, err)
	}
	return nil
}
