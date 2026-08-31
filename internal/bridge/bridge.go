package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/tg"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// Config — что нужно мосту для работы.
type Config struct {
	ChatID      string
	Token       string
	ForumTopics bool
	// AllowedUserIDs — кому позволено говорить от имени human.
	//
	// Пустой список — отказ, а не разрешение всем: см. Run.
	AllowedUserIDs []int64

	// TelegramOptions — настройки клиента Bot API.
	//
	// В бою пусто. Существует ради того, чтобы мост можно было поднять
	// ЦЕЛИКОМ в тесте: адрес API и пауза ограничителя — единственное, что
	// нельзя иметь настоящим, когда настоящего Telegram нет.
	//
	// Раньше адрес был зашит внутри Run, и стенд собирал половинки моста по
	// отдельности, повторяя clientPoster у себя десятью строками. Проверялась
	// копия боевого пути, а не он сам, — тот же класс ошибки, что тест,
	// вызывающий функцию вместо её применения.
	//
	// Подменяется именно клиент, а не Poster: так в тесте остаются и
	// clientPoster, и GetMe, и разбор ответов API. Инъекция Poster была бы
	// проще, но выкинула бы из проверки ровно то, что повторялось руками.
	TelegramOptions []tg.Option
}

// clientPoster приклеивает chat_id к вызовам клиента.
type clientPoster struct {
	client *tg.Client
	chatID string
}

func (p *clientPoster) Send(ctx context.Context, threadID int, post tg.Post) ([]int, error) {
	return p.client.SendMessage(ctx, tg.SendRequest{
		ChatID: p.chatID, Text: post.Text, ThreadID: threadID,
		MarkedLines: post.MarkedLines,
	})
}

func (p *clientPoster) CreateTopic(ctx context.Context, name string) (int, error) {
	return p.client.CreateForumTopic(ctx, p.chatID, name)
}

// Run поднимает обе половины моста и держит их, пока жив контекст.
func Run(ctx context.Context, conn *bus.Conn, cfg Config) error {
	if cfg.ChatID == "" {
		return fmt.Errorf("не задан идентификатор чата")
	}
	if cfg.Token == "" {
		return fmt.Errorf("не задан токен бота")
	}
	// Пустой список — отказ, и мост не поднимается вовсе.
	//
	// Раньше пустота означала «любой участник чата», а предупреждение в лог
	// при старте считалось достаточным. Это неверный размен: право писать от
	// имени human — самое сильное в сети, письмо от человека агенты читают
	// как распоряжение владельца машины. Ставилось оно в зависимость от того,
	// кого позвали в супергруппу, и менялось молча — добавили участника,
	// и он получил это право, никакой записи об этом нигде не появится.
	//
	// Отдельно скверно, что документация обещала ровно обратное поведение
	// («пустой список означает никому, это безопасный отказ»), то есть
	// оператор, читавший runbook, был уверен в защите, которой не было.
	if len(cfg.AllowedUserIDs) == 0 {
		return fmt.Errorf(
			"telegram.allowed_user_ids пуст: некому писать агентам от имени человека. " +
				"Укажите числовые id явно — свой узнаётся у @userinfobot. " +
				"Пустой список раньше означал «любому участнику чата», теперь это отказ")
	}

	client := tg.New(cfg.Token, cfg.TelegramOptions...)

	// Ранняя диагностика: молчащий из-за опечатки в токене мост
	// отлаживается мучительно.
	username, err := client.GetMe(ctx)
	if err != nil {
		return fmt.Errorf("токен не принят Telegram: %w", err)
	}
	log.Printf("мост работает от имени @%s, чат %s", username, cfg.ChatID)

	store, err := NewTopicStore(ctx, conn.JS())
	if err != nil {
		return err
	}

	reg := bus.NewRegistry()
	if err := bus.WatchPresence(ctx, conn.NC(), reg); err != nil {
		return fmt.Errorf("подписка на визитки: %w", err)
	}

	poster := &clientPoster{client: client, chatID: cfg.ChatID}

	// Тема «Общего» — единственная, чьё имя нельзя узнать из визиток: пустой
	// проект не перечисляет никто, «Общее» ничей не проект. Если её запись
	// сделана прежним кодом, имя дописывается здесь и один раз; если темы ещё
	// нет, вызов ничего не создаёт, и она появится уже с именем.
	//
	// Отказ хранилища здесь настоящий: молчаливое «не знаю» оставило бы тему,
	// в которую всё и уходит, навсегда неизвестной.
	if _, err := store.FillProjectName(ctx, ""); err != nil {
		return fmt.Errorf("имя темы «Общего»: %w", err)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return NewShowcase(conn.JS(), store, poster, cfg.ChatID, cfg.ForumTopics).Run(groupCtx)
	})
	state, err := NewStateStore(ctx, conn.JS())
	if err != nil {
		return err
	}

	intake := NewIntake(conn.JS(), store, client, reg, cfg.ChatID, cfg.AllowedUserIDs)
	// Обратный канал: сообщение, которое некому доставить, должно получить
	// внятный отказ, а не исчезнуть.
	intake.SetPoster(poster)
	// Имя бота нужно, чтобы принять `/to@наш_бот` из группового автодополнения
	// и отбросить команду чужому боту. Берётся из уже сделанного GetMe.
	intake.SetBotUsername(username)
	// Позиция чтения переживает рестарт: иначе мост разбирает заново всё, что
	// Telegram успел накопить, и человек получает свои сообщения повторно.
	intake.SetState(state)
	group.Go(func() error {
		return intake.Run(groupCtx)
	})
	group.Go(func() error {
		return handlePresence(groupCtx, conn, poster, store)
	})

	return group.Wait()
}

// handlePresence разбирает визитки: дозаполняет имена проектов и объявляет
// человеку новых агентов.
//
// Две задачи в одной подписке намеренно. Подписок на визитки в мосте уже две
// (реестр приёма и эта), и третья означала бы третью копию одного потока
// сообщений ради одного вызова.
//
// Объявление идёт только на ИЗМЕНЕНИЕ визитки — иначе канал засыпало бы
// повторами каждую минуту, потому что визитка переизлучается по таймеру.
// А дозаполнение имени — на КАЖДОЙ визитке, до этого фильтра, и это
// существенно: при отказе хранилища фильтр проглотил бы повтор, и проект
// остался бы без имени до перезапуска моста или смены визитки, то есть,
// возможно, навсегда. Лишних записей это не даёт — заполненную запись
// хранилище отвечает первым же чтением.
func handlePresence(ctx context.Context, conn *bus.Conn, poster Poster, store *TopicStore) error {
	seen := bus.NewRegistry()

	sub, err := conn.NC().Subscribe("agents.*.presence", func(msg *nats.Msg) {
		var card bus.Card
		if err := json.Unmarshal(msg.Data, &card); err != nil {
			return
		}

		fillProjectNames(ctx, store, card)

		if !seen.Upsert(card) {
			return // визитка та же, человеку это неинтересно
		}
		if _, err := poster.Send(ctx, 0, tg.Post{Text: tg.FormatCard(card)}); err != nil {
			log.Printf("мост: не смог объявить агента %s: %v", card.AgentID, err)
		}
	})
	if err != nil {
		return fmt.Errorf("подписка на визитки: %w", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // при остановке процесса это неважно

	<-ctx.Done()
	return nil
}

// fillProjectNames дописывает имена проектов из визитки в существующие темы.
//
// Имена проектов приходят к мосту сами, визитками живых агентов, — без
// единого письма. Это и есть основной источник: путь «узнать имя из письма с
// проектом» замыкается в круг ровно там, где нужен, потому что письмо без
// имени темы уходит без проекта.
//
// Записи не создаются: их вправе заводить только витрина при первом письме.
// Отказ хранилища логируется и не роняет мост — следующая визитка повторит
// попытку, для того дозаполнение и вызывается на каждой.
func fillProjectNames(ctx context.Context, store *TopicStore, card bus.Card) {
	for _, project := range card.Projects {
		filled, err := store.FillProjectName(ctx, project)
		if err != nil {
			log.Printf("мост: имя проекта %q не записано в тему (%v) — повторю на следующей визитке",
				project, err)
			continue
		}
		if filled {
			log.Printf("мост: тема проекта %q получила имя", project)
		}
	}
}
