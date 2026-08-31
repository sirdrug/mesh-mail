// Package mcpsrv выставляет почту агенту как набор MCP-инструментов.
//
// Отправитель никогда не приходит от агента: он берётся из конфигурации узла.
// Иначе письмо могло бы соврать, от кого оно, и вся адресация перестала бы
// что-либо значить.
package mcpsrv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/claims"
	"github.com/boreevyuri/mesh-mail/internal/config"
	"github.com/boreevyuri/mesh-mail/internal/mail"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go/jetstream"
)

type handlers struct {
	conn *bus.Conn
	reg  *bus.Registry
	node *config.Node

	// recent — письма, которые агент недавно видел, по идентификатору.
	//
	// Нужны reply_message: чтобы ответить, нужно исходное письмо целиком —
	// из него наследуются тред, тема и счётчик пересылок. Искать его
	// перечитыванием ящика стоит целого прохода (см. findInInbox), а отвечают почти
	// всегда на то, что только что прочитали.
	mu     sync.Mutex
	recent map[string]*mail.Message

	// search — параметры поиска письма в ящике (см. inboxSearch).
	search inboxSearch

	// presence — состояние наблюдения за визитками (см. listAgents).
	presence presenceWatch
	order    []string
	claims   *claims.Store
}

// recentCap — сколько писем помним. Ограничение существует, чтобы долгая
// сессия не растила память без предела.
const recentCap = 512

// remember кладёт письмо в кэш недавних, вытесняя самое старое.
func (h *handlers) remember(m *mail.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.recent == nil {
		h.recent = make(map[string]*mail.Message, recentCap)
	}
	if _, seen := h.recent[m.ID]; seen {
		return
	}
	h.recent[m.ID] = m
	h.order = append(h.order, m.ID)
	if len(h.order) > recentCap {
		delete(h.recent, h.order[0])
		h.order = h.order[1:]
	}
}

func (h *handlers) recall(id string) (*mail.Message, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.recent[id]
	return m, ok
}

// MessageView — письмо в том виде, в каком его видит агент.
type MessageView struct {
	Seq         uint64   `json:"seq" jsonschema:"позиция письма, нужна для mark_read"`
	ID          string   `json:"id"`
	ThreadID    string   `json:"thread_id"`
	From        string   `json:"from"`
	To          []string `json:"to"`
	CC          []string `json:"cc,omitempty"`
	Subject     string   `json:"subject"`
	Body        string   `json:"body"`
	Importance  string   `json:"importance"`
	AckRequired bool     `json:"ack_required,omitempty"`
	Project     string   `json:"project,omitempty"`
	CreatedAt   string   `json:"created_at"`
}

func view(env bus.Envelope) MessageView {
	m := env.Message
	return MessageView{
		Seq: env.Seq, ID: m.ID, ThreadID: m.ThreadID,
		From: m.From, To: m.To, CC: m.CC,
		Subject: m.Subject, Body: m.Body,
		Importance: m.Importance, AckRequired: m.AckRequired,
		Project: m.Project, CreatedAt: m.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

// --- fetch_inbox ---

type FetchInboxIn struct {
	UnreadOnly    bool   `json:"unread_only,omitempty" jsonschema:"только непрочитанные письма"`
	Limit         int    `json:"limit,omitempty" jsonschema:"сколько писем вернуть, по умолчанию 50"`
	MinImportance string `json:"min_importance,omitempty" jsonschema:"минимальная важность: normal, high, urgent"`
}

type FetchInboxOut struct {
	Messages []MessageView `json:"messages"`

	// HasMore — за выдачей остались непрочитанные письма.
	//
	// Без него срез неотличим от полного ящика: выдача идёт от позиции
	// прочитанного ВПЕРЁД, то есть отдаёт самые старые письма, и агент,
	// взявший одну порцию, отвечает по устаревшему состоянию, считая, что
	// прочитал всё. Именно так 30.08 разъехались узлы.
	//
	// Поле утвердительное, а не «дочитано»: молчание тогда означало бы
	// полноту, а неполнота обязана заявляться сама.
	HasMore bool `json:"has_more"`
}

func (h *handlers) fetchInbox(ctx context.Context, _ *mcp.CallToolRequest, in FetchInboxIn) (*mcp.CallToolResult, FetchInboxOut, error) {
	page, err := bus.InboxPage(ctx, h.conn.JS(), h.node.AgentID, bus.InboxOptions{
		UnreadOnly:    in.UnreadOnly,
		Limit:         in.Limit,
		MinImportance: in.MinImportance,
	})
	if err != nil {
		return nil, FetchInboxOut{}, fmt.Errorf("чтение ящика: %w", err)
	}

	out := FetchInboxOut{
		Messages: make([]MessageView, 0, len(page.Envelopes)),
		HasMore:  page.HasMore,
	}
	for _, env := range page.Envelopes {
		h.remember(env.Message)
		out.Messages = append(out.Messages, view(env))
	}
	return nil, out, nil
}

// --- send_message ---

type SendMessageIn struct {
	To          []string `json:"to" jsonschema:"идентификаторы получателей, например pi-claude"`
	CC          []string `json:"cc,omitempty"`
	Subject     string   `json:"subject"`
	Body        string   `json:"body" jsonschema:"текст письма в markdown"`
	Importance  string   `json:"importance,omitempty" jsonschema:"normal, high или urgent"`
	AckRequired bool     `json:"ack_required,omitempty"`
	Project     string   `json:"project,omitempty" jsonschema:"проект, к которому относится письмо"`
}

type SendMessageOut struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id"`
}

func (h *handlers) sendMessage(ctx context.Context, _ *mcp.CallToolRequest, in SendMessageIn) (*mcp.CallToolResult, SendMessageOut, error) {
	m := mail.New(h.node.AgentID, in.To, in.Subject, in.Body)
	m.CC = in.CC
	m.Project = in.Project
	m.AckRequired = in.AckRequired
	if in.Importance != "" {
		m.Importance = in.Importance
	}

	if err := bus.Publish(ctx, h.conn.JS(), m); err != nil {
		return nil, SendMessageOut{}, fmt.Errorf("отправка письма: %w", err)
	}
	return nil, SendMessageOut{ID: m.ID, ThreadID: m.ThreadID}, nil
}

// --- reply_message ---

type ReplyIn struct {
	MessageID string `json:"message_id" jsonschema:"идентификатор письма, на которое отвечаем"`
	Body      string `json:"body"`
}

type ReplyOut struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id"`
}

// inboxSearch — как искать письмо в ящике, если его нет в кэше недавних.
//
// Ширина окна и обе зависимости лежат ЗДЕСЬ, а не в пакетных переменных.
// Прежняя редакция подменяла в тестах пакетные `var`, и это создавало гонку на
// ровном месте: тесты пакета идут параллельно, а боевые пределы обязаны быть
// неизменными. Экземпляр же принадлежит одному handlers, и подменять его можно
// без общего состояния.
type inboxSearch struct {
	// window — ширина диапазона в позициях потока, она же предел чтения.
	//
	// Обе роли обязаны совпадать: покрытие держится на том, что диапазон из
	// window позиций содержит не больше window писем, а окно ровно столько и
	// читает. Разойдись эти числа — между окнами появятся дыры.
	window uint64

	scan    func(context.Context, jetstream.JetStream, string, bus.InboxOptions) ([]bus.Envelope, error)
	lastSeq func(context.Context, jetstream.JetStream) (uint64, error)
}

// productionSearch — боевые параметры поиска.
//
// Ширина берётся из bus.ScanCap, а не из своего числа. Два независимых предела
// уже расходились: окно 2000 при чтении, ограниченном 1000, не доставало до
// конца ящика — то есть дефект, который поиск и чинит. Один источник истины
// делает такое расхождение невозможным, а не маловероятным.
func productionSearch() inboxSearch {
	return inboxSearch{
		window:  bus.ScanCap,
		scan:    bus.Inbox,
		lastSeq: bus.StreamLastSeq,
	}
}

func (h *handlers) reply(ctx context.Context, _ *mcp.CallToolRequest, in ReplyIn) (*mcp.CallToolResult, ReplyOut, error) {
	original, ok := h.recall(in.MessageID)
	if !ok {
		// Сначала у КОНЦА ящика, потом с начала.
		//
		// Поиск с начала брал первые несколько сотен писем — самые старые.
		// Отвечают же почти всегда на свежее, а в выросшем ящике оно лежит за
		// этой границей, и отказ выглядел как «письма не существует», хотя
		// оно пришло минуту назад.
		//
		// Старый порядок сохранён вторым шагом: ответы на давние письма
		// работали и должны работать дальше.
		var searchErr error
		original, searchErr, ok = h.findInInbox(ctx, in.MessageID)
		if !ok {
			// Незавершённая проверка и отсутствие письма — разные ответы, и
			// разница обязана быть видна с ПЕРВЫХ слов.
			//
			// Прежний текст начинался с «письмо не найдено», а причину
			// добавлял следом. Читается это как приговор с оговоркой: агент
			// принимает первую половину за факт и попытку не повторяет.
			// Утверждать отсутствие вправе только полный успешный проход.
			if searchErr != nil {
				return nil, ReplyOut{}, fmt.Errorf(
					"не удалось завершить поиск письма %s: %w", in.MessageID, searchErr)
			}
			return nil, ReplyOut{}, fmt.Errorf(
				"письмо %s не найдено: ящик просмотрен целиком, от последнего "+
					"письма до первого", in.MessageID)
		}
	}

	answer := original.Reply(h.node.AgentID, in.Body)
	if err := bus.Publish(ctx, h.conn.JS(), answer); err != nil {
		return nil, ReplyOut{}, fmt.Errorf("отправка ответа: %w", err)
	}
	return nil, ReplyOut{ID: answer.ID, ThreadID: answer.ThreadID}, nil
}

// findInInbox ищет письмо в ящике: сперва у конца, затем с начала.
//
// Два окна, а не одно большое: чтение ящика идёт от заданной позиции вперёд,
// и «просмотреть весь ящик» одной выборкой недостижимо. Порядок окон отражает
// то, как отвечают на письма: свежее вероятнее давнего.
// findInInbox ищет письмо, проходя ящик от последнего письма к первому.
//
// Диапазоны идут подряд, шириной window и без перекрытий: [last-w+1, last],
// затем [last-2w+1, last-w] и так далее до первой позиции или до находки.
// Непрерывность здесь доказуема, а не вероятна — диапазон из w позиций потока
// содержит не больше w писем, поэтому окно, читающее w писем от позиции S,
// покрывает [S, S+w-1] целиком при любой плотности и при любом чередовании
// получателей: чужие письма только уменьшают число наших в диапазоне.
//
// Прежняя редакция удваивала отступ ради глубины и оставляла между окнами
// незакрытые полосы: из шестидесяти позиций плотного ящика находились
// двадцать. Порядок обхода — от конца — сохранён: отвечают чаще на свежее, и в
// типичном случае хватает первого запроса.
//
// Цена полного прохода — ceil(last/window) запросов; предела ей не ставится
// намеренно. Бюджет означал бы, что середина ящика недоступна молча, а это
// тот же дефект, только реже проявляющийся.
func (h *handlers) findInInbox(ctx context.Context, id string) (*mail.Message, error, bool) { //nolint:revive // ошибка сопровождает неудачу поиска, а не результат
	last, err := h.search.lastSeq(ctx, h.conn.JS())
	if err != nil {
		// Конец потока недоступен — остаётся единственный проход, с головы.
		// Он покрывает первые window писем; если письмо там, ответ верен, а
		// если нет, наружу уходит причина, а не «письма не существует».
		m, scanErr := h.scanInbox(ctx, id, bus.InboxOptions{Limit: int(h.search.window)})
		if scanErr != nil {
			return nil, fmt.Errorf(
				"ящик не просмотрен вовсе: конец потока недоступен (%w), "+
					"и чтение начала ящика не удалось: %w", err, scanErr), false
		}
		if m != nil {
			return m, nil, true
		}
		// Письма нет в начальной части — но об остальном ящике мы не знаем
		// ничего, и говорить о нём нельзя.
		return nil, fmt.Errorf(
			"просмотрена только начальная часть ящика (первые %d писем): "+
				"конец потока недоступен, диапазоны считать не от чего: %w",
			h.search.window, err), false
	}

	for back := h.search.window; ; back += h.search.window {
		var start uint64 = 1
		if last >= back {
			start = last - back + 1
		}

		m, err := h.scanInbox(ctx, id, bus.InboxOptions{
			Limit: int(h.search.window), StartSeq: start,
		})
		if err != nil {
			// Отказ окна наружу, с диапазоном.
			//
			// Продолжать нельзя: непросмотренный диапазон сделал бы ответ
			// «письма нет» ложью, неотличимой от правды. Диапазон в тексте —
			// чтобы было видно, какая часть ящика осталась непроверенной.
			return nil, fmt.Errorf(
				"просмотр ящика прерван на позициях %d..%d, дальше не проверено: %w",
				start, start+h.search.window-1, err), false
		}
		if m != nil {
			return m, nil, true
		}
		if start == 1 {
			break // дошли до начала ящика
		}
	}
	return nil, nil, false
}

// scanInbox читает одно окно ящика и попутно пополняет кэш недавних.
func (h *handlers) scanInbox(ctx context.Context, id string, opts bus.InboxOptions) (*mail.Message, error) {
	envs, err := h.search.scan(ctx, h.conn.JS(), h.node.AgentID, opts)
	if err != nil {
		return nil, err
	}
	var found *mail.Message
	for _, env := range envs {
		h.remember(env.Message)
		if env.Message.ID == id {
			found = env.Message
		}
	}
	return found, nil
}

// --- mark_read ---

type MarkReadIn struct {
	Seq uint64 `json:"seq" jsonschema:"позиция письма из fetch_inbox"`
}

type MarkReadOut struct {
	OK bool `json:"ok"`
}

func (h *handlers) markRead(ctx context.Context, _ *mcp.CallToolRequest, in MarkReadIn) (*mcp.CallToolResult, MarkReadOut, error) {
	if err := bus.MarkRead(ctx, h.conn.JS(), h.node.AgentID, in.Seq); err != nil {
		return nil, MarkReadOut{}, fmt.Errorf("отметка о прочтении: %w", err)
	}
	return nil, MarkReadOut{OK: true}, nil
}

// --- list_agents ---

type ListAgentsIn struct {
	Project string `json:"project,omitempty" jsonschema:"фильтр по проекту"`
	Tag     string `json:"tag,omitempty"`
}

type AgentView struct {
	AgentID  string   `json:"agent_id"`
	Host     string   `json:"host"`
	Engine   string   `json:"engine"`
	Projects []string `json:"projects,omitempty"`
	Tags     []string `json:"tags,omitempty"`
}

type ListAgentsOut struct {
	Agents []AgentView `json:"agents"`
	// Note объясняет, чем этот список является, а чем нет.
	Note string `json:"note"`
}

func (h *handlers) listAgents(_ context.Context, _ *mcp.CallToolRequest, in ListAgentsIn) (*mcp.CallToolResult, ListAgentsOut, error) {
	// Список агентов — это отчёт о НАБЛЮДЕНИИ, а не о сети.
	//
	// Полноту доказать нечем: визитки идут Core NATS, без хранения и без
	// переигрывания, поэтому потерянная не восстанавливается ничем, а новый
	// процесс не может узнать о соседе раньше, чем тот объявится снова.
	// Отсюда три разных ответа вместо одного пустого списка, который раньше
	// выдавался за «в сети никого нет».
	nc := h.conn.NC()
	if !nc.IsConnected() {
		return nil, ListAgentsOut{}, fmt.Errorf(
			"связь с хабом потеряна: визитки сейчас не наблюдаются, " +
				"и о составе сети сказать нечего")
	}

	// Переподключение обнуляет наблюдение.
	//
	// Подписка разрыв переживает, но визитки, пришедшиеся на окно разрыва,
	// теряются безвозвратно — проверено: объявление во время разрыва не
	// доходит, отправитель получает таймаут. Значит после переподключения
	// реестр снова неполон, сколько бы времени процесс ни работал до этого.
	// Счётчик берётся через Stats(), а не полем nc.Reconnects.
	//
	// Поле публичное и выглядит безобидно, но библиотека пишет его из своей
	// горутины переподключения — то есть прямое чтение это гонка. Детектор
	// поймал её сразу, как только тест начал ронять связь по-настоящему.
	observed, cards := h.noteObservation(nc.Stats().Reconnects), h.reg.Find(in.Project, in.Tag)

	out := ListAgentsOut{Agents: make([]AgentView, 0, len(cards))}
	for _, card := range cards {
		out.Agents = append(out.Agents, AgentView{
			AgentID: card.AgentID, Host: card.Host, Engine: card.Engine,
			Projects: card.Projects, Tags: card.Tags,
		})
	}

	warm := observed >= h.presence.warmup
	switch {
	case !warm && len(out.Agents) == 0:
		// Пустота на холодную ничего не означает, и выдавать её за ответ
		// нельзя: именно это и был дефект.
		return nil, ListAgentsOut{}, fmt.Errorf(
			"наблюдение за визитками идёт %s из %s: визиток пока не поступало, "+
				"и это не значит, что сеть пуста — узлы объявляются раз в %s",
			observed.Round(time.Second), h.presence.warmup, h.presence.warmup)
	case !warm:
		// Услышанное отдаём: незавершённость — повод не утверждать пустоту,
		// а не повод молчать о том, кто уже объявился.
		out.Note = fmt.Sprintf(
			"наблюдение идёт %s из %s: список неполон, могли объявиться не все",
			observed.Round(time.Second), h.presence.warmup)
	case len(out.Agents) == 0:
		out.Note = fmt.Sprintf(
			"за %s наблюдения визиток не наблюдал: узлы могли не объявиться "+
				"или их визитки потерялись — Core NATS не переигрывает",
			observed.Round(time.Second))
	default:
		out.Note = fmt.Sprintf(
			"визитки, наблюдавшиеся за %s; полноты сети не гарантирует",
			observed.Round(time.Second))
	}
	return nil, out, nil
}

// noteObservation возвращает длительность непрерывного наблюдения, сбрасывая
// его отсчёт после каждого переподключения.
func (h *handlers) noteObservation(reconnects uint64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	if h.presence.since.IsZero() || reconnects != h.presence.reconnects {
		h.presence.reconnects = reconnects
		h.presence.since = now
	}
	return now.Sub(h.presence.since)
}

// --- Реестр занятых зон -----------------------------------------------------

// claimsStore создаётся лениво, при первом обращении.
//
// Не в New: бакет может быть ещё не разрешён на хабе, а почта обязана
// работать независимо от этого. Ошибка не запоминается — права могли
// появиться между вызовами.
func (h *handlers) claimsStore(ctx context.Context) (*claims.Store, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.claims != nil {
		return h.claims, nil
	}
	store, err := claims.NewStore(ctx, h.conn.JS())
	if err != nil {
		return nil, fmt.Errorf("реестр зон недоступен: %w", err)
	}
	h.claims = store
	return store, nil
}

// ClaimView — захват в том виде, в каком его видит агент.
type ClaimView struct {
	Zone    string `json:"zone" jsonschema:"путь, занятый агентом"`
	AgentID string `json:"agent_id" jsonschema:"кто занял"`
	Note    string `json:"note" jsonschema:"зачем занял"`
	Since   string `json:"since" jsonschema:"с какого времени, UTC"`
}

func viewClaims(list []claims.Claim) []ClaimView {
	out := make([]ClaimView, 0, len(list))
	for _, c := range list {
		out = append(out, ClaimView{
			Zone: c.Zone, AgentID: c.AgentID, Note: c.Note,
			Since: c.Since.Format("2006-01-02 15:04 UTC"),
		})
	}
	return out
}

type ClaimZoneIn struct {
	Zones []string `json:"zones" jsonschema:"пути, которые берёшь: internal/bus, README.md"`
	Note  string   `json:"note,omitempty" jsonschema:"что именно собираешься там делать"`
}

type ClaimZoneOut struct {
	Taken []ClaimView `json:"taken" jsonschema:"что удалось занять"`
	Held  *ClaimView  `json:"held,omitempty" jsonschema:"кто держит зону, если занять не вышло"`
	Note  string      `json:"note" jsonschema:"что делать дальше"`
}

func (h *handlers) claimZone(ctx context.Context, _ *mcp.CallToolRequest, in ClaimZoneIn) (
	*mcp.CallToolResult, ClaimZoneOut, error,
) {
	store, err := h.claimsStore(ctx)
	if err != nil {
		return nil, ClaimZoneOut{}, err
	}

	taken, err := store.TakeAll(ctx, in.Zones, h.node.AgentID, in.Note)
	if err != nil {
		var conflict *claims.ConflictError
		if errors.As(err, &conflict) {
			view := viewClaims([]claims.Claim{conflict.Held})[0]
			// Отказ обязан говорить, к кому идти: «занято» без имени
			// бесполезно и толкает обойти реестр.
			return nil, ClaimZoneOut{
				Held: &view,
				Note: fmt.Sprintf("зона %s занята агентом %s — не берись за неё, "+
					"напиши ему письмо и договорись", conflict.Requested, conflict.Held.AgentID),
			}, nil
		}
		return nil, ClaimZoneOut{}, err
	}

	return nil, ClaimZoneOut{
		Taken: viewClaims(taken),
		Note: "зоны твои. Освободи их через release_zone, когда закончишь; " +
			"без этого они снимутся сами через 8 часов",
	}, nil
}

type ReleaseZoneIn struct {
	Zones []string `json:"zones" jsonschema:"пути, которые освобождаешь"`
}

type ReleaseZoneOut struct {
	Released []string `json:"released" jsonschema:"что освобождено"`
}

func (h *handlers) releaseZone(ctx context.Context, _ *mcp.CallToolRequest, in ReleaseZoneIn) (
	*mcp.CallToolResult, ReleaseZoneOut, error,
) {
	store, err := h.claimsStore(ctx)
	if err != nil {
		return nil, ReleaseZoneOut{}, err
	}

	released := make([]string, 0, len(in.Zones))
	for _, zone := range in.Zones {
		if err := store.Release(ctx, zone, h.node.AgentID); err != nil {
			return nil, ReleaseZoneOut{}, err
		}
		released = append(released, zone)
	}
	return nil, ReleaseZoneOut{Released: released}, nil
}

type ListClaimsIn struct {
	Zone string `json:"zone,omitempty" jsonschema:"спросить про конкретный путь; пусто — показать все"`
}

type ListClaimsOut struct {
	Claims []ClaimView `json:"claims" jsonschema:"занятые зоны"`
	Free   bool        `json:"free,omitempty" jsonschema:"свободен ли спрошенный путь"`
}

func (h *handlers) listClaims(ctx context.Context, _ *mcp.CallToolRequest, in ListClaimsIn) (
	*mcp.CallToolResult, ListClaimsOut, error,
) {
	store, err := h.claimsStore(ctx)
	if err != nil {
		return nil, ListClaimsOut{}, err
	}

	if in.Zone != "" {
		holder, busy, err := store.Holder(ctx, in.Zone)
		if err != nil {
			return nil, ListClaimsOut{}, err
		}
		if !busy {
			return nil, ListClaimsOut{Free: true}, nil
		}
		return nil, ListClaimsOut{Claims: viewClaims([]claims.Claim{holder})}, nil
	}

	all, err := store.List(ctx)
	if err != nil {
		return nil, ListClaimsOut{}, err
	}
	return nil, ListClaimsOut{Claims: viewClaims(all)}, nil
}

// presenceWatch — сколько времени идёт непрерывное наблюдение за визитками.
//
// Полноты списка агентов доказать нельзя: визитки идут Core NATS, без
// хранения и без переигрывания, поэтому потерянное не восстанавливается ничем.
// Единственное, что инструмент знает достоверно, — кого он слышал сам и как
// долго слушает. Это состояние и хранится здесь.
type presenceWatch struct {
	since      time.Time
	reconnects uint64
	warmup     time.Duration
}
