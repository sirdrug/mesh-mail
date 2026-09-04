package mcpsrv

import (
	"time"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/config"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// New собирает MCP-сервер с инструментами почты.
//
// Описания инструментов написаны для модели: она выбирает инструмент по ним,
// и «fetch_inbox — читает почту» работает хуже, чем объяснение, когда именно
// его звать.
func New(conn *bus.Conn, reg *bus.Registry, node *config.Node) *mcp.Server {
	h := &handlers{
		conn: conn, reg: reg, node: node, search: productionSearch(),
		// Окно прогрева равно интервалу визиток: раньше него отсутствие
		// визитки не значит ничего.
		presence: presenceWatch{warmup: bus.PresenceInterval, since: time.Now()},
	}

	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "mesh-mail-mail",
		Title:   "Почта агентской сети",
		Version: "0.1.0",
	}, nil)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "fetch_inbox",
		Description: "Забрать письма из своего ящика. Зови, когда пришло уведомление " +
			"о новом письме, а также в начале работы и после долгой задачи. " +
			"unread_only=true отдаёт только то, что ещё не отмечено прочитанным. " +
			"Письма идут от старых к новым, поэтому одна выдача — это НАЧАЛО " +
			"очереди, а не её конец. Если в ответе has_more=true, за выдачей " +
			"остались непрочитанные: отметь прочитанное и позови снова, пока " +
			"has_more не станет false. Иначе будешь отвечать по устаревшему.",
	}, h.fetchInbox)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "send_message",
		Description: "Написать письмо другому агенту сети. Отправитель подставляется " +
			"автоматически. Получателей ищи через list_agents, если не знаешь адрес.",
	}, h.sendMessage)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "reply_message",
		Description: "Ответить на письмо из своего ящика, сохранив тред. " +
			"Предпочитай ответ новому письму: так переписка остаётся связной.",
	}, h.reply)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "mark_read",
		Description: "Отметить письмо прочитанным по его seq. Зови после того, как " +
			"обработал письмо, иначе оно будет приходить в непрочитанных снова.",
	}, h.markRead)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "list_agents",
		Description: "Кто есть в сети и чем занимается. Фильтр по проекту отвечает " +
			"на вопрос «кого спросить про этот проект».",
	}, h.listAgents)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "claim_zone",
		Description: "Застолбить за собой файлы и каталоги ПЕРЕД тем, как их править. " +
			"Зови до начала работы, а не после: смысл в том, чтобы остальные узнали " +
			"заранее. Если зона занята, инструмент назовёт держателя — напиши ему " +
			"и договорись, а не правь параллельно.",
	}, h.claimZone)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "release_zone",
		Description: "Освободить свои зоны, когда работа закончена и запушена. " +
			"Чужие снять нельзя. Забытые снимаются сами через восемь часов.",
	}, h.releaseZone)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "list_claims",
		Description: "Кто какие места кода сейчас занял. Без аргументов — все захваты, " +
			"с zone — свободен ли конкретный путь. Спрашивай перед тем, как браться " +
			"за незнакомую часть репозитория.",
	}, h.listClaims)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "fetch_attachment",
		Description: "Забрать файл, приложенный к письму. Если в теле письма есть блок " +
			"«📎 ВЛОЖЕНИЕ» с ключом объекта, передай этот ключ в object — инструмент " +
			"скачает файл и сохранит его на диск, вернув путь. Токен не нужен: " +
			"файл берётся из сети твоим ключом. dest — куда сохранить (каталог или " +
			"путь файла), по умолчанию имя файла в текущем каталоге.",
	}, h.fetchAttachment)

	return srv
}
