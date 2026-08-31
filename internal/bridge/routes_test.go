package bridge

import (
	"context"
	"testing"
)

// Маршрут поста находится по чату и номеру сообщения.
//
// Это сердце единой темы проекта: в ней рядом лежат посты разных разговоров,
// и понять, к какому относится ответ человека, можно только по номеру
// сообщения, на которое он ответил.
func TestМаршрутПостаНаходитсяПоНомеруСообщения(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	want := Route{
		ThreadID:     "тред-1",
		Project:      "mesh-mail",
		Participants: []string{"pi-claude", "mbp-claude"},
	}
	if err := store.PutRoute(ctx, "-1001", 500, want); err != nil {
		t.Fatalf("запись маршрута: %v", err)
	}

	got, ok, err := store.Route(ctx, "-1001", 500)
	if err != nil {
		t.Fatalf("чтение маршрута: %v", err)
	}
	if !ok {
		t.Fatal("маршрут не найден — ответ на этот пост уйдёт в никуда")
	}
	if got.ThreadID != want.ThreadID || got.Project != want.Project {
		t.Fatalf("маршрут %+v, ожидался %+v", got, want)
	}
	if len(got.Participants) != 2 {
		t.Fatalf("участников %d, ожидалось 2: ответ уйдёт не тем", len(got.Participants))
	}
}

// Маршруты разных постов не путаются между собой.
//
// Контроль к предыдущему: хранилище, возвращающее один и тот же маршрут на
// любой номер, прошло бы проверку выше — и отправило бы ответ участникам
// чужого разговора. Это главный страх всей затеи с общей темой.
func TestМаршрутыРазныхПостовНеПутаются(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutRoute(ctx, "-1001", 100, Route{ThreadID: "первый", Participants: []string{"pi-claude"}}); err != nil {
		t.Fatalf("запись первого: %v", err)
	}
	if err := store.PutRoute(ctx, "-1001", 200, Route{ThreadID: "второй", Participants: []string{"mbp-claude"}}); err != nil {
		t.Fatalf("запись второго: %v", err)
	}

	first, _, err := store.Route(ctx, "-1001", 100)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	second, _, err := store.Route(ctx, "-1001", 200)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}

	if first.ThreadID != "первый" || second.ThreadID != "второй" {
		t.Fatalf("маршруты перепутаны: %q и %q", first.ThreadID, second.ThreadID)
	}
	if first.Participants[0] == second.Participants[0] {
		t.Fatal("участники одинаковы — ответ уйдёт не в тот разговор")
	}
}

// Один и тот же номер в разных чатах — разные маршруты.
//
// Номера сообщений уникальны в пределах чата, а не глобально. Ключ, собранный
// без чата, склеил бы разговоры двух супергрупп.
func TestОдинНомерВРазныхЧатахНеСклеивается(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutRoute(ctx, "-1001", 42, Route{ThreadID: "из первого чата"}); err != nil {
		t.Fatalf("запись: %v", err)
	}
	if err := store.PutRoute(ctx, "-2002", 42, Route{ThreadID: "из второго чата"}); err != nil {
		t.Fatalf("запись: %v", err)
	}

	got, _, err := store.Route(ctx, "-1001", 42)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if got.ThreadID != "из первого чата" {
		t.Fatalf("маршрут %q — чаты склеились", got.ThreadID)
	}
}

// Отсутствующий маршрут — не ошибка.
//
// Так выглядит ответ на пост, показанный до перехода, или на пост, чей
// маршрут истёк. Вызывающий должен получить «нет», а не отказ: ему предстоит
// объяснить это человеку, а не падать.
func TestОтсутствующийМаршрутНеОшибка(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	_, ok, err := store.Route(ctx, "-1001", 999)
	if err != nil {
		t.Fatalf("чтение отсутствующего: %v", err)
	}
	if ok {
		t.Fatal("найден маршрут, которого нет")
	}
}

// Тема проекта находится по его имени.
func TestТемаПроектаНаходитсяПоИмени(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "mesh-mail", 77); err != nil {
		t.Fatalf("запись темы проекта: %v", err)
	}

	got, ok, err := store.ProjectTopic(ctx, "mesh-mail")
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if !ok || got != 77 {
		t.Fatalf("тема проекта %d (найдена: %v), ожидалась 77", got, ok)
	}
}

// Письма без проекта идут в свою тему, а не в тему первого попавшегося.
//
// Пустое имя — полноправное имя: `mail.New` поле проекта не заполняет вовсе,
// поэтому таких писем будет много, и им нужна своя тема, а не отказ.
func TestПустойПроектИмеетСвоюТему(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "", 10); err != nil {
		t.Fatalf("запись: %v", err)
	}
	if err := store.PutProjectTopic(ctx, "mesh-mail", 20); err != nil {
		t.Fatalf("запись: %v", err)
	}

	empty, _, err := store.ProjectTopic(ctx, "")
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if empty != 10 {
		t.Fatalf("тема пустого проекта %d, ожидалась 10", empty)
	}
}

// Тема проекта не выдаётся за тему разговора.
//
// Обе записи лежат в одном бакете, и поиск по номеру темы перебирает его
// целиком. Приняв запись проекта за запись разговора, мост адресовал бы ответ
// человека списку участников, которого там нет, — то есть никому.
func TestТемаПроектаНеПринимаетсяЗаТемуРазговора(t *testing.T) {
	ctx := context.Background()
	store, conn := newStore(t)
	_ = conn

	if err := store.PutProjectTopic(ctx, "mesh-mail", 55); err != nil {
		t.Fatalf("запись темы проекта: %v", err)
	}

	intake := &Intake{store: store}
	_, _, found, err := intake.findByTopic(ctx, 55)
	if err != nil {
		t.Fatalf("поиск: %v", err)
	}
	if found {
		t.Fatal("тема проекта найдена как тема разговора — ответ уйдёт в пустоту")
	}
}

// Старые записи без вида читаются как темы разговоров.
//
// Они лежат в бакете с самого начала и вида не имеют. Считать их чем-то иным
// значит сломать все существующие разговоры разом.
func TestСтараяЗаписьБезВидаОстаётсяТемойРазговора(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	// Ровно то, что лежит в боевом бакете сегодня: ни версии, ни вида.
	if err := store.Put(ctx, "thread-old-1", Topic{
		MessageThreadID: 33,
		Participants:    []string{"pi-claude", "mbp-claude"},
	}); err != nil {
		t.Fatalf("запись: %v", err)
	}

	intake := &Intake{store: store}
	threadID, topic, found, err := intake.findByTopic(ctx, 33)
	if err != nil {
		t.Fatalf("поиск: %v", err)
	}
	if !found {
		t.Fatal("старая тема не найдена — существующие разговоры оборвутся")
	}
	if threadID != "thread-old-1" || len(topic.Participants) != 2 {
		t.Fatalf("найдено %q с %d участниками", threadID, len(topic.Participants))
	}
}

// Запись разговора под ключом проекта темой проекта не считается.
//
// Ключи разные, но бакет один, и запись туда может попасть по ошибке кода —
// например, если кто-то передаст имя проекта в Put вместо PutProjectTopic.
// Принять её за тему проекта значит начать складывать письма проекта в тему
// чужого разговора, и человек увидит их вперемешку.
func TestЗаписьРазговораПодКлючомПроектаНеПринимается(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	// Кладём запись БЕЗ вида — ровно такую, какие лежат в боевом бакете.
	if err := store.Put(ctx, projectKey("mesh-mail"), Topic{
		MessageThreadID: 42,
		Participants:    []string{"pi-claude"},
	}); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	_, ok, err := store.ProjectTopic(ctx, "mesh-mail")
	if err == nil && ok {
		t.Fatal("запись разговора принята за тему проекта")
	}
}

// Маршрут неизвестной версии не используется молча.
//
// Версия в записи есть с самого начала, и это не украшение: формат может
// смениться, а мост на старой сборке продолжит читать бакет. Принять чужой
// формат за свой значит отправить ответ по данным, которых не понимаешь.
func TestМаршрутНеизвестнойВерсииНеИспользуется(t *testing.T) {
	ctx := context.Background()
	store, conn := newStore(t)
	_ = conn

	// Пишем маршрут «из будущего» — так, как это сделала бы более новая сборка.
	future := `{"v":99,"mesh_thread_id":"тред","participants":["pi-claude"]}`
	if _, err := store.routes.Put(ctx, routeKey("-1001", 700), []byte(future)); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	_, ok, err := store.Route(ctx, "-1001", 700)
	if err == nil && ok {
		t.Fatal("маршрут неизвестной версии принят за свой")
	}
}

// Запись без версии не принимается ни за какую.
//
// Version=0 — это не «первая версия», а «версии нет». Для маршрутов и тем
// проектов записей без версии не существует: оба вида появились уже с ней.
// Принять ноль за единицу значит согласиться читать неизвестно что.
func TestЗаписьБезВерсииОтвергается(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if _, err := store.routes.Put(ctx, routeKey("-1001", 800),
		[]byte(`{"mesh_thread_id":"тред","participants":["pi-claude"]}`)); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	if _, ok, err := store.Route(ctx, "-1001", 800); err == nil && ok {
		t.Fatal("маршрут без версии принят за v1")
	}

	if _, err := store.kv.Put(ctx, projectKey("проект"),
		[]byte(`{"kind":"project_topic","message_thread_id":5}`)); err != nil {
		t.Fatalf("подготовка: %v", err)
	}
	if _, ok, err := store.ProjectTopic(ctx, "проект"); err == nil && ok {
		t.Fatal("тема проекта без версии принята за v1")
	}
}
