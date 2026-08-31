package bridge

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/bus"
	"github.com/boreevyuri/mesh-mail/internal/bustest"
)

func newStore(t *testing.T) (*TopicStore, *bus.Conn) {
	t.Helper()
	ctx := context.Background()

	conn, err := bus.Connect(ctx, bus.Options{URLs: []string{bustest.StartTestServer(t)}, Name: "bridge-test"})
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	t.Cleanup(conn.Close)
	if err := bus.EnsureTopology(ctx, conn.JS()); err != nil {
		t.Fatalf("топология: %v", err)
	}

	store, err := NewTopicStore(ctx, conn.JS())
	if err != nil {
		t.Fatalf("хранилище тем: %v", err)
	}
	return store, conn
}

func TestTopicStoreСохраняетИОтдаёт(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	want := Topic{MessageThreadID: 42, Participants: []string{"pi-claude", "m1-codex"}}
	if err := store.Put(ctx, "thread-1", want); err != nil {
		t.Fatalf("запись: %v", err)
	}

	got, ok, err := store.Get(ctx, "thread-1")
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if !ok {
		t.Fatal("тема не найдена сразу после записи")
	}
	if got.MessageThreadID != 42 {
		t.Errorf("message_thread_id = %d", got.MessageThreadID)
	}
	if len(got.Participants) != 2 {
		t.Errorf("участников %d, ожидалось 2", len(got.Participants))
	}
}

func TestTopicStoreМолчитПроНеизвестныйТред(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	_, ok, err := store.Get(ctx, "никогда-не-было")
	if err != nil {
		t.Fatalf("неизвестный тред вернул ошибку вместо признака отсутствия: %v", err)
	}
	if ok {
		t.Fatal("нашлась тема, которой нет")
	}
}

func TestTopicStoreПереживаетПерезапускМоста(t *testing.T) {
	ctx := context.Background()
	store, conn := newStore(t)

	if err := store.Put(ctx, "thread-1", Topic{MessageThreadID: 7}); err != nil {
		t.Fatalf("запись: %v", err)
	}

	// Новый экземпляр хранилища — как после рестарта процесса.
	restarted, err := NewTopicStore(ctx, conn.JS())
	if err != nil {
		t.Fatalf("повторное открытие: %v", err)
	}

	got, ok, err := restarted.Get(ctx, "thread-1")
	if err != nil || !ok {
		t.Fatalf("после рестарта тема потеряна (ok=%v, err=%v)", ok, err)
	}
	// Иначе для продолжающегося разговора создалась бы вторая тема-дубль.
	if got.MessageThreadID != 7 {
		t.Fatalf("message_thread_id = %d, ожидался 7", got.MessageThreadID)
	}
}

// Новая тема проекта приходит с именем и признаком.
//
// Положительная половина пары: без неё «имя неизвестно» было бы зелёным при
// реализации, которая не записывает ничего и никогда.
func TestТемаПроектаЗаписываетсяСИменем(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "mesh-mail", 77); err != nil {
		t.Fatalf("запись темы проекта: %v", err)
	}

	got, found, err := store.ProjectByTopic(ctx, 77)
	if err != nil {
		t.Fatalf("поиск по номеру темы: %v", err)
	}
	if !found {
		t.Fatal("тема проекта не найдена по своему же номеру")
	}
	if !got.Known || got.Name != "mesh-mail" {
		t.Fatalf("имя = %q (известно: %v), ожидалось «mesh-mail» и известно", got.Name, got.Known)
	}
}

// Тема «Общего» — известное ПУСТОЕ имя, а не неизвестное.
//
// Вторая половина пары к следующему тесту. Порознь каждый зелен при
// реализации, которая всегда отвечает «известно» или всегда «неизвестно»;
// различает их только пара.
func TestТемаОбщегоИмеетИзвестноеПустоеИмя(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "", 5); err != nil {
		t.Fatalf("запись темы «Общего»: %v", err)
	}

	got, found, err := store.ProjectByTopic(ctx, 5)
	if err != nil {
		t.Fatalf("поиск: %v", err)
	}
	if !found {
		t.Fatal("тема «Общего» не найдена")
	}
	if !got.Known {
		t.Fatal("пустое имя «Общего» прочитано как неизвестное — признак не работает")
	}
	if got.Name != "" {
		t.Fatalf("имя = %q, ожидалось пустое", got.Name)
	}
}

// Запись, сделанная прежним кодом, читается как «имя неизвестно».
func TestСтараяЗаписьТемыПроектаЧитаетсяКакНеизвестная(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	// Ровно то, что писал прежний PutProjectTopic: вид, версия, номер темы.
	legacy := `{"v":1,"kind":"project_topic","message_thread_id":88,"participants":null}`
	if _, err := store.kv.Put(ctx, projectKey("mesh-mail"), []byte(legacy)); err != nil {
		t.Fatalf("подготовка старой записи: %v", err)
	}

	got, found, err := store.ProjectByTopic(ctx, 88)
	if err != nil {
		t.Fatalf("поиск: %v", err)
	}
	if !found {
		t.Fatal("старая запись темы проекта не найдена — она остаётся темой проекта")
	}
	if got.Known {
		t.Fatalf("имя объявлено известным (%q), хотя в записи его нет", got.Name)
	}
}

// Запись разговора не выдаётся за тему проекта.
//
// Ловит реализацию, которая ищет по номеру темы, не глядя на вид записи:
// legacy-разговор с пустым Kind тогда ответил бы «проект найден, имя пустое»,
// и письмо ушло бы в «Общее» молча, вместо того чтобы остаться в разговоре.
func TestЗаписьРазговораНеПринимаетсяЗаТемуПроекта(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	conversation := Topic{MessageThreadID: 99, Participants: []string{"pi-claude"}}
	if err := store.Put(ctx, "thread-legacy", conversation); err != nil {
		t.Fatalf("подготовка разговора: %v", err)
	}

	_, found, err := store.ProjectByTopic(ctx, 99)
	if err != nil {
		t.Fatalf("поиск: %v", err)
	}
	if found {
		t.Fatal("запись разговора принята за тему проекта")
	}
}

// Две темы проектов с одним номером — ошибка, а не первое попавшееся имя.
func TestДвеТемыСОднимНомеромДаютОшибку(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "первый", 42); err != nil {
		t.Fatalf("запись первой: %v", err)
	}
	if err := store.PutProjectTopic(ctx, "второй", 42); err != nil {
		t.Fatalf("запись второй: %v", err)
	}

	if _, _, err := store.ProjectByTopic(ctx, 42); err == nil {
		t.Fatal("противоречие в данных прошло молча: выбрано одно из двух имён наугад")
	}
}

// Дозаполнение дописывает имя в существующую запись.
func TestДозаполнениеПишетИмяВСтаруюЗапись(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	legacy := `{"v":1,"kind":"project_topic","message_thread_id":11,"participants":null}`
	if _, err := store.kv.Put(ctx, projectKey("mesh-mail"), []byte(legacy)); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	filled, err := store.FillProjectName(ctx, "mesh-mail")
	if err != nil {
		t.Fatalf("дозаполнение: %v", err)
	}
	if !filled {
		t.Fatal("имя не дописано, хотя запись была без него")
	}

	got, found, err := store.ProjectByTopic(ctx, 11)
	if err != nil || !found {
		t.Fatalf("поиск после дозаполнения: found=%v err=%v", found, err)
	}
	if !got.Known || got.Name != "mesh-mail" {
		t.Fatalf("после дозаполнения имя = %q (известно: %v)", got.Name, got.Known)
	}
}

// Дозаполнение НЕ создаёт отсутствующую запись.
//
// Имя проекта приходит мосту из визиток живых агентов. Создавать по нему
// тему значит заводить пустые ветки под каждый проект из чужого конфига —
// а тему в Telegram, в отличие от записи, не откатить.
func TestДозаполнениеНеСоздаётЗапись(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	filled, err := store.FillProjectName(ctx, "нет-такой-темы")
	if err != nil {
		t.Fatalf("дозаполнение отсутствующей: %v", err)
	}
	if filled {
		t.Fatal("дозаполнение отчиталось о записи, которой не было")
	}

	if _, ok, err := store.Get(ctx, projectKey("нет-такой-темы")); err != nil || ok {
		t.Fatalf("запись создана на пустом месте: ok=%v err=%v", ok, err)
	}
}

// Повтор дозаполнения ничего не пишет.
//
// Проверяется по РЕВИЗИИ, а не по отсутствию ошибки: вызывать это будут на
// каждой визитке, то есть примерно раз в минуту на агента, и «писать всегда»
// выглядело бы совершенно исправно — в хранилище то же самое значение.
func TestПовторноеДозаполнениеНеПишет(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "mesh-mail", 21); err != nil {
		t.Fatalf("запись темы: %v", err)
	}
	before, err := store.kv.Get(ctx, projectKey("mesh-mail"))
	if err != nil {
		t.Fatalf("чтение ревизии: %v", err)
	}

	filled, err := store.FillProjectName(ctx, "mesh-mail")
	if err != nil {
		t.Fatalf("повторное дозаполнение: %v", err)
	}
	if filled {
		t.Fatal("заполненная запись переписана заново")
	}

	after, err := store.kv.Get(ctx, projectKey("mesh-mail"))
	if err != nil {
		t.Fatalf("чтение ревизии после: %v", err)
	}
	if after.Revision() != before.Revision() {
		t.Fatalf("ревизия изменилась с %d на %d — запись всё-таки была",
			before.Revision(), after.Revision())
	}
}

// Чужое имя под ключом проекта — повреждение, а не повод переписать.
func TestЧужоеИмяПодКлючомПроектаДаётОшибку(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	// Ключ выведен из «mesh-mail», а имя внутри записи другое.
	broken := `{"v":1,"kind":"project_topic","message_thread_id":31,"project":"чужой","project_known":true}`
	if _, err := store.kv.Put(ctx, projectKey("mesh-mail"), []byte(broken)); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	if _, err := store.FillProjectName(ctx, "mesh-mail"); err == nil {
		t.Fatal("чужое имя молча переписано или пропущено")
	}
}

// Запись чужой версии — ошибка чтения, а не «имени нет».
//
// Различие принципиальное: «не знаю» уводит письмо в «Общее» молча, ошибка
// возвращается наверх и попадает в повтор.
func TestТемаПроектаЧужойВерсииДаётОшибку(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	future := `{"v":99,"kind":"project_topic","message_thread_id":12,"project":"x","project_known":true}`
	if _, err := store.kv.Put(ctx, projectKey("x"), []byte(future)); err != nil {
		t.Fatalf("подготовка: %v", err)
	}

	if _, _, err := store.ProjectByTopic(ctx, 12); err == nil {
		t.Fatal("запись неизвестной версии прочитана как обычная")
	}
	if _, err := store.FillProjectName(ctx, "x"); err == nil {
		t.Fatal("в запись неизвестной версии дописали имя")
	}
}

// Новая запись разбирается СТАРОЙ структурой без ошибки.
//
// Отрицательный тест на откат. Разбирать её нынешним `Get` бессмысленно: он
// знает новые поля, и такой тест доказал бы лишь то, что код понимает
// собственный формат. Старую сборку моделирует локальный тип без Project и
// ProjectKnown — если `encoding/json` перестанет игнорировать незнакомые
// поля, откат на прежний бинарник сломается, и увидеть это надо здесь.
func TestНоваяЗаписьРазбираетсяСтаройСтруктурой(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if err := store.PutProjectTopic(ctx, "mesh-mail", 64); err != nil {
		t.Fatalf("запись: %v", err)
	}

	entry, err := store.kv.Get(ctx, projectKey("mesh-mail"))
	if err != nil {
		t.Fatalf("чтение сырой записи: %v", err)
	}

	// Ровно то, что знала о теме сборка до этой правки.
	var legacy struct {
		Version         int      `json:"v,omitempty"`
		Kind            string   `json:"kind,omitempty"`
		MessageThreadID int      `json:"message_thread_id"`
		Participants    []string `json:"participants"`
	}
	if err := json.Unmarshal(entry.Value(), &legacy); err != nil {
		t.Fatalf("прежняя структура споткнулась о новые поля: %v", err)
	}
	if legacy.Version != topicVersion {
		t.Fatalf("версия %d, ожидалась %d — bump сломал бы прежнее чтение",
			legacy.Version, topicVersion)
	}
	if legacy.Kind != KindProjectTopic {
		t.Fatalf("вид %q, ожидался %q", legacy.Kind, KindProjectTopic)
	}
	if legacy.MessageThreadID != 64 {
		t.Fatalf("номер темы %d, ожидался 64", legacy.MessageThreadID)
	}
}

// Битая запись разговора не мешает найти тему проекта.
//
// До введения приставки обратный поиск читал все ключи подряд, и одна
// нечитаемая запись разговора возвращала ошибку — то есть ломала адресную
// команду сразу во всех проектах, не имея к ним отношения.
func TestБитаяЗаписьРазговораНеЛомаетПоискПроекта(t *testing.T) {
	ctx := context.Background()
	store, _ := newStore(t)

	if _, err := store.kv.Put(ctx, "thread-broken", []byte("не json вовсе")); err != nil {
		t.Fatalf("подготовка битой записи: %v", err)
	}
	if err := store.PutProjectTopic(ctx, "mesh-mail", 70); err != nil {
		t.Fatalf("запись темы проекта: %v", err)
	}

	got, found, err := store.ProjectByTopic(ctx, 70)
	if err != nil {
		t.Fatalf("битая запись разговора сломала поиск проекта: %v", err)
	}
	if !found || got.Name != "mesh-mail" {
		t.Fatalf("проект не найден: found=%v имя=%q", found, got.Name)
	}
}
