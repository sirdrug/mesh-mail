package tg_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/boreevyuri/mesh-mail/internal/tg"
)

// FetchFile проходит два шага: getFile за путём, затем скачивание по нему.
// Токен есть только здесь, у моста; отсюда и качаем.
func TestFetchFileКачаетДваШага(t *testing.T) {
	const body = "содержимое присланного файла"
	var gotGetFile, gotDownload bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/getFile"):
			gotGetFile = true
			_, _ = io.WriteString(w, `{"ok":true,"result":{"file_path":"documents/example.zip"}}`)
		case strings.Contains(r.URL.Path, "/file/bot"):
			gotDownload = true
			// Путь каталога должен сохраниться, а не превратиться в %2F.
			if !strings.HasSuffix(r.URL.Path, "/documents/example.zip") {
				t.Errorf("путь файла искажён: %s", r.URL.Path)
			}
			_, _ = io.WriteString(w, body)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := tg.New("тест-токен", tg.WithBaseURL(srv.URL), tg.WithHTTPClient(srv.Client()))
	data, err := c.FetchFile(context.Background(), "BQACfileID")
	if err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if string(data) != body {
		t.Fatalf("содержимое = %q, ожидалось %q", data, body)
	}
	if !gotGetFile || !gotDownload {
		t.Fatalf("не оба шага сделаны: getFile=%v download=%v", gotGetFile, gotDownload)
	}
}

// Отказ getFile (истёкший file_id) доходит ошибкой, а не пустыми байтами.
func TestFetchFileОшибкаGetFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"ok":false,"description":"file not found"}`)
	}))
	defer srv.Close()

	c := tg.New("тест-токен", tg.WithBaseURL(srv.URL), tg.WithHTTPClient(srv.Client()))
	if _, err := c.FetchFile(context.Background(), "старый"); err == nil {
		t.Fatal("ожидалась ошибка на отказ getFile")
	}
}
