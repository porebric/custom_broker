package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func newTestServer(t *testing.T) string {
	t.Helper()
	b := newBroker()
	mux := http.NewServeMux()
	mux.HandleFunc("/", b.handle)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func put(t *testing.T, base, path string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, base+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func get(t *testing.T, base, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// Точный пример из ТЗ:
// PUT /pet?v=cat, PUT /pet?v=dog, PUT /role?v=manager, PUT /role?v=executive
// GET /pet => cat, dog, 404, 404
// GET /role => manager, executive, 404
func TestSpecExample(t *testing.T) {
	base := newTestServer(t)

	for _, p := range []string{"/pet?v=cat", "/pet?v=dog", "/role?v=manager", "/role?v=executive"} {
		if code := put(t, base, p); code != 200 {
			t.Fatalf("PUT %s: want 200, got %d", p, code)
		}
	}

	cases := []struct {
		path     string
		wantCode int
		wantBody string
	}{
		{"/pet", 200, "cat"},
		{"/pet", 200, "dog"},
		{"/pet", 404, ""},
		{"/pet", 404, ""},
		{"/role", 200, "manager"},
		{"/role", 200, "executive"},
		{"/role", 404, ""},
	}
	for _, c := range cases {
		code, body := get(t, base, c.path)
		if code != c.wantCode || body != c.wantBody {
			t.Errorf("GET %s: want %d %q, got %d %q", c.path, c.wantCode, c.wantBody, code, body)
		}
	}
}

// PUT без параметра v → 400, тело пустое.
func TestPutMissingValue(t *testing.T) {
	base := newTestServer(t)
	code := put(t, base, "/pet")
	if code != 400 {
		t.Fatalf("want 400, got %d", code)
	}
}

// Если сообщения нет и таймаут не задан — сразу 404.
func TestGetEmptyImmediate(t *testing.T) {
	base := newTestServer(t)
	start := time.Now()
	code, body := get(t, base, "/empty")
	if code != 404 || body != "" {
		t.Fatalf("want 404 \"\", got %d %q", code, body)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("expected immediate, took %v", d)
	}
}

// Если таймаут истекает раньше, чем пришло сообщение — 404 после N секунд.
func TestGetTimeoutExpires(t *testing.T) {
	base := newTestServer(t)
	start := time.Now()
	code, body := get(t, base, "/empty?timeout=1")
	d := time.Since(start)
	if code != 404 || body != "" {
		t.Fatalf("want 404 \"\", got %d %q", code, body)
	}
	if d < 900*time.Millisecond || d > 1500*time.Millisecond {
		t.Fatalf("expected ~1s, got %v", d)
	}
}

// Получатель ждёт; в середине ожидания приходит PUT — получает сообщение, не дожидаясь таймаута.
func TestGetTimeoutReceivesMessage(t *testing.T) {
	base := newTestServer(t)
	go func() {
		time.Sleep(200 * time.Millisecond)
		put(t, base, "/late?v=hello")
	}()
	start := time.Now()
	code, body := get(t, base, "/late?timeout=5")
	d := time.Since(start)
	if code != 200 || body != "hello" {
		t.Fatalf("want 200 \"hello\", got %d %q", code, body)
	}
	if d > 1*time.Second {
		t.Fatalf("expected ~200ms, got %v", d)
	}
}

// Два получателя ждут; первое сообщение должен получить тот, кто первый запросил.
func TestWaiterFIFO(t *testing.T) {
	base := newTestServer(t)
	var wg sync.WaitGroup
	got := make([]string, 2)
	codes := make([]int, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		codes[0], got[0] = get(t, base, "/race?timeout=5")
	}()
	time.Sleep(100 * time.Millisecond) // даём первому waiter-у встать в очередь

	wg.Add(1)
	go func() {
		defer wg.Done()
		codes[1], got[1] = get(t, base, "/race?timeout=5")
	}()
	time.Sleep(100 * time.Millisecond) // даём второму встать после первого

	put(t, base, "/race?v=first")
	put(t, base, "/race?v=second")

	wg.Wait()

	if codes[0] != 200 || got[0] != "first" {
		t.Errorf("waiter 1: want 200 \"first\", got %d %q", codes[0], got[0])
	}
	if codes[1] != 200 || got[1] != "second" {
		t.Errorf("waiter 2: want 200 \"second\", got %d %q", codes[1], got[1])
	}
}

// Очереди изолированы по имени: сообщения из /a не утекают в /b.
func TestQueuesAreIsolated(t *testing.T) {
	base := newTestServer(t)
	put(t, base, "/a?v=x")
	put(t, base, "/b?v=y")

	if code, body := get(t, base, "/a"); code != 200 || body != "x" {
		t.Errorf("GET /a: want 200 \"x\", got %d %q", code, body)
	}
	if code, body := get(t, base, "/b"); code != 200 || body != "y" {
		t.Errorf("GET /b: want 200 \"y\", got %d %q", code, body)
	}
}
