package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthzNamesEveryCheck(t *testing.T) {
	bot, nats, index := "ready", "connected", "ready"
	c := Checks{Bots: map[string]func() string{"helper": func() string { return bot }}, NATS: func() string { return nats }, Index: func() string { return index }}
	server := httptest.NewServer(Handler(c))
	defer server.Close()
	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	if code, body := get("/healthz"); code != 200 || body["status"] != "ok" || len(body) != 1 {
		t.Fatal(code, body)
	}
	for _, change := range []func(){func() { bot = "disconnected" }, func() { bot, nats = "ready", "reconnecting" }, func() { nats, index = "connected", "replaying" }} {
		change()
		code, body := get("/healthz")
		if code != 503 || body["status"] != "fail" || body["bots"].(map[string]any)["helper"] != bot || body["nats"] != nats || body["index"] != index {
			t.Fatal("unhealthy answer", code, body)
		}
		if code, body := get("/livez"); code != 200 || body["status"] != "ok" {
			t.Fatal("livez", code, body)
		}
	}
}

func TestLoopbackAddressAndProbe(t *testing.T) {
	for in, want := range map[string]string{":8080": "127.0.0.1:8080", "0.0.0.0:9": "127.0.0.1:9", "[::]:9": "[::1]:9", "127.0.0.1:7": "127.0.0.1:7", "10.1.2.3:7": "10.1.2.3:7"} {
		if got, err := LoopbackAddress(in); err != nil || got != want {
			t.Fatalf("%s: %s %v", in, got, err)
		}
	}
	s, err := Listen("127.0.0.1:0", Checks{NATS: func() string { return "closed" }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	code, body, err := Probe(ctx, s.Addr().String())
	if err != nil || code != 503 || string(body) == "" {
		t.Fatal(code, string(body), err)
	}
	s.Close(ctx)
	if _, _, err = Probe(ctx, s.Addr().String()); err == nil {
		t.Fatal("closed listener answered")
	}
}
