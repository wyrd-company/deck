// ---
// relationships:
//
//	implements:
//	  - service
//
// ---
// Package health serves the service's health listener: /livez while the
// process runs, and /healthz while every bot's gateway session is ready, NATS
// is connected and the activity index has finished its replay.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"
)

// Checks reports each part's state. A bot's state is "ready" when its
// session is; NATS's is "connected"; the index's is "ready".
type Checks struct {
	Bots  map[string]func() string
	NATS  func() string
	Index func() string
}

type report struct {
	Status string            `json:"status"`
	Bots   map[string]string `json:"bots,omitempty"`
	NATS   string            `json:"nats,omitempty"`
	Index  string            `json:"index,omitempty"`
}

// Evaluate takes every check once, naming them all when one fails.
func (c Checks) Evaluate() (bool, any) {
	r := report{Status: "fail", Bots: map[string]string{}}
	healthy := true
	for name, state := range c.Bots {
		r.Bots[name] = state()
		healthy = healthy && r.Bots[name] == "ready"
	}
	if c.NATS != nil {
		r.NATS = c.NATS()
		healthy = healthy && r.NATS == "connected"
	}
	if c.Index != nil {
		r.Index = c.Index()
		healthy = healthy && r.Index == "ready"
	}
	if healthy {
		return true, map[string]string{"status": "ok"}
	}
	return false, r
}

// Handler serves /livez and /healthz.
func Handler(c Checks) http.Handler {
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, code int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	}
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		healthy, body := c.Evaluate()
		code := http.StatusOK
		if !healthy {
			code = http.StatusServiceUnavailable
		}
		write(w, code, body)
	})
	return mux
}

// Server is the bound health listener.
type Server struct {
	http     *http.Server
	listener net.Listener
	done     chan struct{}
}

// Listen binds address and serves the checks until Close.
func Listen(address string, c Checks) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &Server{http: &http.Server{Handler: Handler(c), ReadHeaderTimeout: 5 * time.Second}, listener: listener, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return
		}
	}()
	return s, nil
}

// Addr is the bound address.
func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Close stops the listener within ctx.
func (s *Server) Close(ctx context.Context) {
	if err := s.http.Shutdown(ctx); err != nil {
		_ = s.http.Close()
	}
	<-s.done
}

// Probe requests /healthz at address, returning the status code and body.
// The command `deck service health` uses it on the loopback address.
func Probe(ctx context.Context, address string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/healthz", nil)
	if err != nil {
		return 0, nil, err
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var body json.RawMessage
	if err = json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<16)).Decode(&body); err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// LoopbackAddress turns listen.health into the address the health command
// requests: the loopback address on its port when the host is unspecified,
// otherwise the configured host.
func LoopbackAddress(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if ip := net.ParseIP(host); host == "" || ip != nil && ip.IsUnspecified() {
		host = "127.0.0.1"
		if ip != nil && ip.To4() == nil {
			host = "::1"
		}
	}
	return net.JoinHostPort(host, port), nil
}
