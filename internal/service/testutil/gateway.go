package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Gateway is a fake Discord gateway for one bot: it greets with HELLO,
// answers IDENTIFY with READY and a GUILD_CREATE per guild, answers RESUME
// with an invalid session so every reconnect is a new session, and lets a
// test dispatch events and drop the connection.
type Gateway struct {
	URL      string
	UserID   string
	Guilds   []map[string]any
	mu       sync.Mutex
	conn     *websocket.Conn
	seq      int
	sessions int
	refuse   bool
	ready    chan struct{}
}

func NewGateway(t *testing.T, userID string, guilds ...map[string]any) *Gateway {
	t.Helper()
	g := &Gateway{UserID: userID, Guilds: guilds, ready: make(chan struct{}, 16)}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		refuse := g.refuse
		g.mu.Unlock()
		if refuse {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		g.serve(conn)
	}))
	t.Cleanup(func() { g.Drop(); server.Close() })
	g.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	return g
}

func (g *Gateway) write(conn *websocket.Conn, v any) {
	raw, _ := json.Marshal(v)
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = conn.WriteMessage(websocket.TextMessage, raw)
}

func (g *Gateway) serve(conn *websocket.Conn) {
	g.mu.Lock()
	g.conn = conn
	g.write(conn, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 45000}})
	g.mu.Unlock()
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var m struct {
			Op int `json:"op"`
		}
		_ = json.Unmarshal(raw, &m)
		g.mu.Lock()
		switch m.Op {
		case 1:
			g.write(conn, map[string]any{"op": 11})
		case 2:
			g.sessions++
			guilds := []any{}
			for _, guild := range g.Guilds {
				guilds = append(guilds, map[string]any{"id": guild["id"], "unavailable": true})
			}
			g.dispatchLocked("READY", map[string]any{"v": 10, "user": map[string]any{"id": g.UserID, "username": "deck-bot", "bot": true}, "guilds": guilds, "session_id": "session", "resume_gateway_url": g.URL, "application": map[string]any{"id": g.UserID, "flags": 0}})
			for _, guild := range g.Guilds {
				g.dispatchLocked("GUILD_CREATE", guild)
			}
			select {
			case g.ready <- struct{}{}:
			default:
			}
		case 6:
			g.write(conn, map[string]any{"op": 9, "d": false})
		}
		g.mu.Unlock()
	}
}

func (g *Gateway) dispatchLocked(event string, d any) {
	if g.conn == nil {
		return
	}
	g.seq++
	g.write(g.conn, map[string]any{"op": 0, "t": event, "s": g.seq, "d": d})
}

// Dispatch sends one gateway event to the connected session.
func (g *Gateway) Dispatch(event string, d any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dispatchLocked(event, d)
}

// Drop closes the connection without a close frame.
func (g *Gateway) Drop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		_ = g.conn.Close()
		g.conn = nil
	}
}

// Expire closes the session with Discord's invalid-sequence code 4007: the
// client cannot resume and identifies again, receiving a new READY.
func (g *Gateway) Expire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		_ = g.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4007, "invalid seq"), time.Now().Add(time.Second))
		_ = g.conn.Close()
		g.conn = nil
	}
}

// Refuse makes new connections fail, keeping a dropped session down.
func (g *Gateway) Refuse(refuse bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.refuse = refuse
}

// Sessions counts the IDENTIFYs answered with READY.
func (g *Gateway) Sessions() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sessions
}

// WaitReady waits for the next READY sent.
func (g *Gateway) WaitReady(t *testing.T) {
	t.Helper()
	select {
	case <-g.ready:
	case <-time.After(15 * time.Second):
		t.Fatal("gateway session not identified")
	}
}

// Guild is a GUILD_CREATE payload for server 1 with forum 10, text channel 20
// and a member 7 named "person".
func Guild() map[string]any {
	return map[string]any{"id": "1", "name": "Test server", "channels": []any{
		map[string]any{"id": "10", "type": 15, "name": "forum", "guild_id": "1"},
		map[string]any{"id": "20", "type": 0, "name": "text", "guild_id": "1"},
	}, "threads": []any{}, "members": []any{map[string]any{"user": map[string]any{"id": "7", "username": "person"}, "roles": []any{}, "joined_at": "2026-01-01T00:00:00Z"}}, "roles": []any{}}
}
