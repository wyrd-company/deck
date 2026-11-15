// Package client combines the API with local session state. No session
// identifiers are ever included in an RPC.
package client

import (
	"context"
	"fmt"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/session"
	"github.com/wyrd-company/deck/internal/store"
)

type Client struct {
	API         pb.DeckServiceClient
	Store       *store.Store
	Session     session.Session
	Config      *config.Client
	Fingerprint string
}
type Count struct {
	ThreadID      string `json:"thread_id"`
	Count         uint32 `json:"count"`
	State         string `json:"state,omitempty"`
	HorizonPassed bool   `json:"horizon_passed,omitempty"`
}
type Unread struct {
	Total   uint64  `json:"total"`
	Threads []Count `json:"threads"`
}

// Allowed compares resolved config references; a botless reference matches
// that channel through any bot. Deny always wins over allow.
func Allowed(c *config.Client, bot string, channel uint64) bool {
	match := func(ref string) bool {
		if alias, ok := c.Aliases.Channels[ref]; ok {
			ref = alias
		}
		b, id, err := Reference(ref)
		return err == nil && id == channel && (b == "" || b == bot)
	}
	for _, ref := range c.Channels.Deny {
		if match(ref) {
			return false
		}
	}
	if len(c.Channels.Allow) == 0 {
		return true
	}
	for _, ref := range c.Channels.Allow {
		if match(ref) {
			return true
		}
	}
	return false
}
func (c *Client) Threads() ([]store.Thread, error) {
	ts, err := c.Store.Threads(c.Session)
	if err != nil {
		return nil, err
	}
	kept := []store.Thread{}
	for _, t := range ts {
		if Allowed(c.Config, t.Info.Bot, t.Info.ChannelId) {
			kept = append(kept, t)
		}
	}
	return kept, nil
}
func Cursors(ts []store.Thread) []*pb.Cursor {
	result := make([]*pb.Cursor, 0, len(ts))
	for _, t := range ts {
		result = append(result, &pb.Cursor{ThreadId: t.Info.ThreadId, After: t.After})
	}
	return result
}
func (c *Client) Count(ctx context.Context) (*Unread, error) {
	ts, err := c.Threads()
	if err != nil {
		return nil, err
	}
	if len(ts) == 0 {
		return &Unread{Threads: []Count{}}, nil
	}
	r, err := c.API.CountUnread(ctx, &pb.CountUnreadRequest{Cursors: Cursors(ts)})
	if err != nil {
		return nil, err
	}
	return c.Counts(r.Threads)
}
func (c *Client) Counts(threads []*pb.ThreadCount) (*Unread, error) {
	result := &Unread{Threads: []Count{}}
	for _, t := range threads {
		n := t.Count
		if t.State != pb.ThreadState_THREAD_STATE_TOUCHED {
			n = 0
		} else {
			for _, m := range t.Own {
				own, err := c.Store.Own(c.Session, m.SenderKey, m.Nonce)
				if err != nil {
					return nil, err
				}
				if own && n > 0 {
					n--
				}
			}
		}
		result.Total += uint64(n)
		result.Threads = append(result.Threads, Count{ThreadID: fmt.Sprint(t.ThreadId), Count: n, State: t.State.String(), HorizonPassed: t.HorizonPassed})
	}
	return result, nil
}
func (c *Client) Visible(activity []*pb.Activity) ([]*pb.Activity, error) {
	result := []*pb.Activity{}
	for _, a := range activity {
		own, err := c.Store.Own(c.Session, a.SenderKey, a.Nonce)
		if err != nil {
			return nil, err
		}
		if !own {
			result = append(result, a)
		}
	}
	return result, nil
}
