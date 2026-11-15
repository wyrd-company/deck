package discord

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/cache"
	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

// API is the REST surface needed by sends and history. Client also exposes its
// disgo bot for task 7 to install gateway listeners before opening the gateway.
type API interface {
	Channel(context.Context, uint64) (d.GuildChannel, error)
	Channels(context.Context, uint64) ([]d.GuildChannel, error)
	ServerName(context.Context, uint64) (string, error)
	Send(context.Context, uint64, d.MessageCreate) (*d.Message, error)
	Post(context.Context, uint64, d.ThreadChannelPostCreate) (*d.ThreadChannelPost, error)
	StartThread(context.Context, uint64, uint64, string) (*d.GuildThread, error)
	DeleteMessage(context.Context, uint64, uint64) error
	History(context.Context, uint64, uint64, uint64, int) ([]d.Message, error)
}
type Client struct{ Bot *bot.Client }

func New(token string, options ...bot.ConfigOpt) (*Client, error) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second}}
	opts := []bot.ConfigOpt{bot.WithLogger(logger), bot.WithRestClientConfigOpts(rest.WithLogger(logger), rest.WithRateLimiter(rateLimiter{rest.NewRateLimiter(rest.WithRateLimiterLogger(logger))}), rest.WithHTTPClient(httpClient)), bot.WithGatewayConfigOpts(gateway.WithIntents(gateway.IntentGuilds | gateway.IntentGuildMessages | gateway.IntentMessageContent | gateway.IntentGuildMessagePolls)), bot.WithCacheConfigOpts(cache.WithCaches(cache.FlagGuilds | cache.FlagChannels | cache.FlagMembers))}
	c, err := disgo.New(token, append(opts, options...)...)
	if err != nil {
		return nil, err
	}
	return &Client{Bot: c}, nil
}
func (c *Client) Channel(ctx context.Context, id uint64) (d.GuildChannel, error) {
	ch, err := c.Bot.Rest.GetChannel(snowflake.ID(id), rest.WithCtx(ctx))
	if err != nil {
		return nil, err
	}
	if guild, ok := ch.(d.GuildChannel); ok {
		return guild, nil
	}
	return nil, errNotGuild
}
func (c *Client) Channels(ctx context.Context, id uint64) ([]d.GuildChannel, error) {
	return c.Bot.Rest.GetGuildChannels(snowflake.ID(id), rest.WithCtx(ctx))
}
func (c *Client) ServerName(ctx context.Context, id uint64) (string, error) {
	if g, ok := c.Bot.Caches.Guild(snowflake.ID(id)); ok {
		return g.Name, nil
	}
	g, err := c.Bot.Rest.GetGuild(snowflake.ID(id), false, rest.WithCtx(ctx))
	if err != nil {
		return "", err
	}
	return g.Name, nil
}
func (c *Client) Send(ctx context.Context, id uint64, m d.MessageCreate) (*d.Message, error) {
	return c.Bot.Rest.CreateMessage(snowflake.ID(id), m, rest.WithCtx(ctx))
}
func (c *Client) Post(ctx context.Context, id uint64, p d.ThreadChannelPostCreate) (*d.ThreadChannelPost, error) {
	return c.Bot.Rest.CreatePostInThreadChannel(snowflake.ID(id), p, rest.WithCtx(ctx))
}
func (c *Client) StartThread(ctx context.Context, id, message uint64, title string) (*d.GuildThread, error) {
	return c.Bot.Rest.CreateThreadFromMessage(snowflake.ID(id), snowflake.ID(message), d.ThreadCreateFromMessage{Name: title}, rest.WithCtx(ctx))
}
func (c *Client) DeleteMessage(ctx context.Context, id, message uint64) error {
	return c.Bot.Rest.DeleteMessage(snowflake.ID(id), snowflake.ID(message), rest.WithCtx(ctx))
}
func (c *Client) History(ctx context.Context, id, after, before uint64, limit int) ([]d.Message, error) {
	return c.Bot.Rest.GetMessages(snowflake.ID(id), 0, snowflake.ID(before), snowflake.ID(after), limit, rest.WithCtx(ctx))
}
