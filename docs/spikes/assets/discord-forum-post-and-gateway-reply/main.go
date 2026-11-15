// Command discord-forum-post-and-gateway-reply is the throwaway program behind
// the discord-forum-post-and-gateway-reply spike. It reads the test bot's
// credentials from an env file at run time, creates forum posts in the test
// forum, replies in them as the bot and as a webhook author, and reports what
// the gateway delivered. It prints labels, flags and timings, never a token or
// an identifier from the env file.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/cache"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

type report struct {
	Step   string         `json:"step"`
	Result string         `json:"result"`
	Detail map[string]any `json:"detail,omitempty"`
}

func emit(step, result string, detail map[string]any) {
	line, _ := json.Marshal(report{Step: step, Result: result, Detail: detail})
	fmt.Println(string(line))
}

// discordError reduces an error to the Discord error code and message, so a
// request URL holding an identifier from the env file is never printed.
func discordError(err error) map[string]any {
	var restErr *rest.Error
	if errors.As(err, &restErr) {
		detail := map[string]any{"code": int(restErr.Code), "message": restErr.Message}
		if len(restErr.Errors) > 0 {
			detail["errors"] = json.RawMessage(restErr.Errors)
		}
		if restErr.Response != nil {
			detail["status"] = restErr.Response.StatusCode
		}
		return detail
	}
	return map[string]any{"error": "non-REST error", "type": fmt.Sprintf("%T", err)}
}

func readEnv(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	return values, scanner.Err()
}

// observed is one gateway event the spike saw in a thread it created.
type observed struct {
	kind       string
	threadID   snowflake.ID
	messageID  snowflake.ID
	authorBot  bool
	webhook    bool
	hasContent bool
	nonce      string
	at         time.Time
}

type recorder struct {
	mu      sync.Mutex
	threads map[snowflake.ID]string
	seen    []observed
	notify  chan observed
}

func (r *recorder) track(thread snowflake.ID, label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.threads[thread] = label
}

func (r *recorder) label(thread snowflake.ID) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	label, ok := r.threads[thread]
	return label, ok
}

// onMessage records every guild message, tracked or not, because the
// gateway can deliver a new post's starter message before the REST call that
// created it returns.
func (r *recorder) onMessage(event *events.GuildMessageCreate) {
	item := observed{
		kind:       "MESSAGE_CREATE",
		threadID:   event.ChannelID,
		messageID:  event.MessageID,
		authorBot:  event.Message.Author.Bot,
		webhook:    event.Message.WebhookID != nil,
		hasContent: event.Message.Content != "",
		nonce:      string(event.Message.Nonce),
		at:         time.Now(),
	}
	r.mu.Lock()
	r.seen = append(r.seen, item)
	r.mu.Unlock()
	select {
	case r.notify <- item:
	default:
	}
}

// await waits for the gateway to deliver message in thread and returns how
// long after sent it arrived.
func (r *recorder) await(message snowflake.ID, sent time.Time, timeout time.Duration) (observed, time.Duration, bool) {
	deadline := time.After(timeout)
	for {
		r.mu.Lock()
		for _, item := range r.seen {
			if item.messageID == message {
				r.mu.Unlock()
				return item, item.at.Sub(sent), true
			}
		}
		r.mu.Unlock()
		select {
		case <-r.notify:
		case <-deadline:
			return observed{}, 0, false
		}
	}
}

func main() {
	envPath := flag.String("env", "", "path to the env file holding BOT_TOKEN, SERVER_ID, TEST_FORUM and TEST_CHANNEL")
	archive := flag.Bool("archive", true, "archive the spike's threads when done")
	flag.Parse()
	if *envPath == "" {
		fmt.Fprintln(os.Stderr, "usage: discord-forum-post-and-gateway-reply -env PATH")
		os.Exit(2)
	}
	env, err := readEnv(*envPath)
	if err != nil {
		emit("read-env", "fail", map[string]any{"error": "cannot read env file"})
		os.Exit(1)
	}
	for _, name := range []string{"BOT_TOKEN", "SERVER_ID", "TEST_FORUM", "TEST_CHANNEL"} {
		if env[name] == "" {
			emit("read-env", "skip", map[string]any{"missing": name})
			os.Exit(0)
		}
	}
	forumID := snowflake.MustParse(env["TEST_FORUM"])
	textChannelID := snowflake.MustParse(env["TEST_CHANNEL"])

	// disgo logs at error level only, without attributes, so nothing it logs
	// can carry a request URL.
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	rec := &recorder{threads: map[snowflake.ID]string{}, notify: make(chan observed, 64)}

	// Step 1: what the application is allowed to read.
	probe, err := disgo.New(env["BOT_TOKEN"], bot.WithLogger(quiet))
	if err != nil {
		emit("client", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	app, err := probe.Rest.GetCurrentApplication()
	if err != nil {
		emit("application", "fail", discordError(err))
		os.Exit(1)
	}
	contentIntent := app.Flags.Has(discord.ApplicationFlagGatewayMessageContent) || app.Flags.Has(discord.ApplicationFlagGatewayMessageContentLimited)
	emit("application", "pass", map[string]any{
		"message_content_intent":         app.Flags.Has(discord.ApplicationFlagGatewayMessageContent),
		"message_content_intent_limited": app.Flags.Has(discord.ApplicationFlagGatewayMessageContentLimited),
		"bot_public":                     app.BotPublic,
	})
	probe.Close(context.Background())

	intents := gateway.IntentGuilds | gateway.IntentGuildMessages | gateway.IntentGuildMessagePolls
	if contentIntent {
		intents |= gateway.IntentMessageContent
	}
	client, err := disgo.New(env["BOT_TOKEN"],
		bot.WithLogger(quiet),
		bot.WithGatewayConfigOpts(gateway.WithIntents(intents)),
		bot.WithCacheConfigOpts(cache.WithCaches(cache.FlagGuilds|cache.FlagChannels)),
		bot.WithEventListeners(&events.ListenerAdapter{OnGuildMessageCreate: rec.onMessage}),
	)
	if err != nil {
		emit("client", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	defer client.Close(context.Background())

	ready := make(chan struct{}, 1)
	client.AddEventListeners(bot.NewListenerFunc(func(*events.GuildsReady) { ready <- struct{}{} }))
	started := time.Now()
	if err := client.OpenGateway(context.Background()); err != nil {
		emit("gateway-open", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	select {
	case <-ready:
		emit("gateway-open", "pass", map[string]any{"intents": int(intents), "guilds_ready_after_ms": time.Since(started).Milliseconds(), "message_content_requested": contentIntent})
	case <-time.After(30 * time.Second):
		emit("gateway-open", "fail", map[string]any{"error": "no GUILDS_READY within 30s"})
		os.Exit(1)
	}

	// Step 2: the forum channel's shape.
	channel, err := client.Rest.GetChannel(forumID)
	if err != nil {
		emit("forum-channel", "fail", discordError(err))
		os.Exit(1)
	}
	forum, ok := channel.(discord.GuildForumChannel)
	if !ok {
		emit("forum-channel", "fail", map[string]any{"type": int(channel.Type())})
		os.Exit(1)
	}
	var tags []snowflake.ID
	if forum.Flags().Has(discord.ChannelFlagRequireTag) && len(forum.AvailableTags) > 0 {
		tags = []snowflake.ID{forum.AvailableTags[0].ID}
	}
	emit("forum-channel", "pass", map[string]any{"type": int(channel.Type()), "require_tag": forum.Flags().Has(discord.ChannelFlagRequireTag), "available_tags": len(forum.AvailableTags)})

	stamp := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	persona := discord.Embed{
		Author:      &discord.EmbedAuthor{Name: "deck spike persona", IconURL: "https://cdn.discordapp.com/embed/avatars/0.png"},
		Description: "Text under a persona sits in an embed whose author is the persona.",
	}
	button := discord.NewActionRow(discord.NewPrimaryButton("Acknowledge", "deck-spike:ack"))
	question := "Does a forum starter message carry a poll?"
	yes, no := "yes", "no"
	poll := &discord.PollCreate{
		Question: discord.PollMedia{Text: &question},
		Answers:  []discord.PollMedia{{Text: &yes}, {Text: &no}},
		Duration: 1,
	}
	noMentions := &discord.AllowedMentions{}

	// Step 3: a forum post whose starter message carries text, an embed with a
	// persona, a button and a poll.
	sent := time.Now()
	post, err := client.Rest.CreatePostInThreadChannel(forumID, discord.ThreadChannelPostCreate{
		Name:        "deck spike " + stamp,
		AppliedTags: tags,
		Message: discord.MessageCreate{
			Content:         "deck spike: forum post with embed, component and poll",
			Embeds:          []discord.Embed{persona},
			Components:      []discord.LayoutComponent{button},
			Poll:            poll,
			AllowedMentions: noMentions,
			Nonce:           "deckspikepost" + stamp[17:19],
		},
	})
	pollInStarter := err == nil
	if err != nil {
		emit("forum-post-with-poll", "fail", discordError(err))
		sent = time.Now()
		post, err = client.Rest.CreatePostInThreadChannel(forumID, discord.ThreadChannelPostCreate{
			Name:        "deck spike " + stamp,
			AppliedTags: tags,
			Message: discord.MessageCreate{
				Content:         "deck spike: forum post with embed and component",
				Embeds:          []discord.Embed{persona},
				Components:      []discord.LayoutComponent{button},
				AllowedMentions: noMentions,
			},
		})
		if err != nil {
			emit("forum-post", "fail", discordError(err))
			os.Exit(1)
		}
	}
	returned := time.Now()
	thread := post.ID()
	rec.track(thread, "forum-post")
	emit("forum-post", "pass", map[string]any{
		"poll_in_starter":        pollInStarter,
		"starter_id_is_thread":   post.Message.ID == thread,
		"thread_parent_is_forum": post.ParentID() != nil && *post.ParentID() == forumID,
		"rest_ms":                time.Since(sent).Milliseconds(),
	})
	if item, latency, ok := rec.await(post.Message.ID, sent, 15*time.Second); ok {
		emit("gateway-starter-message", "pass", map[string]any{"latency_ms": latency.Milliseconds(), "before_rest_returned": item.at.Before(returned), "author_bot": item.authorBot, "has_content": item.hasContent, "nonce_echoed": pollInStarter && item.nonce == "deckspikepost"+stamp[17:19], "rest_nonce_echoed": string(post.Message.Nonce) == "deckspikepost"+stamp[17:19]})
	} else {
		emit("gateway-starter-message", "fail", map[string]any{"error": "not delivered within 15s"})
	}

	// Step 4: when the starter cannot carry the poll, the poll is a second
	// message in the thread.
	if !pollInStarter {
		sent = time.Now()
		message, err := client.Rest.CreateMessage(thread, discord.MessageCreate{Content: "deck spike: poll as the thread's second message", Poll: poll, AllowedMentions: noMentions})
		if err != nil {
			emit("poll-in-thread", "fail", discordError(err))
		} else if _, latency, ok := rec.await(message.ID, sent, 15*time.Second); ok {
			emit("poll-in-thread", "pass", map[string]any{"latency_ms": latency.Milliseconds()})
		} else {
			emit("poll-in-thread", "fail", map[string]any{"error": "gateway did not deliver within 15s"})
		}
	}

	// Step 5: three replies by the bot itself, so the history read below has
	// an order to show.
	var reply *discord.Message
	for i := 1; i <= 3; i++ {
		sent = time.Now()
		nonce := fmt.Sprintf("deckspikereply%d%s", i, stamp[17:19])
		reply, err = client.Rest.CreateMessage(thread, discord.MessageCreate{Content: fmt.Sprintf("deck spike: reply %d by the bot", i), AllowedMentions: noMentions, Nonce: nonce, EnforceNonce: true})
		if err != nil {
			emit("bot-reply", "fail", discordError(err))
			break
		}
		if item, latency, ok := rec.await(reply.ID, sent, 15*time.Second); ok {
			emit("bot-reply", "pass", map[string]any{"n": i, "latency_ms": latency.Milliseconds(), "author_bot": item.authorBot, "has_content": item.hasContent, "nonce_echoed": item.nonce == nonce, "rest_nonce_echoed": string(reply.Nonce) == nonce})
		} else {
			emit("bot-reply", "fail", map[string]any{"n": i, "error": "gateway did not deliver within 15s"})
		}
	}

	// Step 5b: the last reply's nonce again, with enforce_nonce: Discord
	// should answer with the same message instead of sending a second one.
	if reply != nil {
		again, err := client.Rest.CreateMessage(thread, discord.MessageCreate{Content: "deck spike: reply 3 by the bot", AllowedMentions: noMentions, Nonce: string(reply.Nonce), EnforceNonce: true})
		if err != nil {
			emit("enforce-nonce-repeat", "fail", discordError(err))
		} else {
			emit("enforce-nonce-repeat", "pass", map[string]any{"same_message": again.ID == reply.ID})
		}
	}

	// Step 6: a reply by another author. A webhook on the forum posts into
	// the thread, standing in for a person, since nobody else is present.
	var other *discord.Message
	hook, err := client.Rest.CreateWebhook(forumID, discord.WebhookCreate{Name: "deck-spike-stand-in"})
	if err != nil {
		emit("webhook-reply", "fail", discordError(err))
	} else {
		sent = time.Now()
		other, err = client.Rest.CreateWebhookMessage(hook.ID(), hook.Token, discord.WebhookMessageCreate{
			Content:         "deck spike: reply by another author (webhook stand-in)",
			Username:        "deck spike stand-in",
			AllowedMentions: noMentions,
		}, rest.CreateWebhookMessageParams{Wait: true, ThreadID: thread})
		if err != nil {
			emit("webhook-reply", "fail", discordError(err))
		} else if item, latency, ok := rec.await(other.ID, sent, 15*time.Second); ok {
			emit("webhook-reply", "pass", map[string]any{"latency_ms": latency.Milliseconds(), "author_bot": item.authorBot, "webhook": item.webhook, "has_content": item.hasContent})
		} else {
			emit("webhook-reply", "fail", map[string]any{"error": "gateway did not deliver within 15s"})
		}
		if err := client.Rest.DeleteWebhook(hook.ID()); err != nil {
			emit("webhook-delete", "fail", discordError(err))
		}
	}

	// Step 7: history after a cursor. The order Discord returns them in, and
	// whether their content is present over REST.
	history, err := client.Rest.GetMessages(thread, 0, 0, post.Message.ID, 100)
	if err != nil {
		emit("history-after-cursor", "fail", discordError(err))
	} else {
		var contents, webhooks int
		newestFirst, oldestFirst := true, true
		for i, message := range history {
			if message.Content != "" {
				contents++
			}
			if message.WebhookID != nil {
				webhooks++
			}
			if i > 0 && history[i-1].ID < message.ID {
				newestFirst = false
			}
			if i > 0 && history[i-1].ID > message.ID {
				oldestFirst = false
			}
		}
		emit("history-after-cursor", "pass", map[string]any{"count": len(history), "with_content": contents, "from_webhook": webhooks, "newest_first": newestFirst, "oldest_first": oldestFirst})
	}

	// Step 8: thread metadata that a cheap unread check can use.
	if refreshed, err := client.Rest.GetChannel(thread); err != nil {
		emit("thread-metadata", "fail", discordError(err))
	} else if guildThread, ok := refreshed.(discord.GuildThread); ok {
		last := guildThread.LastMessageID()
		emit("thread-metadata", "pass", map[string]any{
			"last_message_id_is_latest": last != nil && ((other != nil && *last == other.ID) || (other == nil && reply != nil && *last == reply.ID)),
			"message_count":             guildThread.MessageCount,
			"total_message_sent":        guildThread.TotalMessageSent,
		})
	}

	// Step 9: a text channel has no posts; a message there becomes a thread's
	// starter when a thread is started from it.
	sent = time.Now()
	starter, err := client.Rest.CreateMessage(textChannelID, discord.MessageCreate{Content: "deck spike: text channel starter", AllowedMentions: noMentions})
	if err != nil {
		emit("text-channel-thread", "fail", discordError(err))
	} else {
		textThread, err := client.Rest.CreateThreadFromMessage(textChannelID, starter.ID, discord.ThreadCreateFromMessage{Name: "deck spike " + stamp})
		if err != nil {
			emit("text-channel-thread", "fail", discordError(err))
		} else {
			rec.track(textThread.ID(), "text-thread")
			sent = time.Now()
			inThread, err := client.Rest.CreateMessage(textThread.ID(), discord.MessageCreate{Content: "deck spike: reply in a text channel thread", AllowedMentions: noMentions})
			if err != nil {
				emit("text-channel-thread", "fail", discordError(err))
			} else if _, latency, ok := rec.await(inThread.ID, sent, 15*time.Second); ok {
				emit("text-channel-thread", "pass", map[string]any{"thread_id_is_starter_id": textThread.ID() == starter.ID, "latency_ms": latency.Milliseconds()})
			} else {
				emit("text-channel-thread", "fail", map[string]any{"error": "gateway did not deliver within 15s"})
			}
			if *archive {
				archived := true
				if _, err := client.Rest.UpdateChannel(textThread.ID(), discord.GuildThreadUpdate{Archived: &archived}); err != nil {
					emit("archive", "fail", discordError(err))
				}
			}
		}
	}

	if *archive {
		archived := true
		if _, err := client.Rest.UpdateChannel(thread, discord.GuildThreadUpdate{Archived: &archived}); err != nil {
			emit("archive", "fail", discordError(err))
		} else {
			emit("archive", "pass", nil)
		}
	}
	emit("done", "pass", nil)
}
