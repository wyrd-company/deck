//go:build discordlive

// Package acceptance exercises the distributed product through its real CLI,
// image, Discord gateway, OpenBao Transit and Trowel command relay.
package acceptance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"golang.org/x/crypto/ssh"
)

var fixturePath = flag.String("fixture", "/workspace/tools/deck/.env", "authorized test-server fixture (absent means skip)")
var evidencePath = flag.String("evidence", "", "directory for safe, reusable run evidence")
var humanWait = flag.Duration("human-wait", 0, "optional window for a person's reply, button and add/remove votes")
var nativeDiagnostic = flag.Bool("native-diagnostic", false, "debug the live stack without Docker; does not satisfy container acceptance")

type run struct {
	t                                          *testing.T
	root, dir, evidence, deck, client, session string
	fixture                                    map[string]string
	secrets                                    []string
	mu                                         sync.Mutex
	transcript                                 []string
	http                                       *http.Client
	head                                       string
	completed                                  bool
}

func TestAcceptance(t *testing.T) {
	r := &run{t: t, fixture: readFixture(t), http: &http.Client{Timeout: 15 * time.Second}}
	for _, field := range []string{"SERVER_ID", "TEST_FORUM", "TEST_CHANNEL", "APPLICATION_ID"} {
		if n, err := strconv.ParseUint(r.fixture[field], 10, 64); err != nil || n == 0 {
			t.Fatal("invalid fixture field", field)
		}
	}
	tools := []string{"bao", "trowel", "python3", "go"}
	if !*nativeDiagnostic {
		tools = append(tools, "docker")
	}
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatal("required tool unavailable:", tool)
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	r.root = root
	r.dir = t.TempDir()
	r.evidence = *evidencePath
	if r.evidence == "" {
		r.evidence = filepath.Join(r.dir, "evidence")
	}
	if err := os.MkdirAll(r.evidence, 0700); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(r.evidence); err != nil || len(entries) != 0 {
		t.Fatal("evidence directory must be empty; prior records must not count as this run")
	}
	r.session = "deck-acceptance-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	r.secrets = []string{r.fixture["BOT_TOKEN"], r.fixture["PUBLIC_KEY"]}
	// Start failed. Only the last assertion may replace this with passed.
	r.save("result.json", map[string]any{"status": "failed", "session": r.session})
	t.Cleanup(func() {
		r.save("transcript.json", r.transcript)
		status := "failed"
		if r.completed && !t.Failed() {
			status = "passed"
			if *nativeDiagnostic {
				status = "diagnostic-passed-container-unverified"
			}
		}
		r.save("result.json", map[string]any{"status": status, "source_commit": r.head, "session": r.session, "human_observations": r.observations(filepath.Join(r.evidence, "t3-records.jsonl"))})
	})
	if !*nativeDiagnostic {
		r.command(nil, "docker", "info", "--format", "{{.ServerVersion}}")
	}
	r.deck = filepath.Join(r.dir, "deck")
	r.command(nil, "go", "build", "-o", r.deck, "./cmd/deck")
	head := strings.TrimSpace(string(r.command(nil, "git", "rev-parse", "HEAD")))
	r.head = head
	r.note("source commit " + head)
	image := "deck-acceptance:" + strings.TrimPrefix(r.session, "deck-acceptance-")
	if !*nativeDiagnostic {
		r.command(nil, "docker", "build", "--build-arg", "VERSION=acceptance-"+head[:12], "-t", image, ".")
		r.ledger("acquire", "docker:image/"+image, "--cleanup", "docker image rm "+image)
		t.Cleanup(func() {
			if r.cleanup("docker", "image", "rm", image) {
				r.ledger("release", "docker:image/"+image)
			}
		})
	}

	// The local JWT server has an operator/account/user chain and an isolated
	// account store. No user without its credentials may connect.
	ns, credentials := testutil.JWTNATS(t)
	r.secrets = append(r.secrets, credentials)
	if nc, err := nats.Connect(ns.ClientURL(), nats.Timeout(time.Second), nats.NoReconnect()); err == nil {
		nc.Close()
		t.Fatal("JWT NATS accepted an anonymous client")
	}
	nc, err := nats.Connect(ns.ClientURL(), nats.UserCredentialBytes([]byte(credentials)))
	if err != nil {
		t.Fatal("JWT-authenticated subscriber failed")
	}
	t.Cleanup(nc.Close)
	sub, err := nc.SubscribeSync("deck.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	r.note("PASS: real JetStream accepts JWT credentials and refuses an anonymous connection")

	baoAddress := "http://" + freeAddress(t)
	baoToken := "acceptance-" + randomID(t)
	r.secrets = append(r.secrets, baoToken)
	baoLog := filepath.Join(r.dir, "bao.log")
	r.start(baoLog, "bao", "server", "-dev", "-dev-listen-address="+strings.TrimPrefix(baoAddress, "http://"), "-dev-root-token-id="+baoToken)
	r.until("OpenBao dev health", 15*time.Second, func() bool {
		resp, err := r.http.Get(baoAddress + "/v1/sys/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == 200
	})
	bao := func(path string, data any) map[string]any {
		return r.request("POST", baoAddress+"/v1/"+path, baoToken, data, false)
	}
	bao("sys/mounts/transit", map[string]any{"type": "transit"})
	bao("transit/keys/agent", map[string]any{"type": "ed25519"})
	key := r.request("GET", baoAddress+"/v1/transit/keys/agent", baoToken, nil, false)
	encoded := key["data"].(map[string]any)["keys"].(map[string]any)["1"].(map[string]any)["public_key"].(string)
	pub, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	sshPublic, err := ssh.NewPublicKey(ed25519.PublicKey(pub))
	if err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostBlock, err := ssh.MarshalPrivateKey(private, "acceptance")
	if err != nil {
		t.Fatal(err)
	}
	hostKey := string(pem.EncodeToMemory(hostBlock))
	r.secrets = append(r.secrets, hostKey)
	hostSigner, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	bao("secret/data/deck", map[string]any{"data": map[string]string{"host_key": hostKey, "creds": credentials, "token": r.fixture["BOT_TOKEN"]}})
	tokenFile := filepath.Join(r.dir, "bao-token")
	r.write(tokenFile, []byte(baoToken), 0600)
	sshAddress, healthAddress := freeAddress(t), freeAddress(t)
	serviceConfig := map[string]any{
		"listen":   map[string]any{"ssh": sshAddress, "health": healthAddress},
		"vault":    map[string]any{"address": baoAddress, "auth": map[string]any{"token_file": "/etc/deck/bao-token"}},
		"host_key": map[string]any{"path": "deck", "field": "host_key"},
		"nats":     map[string]any{"servers": []string{ns.ClientURL()}, "creds": map[string]any{"path": "deck", "field": "creds"}, "stream": map[string]any{"name": "DECK", "manage": true, "max_age": "1h"}, "threads": map[string]any{"bucket": "deck-threads", "manage": true}},
		"bots":     []any{map[string]any{"name": "test", "token": map[string]any{"path": "deck", "field": "token"}}},
		"keys":     []any{map[string]any{"name": "agent", "public_key": strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublic))), "grants": []any{map[string]any{"bot": "test", "server": r.fixture["SERVER_ID"], "channels": []string{r.fixture["TEST_FORUM"], r.fixture["TEST_CHANNEL"]}}}}},
	}
	serviceFile := filepath.Join(r.dir, "service.yml")
	if *nativeDiagnostic {
		serviceConfig["vault"].(map[string]any)["auth"].(map[string]any)["token_file"] = tokenFile
	}
	r.writeJSON(serviceFile, serviceConfig, 0600)
	r.save("service-config.json", serviceConfig)
	r.client = filepath.Join(r.dir, "client.yml")
	r.writeJSON(r.client, map[string]any{
		"service":  map[string]any{"address": sshAddress, "host_key": strings.TrimSpace(string(ssh.MarshalAuthorizedKey(hostSigner.PublicKey()))), "timeout": "20s"},
		"vault":    map[string]any{"address": baoAddress, "auth": map[string]any{"token_file": tokenFile}},
		"key":      map[string]any{"mount": "transit", "name": "agent"},
		"session":  map[string]any{"variables": []string{"T3CODE_THREAD_ID"}},
		"store":    map[string]any{"path": filepath.Join(r.dir, "deck.db")},
		"channels": map[string]any{"default": "test/" + r.fixture["TEST_FORUM"]},
	}, 0600)
	r.command(nil, r.deck, "config", "validate", "--config", r.client)
	clientEvidence, _ := os.ReadFile(r.client)
	r.save("client-config.json", json.RawMessage(clientEvidence))
	if *nativeDiagnostic {
		r.start(filepath.Join(r.dir, "native-service.log"), r.deck, "service", "start", "--config", serviceFile)
		r.until("native service health", 60*time.Second, func() bool {
			resp, err := r.http.Get("http://" + healthAddress + "/healthz")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			return resp.StatusCode == 200
		})
		r.command(nil, r.deck, "service", "health", "--config", serviceFile)
		r.note("DIAGNOSTIC ONLY: native service healthy against live Discord, JWT NATS and real OpenBao; container image/healthcheck remain unverified")
	} else {
		container := r.session
		r.command(nil, "docker", "create", "--name", container, "--network", "host", image)
		r.ledger("acquire", "docker:container/"+container, "--cleanup", "docker rm -f "+container)
		t.Cleanup(func() {
			if out, err := exec.Command("docker", "logs", container).CombinedOutput(); err == nil {
				r.save("service-log.json", r.safe(string(out)))
			}
			if r.cleanup("docker", "rm", "-f", container) {
				r.ledger("release", "docker:container/"+container)
			}
		})
		// Copy only service config and the local dev credential. Neither the live
		// token nor NATS credentials is ever an image layer or CLI argument.
		containerToken := filepath.Join(r.dir, "container-token")
		r.write(containerToken, []byte(baoToken), 0444)
		if err := os.Chmod(serviceFile, 0444); err != nil {
			t.Fatal(err)
		}
		r.command(nil, "docker", "cp", serviceFile, container+":/etc/deck/config.yml")
		r.command(nil, "docker", "cp", containerToken, container+":/etc/deck/bao-token")
		r.command(nil, "docker", "start", container)
		r.until("container healthcheck", 100*time.Second, func() bool {
			out, err := exec.Command("docker", "inspect", "--format", "{{.State.Health.Status}}", container).CombinedOutput()
			return err == nil && strings.TrimSpace(string(out)) == "healthy"
		})
		r.command(nil, "docker", "exec", container, "/usr/local/bin/deck", "service", "health", "--config", "/etc/deck/config.yml")
		if user := strings.TrimSpace(string(r.command(nil, "docker", "inspect", "--format", "{{.Config.User}}", container))); user != "10001:10001" {
			t.Fatal("image does not run as the documented user")
		}
		r.note("PASS: repository image runs as UID 10001; its Docker healthcheck is healthy against live Discord, JWT NATS and real OpenBao")
	}

	credsFile := filepath.Join(r.dir, "nats.creds")
	r.write(credsFile, []byte(credentials), 0600)
	records := filepath.Join(r.evidence, "t3-records.jsonl")
	trowelConfig := filepath.Join(r.dir, "trowel.yml")
	python, _ := exec.LookPath("python3")
	r.writeJSON(trowelConfig, map[string]any{
		"server":   map[string]any{"listen": freeAddress(t)},
		"nats":     map[string]any{"url": ns.ClientURL(), "creds_file": credsFile},
		"subjects": []any{map[string]any{"name": "deck-acceptance", "stream": "DECK", "subject": "deck.>", "consumer": map[string]any{"deliver_policy": "all", "ack_wait": "5s"}, "destinations": []any{map[string]any{"name": "record-t3", "command": map[string]any{"argv": []string{python, filepath.Join(root, "tests/acceptance/relay.py"), r.deck, r.client, filepath.Join(root, "tests/acceptance/t3.py"), records}, "timeout": "20s"}}}}},
	}, 0600)
	r.command(nil, "trowel", "-config", trowelConfig, "-validate")
	trowelEvidence, _ := os.ReadFile(trowelConfig)
	r.save("trowel-config.json", json.RawMessage(trowelEvidence))
	r.start(filepath.Join(r.dir, "trowel.log"), "trowel", "-config", trowelConfig)
	r.note("Trowel command relay uses deck resolve --event - --json and the absolute t3.py recorder; the session identifier is synthetic")
	r.scenario(sub, records)
	// A dependency disconnect must be reflected by the running image's health.
	ns.Shutdown()
	ns.WaitForShutdown()
	r.until("NATS-disconnected health", 15*time.Second, func() bool {
		resp, err := r.http.Get("http://" + healthAddress + "/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == 503
	})
	r.note("PASS: health becomes HTTP 503 after the JWT JetStream server stops")
	r.completed = true
}

func readFixture(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(*fixturePath)
	if os.IsNotExist(err) {
		t.Skip("authorized Discord fixture absent; live acceptance skipped")
	}
	if err != nil {
		t.Fatal("cannot open authorized Discord fixture")
	}
	defer f.Close()
	values := map[string]string{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			values[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), "\"'")
		}
	}
	if s.Err() != nil {
		t.Fatal("cannot read Discord fixture")
	}
	if values["BOT_TOKEN"] == "" {
		t.Skip("test bot absent; live acceptance skipped")
	}
	return values
}

func (r *run) scenario(sub *nats.Subscription, records string) {
	// Check both parent channels against the test server BEFORE sending.
	forum := r.discord("GET", "/channels/"+r.fixture["TEST_FORUM"], nil)
	text := r.discord("GET", "/channels/"+r.fixture["TEST_CHANNEL"], nil)
	if forum["guild_id"] != r.fixture["SERVER_ID"] || text["guild_id"] != r.fixture["SERVER_ID"] {
		r.t.Fatal("test channels are outside authorized test server")
	}
	if forum["type"] != float64(15) && forum["type"] != float64(16) || text["type"] != float64(0) && text["type"] != float64(5) {
		r.t.Fatal("unexpected test channel types")
	}
	user := r.discord("GET", "/users/@me", nil)
	if user["id"] != r.fixture["APPLICATION_ID"] {
		r.t.Fatal("test bot identity mismatch")
	}
	// The test server's owner is a person. This does not require the privileged
	// guild-members intent just to choose a mention target.
	guild := r.discord("GET", "/guilds/"+r.fixture["SERVER_ID"], nil)
	mention, ok := guild["owner_id"].(string)
	if !ok || mention == "" {
		r.t.Fatal("test guild has no owner mention target")
	}
	embedFile, componentFile := filepath.Join(r.dir, "embed.json"), filepath.Join(r.dir, "components.json")
	r.writeJSON(embedFile, map[string]any{"title": "Container acceptance", "description": "CLI → SSH/Transit → service image → Discord"}, 0600)
	r.writeJSON(componentFile, []any{map[string]any{"type": 1, "components": []any{map[string]any{"type": 2, "style": 1, "label": "Acceptance button", "custom_id": "deck-acceptance"}}}}, 0600)
	attachment := filepath.Join(r.dir, "acceptance.txt")
	r.write(attachment, []byte("deck container acceptance\n"), 0600)
	posts := []map[string]any{}
	polls := []string{}
	for _, kind := range []string{"forum", "text"} {
		channel := r.fixture["TEST_FORUM"]
		if kind == "text" {
			channel = r.fixture["TEST_CHANNEL"]
		}
		content := "Container acceptance " + r.session + ": reply here, press the button and add/remove a poll vote if available."
		question := "Does " + r.session + " work?"
		args := []string{"post", "test/" + channel, "--title", "Deck acceptance " + kind + " " + r.session, "--text", content, "--mention", mention, "--poll", question, "--answer", "Yes", "--answer", "No", "--embed", embedFile, "--components", componentFile, "--nonce", strings.ToUpper(kind[:1]) + randomID(r.t)[:20]}
		// Measured API constraint: a text message combining a poll with file
		// attachments returns INVALID_POLL_ATTACHMENTS. Forum polls are sent
		// separately, so their starters can retain the attachment.
		if kind == "forum" {
			args = append(args, "--file", attachment)
		}
		flags, _ := forum["flags"].(float64)
		if kind == "forum" && int(flags)&16 != 0 {
			tags := forum["available_tags"].([]any)
			if len(tags) == 0 {
				r.t.Fatal("test forum needs a tag but has none")
			}
			args = append(args, "--tag", tags[0].(map[string]any)["id"].(string))
		}
		post := r.cli(args...)
		thread := post["thread"].(map[string]any)["thread_id"].(string)
		message := post["message"].(map[string]any)
		body := message["message"].(map[string]any)
		if len(body["embeds"].([]any)) != 1 || len(body["components"].([]any)) != 1 || body["mention_user_ids"].([]any)[0] != mention {
			r.t.Fatal("rich content missing from returned Discord post")
		}
		if kind == "forum" && len(body["attachments"].([]any)) != 1 {
			r.t.Fatal("forum attachment missing")
		}
		if body["embeds"].([]any)[0].(map[string]any)["title"] != "Container acceptance" || body["components"].([]any)[0].(map[string]any)["components"].([]any)[0].(map[string]any)["custom_id"] != "deck-acceptance" {
			r.t.Fatal("returned embed/button changed")
		}
		poll := message
		if kind == "forum" {
			var ok bool
			poll, ok = post["poll_message"].(map[string]any)
			if !ok || poll["id"] == message["id"] {
				r.t.Fatal("forum poll was not a separate message")
			}
		}
		if poll["message"].(map[string]any)["poll"] == nil {
			r.t.Fatal("poll missing from Discord response")
		}
		polls = append(polls, poll["id"].(string))
		posts = append(posts, post)
		r.save(kind+"-post.json", post)
		link := "https://discord.com/channels/" + r.fixture["SERVER_ID"] + "/" + thread + "/" + message["id"].(string)
		r.note("PASS: " + kind + " post with mention, embed, button and poll (forum also has a file): " + link)
		// Repeat the exact CLI command. It must retain the first IDs and leave
		// only the original starter/poll messages in Discord.
		again := r.cli(args...)
		if again["message"].(map[string]any)["id"] != message["id"] || again["thread"].(map[string]any)["thread_id"] != thread {
			r.t.Fatal("post nonce repeat changed message/thread")
		}
		messageChannel := thread
		if kind == "text" {
			messageChannel = channel
		}
		history := r.discord("GET", "/channels/"+messageChannel+"/messages?limit=100", nil)
		copies, pollCopies := 0, 0
		for _, entry := range history["items"].([]any) {
			m := entry.(map[string]any)
			if m["content"] == body["content"] {
				copies++
			}
			if p, ok := m["poll"].(map[string]any); ok && p["question"].(map[string]any)["text"] == question {
				pollCopies++
			}
		}
		if copies != 1 || pollCopies != 1 {
			r.t.Fatal("nonce repeat did not preserve exactly one original starter and poll")
		}
		r.note("PASS: " + kind + " exact post nonce repeat retained IDs; Discord history contains one starter and one poll")
	}
	if *humanWait > 0 {
		r.note("Waiting " + humanWait.String() + " for optional person reply/button/poll add/remove on both posts")
		deadline := time.Now().Add(*humanWait)
		for time.Now().Before(deadline) {
			time.Sleep(time.Second)
		}
	}
	// Poll-result messages are real gateway traffic but do not prove person
	// replies, button interactions or add/remove votes. Keep that distinction.
	for i, post := range posts {
		thread := post["thread"].(map[string]any)["thread_id"].(string)
		pollChannel := thread
		if i == 1 {
			pollChannel = r.fixture["TEST_CHANNEL"]
		}
		r.discord("POST", "/channels/"+pollChannel+"/polls/"+polls[i]+"/expire", map[string]any{})
	}
	r.until("poll results via gateway and Trowel", 45*time.Second, func() bool {
		seen := map[string]bool{}
		for _, record := range readRecords(records) {
			a := record["event"].(map[string]any)
			m, _ := a["message"].(map[string]any)
			if m["type"] == float64(46) && record["session_identifier"] == r.session {
				seen[a["thread_id"].(string)] = true
			}
		}
		return len(seen) == 2
	})
	r.note("PASS: real Discord poll-result gateway messages on both forms reached JetStream → Trowel → deck resolve → t3 stand-in with the original synthetic session identifier")
	// The raw subject capture is independent of Trowel/resolve.
	events := []any{}
	for {
		m, err := sub.NextMsg(200 * time.Millisecond)
		if err != nil {
			break
		}
		var a any
		if json.Unmarshal(m.Data, &a) != nil {
			r.t.Fatal("invalid event JSON")
		}
		events = append(events, map[string]any{"subject": m.Subject, "headers": m.Header, "activity": a})
	}
	r.save("events.json", events)
	for _, post := range posts {
		thread := post["thread"].(map[string]any)["thread_id"].(string)
		want := "deck." + r.fixture["SERVER_ID"] + "." + post["thread"].(map[string]any)["channel_id"].(string) + "." + thread
		found := false
		for _, row := range readRecords(records) {
			a := row["event"].(map[string]any)
			if a["thread_id"] == thread {
				if a["sent_by"] != nil {
					r.t.Fatal("own send woke its sending session")
				}
				if row["subject"] != want || row["session_identifier"] != r.session {
					r.t.Fatal("relay subject/session mismatch")
				}
				found = true
			}
		}
		if !found {
			r.t.Fatal("no resolved event for thread")
		}
		rawFound := false
		for _, raw := range events {
			row := raw.(map[string]any)
			a := row["activity"].(map[string]any)
			m, _ := a["message"].(map[string]any)
			if a["thread_id"] == thread && m["type"] == float64(46) {
				if row["subject"] != want {
					r.t.Fatal("raw capture subject mismatch")
				}
				rawFound = true
			}
		}
		if !rawFound {
			r.t.Fatal("independent subject capture missed gateway result")
		}
	}
	read := r.cli("read")
	r.save("read.json", read)
	readResults := map[string]bool{}
	for _, entry := range read["activity"].([]any) {
		a := entry.(map[string]any)
		m, _ := a["message"].(map[string]any)
		if m["type"] == float64(46) {
			readResults[a["thread_id"].(string)] = true
		}
	}
	if len(readResults) != 2 {
		r.t.Fatal("deck read missed gateway activity")
	}
	if read["unread"].(map[string]any)["total"] != float64(0) {
		r.t.Fatal("read did not report zero unread")
	}
	if inbox := r.cli("inbox"); inbox["unread"].(map[string]any)["total"] != float64(0) {
		r.t.Fatal("reading did not persist zero unread")
	}
	r.note("PASS: deck read shows gateway activity, then deck inbox independently reports zero unread")
	for _, post := range posts {
		thread := post["thread"].(map[string]any)["thread_id"].(string)
		answer := "Acceptance complete: I read the incoming activity and can answer through deck."
		result := r.cli("reply", thread, "--text", answer)
		id := result["message"].(map[string]any)["id"].(string)
		actual := r.discord("GET", "/channels/"+thread+"/messages/"+id, nil)
		if actual["content"] != answer {
			r.t.Fatal("deck answer missing from Discord")
		}
		r.note("PASS: answer read back from Discord: https://discord.com/channels/" + r.fixture["SERVER_ID"] + "/" + thread + "/" + id)
	}
	obs := r.observations(records)
	r.save("human-observations.json", obs)
	for assumption, proved := range obs {
		if proved {
			r.note("PASS: observed " + assumption)
		} else {
			r.note("UNVERIFIED: " + assumption + "; real poll-result messages stood in for a reply; no synthetic person/button/vote event is counted as live proof")
		}
	}
}

func (r *run) observations(path string) map[string]bool {
	result := map[string]bool{"person-replies-arrive-with-content": false, "component-interactions-reach-the-gateway": false, "poll-votes-reach-the-gateway-forum-add-remove": false, "poll-votes-reach-the-gateway-text-add-remove": false}
	votes := map[string]map[bool]bool{}
	for _, row := range readRecords(path) {
		a := row["event"].(map[string]any)
		author, _ := a["author"].(map[string]any)
		m, _ := a["message"].(map[string]any)
		if author != nil && author["bot"] != true && author["webhook_id"] == nil && m != nil && m["content"] != "" && m["content"] != nil {
			result["person-replies-arrive-with-content"] = true
		}
		if a["component"] != nil {
			result["component-interactions-reach-the-gateway"] = true
		}
		if vote, ok := a["poll_vote"].(map[string]any); ok {
			channel := a["channel_id"].(string)
			if votes[channel] == nil {
				votes[channel] = map[bool]bool{}
			}
			votes[channel][vote["removed"] == true] = true
		}
	}
	for _, kind := range []string{"forum", "text"} {
		channel := r.fixture["TEST_FORUM"]
		if kind == "text" {
			channel = r.fixture["TEST_CHANNEL"]
		}
		result["poll-votes-reach-the-gateway-"+kind+"-add-remove"] = votes[channel][false] && votes[channel][true]
	}
	return result
}

func (r *run) cli(args ...string) map[string]any {
	args = append(args, "--config", r.client, "--session", "T3CODE_THREAD_ID="+r.session, "--json")
	out := r.command(nil, r.deck, args...)
	var value map[string]any
	if json.Unmarshal(out, &value) != nil {
		r.t.Fatal("CLI returned invalid JSON")
	}
	return value
}
func (r *run) discord(method, path string, data any) map[string]any {
	return r.request(method, "https://discord.com/api/v10"+path, r.fixture["BOT_TOKEN"], data, true)
}
func (r *run) request(method, url, token string, data any, discord bool) map[string]any {
	var body io.Reader
	if data != nil {
		raw, _ := json.Marshal(data)
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		r.t.Fatal("invalid HTTP request")
	}
	req.Header.Set("Content-Type", "application/json")
	if discord {
		req.Header.Set("Authorization", "Bot "+token)
	} else {
		req.Header.Set("X-Vault-Token", token)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		r.t.Fatal("HTTP transport failed (upstream details suppressed)")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.t.Fatalf("%s %s returned HTTP %d (body suppressed)", method, req.URL.Path, resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		r.t.Fatal(err)
	}
	if len(raw) == 0 {
		return map[string]any{}
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		r.t.Fatal("invalid HTTP response JSON")
	}
	if list, ok := value.([]any); ok {
		return map[string]any{"items": list}
	}
	return value.(map[string]any)
}
func (r *run) command(input []byte, name string, args ...string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = r.root
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	r.note("$ " + filepath.Base(name) + " " + strings.Join(args, " ") + "\n" + r.safe(string(out)) + r.safe(stderr.String()))
	if err != nil {
		r.t.Fatalf("command %s failed: %v", filepath.Base(name), err)
	}
	return out
}
func (r *run) start(logPath, name string, args ...string) {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		r.t.Fatal(err)
	}
	cmd := exec.Command(name, args...)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		f.Close()
		r.t.Fatal("could not start", name)
	}
	key := "process:" + strconv.Itoa(cmd.Process.Pid)
	r.ledger("acquire", key, "--cleanup", fmt.Sprintf("kill -- -%d 2>/dev/null || true", cmd.Process.Pid))
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	r.t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		f.Close()
		r.ledger("release", key)
		if raw, err := os.ReadFile(logPath); err == nil {
			r.save(filepath.Base(logPath)+".json", r.safe(string(raw)))
		}
	})
}
func (r *run) cleanup(name string, args ...string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, name, args...).CombinedOutput(); err != nil {
		r.t.Errorf("cleanup %s failed: %s", name, r.safe(string(out)))
		return false
	}
	return true
}
func (r *run) ledger(args ...string) {
	if os.Getenv("T3CODE_THREAD_ID") == "" {
		return
	}
	out, err := exec.Command("ledger", args...).CombinedOutput()
	if err != nil {
		r.t.Errorf("ledger failed: %s", r.safe(string(out)))
	}
}
func (r *run) until(label string, timeout time.Duration, f func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.t.Fatal("timed out waiting for " + label)
}
func (r *run) safe(s string) string {
	for _, secret := range r.secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	return s
}
func (r *run) note(s string) {
	s = r.safe(s)
	r.mu.Lock()
	r.transcript = append(r.transcript, s)
	r.mu.Unlock()
	r.t.Log(s)
}
func (r *run) save(name string, value any) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	r.write(filepath.Join(r.evidence, name), []byte(r.safe(string(raw))+"\n"), 0600)
}
func (r *run) writeJSON(path string, value any, mode os.FileMode) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	r.write(path, raw, mode)
}
func (r *run) write(path string, raw []byte, mode os.FileMode) {
	if err := os.WriteFile(path, raw, mode); err != nil {
		r.t.Fatal(err)
	}
}
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
func randomID(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", raw)
}
func readRecords(path string) []map[string]any {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	result := []map[string]any{}
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		var row map[string]any
		if json.Unmarshal(line, &row) == nil {
			result = append(result, row)
		}
	}
	return result
}
