#!/usr/bin/env python3
"""Restore every source mutation and require the corresponding behavior test to fail.

Run alone in an implementation worktree. A compile failure is not evidence that
its guard works. Each probe runs only its named behavior test; this is targeted
mutation evidence, not a claim of exhaustive coverage. The final PASS marker is
emitted only after every probe ends.
"""
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
helpers = runpy.run_path(str(ROOT / 'scripts/prove-guards.py'))
mutate, require_pass = helpers['mutate'], helpers['require_pass']
require_pass(['go', 'test', '-count=1', './internal/service/...', './cmd/deck/...'])


completed = 0

def probe_source(name, path, before, after, package, test):
    global completed
    mutate(name, path, before, after,
           ['go', 'test', '-count=1', '-timeout=20s', '-run', '^' + test + '$', package])
    completed += 1


def probe(name, path, before, after, package, test):
    probe_source(name, 'internal/service/' + path, before, after, package, test)


probe('Discord nonce includes sending fingerprint', 'discord/content.go',
      'fingerprint + ":" + nonce', 'nonce', './internal/service/discord', 'TestNonceFingerprintScope')
probe('registry check precedes posting', 'send.go',
      'if h.Registry.Check(run) != nil {', 'if false {',
      './internal/service', 'TestRegistryUnavailablePreventsPost')
probe('unknown outcomes keep the pending touch', 'registry/registry.go',
      '!success && !a.unknown && a.removable', '!success && a.removable',
      './internal/service/registry', 'TestReplyStatesAndBoundary')
probe('failed persistence cannot mutate an existing touch', 'registry/registry.go',
      'e = clone(old)\n\t}\n\tif e.Keys == nil {', 'e = old\n\t}\n\tif e.Keys == nil {',
      './internal/service/registry', 'TestPersistenceRetryAndFailedBegin')
probe('lost forum response is not retried', 'send.go',
      'post, err := client.Post(restCtx, r.Channel.ChannelId, d.ThreadChannelPostCreate{Name: r.Title, Message: message, AppliedTags: tags})',
      'post, err := client.Post(restCtx, r.Channel.ChannelId, d.ThreadChannelPostCreate{Name: r.Title, Message: message, AppliedTags: tags}); if err != nil {post,err=client.Post(restCtx,r.Channel.ChannelId,d.ThreadChannelPostCreate{Name:r.Title,Message:message,AppliedTags:tags})}',
      './internal/service', 'TestLostAnswersKeepReplyTouch')
probe('multiple-bot empty references do not disclose touches', 'handler.go',
      'if len(bots) != 1 {', 'if false {',
      './internal/service', 'TestGrantOrderAndThreadState')
probe('Discord response bodies stay out of API errors', 'discord/errors.go',
      '"Discord permission missing for this operation"', '"Discord permission missing for this operation: "+r.Message',
      './internal/service/discord', 'TestSanitizedErrors')
probe('mentions never parse everyone or here', 'discord/content.go',
      'Parse: []d.AllowedMentionType{}', 'Parse: []d.AllowedMentionType{d.AllowedMentionTypeEveryone}',
      './internal/service', 'TestContentValidationAndConversion')
probe('events exclude API-only sequence', 'activity/publisher.go',
      'copy.Sequence = 0', 'copy.Sequence = a.Sequence',
      './internal/service/activity', 'TestPublishRetryAndEnvelope')
probe('thread recovery boundary precedes the first reply', 'handler.go',
      'e.LastMessageID = uint64(*message.LastMessageID())', 'e.LastMessageID = uint64(ch.ID())',
      './internal/service', 'TestReplyInFlightAndBoundary')
probe('post poll is a second message', 'send.go',
      'message.Poll = nil', '// mutation: keep the poll on the starter',
      './internal/service', 'TestForumPollAndHeaders')
probe('flight persists until the index can find its publication', 'send.go',
      'if err = h.Index.Confirm(run, &CompletedSend{Thread: out.Thread, Messages: messages, Position: position, PollError: out.PollError}); err != nil {',
      'if false {', './internal/service', 'TestRepeatWaitsForIndexAndRestoresPollFailure')
probe('partial poll failures survive event replay', 'activity/publisher.go',
      'm.Header.Set("Deck-Poll-Error", metadata[0].PollError)',
      'm.Header.Set("Deck-Poll-ErroX", metadata[0].PollError)',
      './internal/service', 'TestRepeatWaitsForIndexAndRestoresPollFailure')
probe('completed sends refresh metadata and recheck their own grants', 'handler.go',
      'if c.Thread == nil {', 'return c.Thread,nil; if c.Thread == nil {',
      './internal/service', 'TestRepeatWaitsForIndexAndRestoresPollFailure')

probe('configured ungranted bot is refused before metadata', 'handler.go',
      'func botGranted(k config.Key, bot string) bool {',
      'func botGranted(k config.Key, bot string) bool { return true;',
      './internal/service', 'TestConfiguredUngrantedBotBeforeMetadata')
probe('metadata never precedes bot selection', 'handler.go',
      'var existing registry.Entry',
      'if client:=h.bots[bot];client!=nil { _,_=client.Channel(ctx,id) }; var existing registry.Entry',
      './internal/service', 'TestConfiguredUngrantedBotBeforeMetadata')
probe('explicit channel grants refuse sibling channels', 'handler.go',
      'ch == "*" || ch == strconv.FormatUint(e.ChannelID, 10)', 'ch == "*" || true',
      './internal/service', 'TestExplicitChannelRefusals')
probe('separate known failure retains earlier unknown touch', 'registry/registry.go',
      '!success && !a.unknown && a.removable', '!success && !a.unknown',
      './internal/service/registry', 'TestReplyStatesAndBoundary')
probe('each bot reads its configured token reference', 'runtime.go',
      'secrets.ReadSecret(ctx, where, b.Token)', 'secrets.ReadSecret(ctx, where, cfg.Bots[0].Token)',
      './internal/service', 'TestRuntimeVaultPathsAndSecretIsolation')
probe('publisher retries its own returned publish errors', 'activity/publisher.go',
      'if delay < 2*time.Second {',
      'p.mu.Lock();p.queue=p.queue[1:];p.mu.Unlock();break; if delay < 2*time.Second {',
      './internal/service/activity', 'TestPublishRetryAndEnvelope')
probe('text-channel post enforces derived nonce', 'send.go',
      'message.Nonce = discord.Nonce(h.fingerprints[k.Name], r.Nonce)\n\t\t\tmessage.EnforceNonce = true',
      '// mutation: omit text-channel nonce',
      './internal/service', 'TestExistingTextThread')
probe('forum poll has separately derived nonce', 'send.go',
      'r.Nonce+":poll"', 'r.Nonce',
      './internal/service', 'TestForumPollAndHeaders')
probe('unmapped REST errors never expose upstream messages', 'discord/errors.go',
      'if r != nil {\n\t\treturn status.Error(codes.Internal, "Discord operation failed")',
      'if r != nil {\n\t\treturn status.Error(codes.Internal, "Discord operation failed: "+r.Message)',
      './internal/service/discord', 'TestSanitizedErrors')
probe('registry durable sent touch wins over stale pending', 'registry/registry.go',
      't.State == "sent" && current.State != "sent"',
      't.State == "sent" && current.State != "sent" && false',
      './internal/service/registry', 'TestDurableMergePrecedence')
probe('registry durable higher boundary wins over stale local', 'registry/registry.go',
      'if previous.LastMessageID > e.LastMessageID {', 'if false {',
      './internal/service/registry', 'TestDurableMergePrecedence')
probe('first-reply ownership race has a definitive refusal', 'send.go',
      'if errors.As(err, &conflict) {', 'if errors.As(err, &conflict) && false {',
      './internal/service', 'TestReplyBotOwnershipRace')
probe('generic errors never expose underlying messages', 'discord/errors.go',
      '\n\treturn status.Error(codes.Internal, "Discord operation failed")\n}',
      '\n\treturn status.Error(codes.Internal, "Discord operation failed: "+err.Error())\n}',
      './internal/service/discord', 'TestSanitizedErrors')
probe('text-channel nonce enforcement cannot be omitted', 'send.go',
      'message.EnforceNonce = true\n\t\t\tsent, err = client.Send',
      'message.EnforceNonce = false\n\t\t\tsent, err = client.Send',
      './internal/service', 'TestExistingTextThread')
probe('forum poll nonce enforcement cannot be omitted', 'send.go',
      'r.Nonce+":poll"), EnforceNonce: true', 'r.Nonce+":poll"), EnforceNonce: false',
      './internal/service', 'TestForumPollAndHeaders')
probe('target REST client belongs to the chosen bot', 'handler.go',
      'client := h.bots[bot]\n\tch, err := client.Channel(ctx, id)',
      'client := h.bots["a"]\n\tch, err := client.Channel(ctx, id)',
      './internal/service', 'TestRequestsUseGrantedBotClient')
probe('channel listing REST client belongs to the granted bot', 'handler.go',
      'client := h.bots[grant.Bot]', 'client := h.bots["a"]',
      './internal/service', 'TestRequestsUseGrantedBotClient')
probe('GetThread returns the complete current thread answer', 'handler.go',
      'return &pb.GetThreadResponse{Thread: threadValue(ch, e, k.Name)}, nil',
      '_ = e; return &pb.GetThreadResponse{Thread: &pb.Thread{ThreadId:uint64(ch.ID())}},nil',
      './internal/service', 'TestGetThreadAnswer')
probe('ListChannels excludes channels that cannot take posts', 'handler.go',
      'if ch.Type() != 0 && ch.Type() != 5 && ch.Type() != 15 && ch.Type() != 16 {',
      'if false {', './internal/service', 'TestListChannelsAnswer')
probe('ListChannels returns channel type', 'handler.go',
      'Type: uint32(ch.Type())', 'Type: 0', './internal/service', 'TestListChannelsAnswer')
probe('ListChannels returns forum tags', 'handler.go',
      'c.Tags = append(c.Tags, &pb.ForumTag{Id: uint64(tag.ID), Name: tag.Name})',
      '_ = tag', './internal/service', 'TestListChannelsAnswer')
probe('ListChannels returns required-tag flag', 'handler.go',
      'c.RequiresTag = requires', 'c.RequiresTag = requires && false',
      './internal/service', 'TestListChannelsAnswer')
probe('metadata outages retain unavailable classification', 'handler.go',
      'return nil, nil, existing, discord.MetadataError(err)', 'return nil, nil, existing, denied()',
      './internal/service', 'TestMetadataReadErrors')
probe('non-guild metadata uses the uniform refusal', 'discord/errors.go',
      'errors.Is(err, errNotGuild) ||', 'errors.Is(err, errNotGuild) && false ||',
      './internal/service', 'TestMetadataReadErrors')
probe('empty bot does not disclose an unreachable touched thread', 'handler.go',
      'if bot == "" && !reachable {', 'if false {',
      './internal/service', 'TestEmptyBotTouchedAndSingleBot')
probe('single-bot grants accept empty bot references', 'handler.go',
      'if len(bots) != 1 {', 'if len(bots) != 1 || true {',
      './internal/service', 'TestEmptyBotTouchedAndSingleBot')
probe('flight identity includes the key fingerprint', 'send.go',
      'id := h.fingerprints[k.Name] + ":" + nonce', 'id := nonce',
      './internal/service', 'TestFlightsSeparateKeyFingerprints')
probe('a flight refuses another operation', 'send.go',
      'if f.operation != operation {', 'if false {',
      './internal/service', 'TestFlightRejectsDifferentOperation')
probe('failed flights are removed so a repeat can send', 'send.go',
      'delete(h.flights, id)', '_ = id',
      './internal/service', 'TestFailedFlightCanRetry')
probe('completed Reply returns the retained result without sending', 'send.go',
      'if completed != nil && len(completed.Messages) > 0 {', 'if false {',
      './internal/service', 'TestCompletedReplyReplay')
probe('completed Reply refreshes metadata and rechecks actual thread grants', 'handler.go',
      'if c.Thread == nil {', 'return c.Thread,nil; if c.Thread == nil {',
      './internal/service', 'TestCompletedReplyReplay')
probe('publisher continues beyond four returned publish errors', 'activity/publisher.go',
      'if delay < 2*time.Second {',
      'if delay >= 400*time.Millisecond { q.done<-Result{Err:err};close(q.done);p.mu.Lock();p.queue=p.queue[1:];p.mu.Unlock();break }; if delay < 2*time.Second {',
      './internal/service/activity', 'TestPublishRetryAndEnvelope')
probe('history full-page probe exposes more results', 'handler.go',
      'out.More = len(more) > 0', 'out.More = len(more) > 0 && false',
      './internal/service', 'TestHistoryMoreAndBeforeCursor')
probe('history before probe uses the oldest page message', 'handler.go',
      'before = uint64(messages[0].ID)', 'before = 0',
      './internal/service', 'TestHistoryMoreAndBeforeCursor')
probe('history REST page passes the requested before cursor', 'discord/client.go',
      'snowflake.ID(before), snowflake.ID(after), limit',
      'snowflake.ID(0), snowflake.ID(after), limit',
      './internal/service', 'TestHistoryMoreAndBeforeCursor')
probe('known StartThread failures delete the starter', 'send.go',
      'if !discord.Unknown(err) {', 'if false {',
      './internal/service', 'TestStarterCleanupAfterThreadFailure')
probe('unknown StartThread outcomes retain the starter', 'send.go',
      'if !discord.Unknown(err) {', 'if true {',
      './internal/service', 'TestStarterCleanupAfterThreadFailure')
probe('position lookup failure finishes the pending reply attempt', 'send.go',
      'h.Registry.FinishReply(id, k.Name, false, false)\n\t\t\treturn nil, unavailable()',
      'return nil, unavailable()',
      './internal/service', 'TestReplyPositionFailureCleansTouch')
probe('metadata HTTP refusals take precedence over Discord error codes', 'discord/errors.go',
      'errors.As(err, &r) && r.Response != nil && (r.Response.StatusCode == 403 || r.Response.StatusCode == 404)',
      'errors.As(err, &r) && false', './internal/service', 'TestMetadataReadErrors')
probe('real SSH handler enforces explicit channel grants', 'handler.go',
      'ch == "*" || ch == strconv.FormatUint(e.ChannelID, 10)', 'ch == "*" || true',
      './internal/service', 'TestSSHHandlerGrants')
probe_source('service command handles native SIGTERM', 'cmd/deck/service_start.go',
      'signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)',
      'signal.NotifyContext(ctx, os.Interrupt, syscall.SIGINT)',
      './cmd/deck', 'TestServiceStartSignal')
probe('transport joins handlers before runtime cancellation', 'startup.go',
      'context.WithCancel(context.WithoutCancel(ctx))', 'context.WithCancel(ctx)',
      './internal/service', 'TestServiceSIGTERM')
probe('adapter closes runtime after transport stops', 'startup.go',
      'defer r.Close()', '_ = r', './internal/service', 'TestServiceSIGTERM')
probe('startup secret reads observe cancellation', 'startup.go',
      'stopStartup := context.AfterFunc(ctx, cancel)',
      'stopStartup := context.AfterFunc(context.Background(), cancel)',
      './internal/service', 'TestServiceStartupCancellation')
probe('real SSH unlisted Post refuses before sending, independently', 'handler.go',
      'ch == "*" || ch == strconv.FormatUint(e.ChannelID, 10)', 'ch == "*" || true',
      './internal/service', 'TestSSHHandlerGrants/unlisted/Post')
probe('bot-token startup read observes cancellation', 'runtime.go',
      'secrets.ReadSecret(ctx, where, b.Token)', 'secrets.ReadSecret(context.Background(), where, b.Token)',
      './internal/service', 'TestServiceStartupCancellation')
probe_source('startup signal returns success rather than a startup error', 'cmd/deck/service_start.go',
      'if ctx.Err() != nil && (err == nil || errors.Is(err, ctx.Err())) {', 'if false {', './cmd/deck', 'TestServiceStartSignal')
probe_source('second signal regains its default action', 'cmd/deck/service_start.go',
      'context.AfterFunc(ctx, stop)', '_ = ctx', './cmd/deck', 'TestServiceStartSecondSignal')
probe_source('a real startup failure after a signal is still reported', 'cmd/deck/service_start.go',
      'if ctx.Err() != nil && (err == nil || errors.Is(err, ctx.Err())) {',
      'if ctx.Err() != nil {', './cmd/deck', 'TestServiceStartRealFailureAfterSignal')
probe('sanitized startup failures preserve cancellation identity', 'runtime.go',
      'if errors.Is(cause, context.Canceled) {', 'if false {',
      './cmd/deck', 'TestServiceStartSignal')
probe('bucket open preserves native startup cancellation', 'registry/registry.go',
      'bucketFailure("nats.threads: cannot open bucket", err)',
      'errors.New("nats.threads: cannot open bucket")',
      './cmd/deck', 'TestServiceStartSignal/registry')
probe('bucket status preserves cancellation', 'registry/registry.go',
      'bucketFailure("nats.threads: cannot check bucket", err)',
      'errors.New("nats.threads: cannot check bucket")',
      './internal/service/registry', 'TestOpenStartupErrors')
probe('bucket errors remain failures after a concurrent stop', 'registry/registry.go',
      'if errors.Is(cause, context.Canceled) {', 'if true {',
      './internal/service/registry', 'TestOpenStartupErrors')
probe('bucket errors retain deadline identity', 'registry/registry.go',
      'if errors.Is(cause, context.DeadlineExceeded) {', 'if false {',
      './internal/service/registry', 'TestOpenStartupErrors')
probe('real Run-path secret failures remain failures after a stop', 'runtime.go',
      'if errors.Is(cause, context.Canceled) {', 'if true {',
      './internal/service', 'TestServiceStartupIndependentFailure')
print(f'PASS all {completed} targeted service mutation probes completed', flush=True)
