#!/usr/bin/env python3
"""Restore every source mutation and require the corresponding behavior test to fail.

Run alone in an implementation worktree. A compile failure is not evidence that
its guard works. The final PASS marker is emitted only after every probe ends.
"""
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
helpers = runpy.run_path(str(ROOT / 'scripts/prove-guards.py'))
mutate, require_pass = helpers['mutate'], helpers['require_pass']
require_pass(['go', 'test', '-count=1', './internal/service/...'])


completed = 0

def probe(name, path, before, after, package, test):
    global completed
    mutate(name, 'internal/service/' + path, before, after,
           ['go', 'test', '-count=1', '-timeout=20s', '-run', '^' + test + '$', package])
    completed += 1


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
print(f'PASS all {completed} service guard probes completed', flush=True)
