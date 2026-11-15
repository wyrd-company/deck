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


def probe(name, path, before, after, package, test):
    mutate(name, 'internal/service/' + path, before, after,
           ['go', 'test', '-count=1', '-timeout=20s', '-run', '^' + test + '$', package])


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
print('PASS all 14 service guard probes completed', flush=True)
