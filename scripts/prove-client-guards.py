#!/usr/bin/env python3
"""Prove client guards by restoring each deliberate mutation before advancing.

Run alone in an implementation worktree. Every mutation must fail a meaningful
assertion, never merely compilation. The final marker appears only on completion.
"""
from pathlib import Path
import runpy

helpers = runpy.run_path(str(Path(__file__).with_name('prove-guards.py')))
mutate = helpers['mutate']
helpers['require_pass'](['go', 'test', '-count=1', './internal/cli', './internal/store'])
probes = [
    ('pre-RPC durable send record', 'internal/cli/commands.go',
     'if err = i.client.Store.Pending(i.client.Session, i.client.Fingerprint, nonce, i.flags.command, threadID); err != nil {',
     '_ = threadID; if err = i.client.Store.Session(i.client.Session); err != nil {',
     ['go', 'test', '-count=1', '-run', '^TestPostReplyPreRecordAndCursor$', './internal/cli']),
    ('no automatic send repeat', 'internal/cli/commands.go',
     'r, e := i.client.API.Post(call, &pb.PostRequest{Channel: channel, Title: i.flags.one("title"), Tags: i.flags.values["tag"], Content: content, Nonce: nonce})',
     'r, e := i.client.API.Post(call, &pb.PostRequest{Channel: channel, Title: i.flags.one("title"), Tags: i.flags.values["tag"], Content: content, Nonce: nonce}); if e != nil { r,e = i.client.API.Post(call, &pb.PostRequest{Channel:channel,Title:i.flags.one("title"),Content:content,Nonce:nonce}) }',
     ['go', 'test', '-count=1', '-run', '^TestUnknownSendNoRetryAndExplicitNonce$', './internal/cli']),
    ('unknown send outcome', 'internal/cli/errors.go',
     'func unknownSend(err error) bool {', 'func unknownSend(err error) bool { return false;',
     ['go', 'test', '-count=1', '-run', '^TestUnknownSendNoRetryAndExplicitNonce$', './internal/cli']),
    ('cursor crosses epochs in either direction', 'internal/store/store.go',
     'cursor_epoch <> ?', 'cursor_epoch > ?',
     ['go', 'test', '-count=1', '-run', '^TestCursorEpochAndConcurrentReaders$', './internal/store']),
    ('cursor does not regress in same epoch', 'internal/store/store.go',
     'cursor_sequence < ?', 'cursor_sequence <> ?',
     ['go', 'test', '-count=1', '-run', '^TestCursorEpochAndConcurrentReaders$', './internal/store']),
    ('each own counted copy subtracted', 'internal/client/client.go',
     'if own && n > 0 {', 'if false && own && n > 0 {',
     ['go', 'test', '-count=1', '-run', '^TestRotationRenameAndSameNonce$', './internal/cli']),
    ('own match includes fingerprint', 'internal/store/store.go',
     'WHERE session_key=? AND key_fingerprint=? AND nonce=?',
     'WHERE session_key=? AND (key_fingerprint=? OR 1=1) AND nonce=?',
     ['go', 'test', '-count=1', '-run', '^TestRotationRenameAndSameNonce$', './internal/cli']),
    ('read empty response advances through', 'internal/cli/read.go',
     'if !i.flags.has("keep-unread") && len(ids) > 0 {',
     'if !i.flags.has("keep-unread") && len(ids) > 0 && len(kept) > 0 {',
     ['go', 'test', '-count=1', '-run', '^TestReadThroughEmptyAndKeepUnread$', './internal/cli']),
    ('keep unread preserves cursor', 'internal/cli/read.go',
     'if !i.flags.has("keep-unread") && len(ids) > 0 {',
     'if len(ids) > 0 {',
     ['go', 'test', '-count=1', '-run', '^TestReadThroughEmptyAndKeepUnread$', './internal/cli']),
    ('resolve pending send excluded', 'internal/store/store.go',
     'key, nonce = event.SenderKey, event.Nonce', 'key, nonce = "", event.Nonce',
     ['go', 'test', '-count=1', '-run', '^TestResolvePendingSuppressesExistingSession$', './internal/cli']),
    ('channel deny wins', 'internal/client/client.go',
     'for _, ref := range c.Channels.Deny {', 'for _, ref := range []string{} {',
     ['go', 'test', '-count=1', '-run', '^TestAllowDenyBeforeCalls$', './internal/cli']),
]
probes.extend([
    ('no automatic reply repeat', 'internal/cli/commands.go',
     'r, e := i.client.API.Reply(call, &pb.ReplyRequest{Thread: thread, Content: content, ReplyToMessageId: replyTo, Nonce: nonce})',
     'r, e := i.client.API.Reply(call, &pb.ReplyRequest{Thread: thread, Content: content, ReplyToMessageId: replyTo, Nonce: nonce}); if e != nil {r,e=i.client.API.Reply(call,&pb.ReplyRequest{Thread:thread,Content:content,Nonce:nonce})}',
     ['go', 'test', '-count=1', '-run', '^TestUnknownSendNoRetryAndExplicitNonce/reply$', './internal/cli']),
    ('own matches only within its session', 'internal/store/store.go',
     'WHERE session_key=? AND key_fingerprint=? AND nonce=?',
     'WHERE (session_key=? OR 1=1) AND key_fingerprint=? AND nonce=?',
     ['go', 'test', '-count=1', '-run', '^TestSameKeyDifferentSessionsSeeEachOthersSends$', './internal/cli']),
    ('failed and unknown send states are persisted', 'internal/store/store.go',
     'func (s *Store) SendState(key, nonce, state string) error {',
     'func (s *Store) SendState(key, nonce, state string) error { return nil;',
     ['go', 'test', '-count=1', '-run', '^TestSendStatesAndFailedRepeat$', './internal/cli']),
    ('successful send transitions out of pending', 'internal/store/store.go',
     "UPDATE sends SET state='sent',thread_id", "UPDATE sends SET state='pending',thread_id",
     ['go', 'test', '-count=1', '-run', '^TestSendStatesAndFailedRepeat$', './internal/cli']),
    ('failed repeat preserves a successful send', 'internal/store/store.go',
     " AND state <> 'sent'", '',
     ['go', 'test', '-count=1', '-run', '^TestSendStatesAndFailedRepeat$', './internal/cli']),
    ('remote listen deadline precedes local timer', 'internal/cli/read.go',
     'status.Code(err) == codes.DeadlineExceeded || ', '',
     ['go', 'test', '-count=1', '-run', '^TestListenRemoteDeadlineBeforeLocalTimer$', './internal/cli']),
    ('bot-qualified deny blocks botless target', 'internal/client/client.go',
     ' || deny && bot == ""', '',
     ['go', 'test', '-count=1', '-run', '^TestBotQualifiedDenyOnUnqualifiedPost$', './internal/cli']),
    ('present empty allow blocks every channel', 'internal/client/client.go',
     'if c.Channels.Allow == nil {', 'if len(c.Channels.Allow) == 0 {',
     ['go', 'test', '-count=1', '-run', '^TestPresentEmptyAllowRefusesBeforeDial$', './internal/cli']),
])
probes.extend([
    ('SSH record bound to server-authenticated key', 'internal/cli/ssh_test.go',
     'Fingerprint: fingerprint,', 'Fingerprint: keyB,',
     ['go', 'test', '-count=1', '-run', '^TestCommandsOverSSH$', './internal/cli']),
    ('terminal controls escaped', 'internal/cli/text.go',
     'unicode.IsControl(r) || unicode.Is(unicode.Cf, r)', 'false && (unicode.IsControl(r) || unicode.Is(unicode.Cf, r))',
     ['go', 'test', '-count=1', '-run', '^TestTextEscapesUntrustedControls$', './internal/cli']),
    ('resolve accepts store creation window', 'internal/store/store.go',
     'if s.empty {', 'if false {',
     ['go', 'test', '-count=1', '-run', '^TestResolveWhileStoreIsBeingCreated$', './internal/store']),
    ('session config diagnostics name file', 'internal/cli/cli.go',
     'lines = append(lines, path+": "+field.Error())', 'lines = append(lines, field.Error())',
     ['go', 'test', '-count=1', '-run', '^TestSessionConfigurationErrorsNameFileAndFix$', './internal/cli']),
])
for probe in probes:
    mutate(*probe)
print(f'PASS all {len(probes)} client guard probes completed', flush=True)
