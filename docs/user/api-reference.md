---
title: gRPC API reference
product: deck
---

# gRPC API reference

Generated from the service definition. [Download deck.proto](definitions/deck/v1/deck.proto).

Uses the standard protobuf `google/protobuf/struct.proto` and `google/protobuf/timestamp.proto` imports.

## DeckService

```protobuf
service DeckService {
  // ListChannels returns every channel the calling key's grants cover whose
  // type takes posts (0 text, 5 announcement, 15 forum, 16 media).
  rpc ListChannels(ListChannelsRequest) returns (ListChannelsResponse);

  // Post creates a thread: a forum post in a forum or media channel, or a
  // message and a public thread started from it in a text or announcement
  // channel. The thread is recorded as touched by the calling key, and its
  // first message is published, before the call returns.
  rpc Post(PostRequest) returns (PostResponse);

  // Reply sends a message into a thread whose parent channel is granted. The
  // calling key is recorded as having touched the thread before the message
  // is sent.
  rpc Reply(ReplyRequest) returns (ReplyResponse);

  // GetThread returns a thread's metadata. Any thread whose parent channel is
  // granted can be read; reading does not touch it.
  rpc GetThread(GetThreadRequest) returns (GetThreadResponse);

  // ListHistory returns a thread's messages from Discord. Any thread whose
  // parent channel is granted can be read; reading does not touch it.
  rpc ListHistory(ListHistoryRequest) returns (ListHistoryResponse);

  // CountUnread counts, for each cursor, the activity in its thread after the
  // cursor that is not a system message, and lists own messages among it in
  // ThreadCount.own. It reads only the activity index.
  rpc CountUnread(CountUnreadRequest) returns (CountUnreadResponse);

  // ListUnread returns the activity CountUnread counts, in stream order.
  rpc ListUnread(ListUnreadRequest) returns (ListUnreadResponse);

  // Listen sends one response at once with every thread's state and any
  // unread activity, then one response for each further activity that is
  // published in the threads, until the client ends the call.
  rpc Listen(ListenRequest) returns (stream ListenResponse);
}
```

## ChannelRef

```protobuf
// ChannelRef names a channel. The bot may be empty when the calling key's
// grants name exactly one bot.
message ChannelRef {
  string bot = 1;
  uint64 channel_id = 2;
}
```

## ThreadRef

```protobuf
// ThreadRef names a thread. A touched thread has one bot, the one that first
// touched it through deck, and every call on it uses that bot: the bot may be
// empty, and naming another bot is INVALID_ARGUMENT for a key granted the
// thread through its bot (PERMISSION_DENIED for any other key). For a thread
// not touched the bot may be empty when the calling key's grants name exactly
// one bot.
message ThreadRef {
  string bot = 1;
  uint64 thread_id = 2;
}
```

## Channel

```protobuf
message Channel {
  string bot = 1;
  uint64 server_id = 2;
  string server_name = 3;
  uint64 channel_id = 4;
  string name = 5;
  // Discord's channel type: 0 text, 5 announcement, 15 forum, 16 media.
  uint32 type = 6;
  // The tags a forum or media channel offers for posts.
  repeated ForumTag tags = 7;
  // Whether a post in this forum or media channel needs at least one tag.
  bool requires_tag = 8;
}
```

## ForumTag

```protobuf
message ForumTag {
  uint64 id = 1;
  string name = 2;
}
```

## ListChannelsRequest

```protobuf
message ListChannelsRequest {}
```

## ListChannelsResponse

```protobuf
message ListChannelsResponse {
  repeated Channel channels = 1;
}
```

## Content

```protobuf
// Content is what a post's first message or a reply carries. At least one of
// text, embeds, components, poll and files is set.
message Content {
  // Discord markdown. At most 2000 characters, or 4096 with a persona.
  string text = 1;
  // Users and roles to mention. The service writes each mention at the start
  // of the message content and allows exactly these in allowed_mentions;
  // everyone and here are never allowed.
  repeated uint64 mention_user_ids = 2;
  repeated uint64 mention_role_ids = 3;
  // Discord embed objects, as Discord's API defines them. At most 10, or 9
  // with a persona.
  repeated google.protobuf.Struct embeds = 4;
  // Discord top-level component objects, as Discord's API defines them. A
  // list that holds a Components V2 layout component (type 9 or above) is
  // sent with the IS_COMPONENTS_V2 flag and needs empty text, embeds, poll
  // and persona.
  repeated google.protobuf.Struct components = 5;
  PollCreate poll = 6;
  // At most 10 files.
  repeated File files = 7;
  // When set, the text becomes the description of a leading embed whose
  // author is the persona.
  Persona persona = 8;
}
```

## PollCreate

```protobuf
message PollCreate {
  // At most 300 characters.
  string question = 1;
  // 1 to 10 answers of at most 55 characters each.
  repeated string answers = 2;
  // 1 to 768; zero means 24.
  uint32 duration_hours = 3;
  bool allow_multiselect = 4;
}
```

## File

```protobuf
message File {
  // The file name Discord shows; the attachment's URL ends with it.
  string name = 1;
  bytes data = 2;
  // Alt text, at most 1024 characters.
  string description = 3;
}
```

## Persona

```protobuf
message Persona {
  // At most 256 characters.
  string name = 1;
  // An http or https URL.
  string icon_url = 2;
}
```

## PostRequest

```protobuf
message PostRequest {
  ChannelRef channel = 1;
  // The thread's name: 1 to 100 characters.
  string title = 2;
  // Forum tags by name or by snowflake in decimal, for forum and media
  // channels only; at most 5.
  repeated string tags = 3;
  Content content = 4;
  // The client's identifier for this send: 1 to 25 letters and digits,
  // unique per send for its key. A repeat with the same key and nonce returns
  // the first send's result, and sends nothing, while that send is in flight
  // or the event stream holds a message it published; otherwise it sends.
  // The nonce travels on the published messages with the key's fingerprint,
  // so the client recognises its own messages by the two together.
  string nonce = 5;
}
```

## PostResponse

```protobuf
message PostResponse {
  Thread thread = 1;
  // The thread's first message, as published.
  Activity message = 2;
  // Where a cursor starts for the session that posted: the newest position
  // the service's activity index held before the send called Discord, so
  // everything published during the send is after it.
  StreamPosition position = 3;
  // In a forum or media channel, the poll, sent as the thread's second
  // message because Discord drops a poll from a forum post's starter.
  Activity poll_message = 4;
  // Set when the post succeeded and its poll could not be sent.
  string poll_error = 5;
}
```

## ReplyRequest

```protobuf
message ReplyRequest {
  ThreadRef thread = 1;
  Content content = 2;
  // When set, the reply is a Discord reply to this message in the thread.
  uint64 reply_to_message_id = 3;
  // As PostRequest.nonce. The service also sends Discord a nonce derived
  // from it and the key's fingerprint, with enforce_nonce, so a repeat that
  // Discord still remembers returns the first message.
  string nonce = 4;
}
```

## ReplyResponse

```protobuf
message ReplyResponse {
  Thread thread = 1;
  Activity message = 2;
  // As PostResponse.position.
  StreamPosition position = 3;
}
```

## StreamPosition

```protobuf
// StreamPosition is a place in the event stream. The epoch names the
// stream's generation: Unix nanoseconds when the service began following it.
// Each generation has its own epoch and its sequences start again. Positions
// of one epoch order by sequence; positions of different epochs do not order,
// and a client moves a cursor to a position of another epoch whenever the
// service reports one.
message StreamPosition {
  uint64 epoch = 1;
  uint64 sequence = 2;
}
```

## Thread

```protobuf
message Thread {
  string bot = 1;
  uint64 server_id = 2;
  // The thread's parent channel.
  uint64 channel_id = 3;
  uint64 thread_id = 4;
  string name = 5;
  bool archived = 6;
  bool locked = 7;
  uint64 last_message_id = 8;
  // Whether deck created the thread.
  bool created_by_deck = 9;
  // Whether the calling key touched the thread.
  bool touched = 10;
  // https://discord.com/channels/<server id>/<thread id>
  string url = 11;
}
```

## GetThreadRequest

```protobuf
message GetThreadRequest {
  ThreadRef thread = 1;
}
```

## GetThreadResponse

```protobuf
message GetThreadResponse {
  Thread thread = 1;
}
```

## ListHistoryRequest

```protobuf
message ListHistoryRequest {
  ThreadRef thread = 1;
  // At most one of after_id and before_id. With neither, the newest messages.
  uint64 after_id = 2;
  uint64 before_id = 3;
  // 1 to 100; zero means 50.
  uint32 limit = 4;
}
```

## ListHistoryResponse

```protobuf
message ListHistoryResponse {
  // Message activities, oldest first, with Discord's current content.
  repeated Activity activity = 1;
  // Whether Discord holds more messages beyond the ones returned, in the
  // direction of the cursor.
  bool more = 2;
}
```

## Cursor

```protobuf
// Cursor is a session's read position in one thread: the stream position of
// the newest activity the session has been shown there, or where its cursor
// started. Activity at a later position is after the cursor. A cursor from
// another epoch than the stream's has everything the stream holds after it.
message Cursor {
  uint64 thread_id = 1;
  StreamPosition after = 2;
}
```

## ThreadState

```protobuf
enum ThreadState {
  THREAD_STATE_UNSPECIFIED = 0;
  // The calling key touched the thread; its count is valid.
  THREAD_STATE_TOUCHED = 1;
  // The thread registry has no entry naming the calling key for the thread.
  THREAD_STATE_NOT_TOUCHED = 2;
  // The thread's parent channel is outside the calling key's grants.
  THREAD_STATE_NOT_GRANTED = 3;
}
```

## ThreadCount

```protobuf
message ThreadCount {
  uint64 thread_id = 1;
  ThreadState state = 2;
  // Activity after the cursor that counts, the calling key's own messages
  // included; zero unless the state is touched.
  uint32 count = 3;
  // One entry per counted copy of a deck-sent message: every counted
  // index entry with sender_key and nonce, whichever key or name sent it.
  repeated OwnMessage own = 4;
  // Whether the stream no longer holds everything published after the
  // cursor: the cursor is from another epoch, or the stream's oldest message
  // is later than the cursor. Older activity is then read with ListHistory.
  bool horizon_passed = 5;
}
```

## OwnMessage

```protobuf
// OwnMessage is one counted copy of a deck-sent message, from any key/name.
// The client subtracts it when sender_key and nonce together match a send
// recorded by the session, including a send made before rotation or rename.
message OwnMessage {
  uint64 id = 1;
  string nonce = 2;
  // The SHA256 fingerprint of the key that sent this message, as on
  // Activity.sender_key; it is not the caller's current fingerprint.
  string sender_key = 3;
}
```

## CountUnreadRequest

```protobuf
message CountUnreadRequest {
  repeated Cursor cursors = 1;
}
```

## CountUnreadResponse

```protobuf
message CountUnreadResponse {
  repeated ThreadCount threads = 1;
}
```

## ListUnreadRequest

```protobuf
message ListUnreadRequest {
  repeated Cursor cursors = 1;
  // At most this many activities across all threads, in stream order; zero
  // means 500.
  uint32 limit = 2;
}
```

## ListUnreadResponse

```protobuf
message ListUnreadResponse {
  repeated Activity activity = 1;
  // Each cursor's thread, with its count before this list.
  repeated ThreadCount threads = 2;
  // Whether unread activity remains beyond the limit.
  bool more = 3;
  // The position through which this response is complete for its threads:
  // the last listed activity's when `more` is set, otherwise the stream's
  // last position when the list was taken. Every activity's sequence in
  // this response has this epoch. A client that marks read moves each
  // cursor here.
  StreamPosition through = 4;
}
```

## ListenRequest

```protobuf
message ListenRequest {
  repeated Cursor cursors = 1;
}
```

## ListenResponse

```protobuf
message ListenResponse {
  repeated Activity activity = 1;
  // Set on the first response only.
  repeated ThreadCount threads = 2;
  // As ListUnreadResponse.through, for this response.
  StreamPosition through = 3;
}
```

## Activity

```protobuf
// Activity is one new thing in a touched thread, and the payload of every
// event on the stream, in its protojson form with proto field names.
message Activity {
  // The message's or interaction's snowflake, or for a poll vote one the
  // service makes from the millisecond it received the vote.
  uint64 id = 1;
  google.protobuf.Timestamp time = 2;
  // The name of the bot whose thread this is.
  string bot = 3;
  uint64 server_id = 4;
  // The thread's parent channel.
  uint64 channel_id = 5;
  uint64 thread_id = 6;
  User author = 7;
  // The name of the client key that sent the message through deck; empty for
  // everything deck did not send.
  string sent_by = 8;
  // The SHA256 fingerprint of that key, as `ssh-keygen -l` prints it, which
  // the client computes from its own key before it sends.
  string sender_key = 15;
  oneof kind {
    Message message = 10;
    ComponentUse component = 11;
    PollVote poll_vote = 12;
  }
  // The activity's sequence in the event stream, set in API responses for
  // activity the stream holds. Zero, and so absent, in event payloads, where
  // the stream's own metadata carries it.
  uint64 sequence = 13;
  // The client's nonce for a message deck sent; empty for everything else.
  // A message is a session's own when its sender_key and nonce match a send
  // the session recorded.
  string nonce = 14;
}
```

## User

```protobuf
message User {
  uint64 id = 1;
  string username = 2;
  // The user's global display name, when set.
  string display_name = 3;
  bool bot = 4;
  // Set when a webhook sent the message; the user is then the webhook.
  uint64 webhook_id = 5;
}
```

## Message

```protobuf
message Message {
  // Discord's message type: 0 default, 19 reply, 46 poll result, and the
  // system types.
  uint32 type = 1;
  string content = 2;
  repeated google.protobuf.Struct embeds = 3;
  repeated google.protobuf.Struct components = 4;
  repeated Attachment attachments = 5;
  Poll poll = 6;
  uint64 reply_to_message_id = 7;
  repeated uint64 mention_user_ids = 8;
  repeated uint64 mention_role_ids = 9;
}
```

## Attachment

```protobuf
message Attachment {
  uint64 id = 1;
  string filename = 2;
  string content_type = 3;
  uint64 size = 4;
  // Discord's CDN URL. In an event it is the URL Discord gave when the
  // message arrived, which expires; the API returns a current one.
  string url = 5;
  string description = 6;
}
```

## Poll

```protobuf
message Poll {
  string question = 1;
  repeated PollAnswer answers = 2;
  google.protobuf.Timestamp expires_at = 3;
  bool allow_multiselect = 4;
}
```

## PollAnswer

```protobuf
message PollAnswer {
  uint32 answer_id = 1;
  string text = 2;
}
```

## ComponentUse

```protobuf
// ComponentUse is a person pressing a button or choosing from a select menu
// on a message deck sent. The service has acknowledged it to Discord with a
// deferred message update.
message ComponentUse {
  // The message that holds the component.
  uint64 message_id = 1;
  string custom_id = 2;
  // Discord's component type: 2 button, 3 string select, 5 user select,
  // 6 role select, 7 mentionable select, 8 channel select.
  uint32 component_type = 3;
  // The chosen values of a select menu, as Discord sends them.
  repeated string values = 4;
}
```

## PollVote

```protobuf
message PollVote {
  uint64 message_id = 1;
  uint32 answer_id = 2;
  // Whether the vote was removed rather than added.
  bool removed = 3;
}
```
