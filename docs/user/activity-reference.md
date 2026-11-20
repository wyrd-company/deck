---
title: Activity schema reference
product: deck
---

# Activity schema reference

Generated from the activity JSON Schema. [Download the schema](definitions/activity.schema.json).

The following entries preserve every constraint, description and default from the definition.

## $schema

```json
"https://json-schema.org/draft/2020-12/schema"
```

## $id

```json
"https://wyrd.foo/deck/schemas/activity.json"
```

## title

```json
"deck.v1.Activity"
```

## description

```json
"The payload of every deck event: a deck.v1.Activity in protojson with proto field names. protojson omits fields that hold their zero value, carries uint64 values as decimal strings and timestamps as RFC 3339 strings, and adds insignificant whitespace that varies between runs."
```

## type

```json
"object"
```

## additionalProperties

```json
false
```

## required

```json
[
  "id",
  "time",
  "bot",
  "server_id",
  "channel_id",
  "thread_id"
]
```

## oneOf

```json
[
  {
    "required": [
      "message"
    ]
  },
  {
    "required": [
      "component"
    ]
  },
  {
    "required": [
      "poll_vote"
    ]
  }
]
```

## properties: id

```json
{
  "$ref": "#/$defs/snowflake",
  "description": "The message's or interaction's snowflake, or for a poll vote one the service makes from the millisecond it received the vote."
}
```

## properties: time

```json
{
  "$ref": "#/$defs/timestamp"
}
```

## properties: bot

```json
{
  "type": "string",
  "pattern": "^[a-z0-9][a-z0-9-]{0,62}$",
  "description": "The name of the bot in the service configuration."
}
```

## properties: server_id

```json
{
  "$ref": "#/$defs/snowflake"
}
```

## properties: channel_id

```json
{
  "$ref": "#/$defs/snowflake",
  "description": "The thread's parent channel."
}
```

## properties: thread_id

```json
{
  "$ref": "#/$defs/snowflake"
}
```

## properties: author

```json
{
  "$ref": "#/$defs/user"
}
```

## properties: sent_by

```json
{
  "type": "string",
  "pattern": "^[a-z0-9][a-z0-9-]{0,62}$",
  "description": "The name of the client key that sent the message through deck."
}
```

## properties: sender_key

```json
{
  "type": "string",
  "pattern": "^SHA256:[A-Za-z0-9+/]{43}$",
  "description": "The SHA256 fingerprint of the client key that sent the message through deck."
}
```

## properties: nonce

```json
{
  "type": "string",
  "pattern": "^[A-Za-z0-9]{1,25}$",
  "description": "The client's nonce for a message deck sent, by which the sending session recognises its own message."
}
```

## properties: message

```json
{
  "$ref": "#/$defs/message"
}
```

## properties: component

```json
{
  "$ref": "#/$defs/component"
}
```

## properties: poll_vote

```json
{
  "$ref": "#/$defs/poll_vote"
}
```

## $defs: snowflake

```json
{
  "type": "string",
  "pattern": "^[1-9][0-9]{0,19}$"
}
```

## $defs: timestamp

```json
{
  "type": "string",
  "format": "date-time",
  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?Z$"
}
```

## $defs: discord_object

```json
{
  "type": "object",
  "description": "A Discord API object, as Discord's API defines it."
}
```

## $defs: user

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "id"
  ],
  "properties": {
    "id": {
      "$ref": "#/$defs/snowflake"
    },
    "username": {
      "type": "string"
    },
    "display_name": {
      "type": "string"
    },
    "bot": {
      "type": "boolean"
    },
    "webhook_id": {
      "$ref": "#/$defs/snowflake"
    }
  }
}
```

## $defs: message

```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "type": {
      "type": "integer",
      "minimum": 1,
      "description": "Discord's message type; absent means 0, a default message."
    },
    "content": {
      "type": "string"
    },
    "embeds": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/discord_object"
      }
    },
    "components": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/discord_object"
      }
    },
    "attachments": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/attachment"
      }
    },
    "poll": {
      "$ref": "#/$defs/poll"
    },
    "reply_to_message_id": {
      "$ref": "#/$defs/snowflake"
    },
    "mention_user_ids": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/snowflake"
      }
    },
    "mention_role_ids": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/snowflake"
      }
    }
  }
}
```

## $defs: attachment

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "id",
    "filename",
    "url"
  ],
  "properties": {
    "id": {
      "$ref": "#/$defs/snowflake"
    },
    "filename": {
      "type": "string"
    },
    "content_type": {
      "type": "string"
    },
    "size": {
      "type": "string",
      "pattern": "^[1-9][0-9]*$"
    },
    "url": {
      "type": "string",
      "format": "uri",
      "pattern": "^https://"
    },
    "description": {
      "type": "string"
    }
  }
}
```

## $defs: poll

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "question",
    "answers"
  ],
  "properties": {
    "question": {
      "type": "string"
    },
    "answers": {
      "type": "array",
      "minItems": 1,
      "maxItems": 10,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "answer_id",
          "text"
        ],
        "properties": {
          "answer_id": {
            "type": "integer",
            "minimum": 1
          },
          "text": {
            "type": "string"
          }
        }
      }
    },
    "expires_at": {
      "$ref": "#/$defs/timestamp"
    },
    "allow_multiselect": {
      "type": "boolean"
    }
  }
}
```

## $defs: component

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "message_id",
    "custom_id",
    "component_type"
  ],
  "properties": {
    "message_id": {
      "$ref": "#/$defs/snowflake"
    },
    "custom_id": {
      "type": "string",
      "minLength": 1,
      "maxLength": 100
    },
    "component_type": {
      "enum": [
        2,
        3,
        5,
        6,
        7,
        8
      ]
    },
    "values": {
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  }
}
```

## $defs: poll_vote

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "message_id",
    "answer_id"
  ],
  "properties": {
    "message_id": {
      "$ref": "#/$defs/snowflake"
    },
    "answer_id": {
      "type": "integer",
      "minimum": 1
    },
    "removed": {
      "type": "boolean"
    }
  }
}
```
