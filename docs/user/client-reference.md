---
title: Client schema reference
product: deck
---

# Client schema reference

Generated from the client JSON Schema. [Download the schema](definitions/client.schema.json).

The following entries preserve every constraint, description and default from the definition.

## $schema

```json
"https://json-schema.org/draft/2020-12/schema"
```

## $id

```json
"https://wyrd.foo/deck/schemas/client-configuration.json"
```

## title

```json
"deck client configuration"
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
  "service",
  "vault",
  "key"
]
```

## properties: service

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "address",
    "host_key"
  ],
  "properties": {
    "address": {
      "type": "string",
      "pattern": "^[^\\s/]+:[0-9]{1,5}$",
      "description": "host:port of the service's SSH listener."
    },
    "host_key": {
      "$ref": "#/$defs/authorized_key",
      "description": "The service's host key; the client refuses any other."
    },
    "timeout": {
      "$ref": "#/$defs/duration",
      "description": "Bounds connecting and each call other than listen. Default 15s."
    }
  }
}
```

## properties: vault

```json
{
  "$ref": "#/$defs/vault",
  "unevaluatedProperties": false
}
```

## properties: key

```json
{
  "type": "object",
  "description": "The Transit key that is this client's identity. Its type is ed25519.",
  "additionalProperties": false,
  "required": [
    "name"
  ],
  "properties": {
    "mount": {
      "type": "string",
      "pattern": "^[A-Za-z0-9._/-]+$",
      "description": "The Transit mount. Default transit."
    },
    "name": {
      "type": "string",
      "pattern": "^[A-Za-z0-9._-]+$"
    }
  }
}
```

## properties: session

```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "variables": {
      "type": "array",
      "description": "Environment variables that identify the session, in order of preference. Default none.",
      "uniqueItems": true,
      "items": {
        "type": "string",
        "pattern": "^[A-Za-z_][A-Za-z0-9_]*$"
      }
    }
  }
}
```

## properties: store

```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "path": {
      "$ref": "#/$defs/absolute_path",
      "description": "The local store. Default deck.db in the configuration file's directory."
    }
  }
}
```

## properties: channels

```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "default": {
      "$ref": "#/$defs/channel_reference",
      "description": "The channel deck post uses when none is given."
    },
    "allow": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/channel_reference"
      },
      "description": "When present, the only channels this client posts to or reads from."
    },
    "deny": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/channel_reference"
      },
      "description": "Channels this client never posts to or reads from. Deny wins over allow."
    }
  }
}
```

## properties: aliases

```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "channels": {
      "type": "object",
      "propertyNames": {
        "$ref": "#/$defs/name"
      },
      "additionalProperties": {
        "type": "string",
        "pattern": "^([a-z0-9][a-z0-9-]{0,62}/)?[1-9][0-9]{0,19}$"
      }
    },
    "users": {
      "type": "object",
      "propertyNames": {
        "$ref": "#/$defs/name"
      },
      "additionalProperties": {
        "$ref": "#/$defs/snowflake"
      }
    },
    "roles": {
      "type": "object",
      "propertyNames": {
        "$ref": "#/$defs/name"
      },
      "additionalProperties": {
        "$ref": "#/$defs/snowflake"
      }
    }
  }
}
```

## properties: personas

```json
{
  "type": "object",
  "propertyNames": {
    "$ref": "#/$defs/name"
  },
  "additionalProperties": {
    "type": "object",
    "additionalProperties": false,
    "required": [
      "name"
    ],
    "properties": {
      "name": {
        "type": "string",
        "minLength": 1,
        "maxLength": 256
      },
      "icon_url": {
        "type": "string",
        "pattern": "^https?://"
      }
    }
  }
}
```

## properties: default_persona

```json
{
  "$ref": "#/$defs/name",
  "description": "A persona every post and reply uses unless told otherwise."
}
```

## $defs: name

```json
{
  "type": "string",
  "pattern": "^[a-z0-9][a-z0-9-]{0,62}$",
  "description": "A name for a bot, a client key, an alias or a persona: lower-case letters, digits and hyphens, starting with a letter or digit, at most 63 characters."
}
```

## $defs: snowflake

```json
{
  "type": "string",
  "pattern": "^[1-9][0-9]{0,19}$",
  "description": "A Discord snowflake in decimal, quoted so YAML keeps it a string."
}
```

## $defs: duration

```json
{
  "type": "string",
  "pattern": "^([0-9]+(\\.[0-9]+)?(ns|us|\u00b5s|ms|s|m|h))+$",
  "description": "A Go duration, such as 10s, 2m or 168h."
}
```

## $defs: absolute_path

```json
{
  "type": "string",
  "pattern": "^/"
}
```

## $defs: authorized_key

```json
{
  "type": "string",
  "pattern": "^ssh-ed25519 [A-Za-z0-9+/]+={0,2}( .*)?$",
  "description": "An ed25519 public key in OpenSSH authorized-keys form; a trailing comment is ignored."
}
```

## $defs: kv_reference

```json
{
  "type": "object",
  "description": "A field of a KV version 2 secret in OpenBao or Vault.",
  "additionalProperties": false,
  "required": [
    "path",
    "field"
  ],
  "properties": {
    "mount": {
      "type": "string",
      "pattern": "^[A-Za-z0-9._-]+$",
      "description": "The KV v2 mount; defaults to vault.kv_mount."
    },
    "path": {
      "type": "string",
      "pattern": "^[^/].*[^/]$|^[^/]$",
      "description": "The secret's path under the mount, with no leading or trailing slash."
    },
    "field": {
      "type": "string",
      "minLength": 1
    }
  }
}
```

## $defs: vault

```json
{
  "type": "object",
  "description": "How deck reaches OpenBao or Vault. deck reads no BAO_* or VAULT_* environment variable.",
  "required": [
    "address"
  ],
  "properties": {
    "address": {
      "type": "string",
      "pattern": "^(unix:///.+|https://[^/@\\s]+/?|http://(127\\.[0-9]+\\.[0-9]+\\.[0-9]+|localhost|\\[::1\\])(:[0-9]+)?/?)$",
      "description": "unix:///path for a local socket, https://host:port, or http:// on a loopback host only; never with user information."
    },
    "namespace": {
      "type": "string",
      "minLength": 1
    },
    "ca_file": {
      "$ref": "#/$defs/absolute_path",
      "description": "A PEM bundle for an https address with a private CA."
    },
    "timeout": {
      "$ref": "#/$defs/duration",
      "description": "Bounds the login and each request. Default 10s."
    },
    "auth": {
      "type": "object",
      "description": "At most one method. Without auth, requests carry no token, for a socket or agent that injects its own.",
      "additionalProperties": false,
      "maxProperties": 1,
      "properties": {
        "token_file": {
          "$ref": "#/$defs/absolute_path",
          "description": "A file holding a token, read at start and trimmed."
        },
        "approle": {
          "type": "object",
          "additionalProperties": false,
          "required": [
            "role_id",
            "secret_id_file"
          ],
          "properties": {
            "mount": {
              "type": "string",
              "pattern": "^[A-Za-z0-9._-]+$",
              "description": "Default approle."
            },
            "role_id": {
              "type": "string",
              "minLength": 1
            },
            "secret_id_file": {
              "$ref": "#/$defs/absolute_path"
            }
          }
        }
      }
    }
  }
}
```

## $defs: channel_reference

```json
{
  "type": "string",
  "pattern": "^(([a-z0-9][a-z0-9-]{0,62}/)?[1-9][0-9]{0,19}|[a-z0-9][a-z0-9-]{0,62})$",
  "description": "A channel: [<bot>/]<snowflake>, or the name of a channel alias."
}
```
