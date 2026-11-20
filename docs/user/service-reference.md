---
title: Service schema reference
product: deck
---

# Service schema reference

Generated from the service JSON Schema. [Download the schema](definitions/service.schema.json).

The following entries preserve every constraint, description and default from the definition.

## $schema

```json
"https://json-schema.org/draft/2020-12/schema"
```

## $id

```json
"https://wyrd.foo/deck/schemas/service-configuration.json"
```

## title

```json
"deck service configuration"
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
  "vault",
  "host_key",
  "nats",
  "bots",
  "keys"
]
```

## properties: listen

```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "ssh": {
      "type": "string",
      "pattern": "^[^\\s/]*:[0-9]{1,5}$",
      "description": "The SSH listener. Default :2222."
    },
    "health": {
      "type": "string",
      "pattern": "^[^\\s/]*:[0-9]{1,5}$",
      "description": "The HTTP healthcheck listener. Default :8080."
    }
  }
}
```

## properties: vault

```json
{
  "$ref": "#/$defs/vault",
  "properties": {
    "kv_mount": {
      "type": "string",
      "pattern": "^[A-Za-z0-9._-]+$",
      "description": "The KV v2 mount for references that name none. Default secret."
    }
  },
  "unevaluatedProperties": false
}
```

## properties: host_key

```json
{
  "$ref": "#/$defs/kv_reference",
  "description": "An OpenSSH private key of type ed25519, the service's SSH host key."
}
```

## properties: nats

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "servers",
    "creds"
  ],
  "properties": {
    "servers": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "string",
        "pattern": "^(nats|tls|ws|wss)://[^/@\\s]+(/[^@\\s]*)?$",
        "description": "A NATS server URL with no user information; credentials come from nats.creds."
      }
    },
    "name": {
      "type": "string",
      "minLength": 1,
      "description": "The NATS connection name. Default deck."
    },
    "creds": {
      "$ref": "#/$defs/kv_reference",
      "description": "The whole text of a NATS .creds file: the user JWT and its nkey seed."
    },
    "ca_file": {
      "$ref": "#/$defs/absolute_path"
    },
    "timeout": {
      "$ref": "#/$defs/duration",
      "description": "Bounds connecting and each JetStream request. Default 10s."
    },
    "subject_prefix": {
      "type": "string",
      "pattern": "^[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*$",
      "description": "Default deck."
    },
    "stream": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "name": {
          "type": "string",
          "pattern": "^[A-Za-z0-9_-]+$",
          "description": "Default DECK."
        },
        "max_age": {
          "$ref": "#/$defs/duration",
          "description": "How long the stream holds activity. Default 168h."
        },
        "duplicate_window": {
          "$ref": "#/$defs/duration",
          "description": "Default 2m."
        },
        "replicas": {
          "type": "integer",
          "minimum": 1,
          "maximum": 5,
          "description": "Default 1."
        },
        "manage": {
          "type": "boolean",
          "description": "Create the stream at startup when it is missing, and update its settings. Default true."
        }
      }
    },
    "threads": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "bucket": {
          "type": "string",
          "pattern": "^[A-Za-z0-9_-]+$",
          "description": "The key-value bucket of the thread registry. Default deck-threads."
        },
        "max_age": {
          "$ref": "#/$defs/duration",
          "description": "When set, the service's hourly sweep deletes a thread entry whose latest touch or publish is older than this. Default unset: entries stay."
        },
        "replicas": {
          "type": "integer",
          "minimum": 1,
          "maximum": 5,
          "description": "Default 1."
        },
        "manage": {
          "type": "boolean",
          "description": "Create and update the bucket. Default true."
        }
      }
    }
  }
}
```

## properties: bots

```json
{
  "type": "array",
  "minItems": 1,
  "items": {
    "type": "object",
    "additionalProperties": false,
    "required": [
      "name",
      "token"
    ],
    "properties": {
      "name": {
        "$ref": "#/$defs/name"
      },
      "token": {
        "$ref": "#/$defs/kv_reference",
        "description": "The bot token."
      }
    }
  }
}
```

## properties: keys

```json
{
  "type": "array",
  "items": {
    "type": "object",
    "additionalProperties": false,
    "required": [
      "name",
      "public_key",
      "grants"
    ],
    "properties": {
      "name": {
        "$ref": "#/$defs/name"
      },
      "public_key": {
        "$ref": "#/$defs/authorized_key"
      },
      "grants": {
        "type": "array",
        "items": {
          "type": "object",
          "additionalProperties": false,
          "required": [
            "bot",
            "server",
            "channels"
          ],
          "properties": {
            "bot": {
              "$ref": "#/$defs/name"
            },
            "server": {
              "$ref": "#/$defs/snowflake"
            },
            "channels": {
              "oneOf": [
                {
                  "type": "array",
                  "minItems": 1,
                  "uniqueItems": true,
                  "items": {
                    "$ref": "#/$defs/snowflake"
                  }
                },
                {
                  "type": "array",
                  "const": [
                    "*"
                  ]
                }
              ],
              "description": "Channel snowflakes, or [\"*\"] for every channel in the server."
            }
          }
        }
      }
    }
  }
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
