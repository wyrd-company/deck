---
title: Validate your first activity
product: deck
---

# Validate your first activity

We will validate a synthetic message before connecting a consumer to JetStream.
This lesson needs Python 3 and [uv](https://docs.astral.sh/uv/).

1. Download the [activity schema](definitions/activity.schema.json) as
   `activity.schema.json` and the [message example](examples/message.json) as
   `message.json` into the same directory.
2. Now validate the event against the published schema:

   ```sh
   uv run --no-project --with jsonschema==4.26.0 python3 - <<'PY'
   import json
   from jsonschema import Draft202012Validator, FormatChecker
   schema = json.load(open('activity.schema.json'))
   event = json.load(open('message.json'))
   Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)
   print('valid activity:', event['id'])
   PY
   ```

   ```text
   valid activity: 1571485045016997890
   ```

We have checked a message using the same definition as the generated
[reference](activity-reference.md). Next, [subscribe and resolve](events.md#subscribe-and-resolve)
using your own local store and NATS consumer. The
[delivery model](delivery-explanation.md) explains retries and trust.
