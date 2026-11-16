---
title: Running the service
product: deck
order: 7
---

Validate the service configuration, then start the service in the foreground:

```sh
deck service validate --config /etc/deck/config.yml
deck service start --config /etc/deck/config.yml
```

The default service configuration is `/etc/deck/config.yml`. The service reads
its NATS credentials, bot tokens and SSH host key from the configured OpenBao
or Vault references. It connects to NATS and creates or checks the activity
stream and thread bucket before accepting SSH connections on `listen.ssh`.
See [Configuration](configuration.md) and [Authentication](authentication.md)
for credentials, host-key pinning and client grants.

Once NATS and the stream are ready, the service opens every bot's gateway
session, then listens for SSH on `listen.ssh` and for health checks on
`listen.health`. It writes JSON log lines to stderr: start and stop, gateway
and NATS state changes, refused calls with the key name and gRPC code, and
publish failures. Logs never hold a token, a message's content or a Discord
response body. A start that fails logs the step and the configuration field
involved and exits 1; an invalid configuration exits 2.

## Health

`GET /healthz` on `listen.health` answers 200 while every bot's gateway session
is ready, NATS is connected and the activity index has caught up with the
stream; otherwise 503, with a JSON body naming each check. `GET /livez`
answers 200 while the process runs. On the same machine, ask for it with:

```sh
deck service health --config /etc/deck/config.yml
```

It prints the body and exits 0 when healthy, 1 when unhealthy and 4 when the
service cannot be reached. The container image runs it as its HEALTHCHECK.

## Stopping

Send SIGTERM or SIGINT to stop the service. It closes the SSH listener and ends
open RPCs, closes the gateway sessions, gives sends in flight, waiting activity
and registry writes a short budget each, and drains NATS, within 10 seconds. A
canceled caller does not by itself cancel an accepted Discord send; process
shutdown does. Use the same nonce when retrying a send whose outcome is
unknown, as described in [Posting](posting.md).

Messages sent while the service was stopped are read back from Discord when it
starts. Button presses and poll votes in that time are lost.

## Deleting, recreating or restoring the event stream

The service creates a missing event stream only when it starts. Use one
procedure to delete, recreate or restore the stream:

1. Stop the service.
2. Delete the stream, and wait for the delete command to return.
3. Recreate the stream, or restore it from a snapshot. After a restore, also
   delete the `stream.generation` key from the thread bucket.
4. Start the service. It creates the stream if it is still missing.

After a restore, every retained activity is unread again rather than missed.

If the stream is deleted while the service runs, the service stops answering
calls (UNAVAILABLE), its healthcheck reports the stream as missing, and it logs
the remedy: restart it once the deletion has finished, or create the stream.
It recovers by itself once the stream exists. Until then it keeps retrying the
activity it has received; after a restart it reads missed messages back from
Discord, but button presses and poll votes that were still waiting are lost.

On a single NATS server, the delete command returns only after the stream's
files are gone, so waiting for it is enough. This has not been verified on a
clustered NATS server. A service that restarts while someone else's deletion is
still running can still collide with it.

The complete startup, retention and health contract is in the
[service design](../technical-designs/service.yml).
