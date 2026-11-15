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

Send SIGTERM or SIGINT to stop the service. It closes the SSH listener and ends
open RPCs before closing bot clients and the NATS connection. A canceled caller
does not by itself cancel an accepted Discord send; process shutdown does.
Use the same nonce when retrying a send whose outcome is unknown, as described
in [Posting](posting.md).

The complete startup, retention and health contract is in the
[service design](../technical-designs/service.yml).
