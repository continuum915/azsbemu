# AzsbEmu

A local Azure Service Bus emulator for development and integration tests. It speaks AMQP 1.0 and the Service Bus HTTP management API, so `Azure.Messaging.ServiceBus` (sending, receiving, and `ServiceBusAdministrationClient`) and MassTransit work against it unmodified. Entities and messages persist across restarts.

## Run

```bash
docker run -d --name azsbemu -p 5672:5672 -p 80:8080 -v azsbemu-data:/app/data ghcr.io/continuum915/azsbemu:latest
```

Pin a version tag (for example `:0.1.0`) in shared setups. Images are built for amd64 and arm64.

## Connect

```text
Endpoint=sb://localhost/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true
```

Leave the port out of the endpoint. The client then uses AMQP on 5672 and the administration client uses HTTP on 80, so one string drives both. This needs Azure.Messaging.ServiceBus 7.18 or later.

## MassTransit

```csharp
x.UsingAzureServiceBus((context, cfg) =>
{
    cfg.Host("Endpoint=sb://localhost/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true");
    cfg.ConfigureEndpoints(context);
});
```

- **MassTransit 8** deploys its topology and runs as-is.
- **MassTransit 7.3** works too, and interoperates with MassTransit 8 on the same emulator. Add a direct reference to `Azure.Messaging.ServiceBus` 7.21.0, since MassTransit 7.3 alone resolves an SDK too old for `UseDevelopmentEmulator`.
- **MassTransit 7.0–7.2** is not supported: those versions use an older SDK that requires TLS.

## Supported

- Queues, topics, subscriptions, and rules (SQL and correlation filters, SQL rule actions), over the HTTP API or AMQP
- Peek-lock and receive-and-delete; complete, abandon, defer, and dead-letter; lock renewal
- Sessions, transactions, scheduled messages, and message TTL
- Auto-forwarding, duplicate detection, and auto-delete on idle

## Known gaps

- Delivery counts, properties changed by abandon or defer, and the duplicate detection history are held in memory. A restart resets them.
- Partitioning, size limits, and entity status are stored but not enforced. `SizeInBytes` is always 0.
- No TLS, no authentication, and a 1 MB maximum message size.

## Configure

| Setting | Environment variable | Default (container) |
| :--- | :--- | :--- |
| Listen address | `AZSB_EMU_Host` | `0.0.0.0` |
| AMQP port | `AZSB_EMU_Port` | `5672` |
| HTTP management port | `AZSB_EMU_HttpPort` | `8080` (publish as host port 80) |
| Data directory | `AZSB_EMU_DataDirectory` | `/app/data` |

## Build and test

```bash
dotnet run --project AzsbEmu.Server
dotnet test AzsbEmu.slnx
```

Published images come from `.github/workflows/docker-publish.yml` when a `v*.*.*` tag is pushed.
