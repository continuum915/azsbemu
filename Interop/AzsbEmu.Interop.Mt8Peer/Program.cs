// A MassTransit 8 bus for the MassTransit 7.3 interop tests, run as a child process because the two MassTransit
// versions cannot load into one process. It reports everything it receives by publishing PeerObserved.
//
// Usage: AzsbEmu.Interop.Mt8Peer <amqp-port> <http-port>
// Prints "READY" once the bus has started, and stops when its standard input closes or reads "stop".

using AzsbEmu.Interop.Contracts;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using MassTransit;

int amqpPort = int.Parse(args[0]);
int httpPort = int.Parse(args[1]);
string connectionString = $"Endpoint=sb://localhost:{amqpPort}/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true";

// With UseDevelopmentEmulator the admin client always targets http://host:80; send it to the emulator's port instead.
var adminOptions = new ServiceBusAdministrationClientOptions
{
    Transport = new Azure.Core.Pipeline.HttpClientTransport(new HttpClient(new PortRewriteHandler(httpPort) { InnerHandler = new HttpClientHandler() })),
};

var bus = Bus.Factory.CreateUsingAzureServiceBus(cfg =>
{
    cfg.Host(new Uri("sb://localhost/"), new ServiceBusClient(connectionString), new ServiceBusAdministrationClient(connectionString, adminOptions));

    cfg.ReceiveEndpoint(PeerNames.Endpoint, e => e.Consumer<PeerConsumer>());
    cfg.ReceiveEndpoint(PeerNames.SharedEndpoint, e => e.Consumer<SharedConsumer>());
});

PeerNames.Bus = bus;
await bus.StartAsync(new CancellationTokenSource(TimeSpan.FromSeconds(60)).Token);
Console.WriteLine("READY");

while (Console.ReadLine() is { } line && line != "stop") { }

await bus.StopAsync(new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

static class PeerNames
{
    public const string Endpoint = "mt8-peer";
    public const string SharedEndpoint = "shared-endpoint";
    public const string Mt7Endpoint = "mt7-endpoint";

    /// <summary>The bus, for request clients (a consume context cannot create one in MassTransit 8).</summary>
    public static IBus Bus = null!;
}

class PeerConsumer :
    IConsumer<InteropEvent>,
    IConsumer<InteropCommand>,
    IConsumer<EchoRequest>,
    IConsumer<Poison>,
    IConsumer<Fault<Poison>>,
    IConsumer<PeerInstruction>
{
    public Task Consume(ConsumeContext<InteropEvent> context) =>
        context.Message.Origin == "mt8" ? Task.CompletedTask // its own publishes come back here too
            : context.Publish(new PeerObserved { Kind = context.Message.Kind, Via = "publish", Payload = context.Message.Payload });

    public Task Consume(ConsumeContext<InteropCommand> context) =>
        context.Publish(new PeerObserved { Kind = context.Message.Kind, Via = "send", Payload = context.Message.Payload });

    public Task Consume(ConsumeContext<EchoRequest> context) =>
        context.RespondAsync(new EchoResponse { Kind = context.Message.Kind, RespondedBy = "mt8", Payload = context.Message.Payload });

    public Task Consume(ConsumeContext<Poison> context) =>
        throw new InvalidOperationException($"mt8 refuses {context.Message.Kind}");

    public Task Consume(ConsumeContext<Fault<Poison>> context) =>
        context.Publish(new PeerObserved
        {
            Kind = context.Message.Message.Kind,
            Via = "fault",
            Detail = string.Join(" | ", context.Message.Exceptions.Select(e => $"{e.ExceptionType}: {e.Message}")),
        });

    public async Task Consume(ConsumeContext<PeerInstruction> context)
    {
        var instruction = context.Message;
        switch (instruction.Action)
        {
            case "publish":
                await context.Publish(new InteropEvent { Kind = instruction.Kind, Origin = "mt8", Payload = instruction.Payload });
                break;
            case "send":
                var mt7 = await context.GetSendEndpoint(new Uri($"queue:{PeerNames.Mt7Endpoint}"));
                await mt7.Send(new InteropCommand { Kind = instruction.Kind, Origin = "mt8", Payload = instruction.Payload });
                break;
            case "request":
                var client = PeerNames.Bus.CreateRequestClient<EchoRequest>(new Uri($"queue:{PeerNames.Mt7Endpoint}"), RequestTimeout.After(s: 20));
                var response = await client.GetResponse<EchoResponse>(new EchoRequest { Kind = instruction.Kind, Payload = instruction.Payload });
                await context.Publish(new PeerObserved { Kind = instruction.Kind, Via = "response", Payload = response.Message.Payload, Detail = response.Message.RespondedBy });
                break;
            case "poison":
                var target = await context.GetSendEndpoint(new Uri($"queue:{PeerNames.Mt7Endpoint}"));
                await target.Send(new Poison { Kind = instruction.Kind });
                break;
            default:
                throw new ArgumentException($"Unknown action '{instruction.Action}'.");
        }
    }
}

class SharedConsumer : IConsumer<SharedEvent>
{
    public Task Consume(ConsumeContext<SharedEvent> context) =>
        context.Publish(new PeerObserved { Kind = context.Message.Kind, Via = "shared", Detail = context.Message.Index.ToString() });
}

sealed class PortRewriteHandler(int port) : DelegatingHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.RequestUri = new UriBuilder(request.RequestUri!) { Port = port }.Uri;
        return base.SendAsync(request, cancellationToken);
    }
}
