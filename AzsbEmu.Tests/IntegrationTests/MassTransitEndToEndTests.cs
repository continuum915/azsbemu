using System.Collections.Concurrent;
using MassTransit;
using static AzsbEmu.Tests.IntegrationTests.MassTransitWait;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>MassTransit 8 buses running their default topology (topics, forwarding subscriptions, queues) on the emulator.</summary>
public class MassTransitEndToEndTests : IDisposable
{
    private readonly BrokerFixture _broker = new();

    private IBusControl CreateBus(Action<IServiceBusBusFactoryConfigurator> configure, Action<HttpRequestMessage>? observeAdminRequests = null) =>
        Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new Uri("sb://localhost/"), _broker.CreateServiceBusClient(), _broker.CreateAdministrationClient(observeAdminRequests));
            configure(cfg);
        });

    [Fact]
    public async Task Publish_ReachesEveryEndpointConsumingTheMessage_AndSend_ReachesOnlyItsQueue()
    {
        var billing = new ConcurrentQueue<OrderPlaced>();
        var shipping = new ConcurrentQueue<OrderPlaced>();
        var bus = CreateBus(cfg =>
        {
            cfg.ReceiveEndpoint("billing", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(billing)));
            cfg.ReceiveEndpoint("shipping", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(shipping)));
        });
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            await bus.Publish(new OrderPlaced("published"));
            await UntilAsync(() => billing.Count == 1 && shipping.Count == 1, "the published message at both endpoints");

            // Both endpoints consume OrderPlaced, so a sent one leaking anywhere else would be visible in billing.
            var shippingQueue = await bus.GetSendEndpoint(new Uri("queue:shipping"));
            await shippingQueue.Send(new OrderPlaced("sent"));
            await UntilAsync(() => shipping.Count == 2, "the sent message at shipping");
            await Task.Delay(500);

            Assert.Equal(new[] { "published" }, billing.Select(m => m.OrderId));
            Assert.Equal(new[] { "published", "sent" }, shipping.Select(m => m.OrderId));
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task ASecondBusStart_AgainstDeployedTopology_SucceedsWithoutChangingAnyEntity()
    {
        var received = new ConcurrentQueue<OrderPlaced>();
        void Configure(IServiceBusBusFactoryConfigurator cfg) =>
            cfg.ReceiveEndpoint("restartable", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(received)));

        var first = CreateBus(Configure);
        await first.StartAsync(NewTimeoutToken());
        await first.StopAsync(NewTimeoutToken());

        var reads = new ConcurrentQueue<string>();
        var writes = new ConcurrentQueue<string>();
        var second = CreateBus(Configure, request =>
        {
            var target = Uri.UnescapeDataString(request.RequestUri!.AbsolutePath);
            (request.Method == HttpMethod.Get ? reads : writes).Enqueue($"{request.Method} {target}");
        });
        await second.StartAsync(NewTimeoutToken());
        try
        {
            // MassTransit read the deployed queue, topic and forwarding subscription back and found nothing to change.
            Assert.Contains(reads, r => r.EndsWith("/restartable"));
            Assert.Contains(reads, r => r.Contains("/Subscriptions/", StringComparison.OrdinalIgnoreCase));
            Assert.Empty(writes);

            await second.Publish(new OrderPlaced("after-restart"));
            await UntilAsync(() => received.Count == 1, "the message after the restart");
        }
        finally
        {
            await second.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task TwoBusesStartingAtOnce_WithTheSameEndpoint_BothSucceed_AndShareTheWork()
    {
        // The same endpoint on both buses: both create the same queue, topic and subscription at the same moment.
        var a = new ConcurrentQueue<OrderPlaced>();
        var b = new ConcurrentQueue<OrderPlaced>();
        var busA = CreateBus(cfg => cfg.ReceiveEndpoint("shared-endpoint", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(a))));
        var busB = CreateBus(cfg => cfg.ReceiveEndpoint("shared-endpoint", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(b))));

        await Task.WhenAll(busA.StartAsync(NewTimeoutToken()), busB.StartAsync(NewTimeoutToken()));
        try
        {
            for (int i = 0; i < 10; i++) await busA.Publish(new OrderPlaced($"o-{i}"));
            await UntilAsync(() => a.Count + b.Count == 10, "ten messages across both buses");
            await Task.Delay(500);
            Assert.Equal(10, a.Count + b.Count); // competing consumers: each message consumed once
        }
        finally
        {
            await Task.WhenAll(busA.StopAsync(NewTimeoutToken()), busB.StopAsync(NewTimeoutToken()));
        }
    }

    [Fact]
    public async Task AFaultingConsumersMessage_GoesToTheEndpointsErrorQueue()
    {
        var bus = CreateBus(cfg => cfg.ReceiveEndpoint("faulty", e => e.Consumer<ThrowingConsumer>()));
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            await bus.Publish(new OrderPlaced("bad"));

            // MassTransit creates the error queue when the first fault happens.
            var admin = _broker.CreateAdministrationClient();
            await UntilAsync(async () => (await admin.QueueExistsAsync("faulty_error")).Value, "faulty_error to be created");

            await using var client = _broker.CreateServiceBusClient();
            await using var errors = client.CreateReceiver("faulty_error");
            var moved = await errors.ReceiveMessageAsync(TimeSpan.FromSeconds(15));
            Assert.NotNull(moved);
            Assert.Equal("cannot place order", moved.ApplicationProperties["MT-Fault-Message"]);
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task RequestResponse_RepliesThroughTheBusTemporaryEndpoint()
    {
        // Responses go to the bus's own auto-deleting queue (AutoDeleteOnIdle), which MassTransit creates at start.
        var bus = CreateBus(cfg => cfg.ReceiveEndpoint("pricing", e => e.Consumer<PricingConsumer>()));
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            var client = bus.CreateRequestClient<PriceQuery>(new Uri("queue:pricing"), RequestTimeout.After(s: 15));
            var response = await client.GetResponse<PriceQuote>(new PriceQuery("sku-1"));
            Assert.Equal(new PriceQuote("sku-1", 9.99m), response.Message);
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task ScheduledSends_UsingTheServiceBusScheduler_ArriveWhenDue()
    {
        var received = new ConcurrentQueue<OrderPlaced>();
        var bus = CreateBus(cfg =>
        {
            cfg.UseServiceBusMessageScheduler();
            cfg.ReceiveEndpoint("scheduled", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(received)));
        });
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            var due = DateTime.UtcNow.AddSeconds(3);
            await bus.CreateServiceBusMessageScheduler().ScheduleSend(new Uri("queue:scheduled"), due, new OrderPlaced("later"));

            await Task.Delay(due - DateTime.UtcNow - TimeSpan.FromSeconds(1));
            Assert.Empty(received);
            await UntilAsync(() => received.Count == 1, "the scheduled message");
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task ASessionEndpoint_ConsumesEachSessionsMessagesInOrder()
    {
        var received = new ConcurrentQueue<OrderPlaced>();
        var bus = CreateBus(cfg => cfg.ReceiveEndpoint("by-customer", e =>
        {
            e.RequiresSession = true;
            e.Consumer(() => new RecordingConsumer<OrderPlaced>(received));
        }));
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            var endpoint = await bus.GetSendEndpoint(new Uri("queue:by-customer"));
            foreach (var customer in new[] { "alice", "bob" })
                for (int i = 0; i < 3; i++)
                    await endpoint.Send(new OrderPlaced($"{customer}-{i}"), ctx => ctx.SetSessionId(customer));

            await UntilAsync(() => received.Count == 6, "six session messages");
            foreach (var customer in new[] { "alice", "bob" })
                Assert.Equal(Enumerable.Range(0, 3).Select(i => $"{customer}-{i}"),
                    received.Select(m => m.OrderId).Where(id => id.StartsWith(customer)));
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    public void Dispose() => _broker.Dispose();
}
