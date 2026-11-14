using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus.Administration;
using MassTransit;
using static AzsbEmu.Tests.IntegrationTests.MassTransitWait;

namespace AzsbEmu.Tests.IntegrationTests;

public record FilteredOrder(string OrderId);

/// <summary>Records each message with the headers the filter tests look at.</summary>
public class HeaderRecordingConsumer(ConcurrentQueue<(string OrderId, string? Channel)> received) : IConsumer<FilteredOrder>
{
    public Task Consume(ConsumeContext<FilteredOrder> context)
    {
        // A rule action changes the Service Bus message's application properties, which MassTransit exposes as transport
        // headers; ConsumeContext.Headers are the JSON envelope's headers, which the broker never sees.
        received.Enqueue((context.Message.OrderId, context.ReceiveContext.TransportHeaders.Get<string>("Channel")));
        return Task.CompletedTask;
    }
}

/// <summary>
/// Spec stories 10 and 7: subscription filters that MassTransit creates (SQL and correlation, with a SQL rule action),
/// and endpoints whose names are too long for a subscription name, which MassTransit shortens.
/// </summary>
public class MassTransitFilterAndNamingTests : IDisposable
{
    private readonly BrokerFixture _broker = new();

    public void Dispose() => _broker.Dispose();

    private IBusControl CreateBus(Action<IServiceBusBusFactoryConfigurator> configure) =>
        Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new Uri("sb://localhost/"), _broker.CreateServiceBusClient(), _broker.CreateAdministrationClient());
            configure(cfg);
        });

    private static Task PublishAsync(IBus bus, string orderId, string priority, string region) =>
        bus.Publish(new FilteredOrder(orderId), ctx =>
        {
            ctx.Headers.Set("Priority", priority);
            ctx.Headers.Set("Region", region);
        });

    [Fact]
    public async Task MassTransitSubscriptionFilters_DeliverOnlyMatchingMessages_AndApplyRuleActions()
    {
        var urgent = new ConcurrentQueue<(string OrderId, string? Channel)>();
        var european = new ConcurrentQueue<(string OrderId, string? Channel)>();
        var everything = new ConcurrentQueue<(string OrderId, string? Channel)>();
        var bus = CreateBus(cfg =>
        {
            cfg.ReceiveEndpoint("urgent-orders", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Subscribe<FilteredOrder>("urgent-only", s => s.Rule = new CreateRuleOptions("urgent", new SqlRuleFilter("Priority = 'high'"))
                {
                    Action = new SqlRuleAction("SET Channel = 'express'"),
                });
                e.Consumer(() => new HeaderRecordingConsumer(urgent));
            });
            cfg.ReceiveEndpoint("european-orders", e =>
            {
                e.ConfigureConsumeTopology = false;
                e.Subscribe<FilteredOrder>("europe-only", s => s.Filter = new CorrelationRuleFilter { ApplicationProperties = { ["Region"] = "eu" } });
                e.Consumer(() => new HeaderRecordingConsumer(european));
            });
            cfg.ReceiveEndpoint("all-orders", e => e.Consumer(() => new HeaderRecordingConsumer(everything)));
        });
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            await PublishAsync(bus, "high-eu", "high", "eu");
            await PublishAsync(bus, "high-us", "high", "us");
            await PublishAsync(bus, "low-eu", "low", "eu");
            await PublishAsync(bus, "low-us", "low", "us");

            await UntilAsync(() => everything.Count == 4, "all four orders at the unfiltered endpoint");
            await UntilAsync(() => urgent.Count == 2 && european.Count == 2, "the filtered endpoints' orders");
            await Task.Delay(500);

            Assert.Equal(new[] { "high-eu", "high-us" }, urgent.Select(m => m.OrderId).Order());
            Assert.Equal(new[] { "high-eu", "low-eu" }, european.Select(m => m.OrderId).Order());
            Assert.Equal(4, everything.Count);

            // The SQL rule action set a property on the urgent subscription's copies only.
            Assert.All(urgent, m => Assert.Equal("express", m.Channel));
            Assert.All(european, m => Assert.Null(m.Channel));
            Assert.All(everything, m => Assert.Null(m.Channel));
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task ALongEndpointName_GetsAShortenedSubscription_ThatWorksAndSurvivesARestart()
    {
        // Service Bus subscription names are at most 50 characters; MassTransit shortens longer endpoint names.
        const string endpoint = "a-receive-endpoint-name-that-is-much-longer-than-the-fifty-character-subscription-limit";
        var received = new ConcurrentQueue<OrderPlaced>();
        void Configure(IServiceBusBusFactoryConfigurator cfg) =>
            cfg.ReceiveEndpoint(endpoint, e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(received)));

        var first = CreateBus(Configure);
        await first.StartAsync(NewTimeoutToken());
        await first.StopAsync(NewTimeoutToken());

        var second = CreateBus(Configure);
        await second.StartAsync(NewTimeoutToken());
        try
        {
            string topic = $"{typeof(OrderPlaced).Namespace}/{nameof(OrderPlaced)}";
            var subscription = Assert.Single(_broker.EntityManager.GetSubscriptions(topic) ?? []);
            Assert.True(subscription.Name.Length <= 50, $"subscription name '{subscription.Name}' is {subscription.Name.Length} characters");
            Assert.NotEqual(endpoint, subscription.Name);
            Assert.EndsWith(endpoint, subscription.ForwardTo);

            await second.Publish(new OrderPlaced("long-name"));
            await UntilAsync(() => received.Count == 1, "the order at the long-named endpoint");
            await Task.Delay(500);
            Assert.Single(received);
        }
        finally
        {
            await second.StopAsync(NewTimeoutToken());
        }
    }
}
