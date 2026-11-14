using Azure.Messaging.ServiceBus;
using MassTransit;

namespace AzsbEmu.Tests.IntegrationTests;

public record PoisonOrderSubmitted(string OrderId);

public class AlwaysFailingOrderConsumer : IConsumer<PoisonOrderSubmitted>
{
    public Task Consume(ConsumeContext<PoisonOrderSubmitted> context) =>
        throw new InvalidOperationException($"order {context.Message.OrderId} is poison");
}

/// <summary>
/// MassTransit 8 against the emulator: a receive endpoint that dead-letters faults (instead of an _error queue) and
/// forwards its dead-lettered messages to a central poison queue.
/// </summary>
public class MassTransitDeadLetterForwardingTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private IBusControl _bus = null!;

    public async Task InitializeAsync()
    {
        // The dead-letter target must exist before MassTransit creates the endpoint that points at it.
        await _broker.CreateAdministrationClient().CreateQueueAsync("orders-poison");

        _bus = Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new Uri("sb://localhost/"), _broker.CreateServiceBusClient(), _broker.CreateAdministrationClient());
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.ForwardDeadLetteredMessagesTo = "orders-poison";
                e.ConfigureDeadLetterQueueErrorTransport();
                e.Consumer<AlwaysFailingOrderConsumer>();
            });
        });
        await _bus.StartAsync(new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);
    }

    [Fact]
    public async Task AFaultingConsumersMessage_IsDeadLetteredAndForwardedToThePoisonQueue()
    {
        await _bus.Publish(new PoisonOrderSubmitted("42"));

        await using var client = _broker.CreateServiceBusClient();
        await using var poison = client.CreateReceiver("orders-poison");
        var forwarded = await poison.ReceiveMessageAsync(TimeSpan.FromSeconds(15));

        Assert.NotNull(forwarded);
        using var envelope = System.Text.Json.JsonDocument.Parse(forwarded.Body.ToString());
        Assert.Equal("42", envelope.RootElement.GetProperty("message").GetProperty("orderId").GetString());
        // MassTransit records the fault in the properties it modifies when dead-lettering.
        Assert.Equal(typeof(InvalidOperationException).FullName, forwarded.ApplicationProperties["MT-Fault-ExceptionType"]);
        Assert.Equal("order 42 is poison", forwarded.ApplicationProperties["MT-Fault-Message"]);
        Assert.False(forwarded.ApplicationProperties.TryGetValue("DeadLetterReason", out var reason) && Equals(reason, "com.microsoft:dead-letter"),
            "the AMQP error condition is not a dead-letter reason");
        Assert.Equal("orders", forwarded.ApplicationProperties["DeadLetterSource"]);

        await using var ordersDlq = client.CreateReceiver("orders", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        Assert.Null(await ordersDlq.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
    }

    public async Task DisposeAsync()
    {
        await _bus.StopAsync(new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);
        _broker.Dispose();
    }
}
