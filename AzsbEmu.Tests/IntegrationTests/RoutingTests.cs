using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Behaviour that every ingress path shares because it goes through the single message router.</summary>
public class RoutingTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;

    public RoutingTests()
    {
        _broker = new BrokerFixture();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task ScheduledMessage_ToSessionQueue_IsReceivedBySessionReceiver()
    {
        const string queue = "routing-scheduled-session-queue";
        _broker.EnsureQueue(queue, requiresSession: true);

        await using var sender = _client.CreateSender(queue);
        long seq = await sender.ScheduleMessageAsync(
            new ServiceBusMessage("scheduled-session-payload") { SessionId = "s1" },
            DateTimeOffset.UtcNow.AddSeconds(1));

        await Task.Delay(TimeSpan.FromSeconds(3));

        await using var receiver = await _client.AcceptSessionAsync(queue, "s1");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("scheduled-session-payload", received.Body.ToString());
        Assert.Equal(seq, received.SequenceNumber);
    }

    [Fact]
    public async Task TopicFanOut_SessionlessMessageDeadLetteredOnSessionSubscription_DoesNotLeakDeadLetterReasonToOtherSubscriptions()
    {
        const string topic = "routing-fanout-topic";
        _broker.EnsureTopic(topic);
        _broker.EnsureSubscription(topic, "a-session-sub", requiresSession: true);
        _broker.EnsureSubscription(topic, "b-plain-sub");

        await using var sender = _client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage("no-session") { ApplicationProperties = { ["k"] = "v" } });

        await using var plain = _client.CreateReceiver(topic, "b-plain-sub");
        var received = await plain.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("v", received.ApplicationProperties["k"]);
        Assert.False(received.ApplicationProperties.ContainsKey("DeadLetterReason"));

        await using var dlq = _client.CreateReceiver(topic, "a-session-sub",
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var deadLettered = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(deadLettered);
        Assert.Equal("SessionIdIsNull", deadLettered.DeadLetterReason);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
