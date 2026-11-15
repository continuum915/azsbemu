using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public sealed class SendTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private NamespaceConnection _connection = null!;
    private ServiceBusClient _client = null!;

    public Task InitializeAsync()
    {
        _connection = NamespaceConnection.Open(new ConnectionProfile("test",
            "Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
            _broker.HttpPort, _broker.Port));
        _client = _broker.CreateServiceBusClient();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _connection.DisposeAsync();
        _broker.Dispose();
    }

    private async Task<List<ServiceBusReceivedMessage>> ReceiveAllAsync(ServiceBusReceiver receiver, int expected)
    {
        var all = new List<ServiceBusReceivedMessage>();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (all.Count < expected && DateTime.UtcNow < deadline)
            all.AddRange(await receiver.ReceiveMessagesAsync(expected - all.Count, TimeSpan.FromSeconds(2)));
        return all;
    }

    [Fact]
    public async Task EveryDraftField_ArrivesOnTheReceivedMessage()
    {
        _broker.EnsureQueue("orders-s", requiresSession: true);
        var draft = new MessageDraft("{\"id\":42}", "application/json", "OrderPlaced", "msg-1", "corr-1", "sess-1",
            new Dictionary<string, object?> { ["tenant"] = "acme", ["priority"] = 3, ["urgent"] = true });

        await _connection.SendAsync(new QueueRef("orders-s"), draft);

        await using var receiver = await _client.AcceptSessionAsync("orders-s", "sess-1",
            new ServiceBusSessionReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var m = Assert.Single(await ReceiveAllAsync(receiver, 1));
        Assert.Equal("{\"id\":42}", m.Body.ToString());
        Assert.Equal("application/json", m.ContentType);
        Assert.Equal("OrderPlaced", m.Subject);
        Assert.Equal("msg-1", m.MessageId);
        Assert.Equal("corr-1", m.CorrelationId);
        Assert.Equal("sess-1", m.SessionId);
        Assert.Equal("acme", m.ApplicationProperties["tenant"]);
        Assert.Equal(3, m.ApplicationProperties["priority"]);
        Assert.Equal(true, m.ApplicationProperties["urgent"]);
    }

    [Fact]
    public async Task AScheduledSend_IsScheduled_NotActive()
    {
        _broker.EnsureQueue("later");

        await _connection.SendAsync(new QueueRef("later"),
            new MessageDraft("hello", ScheduledEnqueueTime: DateTimeOffset.UtcNow.AddHours(1)), copies: 3);

        var counts = await _connection.GetCountsAsync(new QueueRef("later"));
        Assert.Equal(3, counts.Scheduled);
        Assert.Equal(0, counts.Active);
    }

    [Fact]
    public async Task ManyCopies_AreSentInBatches_AndAllArrive()
    {
        _broker.EnsureQueue("bulk");

        // Bodies large enough that 1,000 copies cannot fit in one batch.
        await _connection.SendAsync(new QueueRef("bulk"), new MessageDraft(new string('x', 2_000)), copies: 1_000);

        await using var receiver = _client.CreateReceiver("bulk", new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        Assert.Equal(1_000, (await ReceiveAllAsync(receiver, 1_000)).Count);
    }

    [Fact]
    public async Task SendingToATopic_ReachesItsSubscriptions()
    {
        _broker.EnsureTopic("events");
        _broker.EnsureSubscription("events", "a");
        _broker.EnsureSubscription("events", "b");

        await _connection.SendAsync(new TopicRef("events"), new MessageDraft("ping"), copies: 2);

        foreach (var sub in new[] { "a", "b" })
        {
            await using var receiver = _client.CreateReceiver("events", sub, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
            Assert.Equal(2, (await ReceiveAllAsync(receiver, 2)).Count);
        }
    }

    [Fact]
    public async Task SendingToASubscription_IsRejectedClearly()
    {
        var ex = await Assert.ThrowsAsync<MessageOperationException>(() =>
            _connection.SendAsync(new SubscriptionRef("events", "a"), new MessageDraft("x")));
        Assert.Contains("Send to the topic 'events'", ex.Message);
    }

    [Fact]
    public async Task AMessageTooLargeToSend_IsRejectedClearly()
    {
        _broker.EnsureQueue("small");
        var ex = await Assert.ThrowsAsync<MessageOperationException>(() =>
            _connection.SendAsync(new QueueRef("small"), new MessageDraft(new string('x', 2_000_000))));
        Assert.Contains("too large", ex.Message);
    }
}
