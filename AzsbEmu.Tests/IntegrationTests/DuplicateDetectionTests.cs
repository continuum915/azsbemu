using System.Collections.Concurrent;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AzsbEmu.Server.Core;
using MassTransit;
using static AzsbEmu.Tests.IntegrationTests.MassTransitWait;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Ticket 19: queues and topics with RequiresDuplicateDetection drop a message whose MessageId already arrived within
/// DuplicateDetectionHistoryTimeWindow; the sender still sees success. The SDK's admin client refuses windows under 20
/// seconds, so tests that need the window to pass create entities through the EntityManager.
/// </summary>
public class DuplicateDetectionTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private ServiceBusClient _client = null!;
    private ServiceBusAdministrationClient _admin = null!;

    public Task InitializeAsync()
    {
        _client = _broker.CreateServiceBusClient();
        _admin = _broker.CreateAdministrationClient();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        _broker.Dispose();
    }

    private Task CreateDedupQueueAsync(string name) =>
        _admin.CreateQueueAsync(new CreateQueueOptions(name) { RequiresDuplicateDetection = true });

    private static ServiceBusMessage Message(string id, string body) => new(body) { MessageId = id };

    /// <summary>Receives everything currently in <paramref name="entity"/> (a queue, or a topic's subscription).</summary>
    private async Task<List<string>> DrainAsync(string queueOrTopic, string? subscription = null)
    {
        await using var receiver = subscription == null
            ? _client.CreateReceiver(queueOrTopic, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete })
            : _client.CreateReceiver(queueOrTopic, subscription, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var bodies = new List<string>();
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)) is { } message) bodies.Add(message.Body.ToString());
        return bodies;
    }

    [Fact]
    public async Task ADuplicateWithinTheWindow_IsAcceptedButNotDelivered()
    {
        await CreateDedupQueueAsync("dedup-q");
        await using var sender = _client.CreateSender("dedup-q");

        await sender.SendMessageAsync(Message("id-1", "first"));
        await sender.SendMessageAsync(Message("id-1", "duplicate")); // succeeds, as with Service Bus
        await sender.SendMessageAsync(Message("id-2", "other"));

        Assert.Equal(2, _broker.GetQueue("dedup-q").MessageCount);
        Assert.Equal(new[] { "first", "other" }, await DrainAsync("dedup-q"));
    }

    [Fact]
    public async Task WithoutDuplicateDetection_DuplicatesAreDelivered()
    {
        await _admin.CreateQueueAsync("plain-q");
        await using var sender = _client.CreateSender("plain-q");
        await sender.SendMessageAsync(Message("id-1", "first"));
        await sender.SendMessageAsync(Message("id-1", "second"));

        Assert.Equal(new[] { "first", "second" }, await DrainAsync("plain-q"));
    }

    [Fact]
    public async Task AfterTheWindow_TheSameMessageIdIsAcceptedAgain()
    {
        Assert.True(_broker.EntityManager.CreateQueue(new QueueInfo
        {
            Name = "short-window-q",
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromSeconds(1),
        }));
        await using var sender = _client.CreateSender("short-window-q");

        await sender.SendMessageAsync(Message("id-1", "first"));
        await sender.SendMessageAsync(Message("id-1", "dropped"));
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        await sender.SendMessageAsync(Message("id-1", "after-window"));

        Assert.Equal(new[] { "first", "after-window" }, await DrainAsync("short-window-q"));
    }

    [Fact]
    public async Task DuplicatesInsideABatch_AndAcrossBatches_AreDropped()
    {
        await CreateDedupQueueAsync("batch-dedup-q");
        await using var sender = _client.CreateSender("batch-dedup-q");

        await sender.SendMessagesAsync([Message("a", "a1"), Message("a", "a2"), Message("b", "b1")]);
        await sender.SendMessagesAsync([Message("b", "b2"), Message("c", "c1")]);

        Assert.Equal(new[] { "a1", "b1", "c1" }, await DrainAsync("batch-dedup-q"));
    }

    [Fact]
    public async Task ATopicWithDuplicateDetection_DropsTheDuplicateForEverySubscription()
    {
        await _admin.CreateTopicAsync(new CreateTopicOptions("dedup-topic") { RequiresDuplicateDetection = true });
        await _admin.CreateSubscriptionAsync("dedup-topic", "s1");
        await _admin.CreateSubscriptionAsync("dedup-topic", "s2");
        await using var sender = _client.CreateSender("dedup-topic");

        await sender.SendMessageAsync(Message("id-1", "first"));
        await sender.SendMessageAsync(Message("id-1", "duplicate"));

        Assert.Equal(new[] { "first" }, await DrainAsync("dedup-topic", "s1"));
        Assert.Equal(new[] { "first" }, await DrainAsync("dedup-topic", "s2"));
    }

    [Fact]
    public async Task ForwardedMessages_AreCheckedAtTheEntityTheyArriveAt()
    {
        // The topic has no duplicate detection, so both copies fan out; the queue they are forwarded to drops the second.
        await CreateDedupQueueAsync("dedup-target");
        await _admin.CreateTopicAsync("forwarding-topic");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("forwarding-topic", "onward") { ForwardTo = "dedup-target" });
        await _admin.CreateSubscriptionAsync("forwarding-topic", "local");
        await using var sender = _client.CreateSender("forwarding-topic");

        await sender.SendMessageAsync(Message("id-1", "first"));
        await sender.SendMessageAsync(Message("id-1", "duplicate"));

        Assert.Equal(new[] { "first", "duplicate" }, await DrainAsync("forwarding-topic", "local"));
        Assert.Equal(new[] { "first" }, await DrainAsync("dedup-target"));
    }

    [Fact]
    public async Task AScheduledMessage_IsCheckedWhenItComesDue()
    {
        await CreateDedupQueueAsync("scheduled-dedup-q");
        await using var sender = _client.CreateSender("scheduled-dedup-q");

        await sender.ScheduleMessageAsync(Message("id-1", "scheduled"), DateTimeOffset.UtcNow.AddSeconds(1));
        await sender.SendMessageAsync(Message("id-1", "sent-now"));
        await Task.Delay(TimeSpan.FromSeconds(2.5));

        Assert.Equal(0, _broker.GetScheduledMessageCount("scheduled-dedup-q"));
        Assert.Equal(new[] { "sent-now" }, await DrainAsync("scheduled-dedup-q"));
    }

    [Fact]
    public async Task TransactionalSends_AreCheckedOnCommit_AndARolledBackSendIsNotRemembered()
    {
        await CreateDedupQueueAsync("tx-dedup-q");
        await using var sender = _client.CreateSender("tx-dedup-q");

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(Message("rolled-back", "never"));
            // no Complete(): rolled back
        }
        await sender.SendMessageAsync(Message("rolled-back", "accepted-after-rollback"));

        await sender.SendMessageAsync(Message("committed", "first"));
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(Message("committed", "duplicate-in-transaction"));
            scope.Complete();
        }

        Assert.Equal(new[] { "accepted-after-rollback", "first" }, await DrainAsync("tx-dedup-q"));
    }

    [Fact]
    public async Task RequiresDuplicateDetection_CannotBeChanged()
    {
        // The SDK makes the property read-only on update; a raw HTTP update reaches this check.
        await CreateDedupQueueAsync("fixed-dedup-q");
        var queue = _broker.GetQueue("fixed-dedup-q");
        var desired = queue.WithConfigurationFrom(queue);
        desired.RequiresDuplicateDetection = false;

        Assert.Equal("RequiresDuplicateDetection cannot be changed.", _broker.EntityManager.UpdateQueue(desired).Error);
        Assert.True(_broker.GetQueue("fixed-dedup-q").RequiresDuplicateDetection);
    }

    [Fact]
    public async Task AMassTransitEndpointWithDuplicateDetection_ConsumesAResentMessageOnce()
    {
        var received = new ConcurrentQueue<OrderPlaced>();
        var bus = Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new Uri("sb://localhost/"), _broker.CreateServiceBusClient(), _broker.CreateAdministrationClient());
            cfg.ReceiveEndpoint("dedup-orders", e =>
            {
                e.EnableDuplicateDetection(TimeSpan.FromMinutes(5));
                e.Consumer(() => new RecordingConsumer<OrderPlaced>(received));
            });
        });
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            Assert.True(_broker.GetQueue("dedup-orders").RequiresDuplicateDetection);

            // A sender retrying after a timeout resends with the same MessageId.
            var messageId = NewId.NextGuid();
            var endpoint = await bus.GetSendEndpoint(new Uri("queue:dedup-orders"));
            await endpoint.Send(new OrderPlaced("once"), ctx => ctx.MessageId = messageId);
            await endpoint.Send(new OrderPlaced("once"), ctx => ctx.MessageId = messageId);
            await endpoint.Send(new OrderPlaced("other"));

            await UntilAsync(() => received.Count == 2, "the two distinct orders");
            await Task.Delay(1000);
            Assert.Equal(new[] { "once", "other" }, received.Select(o => o.OrderId).Order()); // consumed concurrently
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Fact]
    public async Task TheHistory_IsForgottenOnRestart()
    {
        // ADR 0002: the history is in memory only.
        string dataDir;
        using (var first = new BrokerFixture())
        {
            dataDir = first.TakeDataDir();
            first.EntityManager.CreateQueue(new QueueInfo { Name = "restart-dedup-q", RequiresDuplicateDetection = true });
            await using var client = first.CreateServiceBusClient();
            await using var sender = client.CreateSender("restart-dedup-q");
            await sender.SendMessageAsync(Message("id-1", "before-restart"));
        }

        try
        {
            using var second = new BrokerFixture(dataDir);
            await using var client = second.CreateServiceBusClient();
            await using var sender = client.CreateSender("restart-dedup-q");
            await sender.SendMessageAsync(Message("id-1", "after-restart"));
            Assert.Equal(2, second.GetQueue("restart-dedup-q").MessageCount);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }
}
