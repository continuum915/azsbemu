using System.Transactions;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Forwarding chains, limits and failures, and every kind of send going through forwarding entities.</summary>
public class ForwardingEdgeCaseTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ForwardingEdgeCaseTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task QueueToTopicToSubscriptionToQueue_DeliversToTheFinalQueue()
    {
        await _admin.CreateQueueAsync("final");
        await _admin.CreateTopicAsync("middle");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("middle", "onward") { ForwardTo = "final" });
        await _admin.CreateQueueAsync(new CreateQueueOptions("entry") { ForwardTo = "middle" });

        await SendAsync("entry", new ServiceBusMessage("through-the-chain"));

        Assert.Equal("through-the-chain", (await ReceiveAsync("final"))?.Body.ToString());
    }

    [Fact]
    public async Task FourForwards_AreDelivered_AndAFifthIsDeadLetteredWhereTheLimitWasHit()
    {
        // q1 → q2 → q3 → q4 → q5: four forwards, delivered to q5.
        await CreateChainAsync("ok", length: 5);
        await SendAsync("ok-1", new ServiceBusMessage("four-hops"));
        Assert.Equal("four-hops", (await ReceiveAsync("ok-5"))?.Body.ToString());

        // q1 → … → q6: q5 would make the fifth forward, so the message is dead-lettered on q5.
        await CreateChainAsync("over", length: 6);
        await SendAsync("over-1", new ServiceBusMessage("five-hops"));
        Assert.Null(await ReceiveAsync("over-6", TimeSpan.FromSeconds(1)));
        var deadLettered = await ReceiveAsync("over-5", deadLetter: true);
        Assert.Equal("five-hops", deadLettered?.Body.ToString());
        Assert.Equal("MaxTransferHopCountExceeded", deadLettered?.DeadLetterReason);
        Assert.Contains("4", deadLettered?.DeadLetterErrorDescription);
    }

    [Fact]
    public async Task TopicFanOut_DoesNotCountAsAHop()
    {
        // entry → topic → (fan-out) → sub → q2 → q3 → q4: four forwards (entry, sub, q2, q3). If fan-out counted,
        // q3 would be making the fifth forward and dead-letter instead.
        await _admin.CreateQueueAsync("fan-q4");
        await _admin.CreateQueueAsync(new CreateQueueOptions("fan-q3") { ForwardTo = "fan-q4" });
        await _admin.CreateQueueAsync(new CreateQueueOptions("fan-q2") { ForwardTo = "fan-q3" });
        await _admin.CreateTopicAsync("fan-topic");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("fan-topic", "onward") { ForwardTo = "fan-q2" });
        await _admin.CreateQueueAsync(new CreateQueueOptions("fan-entry") { ForwardTo = "fan-topic" });

        await SendAsync("fan-entry", new ServiceBusMessage("via-topic"));

        Assert.Equal("via-topic", (await ReceiveAsync("fan-q4"))?.Body.ToString());
    }

    [Fact]
    public async Task AForwardingLoop_DeadLettersWithTheHopLimitReason()
    {
        await _admin.CreateQueueAsync("b");
        await _admin.CreateQueueAsync(new CreateQueueOptions("a") { ForwardTo = "b" });
        await _admin.DeleteQueueAsync("b");
        await _admin.CreateQueueAsync(new CreateQueueOptions("b") { ForwardTo = "a" });

        await SendAsync("a", new ServiceBusMessage("around-and-around"));

        // a → b → a → b → a: a holds the message after four forwards and would make the fifth.
        var deadLettered = await ReceiveAsync("a", deadLetter: true);
        Assert.Equal("around-and-around", deadLettered?.Body.ToString());
        Assert.Equal("MaxTransferHopCountExceeded", deadLettered?.DeadLetterReason);
    }

    [Fact]
    public async Task AMessageArrivingAfterItsForwardTargetWasDeleted_IsDeadLetteredOnTheSource()
    {
        await _admin.CreateQueueAsync("gone");
        await _admin.CreateQueueAsync(new CreateQueueOptions("source") { ForwardTo = "gone" });
        await _admin.DeleteQueueAsync("gone");

        await SendAsync("source", new ServiceBusMessage("orphaned"));

        Assert.Null(await ReceiveAsync("source", TimeSpan.FromSeconds(1)));
        var deadLettered = await ReceiveAsync("source", deadLetter: true);
        Assert.Equal("orphaned", deadLettered?.Body.ToString());
        Assert.Equal("ForwardToTargetNotFound", deadLettered?.DeadLetterReason);
        Assert.Contains("gone", deadLettered?.DeadLetterErrorDescription);
    }

    [Fact]
    public async Task AScheduledMessageSentToAForwardingQueue_ArrivesAtTheTargetWhenDue()
    {
        await _admin.CreateQueueAsync("target");
        await _admin.CreateQueueAsync(new CreateQueueOptions("forwarder") { ForwardTo = "target" });

        var due = DateTimeOffset.UtcNow.AddSeconds(3);
        await using (var sender = _client.CreateSender("forwarder"))
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later"), due);

        // Nothing until it is due (the scheduler ticks every second, so allow that much slack before the due time).
        Assert.Null(await ReceiveAsync("target", due - DateTimeOffset.UtcNow - TimeSpan.FromSeconds(1)));
        var received = await ReceiveAsync("target", TimeSpan.FromSeconds(10));
        Assert.Equal("later", received?.Body.ToString());
        Assert.True(received!.EnqueuedTime >= due - TimeSpan.FromSeconds(1), $"enqueued {received.EnqueuedTime:O}, due {due:O}");
    }

    [Fact]
    public async Task ATransactionalSendToAForwardingQueue_ArrivesOnlyOnCommit()
    {
        await _admin.CreateQueueAsync("target");
        await _admin.CreateQueueAsync(new CreateQueueOptions("forwarder") { ForwardTo = "target" });
        await using var sender = _client.CreateSender("forwarder");

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("rolled-back"));
            // disposed without Complete(): rollback
        }
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("committed"));
            scope.Complete();
        }

        Assert.Equal("committed", (await ReceiveAsync("target"))?.Body.ToString());
        Assert.Null(await ReceiveAsync("target", TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task AMessageWithASessionId_ForwardedToASessionQueue_IsReceivedInThatSession()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("sessions") { RequiresSession = true });
        await _admin.CreateQueueAsync(new CreateQueueOptions("forwarder") { ForwardTo = "sessions" });

        await SendAsync("forwarder", new ServiceBusMessage("in-session") { SessionId = "order-7" });

        await using var receiver = await _client.AcceptSessionAsync("sessions", "order-7");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("in-session", received?.Body.ToString());
        Assert.Equal("order-7", received?.SessionId);
    }

    [Fact]
    public async Task ASubscriptionsRuleAction_IsAppliedBeforeTheMessageIsForwarded()
    {
        await _admin.CreateQueueAsync("billing");
        await _admin.CreateTopicAsync("orders");
        await _admin.CreateSubscriptionAsync(
            new CreateSubscriptionOptions("orders", "to-billing") { ForwardTo = "billing" },
            new CreateRuleOptions("$Default", new SqlRuleFilter("region = 'eu'"))
            {
                Action = new SqlRuleAction("SET user.routedBy = 'to-billing'; SET sys.Label = 'BillingOrder'; SET copiedRegion = region; REMOVE secret"),
            });

        await SendAsync("orders", new ServiceBusMessage("order")
        {
            Subject = "Order",
            ApplicationProperties = { ["region"] = "eu", ["secret"] = "hunter2" },
        });

        var received = await ReceiveAsync("billing");
        Assert.NotNull(received);
        Assert.Equal("to-billing", received.ApplicationProperties["routedBy"]);
        Assert.Equal("BillingOrder", received.Subject);
        Assert.Equal("eu", received.ApplicationProperties["copiedRegion"]);
        Assert.False(received.ApplicationProperties.ContainsKey("secret"));
    }

    private async Task CreateChainAsync(string prefix, int length)
    {
        await _admin.CreateQueueAsync($"{prefix}-{length}");
        for (int i = length - 1; i >= 1; i--)
            await _admin.CreateQueueAsync(new CreateQueueOptions($"{prefix}-{i}") { ForwardTo = $"{prefix}-{i + 1}" });
    }

    private async Task SendAsync(string entity, ServiceBusMessage message)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessageAsync(message);
    }

    private async Task<ServiceBusReceivedMessage?> ReceiveAsync(string queue, TimeSpan? wait = null, bool deadLetter = false)
    {
        await using var receiver = _client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            SubQueue = deadLetter ? SubQueue.DeadLetter : SubQueue.None,
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        return await receiver.ReceiveMessageAsync(wait ?? TimeSpan.FromSeconds(5));
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
