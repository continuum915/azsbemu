using System.Transactions;
using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Properties to modify sent with abandon or defer change the message's application properties, as Service Bus does,
/// whichever way the settlement arrives: receive link, transaction, or management link.
/// </summary>
public class SettlementPropertiesTests : IDisposable
{
    private const string Queue = "settle-props";
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusClient _client;

    public SettlementPropertiesTests()
    {
        _broker.EnsureQueue(Queue);
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task Abandon_WithPropertiesToModify_RedeliversTheMessageCarryingThem()
    {
        await SendAsync();
        await using var receiver = _client.CreateReceiver(Queue);

        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var id = Guid.NewGuid();
        var at = new DateTime(2026, 11, 13, 12, 0, 0, DateTimeKind.Utc);
        await receiver.AbandonMessageAsync(first!, new Dictionary<string, object>
        {
            ["stage"] = "abandoned", ["attempt"] = 1, ["big"] = 5_000_000_000L, ["id"] = id, ["at"] = at
        });

        var peeked = await receiver.PeekMessageAsync();
        Assert.Equal("abandoned", peeked!.ApplicationProperties["stage"]);

        var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("abandoned", again!.ApplicationProperties["stage"]);
        Assert.Equal(1, again.ApplicationProperties["attempt"]);
        Assert.Equal("kept", again.ApplicationProperties["untouched"]);
        Assert.Equal(5_000_000_000L, again.ApplicationProperties["big"]);
        Assert.Equal(id, again.ApplicationProperties["id"]);
        Assert.Equal(at, again.ApplicationProperties["at"]);
        Assert.Equal(2, again.DeliveryCount);
    }

    [Fact]
    public async Task Defer_WithPropertiesToModify_AppliesThem_OverTheLinkAndOverManagement()
    {
        await SendAsync();
        await using var receiver = _client.CreateReceiver(Queue);

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        long sequenceNumber = received!.SequenceNumber;
        await receiver.DeferMessageAsync(received, new Dictionary<string, object> { ["stage"] = "deferred" }); // receive link

        var deferred = await receiver.ReceiveDeferredMessageAsync(sequenceNumber);
        Assert.Equal("deferred", deferred.ApplicationProperties["stage"]);

        // A message received by sequence number is settled over the management link.
        await receiver.DeferMessageAsync(deferred, new Dictionary<string, object> { ["stage"] = "deferred-again", ["count"] = 2 });

        var deferredAgain = await receiver.ReceiveDeferredMessageAsync(sequenceNumber);
        Assert.Equal("deferred-again", deferredAgain.ApplicationProperties["stage"]);
        Assert.Equal(2, deferredAgain.ApplicationProperties["count"]);
        Assert.Equal("kept", deferredAgain.ApplicationProperties["untouched"]);
        await receiver.CompleteMessageAsync(deferredAgain);
    }

    [Fact]
    public async Task AbandonInATransaction_WithPropertiesToModify_AppliesThemOnCommit()
    {
        await SendAsync();
        await using var receiver = _client.CreateReceiver(Queue);

        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.AbandonMessageAsync(first!, new Dictionary<string, object> { ["stage"] = "abandoned-in-txn" });
            ts.Complete();
        }

        var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("abandoned-in-txn", again!.ApplicationProperties["stage"]);
    }

    [Fact]
    public async Task AbandonInARolledBackTransaction_DoesNotApplyThem()
    {
        await SendAsync();
        await using var receiver = _client.CreateReceiver(Queue);

        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.AbandonMessageAsync(first!, new Dictionary<string, object> { ["stage"] = "rolled-back" });
            // no Complete: the abandon is rolled back and the message stays locked by this receiver
        }
        await receiver.AbandonMessageAsync(first!);

        var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("sent", again!.ApplicationProperties["stage"]);
    }

    [Fact]
    public async Task PropertiesFromAnAbandon_AreKept_WhenTheMessageIsLaterDeadLettered()
    {
        await SendAsync();
        await using var receiver = _client.CreateReceiver(Queue);

        // Generous timeouts: a receive returns as soon as the message arrives, and this once timed out under full-suite load.
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30));
        Assert.NotNull(first);
        await receiver.AbandonMessageAsync(first, new Dictionary<string, object> { ["stage"] = "abandoned" });
        var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30));
        Assert.NotNull(again);
        await receiver.DeadLetterMessageAsync(again, "gave-up", "after one retry");

        await using var dlq = _client.CreateReceiver(Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var deadLettered = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("abandoned", deadLettered!.ApplicationProperties["stage"]);
        Assert.Equal("gave-up", deadLettered.DeadLetterReason);
    }

    [Fact]
    public async Task ADeferredMessageThatWasAbandoned_KeepsItsDeliveryCount_WhenReceivedBySequenceNumber()
    {
        // Ticket 17: receive by sequence number used to report the stored count only, missing the abandon (ADR 0001).
        await SendAsync();
        await using var receiver = _client.CreateReceiver(Queue);

        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        await receiver.AbandonMessageAsync(first!);
        var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, again!.DeliveryCount);
        await receiver.DeferMessageAsync(again);

        // Deferring doesn't count as a failed delivery, so receiving it by sequence number reports the same count as
        // its last receive over the link. (Peek reports one less: the SDK adds the current delivery to received messages.)
        var deferred = await receiver.ReceiveDeferredMessageAsync(again.SequenceNumber);
        Assert.Equal(again.DeliveryCount, deferred.DeliveryCount);
    }

    private async Task SendAsync()
    {
        await using var sender = _client.CreateSender(Queue);
        await sender.SendMessageAsync(new ServiceBusMessage("payload")
        {
            ApplicationProperties = { ["stage"] = "sent", ["untouched"] = "kept" }
        });
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
