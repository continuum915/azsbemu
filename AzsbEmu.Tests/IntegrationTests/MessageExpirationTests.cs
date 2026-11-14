using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Expired messages are dead-lettered only when the entity has DeadLetteringOnMessageExpiration enabled; otherwise
/// they are discarded, as Service Bus does. Expiry is noticed either at delivery or by the background scan.
/// </summary>
public class MessageExpirationTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public MessageExpirationTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task WithTheFlagOff_AMessageExpiringAtDelivery_IsDiscarded()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("discard-at-delivery") { DeadLetteringOnMessageExpiration = false });
        await AfterAScanAsync("discard-at-delivery");
        await SendAsync("discard-at-delivery", TimeSpan.FromSeconds(1));
        await Task.Delay(TimeSpan.FromSeconds(1.5)); // well inside the next 5s scan interval

        await using (var receiver = _client.CreateReceiver("discard-at-delivery"))
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));

        await AssertGoneAsync("discard-at-delivery");
    }

    [Fact]
    public async Task WithTheFlagOff_AMessageExpiringUnread_IsDiscardedByTheScan()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("discard-by-scan") { DeadLetteringOnMessageExpiration = false });
        await SendAsync("discard-by-scan", TimeSpan.FromSeconds(1));

        // No receiver: only the background scan (every 5s) can notice the expiry.
        await WaitUntilAsync(async () => (await _admin.GetQueueRuntimePropertiesAsync("discard-by-scan")).Value.ActiveMessageCount == 0);
        await AssertGoneAsync("discard-by-scan");
    }

    [Fact]
    public async Task TheScan_RetiresEachExpiredMessageOnlyOnce()
    {
        // Retired records stay in the log until truncation passes them, so later scans (every 5s) see them again.
        await _admin.CreateQueueAsync(new CreateQueueOptions("scan-discard-once") { DeadLetteringOnMessageExpiration = false });
        await _admin.CreateQueueAsync(new CreateQueueOptions("scan-dead-letter-once") { DeadLetteringOnMessageExpiration = true });
        await SendAsync("scan-discard-once", TimeSpan.FromSeconds(1));
        await SendAsync("scan-dead-letter-once", TimeSpan.FromSeconds(1));
        await WaitUntilAsync(async () => (await _admin.GetQueueRuntimePropertiesAsync("scan-discard-once")).Value.ActiveMessageCount == 0);
        await WaitUntilAsync(async () => (await _admin.GetQueueRuntimePropertiesAsync("scan-dead-letter-once")).Value.DeadLetterMessageCount == 1);

        await Task.Delay(TimeSpan.FromSeconds(11)); // at least two more scans

        var discarded = (await _admin.GetQueueRuntimePropertiesAsync("scan-discard-once")).Value;
        Assert.Equal(0, discarded.ActiveMessageCount);
        var deadLettered = (await _admin.GetQueueRuntimePropertiesAsync("scan-dead-letter-once")).Value;
        Assert.Equal(0, deadLettered.ActiveMessageCount);
        Assert.Equal(1, deadLettered.DeadLetterMessageCount);
    }

    [Fact]
    public async Task TheScan_DoesNotRetireAMessageAlreadyReceivedAndCompleted()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("completed-then-expired") { DeadLetteringOnMessageExpiration = true });
        await using var sender = _client.CreateSender("completed-then-expired");
        // The first message stays locked (unsettled), holding the log's begin address before the second one.
        await sender.SendMessageAsync(new ServiceBusMessage("held"));
        await sender.SendMessageAsync(new ServiceBusMessage("done") { TimeToLive = TimeSpan.FromSeconds(1) });

        await using var receiver = _client.CreateReceiver("completed-then-expired");
        var held = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var done = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("held", held!.Body.ToString());
        Assert.Equal("done", done!.Body.ToString());
        await receiver.CompleteMessageAsync(done);

        await Task.Delay(TimeSpan.FromSeconds(7)); // "done" is past its TTL and at least one scan has run
        Assert.Equal(0, (await _admin.GetQueueRuntimePropertiesAsync("completed-then-expired")).Value.DeadLetterMessageCount);
        await receiver.CompleteMessageAsync(held);
    }

    [Fact]
    public async Task WithTheFlagOn_ExpiredMessagesAreDeadLettered_ByEitherPath()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("keep-expired") { DeadLetteringOnMessageExpiration = true });
        await SendAsync("keep-expired", TimeSpan.FromSeconds(1)); // noticed by the scan
        await WaitUntilAsync(async () => (await _admin.GetQueueRuntimePropertiesAsync("keep-expired")).Value.DeadLetterMessageCount == 1);

        await SendAsync("keep-expired", TimeSpan.FromSeconds(1)); // noticed at delivery: a scan just ran
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        await using (var receiver = _client.CreateReceiver("keep-expired"))
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));

        await using var dlq = _client.CreateReceiver("keep-expired", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var reasons = new List<string?>();
        while (await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(2)) is { } m)
        {
            reasons.Add(m.DeadLetterReason);
            await dlq.CompleteMessageAsync(m);
        }
        Assert.Equal(new[] { "TTLExpiredException", "TTLExpiredException" }, reasons);
    }

    [Fact]
    public async Task ASubscriptionWithTheFlagOff_DiscardsExpiredMessages()
    {
        await _admin.CreateTopicAsync("t");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("t", "s") { DeadLetteringOnMessageExpiration = false });
        await using (var sender = _client.CreateSender("t"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("expires") { TimeToLive = TimeSpan.FromSeconds(1) });
            await sender.SendMessageAsync(new ServiceBusMessage("lives"));
        }
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        await using (var receiver = _client.CreateReceiver("t", "s"))
        {
            var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("lives", received!.Body.ToString());
            await receiver.CompleteMessageAsync(received);
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        }

        var runtime = (await _admin.GetSubscriptionRuntimePropertiesAsync("t", "s")).Value;
        Assert.Equal(0, runtime.ActiveMessageCount);
        Assert.Equal(0, runtime.DeadLetterMessageCount);
    }

    /// <summary>Returns just after a TTL scan has run, so a message sent now will expire before the next one.</summary>
    private async Task AfterAScanAsync(string queue)
    {
        await SendAsync(queue, TimeSpan.FromMilliseconds(100));
        await WaitUntilAsync(async () => (await _admin.GetQueueRuntimePropertiesAsync(queue)).Value.ActiveMessageCount == 0);
    }

    private async Task SendAsync(string entity, TimeSpan ttl)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessageAsync(new ServiceBusMessage("short-lived") { TimeToLive = ttl });
    }

    private async Task AssertGoneAsync(string queue)
    {
        var runtime = (await _admin.GetQueueRuntimePropertiesAsync(queue)).Value;
        Assert.Equal(0, runtime.ActiveMessageCount);
        Assert.Equal(0, runtime.DeadLetterMessageCount);
        await using var dlq = _client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        Assert.Null(await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for the expiry to be processed.");
            await Task.Delay(250);
        }
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
