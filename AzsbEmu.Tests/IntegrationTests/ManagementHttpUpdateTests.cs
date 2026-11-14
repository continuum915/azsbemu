using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Updating queues, topics and subscriptions through the management API.</summary>
public class ManagementHttpUpdateTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ManagementHttpUpdateTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task UpdateQueue_PersistsTheChanges_AcrossARestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            QueueProperties queue = await admin.CreateQueueAsync("tunable");
            queue.MaxDeliveryCount = 3;
            queue.LockDuration = TimeSpan.FromSeconds(20);
            queue.DefaultMessageTimeToLive = TimeSpan.FromMinutes(30);
            queue.UserMetadata = "tuned";
            queue.Status = EntityStatus.SendDisabled;

            QueueProperties updated = await admin.UpdateQueueAsync(queue);
            Assert.Equal(3, updated.MaxDeliveryCount);
            Assert.Equal(3, (await admin.GetQueueAsync("tunable")).Value.MaxDeliveryCount);
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            QueueProperties after = await restarted.CreateAdministrationClient().GetQueueAsync("tunable");
            Assert.Equal(3, after.MaxDeliveryCount);
            Assert.Equal(TimeSpan.FromSeconds(20), after.LockDuration);
            Assert.Equal(TimeSpan.FromMinutes(30), after.DefaultMessageTimeToLive);
            Assert.Equal("tuned", after.UserMetadata);
            Assert.Equal(EntityStatus.SendDisabled, after.Status);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task UpdateQueue_OfAMissingQueue_RaisesNotFound_AndCreatesNothing()
    {
        QueueProperties queue = await _admin.CreateQueueAsync("short-lived");
        await _admin.DeleteQueueAsync("short-lived");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.UpdateQueueAsync(queue));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
        Assert.False((await _admin.QueueExistsAsync("short-lived")).Value);
    }

    [Fact]
    public async Task UpdatedQueueSettings_TakeEffectWithoutARestart()
    {
        await _admin.CreateQueueAsync("redirected");
        QueueProperties queue = await _admin.CreateQueueAsync("live");
        queue.MaxDeliveryCount = 1;
        queue.LockDuration = TimeSpan.FromSeconds(7);
        await _admin.UpdateQueueAsync(queue);

        await SendAsync("live", "deliver-once");
        await using (var receiver = _client.CreateReceiver("live"))
        {
            var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(first);
            Assert.InRange(first.LockedUntil - DateTimeOffset.UtcNow, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8));
            await receiver.AbandonMessageAsync(first);
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        }
        await using (var dlq = _client.CreateReceiver("live", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }))
            Assert.Equal("MaxDeliveryCountExceeded", (await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.DeadLetterReason);

        queue = await _admin.GetQueueAsync("live");
        queue.DefaultMessageTimeToLive = TimeSpan.FromSeconds(1);
        await _admin.UpdateQueueAsync(queue);
        await SendAsync("live", "expires");
        await Task.Delay(TimeSpan.FromSeconds(2));
        await using (var receiver = _client.CreateReceiver("live"))
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));

        queue = await _admin.GetQueueAsync("live");
        queue.DefaultMessageTimeToLive = TimeSpan.MaxValue;
        queue.ForwardTo = "redirected";
        await _admin.UpdateQueueAsync(queue);
        await SendAsync("live", "forwarded");
        await using (var receiver = _client.CreateReceiver("redirected"))
            Assert.Equal("forwarded", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
    }

    [Fact]
    public async Task AShortenedLockDuration_IsWhenUnsettledLocksActuallyExpire()
    {
        QueueProperties queue = await _admin.CreateQueueAsync("relock"); // created with the default 60s lock
        queue.LockDuration = TimeSpan.FromSeconds(5);
        await _admin.UpdateQueueAsync(queue);
        await SendAsync("relock", "held");

        await using var receiver = _client.CreateReceiver("relock");
        Assert.NotNull(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5))); // locked, never settled

        // Past the new 5s lock, far short of the original 60s one.
        await Task.Delay(TimeSpan.FromSeconds(7));

        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("held", redelivered?.Body.ToString());
        Assert.Equal(2, redelivered!.DeliveryCount);
    }

    [Fact]
    public async Task AShortenedSubscriptionLockDuration_IsWhenUnsettledLocksActuallyExpire()
    {
        await _admin.CreateTopicAsync("relock-topic");
        SubscriptionProperties sub = await _admin.CreateSubscriptionAsync("relock-topic", "s"); // default 60s lock
        sub.LockDuration = TimeSpan.FromSeconds(5);
        await _admin.UpdateSubscriptionAsync(sub);
        await SendAsync("relock-topic", "held");

        await using var receiver = _client.CreateReceiver("relock-topic", "s");
        Assert.NotNull(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5))); // locked, never settled
        await Task.Delay(TimeSpan.FromSeconds(7));

        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("held", redelivered?.Body.ToString());
        Assert.Equal(2, redelivered!.DeliveryCount);
    }

    [Fact]
    public async Task UpdateTopicAndSubscription_PersistAndApply()
    {
        await _admin.CreateQueueAsync("sink");
        TopicProperties topic = await _admin.CreateTopicAsync("news");
        SubscriptionProperties sub = await _admin.CreateSubscriptionAsync("news", "reader");

        topic.UserMetadata = "topic-tuned";
        topic.DefaultMessageTimeToLive = TimeSpan.FromHours(2);
        Assert.Equal("topic-tuned", (await _admin.UpdateTopicAsync(topic)).Value.UserMetadata);
        Assert.Equal(TimeSpan.FromHours(2), (await _admin.GetTopicAsync("news")).Value.DefaultMessageTimeToLive);

        sub.MaxDeliveryCount = 4;
        sub.ForwardTo = "sink";
        Assert.Equal(4, (await _admin.UpdateSubscriptionAsync(sub)).Value.MaxDeliveryCount);
        Assert.Equal(4, (await _admin.GetSubscriptionAsync("news", "reader")).Value.MaxDeliveryCount);

        await SendAsync("news", "headline");
        await using var receiver = _client.CreateReceiver("sink");
        Assert.Equal("headline", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
    }

    [Fact]
    public async Task Updates_ThatChangeImmutableSettingsOrForwardBadly_AreRejected_AndChangeNothing()
    {
        QueueProperties queue = await _admin.CreateQueueAsync("fixed");

        // The SDK makes RequiresSession read-only on QueueProperties, so send that update directly.
        using (var http = new HttpClient())
        {
            var update = new HttpRequestMessage(HttpMethod.Put, $"http://127.0.0.1:{_broker.HttpPort}/fixed?api-version=2021-05")
            {
                Content = new StringContent(
                    """<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><RequiresSession>true</RequiresSession><MaxDeliveryCount>2</MaxDeliveryCount></QueueDescription></content></entry>""",
                    System.Text.Encoding.UTF8, "application/atom+xml"),
            };
            update.Headers.TryAddWithoutValidation("If-Match", "*");
            Assert.Equal(System.Net.HttpStatusCode.BadRequest, (await http.SendAsync(update)).StatusCode);
        }

        queue.ForwardTo = "missing";
        queue.MaxDeliveryCount = 2;
        await Assert.ThrowsAsync<ArgumentException>(() => _admin.UpdateQueueAsync(queue));

        var after = (await _admin.GetQueueAsync("fixed")).Value;
        Assert.False(after.RequiresSession);
        Assert.Null(after.ForwardTo);
        Assert.Equal(10, after.MaxDeliveryCount);
    }

    private async Task SendAsync(string entity, string body)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessageAsync(new ServiceBusMessage(body));
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
