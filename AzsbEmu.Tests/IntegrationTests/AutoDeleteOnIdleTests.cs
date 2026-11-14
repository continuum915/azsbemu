using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;
using MassTransit;
using static AzsbEmu.Tests.IntegrationTests.MassTransitWait;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Ticket 18: entities idle for longer than their AutoDeleteOnIdle are deleted. The SDK's admin client refuses idle
/// times under five minutes (Azure's minimum), so these tests configure short ones through the EntityManager.
/// </summary>
public class AutoDeleteOnIdleTests : IDisposable
{
    private static readonly TimeSpan ShortIdle = TimeSpan.FromSeconds(2);
    private readonly BrokerFixture _broker = new();

    public AutoDeleteOnIdleTests()
    {
        _broker.EntityManager.StartupGracePeriod = TimeSpan.Zero;
    }

    public void Dispose() => _broker.Dispose();

    private EntityManager Entities => _broker.EntityManager;

    private void CreateQueue(string name, TimeSpan autoDeleteOnIdle) =>
        Assert.True(Entities.CreateQueue(new QueueInfo { Name = name, AutoDeleteOnIdle = autoDeleteOnIdle }));

    /// <summary>Waits up to <paramref name="within"/> for <paramref name="condition"/>.</summary>
    private static async Task<bool> EventuallyAsync(Func<bool> condition, TimeSpan within)
    {
        var deadline = DateTime.UtcNow + within;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(100);
        }
        return true;
    }

    [Fact]
    public async Task AnIdleQueue_IsDeleted_WithItsMessagesAndDeadLetters()
    {
        CreateQueue("idle-queue", ShortIdle);
        await using (var client = _broker.CreateServiceBusClient())
        {
            await using var sender = client.CreateSender("idle-queue");
            await sender.SendMessagesAsync([new ServiceBusMessage("kept"), new ServiceBusMessage("dead")]);
            await using var receiver = client.CreateReceiver("idle-queue");
            var toDeadLetter = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeadLetterMessageAsync(toDeadLetter);
        }
        Assert.Equal(1, _broker.GetQueue("idle-queue").DeadLetterMessageCount);

        Assert.True(await EventuallyAsync(() => Entities.GetQueue("idle-queue") is null, TimeSpan.FromSeconds(10)), "the idle queue was not deleted");

        // A new queue of the same name starts empty: the old messages and dead letters went with the queue.
        _broker.EnsureQueue("idle-queue");
        var recreated = _broker.GetQueue("idle-queue");
        Assert.Equal(0, recreated.MessageCount);
        Assert.Equal(0, recreated.DeadLetterMessageCount);
    }

    [Fact]
    public async Task AQueueWithTheDefaultAutoDeleteOnIdle_IsNeverDeleted()
    {
        _broker.EnsureQueue("forever-queue");
        Assert.Equal(TimeSpan.MaxValue, _broker.GetQueue("forever-queue").AutoDeleteOnIdle);
        CreateQueue("control-queue", ShortIdle);

        Assert.True(await EventuallyAsync(() => Entities.GetQueue("control-queue") is null, TimeSpan.FromSeconds(10)));
        Assert.NotNull(Entities.GetQueue("forever-queue"));
    }

    [Fact]
    public async Task Sends_KeepAQueueAlive_UntilTheyStop()
    {
        CreateQueue("busy-queue", ShortIdle);
        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender("busy-queue");

        var until = DateTime.UtcNow + ShortIdle * 2.5;
        while (DateTime.UtcNow < until)
        {
            await sender.SendMessageAsync(new ServiceBusMessage("tick"));
            await Task.Delay(400);
        }
        Assert.NotNull(Entities.GetQueue("busy-queue"));

        Assert.True(await EventuallyAsync(() => Entities.GetQueue("busy-queue") is null, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task AnOpenReceiver_KeepsAQueueAlive_AndClosingItStartsTheIdleClock()
    {
        CreateQueue("watched-queue", ShortIdle);
        await using var client = _broker.CreateServiceBusClient();
        var receiver = client.CreateReceiver("watched-queue");
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500))); // attaches the link

        await Task.Delay(ShortIdle * 2.5);
        Assert.NotNull(Entities.GetQueue("watched-queue"));

        await receiver.DisposeAsync();
        var closedAt = DateTime.UtcNow;
        Assert.True(await EventuallyAsync(() => Entities.GetQueue("watched-queue") is null, TimeSpan.FromSeconds(10)));
        Assert.True(DateTime.UtcNow - closedAt >= ShortIdle - TimeSpan.FromMilliseconds(200), "deleted before it had been idle for its AutoDeleteOnIdle");
    }

    [Fact]
    public async Task ManagementOperations_KeepAQueueAlive()
    {
        CreateQueue("peeked-queue", ShortIdle);
        await using var client = _broker.CreateServiceBusClient();

        // Peeking goes over the management link and opens no receive link, so only the operations themselves count.
        await using var peeker = client.CreateReceiver("peeked-queue");
        var until = DateTime.UtcNow + ShortIdle * 2.5;
        while (DateTime.UtcNow < until)
        {
            await peeker.PeekMessageAsync();
            await Task.Delay(400);
        }
        Assert.NotNull(Entities.GetQueue("peeked-queue"));
    }

    [Fact]
    public async Task AnIdleSubscription_IsDeleted_AndItsTopicStays()
    {
        _broker.EnsureTopic("lasting-topic");
        Assert.True(Entities.CreateSubscription("lasting-topic", new SubscriptionInfo { Name = "fleeting", AutoDeleteOnIdle = ShortIdle }));
        _broker.EnsureSubscription("lasting-topic", "permanent");

        Assert.True(await EventuallyAsync(() => Entities.GetSubscription("lasting-topic", "fleeting") is null, TimeSpan.FromSeconds(10)));
        Assert.NotNull(Entities.GetTopic("lasting-topic"));
        Assert.NotNull(Entities.GetSubscription("lasting-topic", "permanent"));
    }

    [Fact]
    public async Task AnIdleTopic_IsDeletedWithItsSubscriptions()
    {
        Assert.True(Entities.CreateTopic(new TopicInfo { Name = "fleeting-topic", AutoDeleteOnIdle = ShortIdle }));
        _broker.EnsureSubscription("fleeting-topic", "sub");

        Assert.True(await EventuallyAsync(() => Entities.GetTopic("fleeting-topic") is null, TimeSpan.FromSeconds(10)));

        // Recreating the topic does not bring the subscription back.
        _broker.EnsureTopic("fleeting-topic");
        Assert.Null(Entities.GetSubscription("fleeting-topic", "sub"));
    }

    [Fact]
    public async Task MessagesForwardedIntoAQueue_KeepItAlive()
    {
        CreateQueue("forward-target", ShortIdle);
        _broker.EnsureTopic("forwarding-topic");
        Assert.True(Entities.CreateSubscription("forwarding-topic", new SubscriptionInfo { Name = "to-queue", ForwardTo = "forward-target" }));

        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender("forwarding-topic");
        var until = DateTime.UtcNow + ShortIdle * 2.5;
        while (DateTime.UtcNow < until)
        {
            await sender.SendMessageAsync(new ServiceBusMessage("tick"));
            await Task.Delay(400);
        }
        Assert.NotNull(Entities.GetQueue("forward-target"));
    }

    [Fact]
    public async Task AccessedAt_ReportsTheLastActivity()
    {
        _broker.EnsureQueue("accessed-queue");
        var admin = _broker.CreateAdministrationClient();
        var before = (await admin.GetQueueRuntimePropertiesAsync("accessed-queue")).Value.AccessedAt;

        await Task.Delay(1100);
        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender("accessed-queue");
        await sender.SendMessageAsync(new ServiceBusMessage("touch"));

        var after = (await admin.GetQueueRuntimePropertiesAsync("accessed-queue")).Value.AccessedAt;
        Assert.True(after > before, $"AccessedAt did not move: {before:O} -> {after:O}");
    }

    [Fact]
    public async Task AMassTransitBusTemporaryEndpoint_StaysWhileTheBusRuns_AndIsDeletedAfterItStops()
    {
        var bus = Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new Uri("sb://localhost/"), _broker.CreateServiceBusClient(), _broker.CreateAdministrationClient());
            cfg.ReceiveEndpoint("pricing-for-autodelete", e => e.Consumer<PricingConsumer>());
        });
        await bus.StartAsync(NewTimeoutToken());
        List<string> temporary;
        try
        {
            // A request makes sure the bus endpoint (where responses go) is up.
            var requests = bus.CreateRequestClient<PriceQuery>(new Uri("queue:pricing-for-autodelete"), RequestTimeout.After(s: 15));
            await requests.GetResponse<PriceQuote>(new PriceQuery("sku-1"));

            // The bus endpoint is MassTransit's temporary queue (<machine>_<process>_bus_<id>). Its AutoDeleteOnIdle is
            // at least five minutes, Azure's minimum, so shorten it to keep the test quick. (MassTransit gives every
            // queue a finite AutoDeleteOnIdle, 427 days for ordinary endpoints, so that alone does not identify it.)
            temporary = Entities.GetQueues().Where(q => q.Name.Contains("_bus_")).Select(q => q.Name).ToList();
            Assert.Single(temporary);
            Assert.True(_broker.GetQueue(temporary[0]).AutoDeleteOnIdle <= TimeSpan.FromDays(1));
            foreach (var name in temporary)
            {
                var queue = _broker.GetQueue(name);
                var desired = queue.WithConfigurationFrom(queue);
                desired.AutoDeleteOnIdle = ShortIdle;
                Assert.Null(Entities.UpdateQueue(desired).Error);
            }

            await Task.Delay(ShortIdle * 2.5);
            Assert.All(temporary, name => Assert.NotNull(Entities.GetQueue(name)));
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }

        Assert.True(await EventuallyAsync(() => temporary.All(name => Entities.GetQueue(name) is null), TimeSpan.FromSeconds(15)),
            "the bus's temporary queues were not deleted after it stopped");
        Assert.NotNull(Entities.GetQueue("pricing-for-autodelete"));
    }
}

/// <summary>AutoDeleteOnIdle across restarts: idle time is remembered, and nothing is deleted the moment the emulator starts.</summary>
public class AutoDeleteOnIdleRestartTests
{
    [Fact]
    public async Task AQueueThatWentIdleWhileStopped_IsDeletedSoonAfterStart_NotImmediately()
    {
        string dataDir;
        using (var first = new BrokerFixture())
        {
            dataDir = first.TakeDataDir();
            Assert.True(first.EntityManager.CreateQueue(new QueueInfo { Name = "stale-queue", AutoDeleteOnIdle = TimeSpan.FromSeconds(3) }));
        }

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(4)); // idle for longer than its AutoDeleteOnIdle while stopped

            // The grace period is capped at the entity's own AutoDeleteOnIdle: it gets 3s after start, not 30s.
            using var second = new BrokerFixture(dataDir);
            second.EntityManager.StartupGracePeriod = TimeSpan.FromSeconds(30);
            var started = DateTime.UtcNow;

            await Task.Delay(TimeSpan.FromSeconds(1.5));
            Assert.NotNull(second.EntityManager.GetQueue("stale-queue"));

            var deadline = started + TimeSpan.FromSeconds(15);
            while (second.EntityManager.GetQueue("stale-queue") is not null && DateTime.UtcNow < deadline) await Task.Delay(100);
            Assert.Null(second.EntityManager.GetQueue("stale-queue"));
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task TheLastActivity_SurvivesARestart()
    {
        // Created at t=0 with an 8s idle time, last used at about t=4, then restarted. Counting from creation it would
        // go at t=8; counting from the stored last use it stays until about t=12.
        var idle = TimeSpan.FromSeconds(8);
        string dataDir;
        DateTime lastUse;
        using (var first = new BrokerFixture())
        {
            dataDir = first.TakeDataDir();
            Assert.True(first.EntityManager.CreateQueue(new QueueInfo { Name = "used-queue", AutoDeleteOnIdle = idle }));
            await Task.Delay(TimeSpan.FromSeconds(4));
            await using var client = first.CreateServiceBusClient();
            await using var sender = client.CreateSender("used-queue");
            await sender.SendMessageAsync(new ServiceBusMessage("use"));
            lastUse = DateTime.UtcNow;
        }

        try
        {
            using var second = new BrokerFixture(dataDir);
            second.EntityManager.StartupGracePeriod = TimeSpan.Zero;

            var stillThere = lastUse + idle - TimeSpan.FromSeconds(1.5);
            if (stillThere > DateTime.UtcNow) await Task.Delay(stillThere - DateTime.UtcNow);
            Assert.NotNull(second.EntityManager.GetQueue("used-queue"));

            var deadline = lastUse + idle + TimeSpan.FromSeconds(10);
            while (second.EntityManager.GetQueue("used-queue") is not null && DateTime.UtcNow < deadline) await Task.Delay(100);
            Assert.Null(second.EntityManager.GetQueue("used-queue"));
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }
}
