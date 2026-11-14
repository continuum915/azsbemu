using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Queue management through <see cref="ServiceBusAdministrationClient"/> (the HTTP/Atom management API).</summary>
public class ManagementHttpQueueTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ManagementHttpQueueTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task CreateQueue_CreatesAQueueThatCanBeSentToAndReceivedFrom()
    {
        QueueProperties created = await _admin.CreateQueueAsync("http-created");
        Assert.Equal("http-created", created.Name);

        await using var sender = _client.CreateSender("http-created");
        await sender.SendMessageAsync(new ServiceBusMessage("hello"));

        await using var receiver = _client.CreateReceiver("http-created");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("hello", received.Body.ToString());
    }

    [Fact]
    public async Task GetQueue_OnAMissingName_RaisesNotFound_AndQueueExistsReportsBothCases()
    {
        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.GetQueueAsync("never-created"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
        Assert.False((await _admin.QueueExistsAsync("never-created")).Value);

        await _admin.CreateQueueAsync("exists-check");
        Assert.True((await _admin.QueueExistsAsync("exists-check")).Value);
        Assert.Equal("exists-check", (await _admin.GetQueueAsync("exists-check")).Value.Name);
    }

    [Fact]
    public async Task CreateQueue_OnANameAlreadyTakenByAQueueOrTopic_RaisesAlreadyExists()
    {
        await _admin.CreateQueueAsync("taken");
        _broker.EnsureTopic("taken-by-topic");

        var dupQueue = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateQueueAsync("taken"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, dupQueue.Reason);
        var dupTopic = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateQueueAsync("taken-by-topic"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, dupTopic.Reason);
    }

    [Fact]
    public async Task DeleteQueue_RemovesTheQueueAndItsMessages()
    {
        await _admin.CreateQueueAsync("to-delete");
        await using (var sender = _client.CreateSender("to-delete"))
            await sender.SendMessageAsync(new ServiceBusMessage("doomed"));

        await _admin.DeleteQueueAsync("to-delete");

        Assert.False((await _admin.QueueExistsAsync("to-delete")).Value);
        var missing = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.DeleteQueueAsync("to-delete"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, missing.Reason);

        await _admin.CreateQueueAsync("to-delete");
        await using var receiver = _client.CreateReceiver("to-delete");
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task CreateQueue_NonDefaultProperties_RoundTripThroughGet()
    {
        await _admin.CreateQueueAsync("fwd-target");
        await _admin.CreateQueueAsync("dlq-target");
        var options = NonDefaultOptions("round-trip");

        QueueProperties created = await _admin.CreateQueueAsync(options);
        QueueProperties fetched = await _admin.GetQueueAsync("round-trip");

        AssertMatches(options, created);
        AssertMatches(options, fetched);
    }

    [Fact]
    public async Task CreateQueue_DefaultProperties_RoundTripThroughGet()
    {
        var options = new CreateQueueOptions("defaults");

        QueueProperties fetched = (await _admin.CreateQueueAsync(options), await _admin.GetQueueAsync("defaults")).Item2;

        AssertMatches(options, fetched);
        Assert.Equal(TimeSpan.MaxValue, fetched.DefaultMessageTimeToLive);
        Assert.Equal(TimeSpan.MaxValue, fetched.AutoDeleteOnIdle);
    }

    [Fact]
    public async Task CreateQueue_Properties_SurviveRestart()
    {
        string dataDir;
        var options = NonDefaultOptions("persisted");
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateQueueAsync("fwd-target");
            await admin.CreateQueueAsync("dlq-target");
            await admin.CreateQueueAsync(options);
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            AssertMatches(options, await restarted.CreateAdministrationClient().GetQueueAsync("persisted"));
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task QueueDeclaredOutsideTheHttpApi_IsVisibleThroughIt()
    {
        // appsettings.json declarations and the AMQP create-queue operation both go through EntityManager.CreateQueue.
        _broker.EnsureQueue("declared", maxDeliveryCount: 4, lockDuration: TimeSpan.FromSeconds(20));

        QueueProperties fetched = await _admin.GetQueueAsync("declared");

        Assert.Equal(4, fetched.MaxDeliveryCount);
        Assert.Equal(TimeSpan.FromSeconds(20), fetched.LockDuration);
        Assert.Equal(TimeSpan.MaxValue, fetched.DefaultMessageTimeToLive);
    }

    [Fact]
    public async Task CreateQueue_SubSecondDurations_SurviveRestart()
    {
        string dataDir;
        var options = new CreateQueueOptions("precise")
        {
            LockDuration = TimeSpan.FromMilliseconds(1500),
            DefaultMessageTimeToLive = TimeSpan.FromMilliseconds(1500),
        };
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            await broker.CreateAdministrationClient().CreateQueueAsync(options);
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            var fetched = (await restarted.CreateAdministrationClient().GetQueueAsync("precise")).Value;
            Assert.Equal(options.LockDuration, fetched.LockDuration);
            Assert.Equal(options.DefaultMessageTimeToLive, fetched.DefaultMessageTimeToLive);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task CreateQueue_ConcurrentCreatesOfTheSameNameInAnyCasing_ExactlyOneSucceeds()
    {
        var names = Enumerable.Range(0, 12).Select(i => i % 2 == 0 ? "Racer" : "racer");
        var results = await Task.WhenAll(names.Select(async name =>
        {
            try { await _admin.CreateQueueAsync(name); return "created"; }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists) { return "exists"; }
        }));

        Assert.Single(results, r => r == "created");
        Assert.Equal(11, results.Count(r => r == "exists"));
    }

    [Fact]
    public async Task Queue_WithSlashesInItsName_CanBeCreatedFetchedAndDeleted()
    {
        await _admin.CreateQueueAsync("myapp.contracts/order-submitted");

        Assert.Equal("myapp.contracts/order-submitted", (await _admin.GetQueueAsync("myapp.contracts/order-submitted")).Value.Name);
        await _admin.DeleteQueueAsync("myapp.contracts/order-submitted");
        Assert.False((await _admin.QueueExistsAsync("myapp.contracts/order-submitted")).Value);
    }

    [Fact]
    public async Task GetQueue_OnATopicsName_RaisesNotFound()
    {
        _broker.EnsureTopic("a-topic");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.GetQueueAsync("a-topic"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Theory]
    [InlineData("orders/Subscriptions/sub")]
    [InlineData("orders/subscriptions/sub/Rules/r")]
    [InlineData("$Resources/queues")]
    public async Task CreateQueue_WithANameTheApiReservesForRouting_IsRejected(string name)
    {
        await Assert.ThrowsAnyAsync<Exception>(() => _admin.CreateQueueAsync(name));
        Assert.Null(_broker.EntityManager.GetQueue(name));
    }

    [Fact]
    public async Task ManagementRequests_ToLocalhost_DoNotWaitOnAnIpv6Fallback()
    {
        await _admin.QueueExistsAsync("warm-up"); // JIT and server start-up out of the measurement

        // The fallback costs ~2s per new connection on Windows, so measure a fresh client's first request.
        var fresh = _broker.CreateAdministrationClient();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await fresh.QueueExistsAsync("latency");
        Assert.True(sw.ElapsedMilliseconds < 1000, $"took {sw.ElapsedMilliseconds}ms");
    }

    internal static CreateQueueOptions NonDefaultOptions(string name) => new(name)
    {
        LockDuration = TimeSpan.FromSeconds(45),
        MaxSizeInMegabytes = 2048,
        RequiresDuplicateDetection = true,
        RequiresSession = true,
        DefaultMessageTimeToLive = TimeSpan.FromHours(6),
        DeadLetteringOnMessageExpiration = true,
        DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(3),
        MaxDeliveryCount = 7,
        EnableBatchedOperations = false,
        Status = EntityStatus.SendDisabled,
        ForwardTo = "fwd-target",
        ForwardDeadLetteredMessagesTo = "dlq-target",
        UserMetadata = "owner=tests",
        AutoDeleteOnIdle = TimeSpan.FromDays(2),
        EnablePartitioning = true,
        MaxMessageSizeInKilobytes = 2048,
    };

    internal static void AssertMatches(CreateQueueOptions expected, QueueProperties actual)
    {
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.LockDuration, actual.LockDuration);
        Assert.Equal(expected.MaxSizeInMegabytes, actual.MaxSizeInMegabytes);
        Assert.Equal(expected.RequiresDuplicateDetection, actual.RequiresDuplicateDetection);
        Assert.Equal(expected.RequiresSession, actual.RequiresSession);
        Assert.Equal(expected.DefaultMessageTimeToLive, actual.DefaultMessageTimeToLive);
        Assert.Equal(expected.DeadLetteringOnMessageExpiration, actual.DeadLetteringOnMessageExpiration);
        Assert.Equal(expected.DuplicateDetectionHistoryTimeWindow, actual.DuplicateDetectionHistoryTimeWindow);
        Assert.Equal(expected.MaxDeliveryCount, actual.MaxDeliveryCount);
        Assert.Equal(expected.EnableBatchedOperations, actual.EnableBatchedOperations);
        Assert.Equal(expected.Status, actual.Status);
        // The SDK sends ForwardTo targets as absolute URIs (http://host/name); the API returns them as received.
        AssertSameTarget(expected.ForwardTo, actual.ForwardTo);
        AssertSameTarget(expected.ForwardDeadLetteredMessagesTo, actual.ForwardDeadLetteredMessagesTo);
        Assert.Equal(expected.UserMetadata, actual.UserMetadata);
        Assert.Equal(expected.AutoDeleteOnIdle, actual.AutoDeleteOnIdle);
        Assert.Equal(expected.EnablePartitioning, actual.EnablePartitioning);
        Assert.Equal(expected.MaxMessageSizeInKilobytes, actual.MaxMessageSizeInKilobytes);
    }

    private static void AssertSameTarget(string? expected, string? actual)
    {
        if (expected is null) { Assert.Null(actual); return; }
        Assert.NotNull(actual);
        Assert.EndsWith("/" + expected, actual);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
