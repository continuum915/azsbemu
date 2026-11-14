using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Entity names that differ only in characters the storage layout has to encode must not share storage.</summary>
public class StorageNamingTests
{
    // Log commits run on a 50ms tick; wait well past one so a second entity opening the same directory would recover the first's data.
    private static readonly TimeSpan PastCommitTick = TimeSpan.FromMilliseconds(300);

    [Fact]
    public async Task Queues_WithSlashAndUnderscoreVariantsOfTheSameName_DoNotShareStorage()
    {
        using var broker = new BrokerFixture();
        await using var client = broker.CreateServiceBusClient();

        broker.EnsureQueue("orders/created");
        await SendAsync(client, "orders/created", "slash");
        await Task.Delay(PastCommitTick);

        broker.EnsureQueue("orders_created");

        Assert.Empty(await DrainAsync(client.CreateReceiver("orders_created")));
        Assert.Equal(new[] { "slash" }, await DrainAsync(client.CreateReceiver("orders/created")));
    }

    [Fact]
    public async Task Topics_WithSlashAndUnderscoreVariantsOfTheSameName_DoNotShareStorage()
    {
        using var broker = new BrokerFixture();
        await using var client = broker.CreateServiceBusClient();

        broker.EnsureTopic("events/created");
        broker.EnsureSubscription("events/created", "s");
        await SendAsync(client, "events/created", "slash");
        await Task.Delay(PastCommitTick);

        broker.EnsureTopic("events_created");
        broker.EnsureSubscription("events_created", "s");

        Assert.Empty(await DrainAsync(client.CreateReceiver("events_created", "s")));
        Assert.Equal(new[] { "slash" }, await DrainAsync(client.CreateReceiver("events/created", "s")));
    }

    [Fact]
    public async Task Queue_StoredUnderTheLegacyDirectoryName_IsMigratedOnRestart()
    {
        await WithLegacyLayoutAsync(
            setUp: async (broker, client) =>
            {
                broker.EnsureQueue("legacy/orders");
                await SendAsync(client, "legacy/orders", "from-before-upgrade");
            },
            toLegacyLayout: dataDir => RenameDir(dataDir, "entities/legacy_2Forders", "entities/legacy_orders"),
            verify: async client =>
                Assert.Equal(new[] { "from-before-upgrade" }, await DrainAsync(client.CreateReceiver("legacy/orders"))));
    }

    [Fact]
    public async Task TopicAndSubscription_StoredUnderTheLegacyDirectoryNames_AreMigratedOnRestart()
    {
        await WithLegacyLayoutAsync(
            setUp: async (broker, client) =>
            {
                broker.EnsureTopic("legacy/events");
                broker.EnsureSubscription("legacy/events", "my_sub");
                await SendAsync(client, "legacy/events", "from-before-upgrade");
            },
            toLegacyLayout: dataDir =>
            {
                RenameDir(dataDir, "entities/legacy_2Fevents/subscriptions/my_5Fsub", "entities/legacy_2Fevents/subscriptions/my_sub");
                RenameDir(dataDir, "entities/legacy_2Fevents", "entities/legacy_events");
            },
            verify: async client =>
                Assert.Equal(new[] { "from-before-upgrade" }, await DrainAsync(client.CreateReceiver("legacy/events", "my_sub"))));
    }

    [Theory]
    [InlineData("a.", "a")] // Windows strips a trailing '.'
    public async Task Queues_WithNamesTheFileSystemTreatsSpecially_DoNotShareStorage(string special, string other)
    {
        using var broker = new BrokerFixture();
        await using var client = broker.CreateServiceBusClient();

        broker.EnsureQueue(special);
        await SendAsync(client, special, "special");
        await Task.Delay(PastCommitTick);

        broker.EnsureQueue(other);

        Assert.Empty(await DrainAsync(client.CreateReceiver(other)));
        Assert.Equal(new[] { "special" }, await DrainAsync(client.CreateReceiver(special)));
    }

    [Fact]
    public async Task DeletingAQueueNamedDotDot_LeavesOtherEntitiesIntact()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            broker.EnsureQueue("keep");
            await using var client = broker.CreateServiceBusClient();
            await SendAsync(client, "keep", "kept");
            broker.EnsureQueue("..");
            await Task.Delay(PastCommitTick);
            broker.EntityManager.DeleteQueue("..");
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            await using var client = restarted.CreateServiceBusClient();
            Assert.Equal(new[] { "kept" }, await DrainAsync(client.CreateReceiver("keep")));
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task LegacyLayout_WhereOneEntitysNewDirectoryIsAnothersLegacyDirectory_MigratesBoth()
    {
        // Legacy: "x/y" lived in "x_y", "x_2Fy" lived in "x_2Fy". New: "x/y" → "x_2Fy", "x_2Fy" → "x_5F2Fy".
        // "x/y"'s target is occupied by "x_2Fy"'s legacy data until that has moved out.
        await WithLegacyLayoutAsync(
            setUp: async (broker, client) =>
            {
                broker.EnsureQueue("x/y");
                broker.EnsureQueue("x_2Fy");
                await SendAsync(client, "x/y", "slash");
                await SendAsync(client, "x_2Fy", "literal");
            },
            toLegacyLayout: dataDir =>
            {
                RenameDir(dataDir, "entities/x_2Fy", "entities/x_y");
                RenameDir(dataDir, "entities/x_5F2Fy", "entities/x_2Fy");
            },
            verify: async client =>
            {
                Assert.Equal(new[] { "slash" }, await DrainAsync(client.CreateReceiver("x/y")));
                Assert.Equal(new[] { "literal" }, await DrainAsync(client.CreateReceiver("x_2Fy")));
            });
    }

    [Fact]
    public async Task Migration_InterruptedBetweenStagingAndFinalMove_IsCompletedOnNextStart()
    {
        await WithLegacyLayoutAsync(
            setUp: async (broker, client) =>
            {
                broker.EnsureQueue("legacy/orders");
                await SendAsync(client, "legacy/orders", "from-before-crash");
            },
            // State after a crash between the two phases: the directory sits under its staging name.
            toLegacyLayout: dataDir => RenameDir(dataDir, "entities/legacy_2Forders", "entities/legacy_2Forders.migrating"),
            verify: async client =>
                Assert.Equal(new[] { "from-before-crash" }, await DrainAsync(client.CreateReceiver("legacy/orders"))));
    }

    /// <summary>
    /// Writes data with the current broker, rewrites the directory names into the pre-encoding layout
    /// (where '/' became '_' and '_' was kept), then restarts twice: migration must work and be harmless to repeat.
    /// </summary>
    private static async Task WithLegacyLayoutAsync(
        Func<BrokerFixture, ServiceBusClient, Task> setUp,
        Action<string> toLegacyLayout,
        Func<ServiceBusClient, Task> verify)
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            await using var client = broker.CreateServiceBusClient();
            await setUp(broker, client);
        }

        try
        {
            toLegacyLayout(dataDir);
            File.Delete(Path.Combine(dataDir, "storage-layout-v2")); // legacy data directories predate the layout marker

            using (new BrokerFixture(dataDir)) { } // first restart migrates

            using var broker = new BrokerFixture(dataDir); // second restart must find the migrated layout
            await using var client = broker.CreateServiceBusClient();
            await verify(client);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    private static void RenameDir(string dataDir, string from, string to) =>
        Directory.Move(Path.Combine(dataDir, from), Path.Combine(dataDir, to));

    private static async Task SendAsync(ServiceBusClient client, string entity, string body)
    {
        await using var sender = client.CreateSender(entity);
        await sender.SendMessageAsync(new ServiceBusMessage(body));
    }

    private static async Task<List<string>> DrainAsync(ServiceBusReceiver receiver)
    {
        await using var _ = receiver;
        var bodies = new List<string>();
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)) is { } msg)
        {
            bodies.Add(msg.Body.ToString());
            await receiver.CompleteMessageAsync(msg);
        }
        return bodies;
    }
}
