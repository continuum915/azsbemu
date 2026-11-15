using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;

namespace SbBrowser.Tests.Entities;

public sealed class NamespaceTreeTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private NamespaceConnection _connection = null!;

    public Task InitializeAsync()
    {
        _connection = NamespaceConnection.Open(new ConnectionProfile("test",
            "Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
            _broker.HttpPort, _broker.Port));
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
        _broker.Dispose();
    }

    [Fact]
    public async Task AnEmptyNamespace_GivesAnEmptyTree()
    {
        var tree = await _connection.GetTreeAsync();
        Assert.Empty(tree.Queues);
        Assert.Empty(tree.Topics);
    }

    [Fact]
    public async Task TheTree_ListsEntitiesWithCountsAndSettings()
    {
        _broker.EnsureQueue("orders", maxDeliveryCount: 3);
        _broker.EnsureQueue("checkout", requiresSession: true);
        _broker.EnsureTopic("events");
        _broker.EnsureSubscription("events", "all");
        _broker.EnsureSubscription("events", "audit", maxDeliveryCount: 7);

        await using var client = _broker.CreateServiceBusClient();
        await using (var sender = client.CreateSender("orders"))
        {
            await sender.SendMessagesAsync([new ServiceBusMessage("1"), new ServiceBusMessage("2"), new ServiceBusMessage("3")]);
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later"), DateTimeOffset.UtcNow.AddHours(1));
        }
        await using (var receiver = client.CreateReceiver("orders"))
            await receiver.DeadLetterMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)))!);
        await using (var sender = client.CreateSender("events"))
        {
            await sender.SendMessagesAsync([new ServiceBusMessage("a"), new ServiceBusMessage("b")]);
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later"), DateTimeOffset.UtcNow.AddHours(1));
        }

        var tree = await _connection.GetTreeAsync();

        Assert.Equal(["checkout", "orders"], tree.Queues.Select(q => q.Ref.Name));
        var orders = tree.Queues[1];
        Assert.Equal(new EntityCounts(Active: 2, DeadLetter: 1, Scheduled: 1, Transfer: 0, TransferDeadLetter: 0), orders.Counts);
        Assert.Equal(3, orders.Settings.MaxDeliveryCount);
        Assert.True(tree.Queues[0].Settings.RequiresSession);

        var topic = Assert.Single(tree.Topics);
        Assert.Equal("events", topic.Ref.Name);
        Assert.Equal(1, topic.ScheduledCount);
        Assert.Equal(["all", "audit"], topic.Subscriptions.Select(s => s.Ref.Name));
        Assert.All(topic.Subscriptions, s => Assert.Equal(2, s.Counts.Active));
        Assert.Equal(7, topic.Subscriptions[1].Settings.MaxDeliveryCount);
        Assert.Equal("events/Subscriptions/audit", topic.Subscriptions[1].Ref.Path);
    }

    [Fact]
    public async Task MoreThanAPageOfEachKind_IsAllListed()
    {
        // The management API pages listings 100 at a time.
        const int count = 120;
        for (int i = 0; i < count; i++) _broker.EnsureQueue($"q-{i:D3}");
        for (int i = 0; i < count; i++) _broker.EnsureTopic($"t-{i:D3}");
        for (int i = 0; i < count; i++) _broker.EnsureSubscription("t-000", $"s-{i:D3}");

        var tree = await _connection.GetTreeAsync();

        Assert.Equal(count, tree.Queues.Count);
        Assert.Equal(count, tree.Topics.Count);
        Assert.Equal(count, tree.Topics[0].Subscriptions.Count);
    }

    [Fact]
    public async Task TopicNamesWithSlashes_AppearWithTheirSubscriptions()
    {
        _broker.EnsureTopic("myapp.contracts/ordersubmitted");
        _broker.EnsureSubscription("myapp.contracts/ordersubmitted", "billing");

        var topic = Assert.Single((await _connection.GetTreeAsync()).Topics);

        Assert.Equal("myapp.contracts/ordersubmitted", topic.Ref.Name);
        Assert.Equal("billing", Assert.Single(topic.Subscriptions).Ref.Name);
    }

    [Fact]
    public async Task GetCounts_RefreshesOneEntity()
    {
        _broker.EnsureQueue("single");
        _broker.EnsureTopic("single-topic");
        _broker.EnsureSubscription("single-topic", "sub");
        await using var client = _broker.CreateServiceBusClient();
        await using (var sender = client.CreateSender("single")) await sender.SendMessageAsync(new ServiceBusMessage("x"));
        await using (var sender = client.CreateSender("single-topic")) await sender.SendMessageAsync(new ServiceBusMessage("y"));

        Assert.Equal(1, (await _connection.GetCountsAsync(new QueueRef("single"))).Active);
        Assert.Equal(1, (await _connection.GetCountsAsync(new SubscriptionRef("single-topic", "sub"))).Active);
        Assert.Equal(0, (await _connection.GetCountsAsync(new TopicRef("single-topic"))).Total);
    }
}
