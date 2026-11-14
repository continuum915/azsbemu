using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Runtime properties (message counts, timestamps) and listings through the management API.</summary>
public class ManagementHttpRuntimeTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ManagementHttpRuntimeTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task QueueRuntimeProperties_ReportActiveDeadLetterAndScheduledCounts_AndTimestamps()
    {
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        await _admin.CreateQueueAsync("counted");
        await using (var sender = _client.CreateSender("counted"))
        {
            await sender.SendMessagesAsync(new[] { new ServiceBusMessage("1"), new ServiceBusMessage("2"), new ServiceBusMessage("3") });
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later"), DateTimeOffset.UtcNow.AddHours(1));
        }
        await using (var receiver = _client.CreateReceiver("counted"))
            await receiver.DeadLetterMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!);

        QueueRuntimeProperties runtime = await _admin.GetQueueRuntimePropertiesAsync("counted");

        Assert.Equal("counted", runtime.Name);
        Assert.Equal(2, runtime.ActiveMessageCount);
        Assert.Equal(1, runtime.DeadLetterMessageCount);
        Assert.Equal(1, runtime.ScheduledMessageCount);
        Assert.Equal(4, runtime.TotalMessageCount);
        Assert.InRange(runtime.CreatedAt, before, DateTimeOffset.UtcNow.AddSeconds(1));
        Assert.True(runtime.UpdatedAt >= runtime.CreatedAt);
    }

    [Fact]
    public async Task ScheduledMessageCount_IsReportedWhateverCasingTheSenderUsed()
    {
        await _admin.CreateQueueAsync("Mixed-Case");
        await using (var sender = _client.CreateSender("Mixed-Case"))
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later"), DateTimeOffset.UtcNow.AddHours(1));
        await using (var sender = _client.CreateSender("mixed-case"))
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later-too"), DateTimeOffset.UtcNow.AddHours(1));

        Assert.Equal(2, (await _admin.GetQueueRuntimePropertiesAsync("Mixed-Case")).Value.ScheduledMessageCount);
    }

    [Fact]
    public async Task TopicAndSubscriptionRuntimeProperties_ReportSubscriptionAndMessageCounts()
    {
        await _admin.CreateTopicAsync("events");
        await _admin.CreateSubscriptionAsync("events", "a");
        await _admin.CreateSubscriptionAsync("events", "b");
        await using (var sender = _client.CreateSender("events"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("1"));
            await sender.SendMessageAsync(new ServiceBusMessage("2"));
            await sender.ScheduleMessageAsync(new ServiceBusMessage("later"), DateTimeOffset.UtcNow.AddHours(1));
        }
        await using (var receiver = _client.CreateReceiver("events", "a"))
            await receiver.CompleteMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!);

        TopicRuntimeProperties topic = await _admin.GetTopicRuntimePropertiesAsync("events");
        SubscriptionRuntimeProperties a = await _admin.GetSubscriptionRuntimePropertiesAsync("events", "a");
        SubscriptionRuntimeProperties b = await _admin.GetSubscriptionRuntimePropertiesAsync("events", "b");

        Assert.Equal(2, topic.SubscriptionCount);
        Assert.Equal(1, topic.ScheduledMessageCount);
        Assert.Equal(1, a.ActiveMessageCount);
        Assert.Equal(2, b.ActiveMessageCount);
        Assert.Equal(0, b.DeadLetterMessageCount);
        Assert.Equal(2, b.TotalMessageCount);
        Assert.True(b.CreatedAt > DateTimeOffset.UtcNow.AddMinutes(-1));
    }

    [Fact]
    public async Task Listings_ReturnEveryEntityInItsStoredCasing_WithRuntimeVariants()
    {
        await _admin.CreateQueueAsync("Orders");
        await _admin.CreateQueueAsync("myapp/Invoices");
        await _admin.CreateTopicAsync("Events");
        await _admin.CreateSubscriptionAsync("Events", "Billing");
        await _admin.CreateSubscriptionAsync("Events", "Audit");
        await using (var sender = _client.CreateSender("Orders"))
            await sender.SendMessageAsync(new ServiceBusMessage("1"));

        Assert.Equal(new[] { "myapp/Invoices", "Orders" }, (await ToListAsync(_admin.GetQueuesAsync())).Select(q => q.Name).Order());
        Assert.Equal(new[] { "Events" }, (await ToListAsync(_admin.GetTopicsAsync())).Select(t => t.Name));
        Assert.Equal(new[] { "Audit", "Billing" }, (await ToListAsync(_admin.GetSubscriptionsAsync("events"))).Select(s => s.SubscriptionName).Order());

        var queueRuntime = await ToListAsync(_admin.GetQueuesRuntimePropertiesAsync());
        Assert.Equal(1, queueRuntime.Single(q => q.Name == "Orders").ActiveMessageCount);
        Assert.Equal(2, (await ToListAsync(_admin.GetTopicsRuntimePropertiesAsync())).Single().SubscriptionCount);
        Assert.Equal(2, (await ToListAsync(_admin.GetSubscriptionsRuntimePropertiesAsync("Events"))).Count);
    }

    [Fact]
    public async Task Listings_PageWithSkipAndTop()
    {
        foreach (var name in new[] { "q-a", "q-b", "q-c", "q-d", "q-e" })
            await _admin.CreateQueueAsync(name);

        using var http = new HttpClient();
        async Task<List<string>> PageAsync(int skip, int top)
        {
            var feed = System.Xml.Linq.XDocument.Parse(await http.GetStringAsync(
                $"http://127.0.0.1:{_broker.HttpPort}/$Resources/queues?$skip={skip}&$top={top}&api-version=2021-05"));
            System.Xml.Linq.XNamespace atom = "http://www.w3.org/2005/Atom";
            return feed.Root!.Elements(atom + "entry").Select(e => e.Element(atom + "title")!.Value).ToList();
        }

        Assert.Equal(new[] { "q-a", "q-b" }, await PageAsync(0, 2));
        Assert.Equal(new[] { "q-c", "q-d" }, await PageAsync(2, 2));
        Assert.Equal(new[] { "q-e" }, await PageAsync(4, 2));
        Assert.Empty(await PageAsync(5, 2));
    }

    [Fact]
    public async Task SdkListings_FollowPagesBeyondTheFirstHundred()
    {
        for (int i = 0; i < 105; i++)
            _broker.EnsureQueue($"bulk-{i:D3}");

        var names = (await ToListAsync(_admin.GetQueuesAsync())).Select(q => q.Name).ToList();

        Assert.Equal(105, names.Count);
        Assert.Equal(105, names.Distinct().Count());
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source) list.Add(item);
        return list;
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
