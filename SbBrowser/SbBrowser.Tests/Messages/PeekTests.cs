using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public sealed class PeekTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private NamespaceConnection _connection = null!;
    private ServiceBusClient _client = null!;

    public Task InitializeAsync()
    {
        _connection = NamespaceConnection.Open(new ConnectionProfile("test",
            "Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
            _broker.HttpPort, _broker.Port));
        _client = _broker.CreateServiceBusClient();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _connection.DisposeAsync();
        _broker.Dispose();
    }

    private async Task SendAsync(string entity, int count)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessagesAsync(Enumerable.Range(0, count).Select(i => new ServiceBusMessage($"m{i}") { MessageId = $"id-{i}" }));
    }

    private async Task<List<MessageView>> PeekAllAsync(EntityRef entity, MessageSource source, int pageSize, List<int> pageSizes)
    {
        var all = new List<MessageView>();
        long? from = 0;
        while (from is long start)
        {
            var page = await _connection.PeekAsync(entity, source, start, pageSize);
            pageSizes.Add(page.Messages.Count);
            all.AddRange(page.Messages);
            from = page.NextFromSequenceNumber;
            Assert.True(pageSizes.Count < 100, "paging did not end");
        }
        return all;
    }

    [Theory]
    [InlineData(25, 10, new[] { 10, 10, 5 })]
    [InlineData(20, 10, new[] { 10, 10 })] // an exact multiple ends without an empty extra page
    [InlineData(0, 10, new[] { 0 })]
    public async Task Paging_ReturnsEveryMessageOnce_AndEnds(int messages, int pageSize, int[] expectedPages)
    {
        _broker.EnsureQueue("paged");
        await SendAsync("paged", messages);

        var pageSizes = new List<int>();
        var all = await PeekAllAsync(new QueueRef("paged"), MessageSource.Active, pageSize, pageSizes);

        Assert.Equal(expectedPages, pageSizes);
        Assert.Equal(Enumerable.Range(0, messages).Select(i => $"id-{i}"), all.Select(m => m.MessageId));
        Assert.Equal(all.Count, all.Select(m => m.SequenceNumber).Distinct().Count());
        Assert.Equal(messages, _broker.GetQueue("paged").MessageCount); // peeking removed nothing
    }

    [Fact]
    public async Task ASubscription_PagesLikeAQueue()
    {
        _broker.EnsureTopic("paged-topic");
        _broker.EnsureSubscription("paged-topic", "sub");
        await SendAsync("paged-topic", 7);

        var pageSizes = new List<int>();
        var all = await PeekAllAsync(new SubscriptionRef("paged-topic", "sub"), MessageSource.Active, 3, pageSizes);

        Assert.Equal([3, 3, 1], pageSizes);
        Assert.Equal(7, all.Count);
    }

    [Fact]
    public async Task DeadLetterPages_CarryTheReasonAndDescription()
    {
        _broker.EnsureQueue("dlq-source");
        await SendAsync("dlq-source", 2);
        await using (var receiver = _client.CreateReceiver("dlq-source"))
            foreach (var m in await receiver.ReceiveMessagesAsync(2, TimeSpan.FromSeconds(10)))
                await receiver.DeadLetterMessageAsync(m, "ValidationFailed", $"bad {m.MessageId}");

        var page = await _connection.PeekAsync(new QueueRef("dlq-source"), MessageSource.DeadLetter, 0, 10);

        Assert.Equal(2, page.Messages.Count);
        Assert.All(page.Messages, m => Assert.Equal("ValidationFailed", m.DeadLetterReason));
        Assert.Equal(["bad id-0", "bad id-1"], page.Messages.Select(m => m.DeadLetterErrorDescription));
        Assert.Empty((await _connection.PeekAsync(new QueueRef("dlq-source"), MessageSource.Active, 0, 10)).Messages);
    }

    [Fact]
    public async Task ForwardedDeadLetters_CarryTheirSource()
    {
        _broker.EnsureQueue("dlq-target");
        Assert.True(_broker.EntityManager.CreateQueue(new QueueInfo { Name = "dlq-forwarder", ForwardDeadLetteredMessagesTo = "dlq-target" }));
        await SendAsync("dlq-forwarder", 1);
        await using (var receiver = _client.CreateReceiver("dlq-forwarder"))
            await receiver.DeadLetterMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)))!, "Rejected", "no");

        var forwarded = Assert.Single((await _connection.PeekAsync(new QueueRef("dlq-target"), MessageSource.Active, 0, 10)).Messages);

        Assert.Equal("dlq-forwarder", forwarded.DeadLetterSource);
        Assert.Equal("Rejected", forwarded.DeadLetterReason);
    }

    [Fact]
    public async Task MessageDetails_AreMapped_IncludingTypedApplicationProperties()
    {
        _broker.EnsureQueue("detailed");
        var at = new DateTime(2026, 11, 14, 9, 30, 0, DateTimeKind.Utc);
        var id = Guid.Parse("6f1f1c1e-0000-4000-8000-000000000001");
        var message = new ServiceBusMessage("{\"orderId\":42}")
        {
            MessageId = "order-42", CorrelationId = "corr", Subject = "OrderPlaced", ContentType = "application/json",
            To = "billing", ReplyTo = "replies", TimeToLive = TimeSpan.FromHours(2),
        };
        message.ApplicationProperties["text"] = "hello";
        message.ApplicationProperties["count"] = 7;
        message.ApplicationProperties["big"] = 1234567890123L;
        message.ApplicationProperties["ratio"] = 0.5;
        message.ApplicationProperties["flag"] = true;
        message.ApplicationProperties["at"] = at;
        message.ApplicationProperties["id"] = id;
        await using (var sender = _client.CreateSender("detailed")) await sender.SendMessageAsync(message);

        var view = Assert.Single((await _connection.PeekAsync(new QueueRef("detailed"), MessageSource.Active, 0, 10)).Messages);

        Assert.Equal(("order-42", "corr", "OrderPlaced", "application/json", "billing", "replies"),
            (view.MessageId, view.CorrelationId, view.Subject, view.ContentType, view.To, view.ReplyTo));
        Assert.Equal(TimeSpan.FromHours(2), view.TimeToLive);
        Assert.Equal(BodyKind.Json, view.Body.Kind);
        Assert.Contains("\"orderId\": 42", view.Body.Text);
        Assert.Equal("Active", view.State);
        Assert.Null(view.ScheduledEnqueueTime);

        var props = view.ApplicationProperties.ToDictionary(p => p.Name);
        Assert.Equal(["at", "big", "count", "flag", "id", "ratio", "text"], view.ApplicationProperties.Select(p => p.Name));
        Assert.Equal(("hello", "String"), (props["text"].Value, props["text"].TypeName));
        Assert.Equal(("7", "Int32"), (props["count"].Value, props["count"].TypeName));
        Assert.Equal(("1234567890123", "Int64"), (props["big"].Value, props["big"].TypeName));
        Assert.Equal(("0.5", "Double"), (props["ratio"].Value, props["ratio"].TypeName));
        Assert.Equal(("true", "Boolean"), (props["flag"].Value, props["flag"].TypeName));
        Assert.Equal(id.ToString(), props["id"].Value);
        Assert.Equal(at, DateTime.Parse(props["at"].Value, null, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime());
    }

    [Fact]
    public async Task PeekingATopic_IsRejected()
    {
        _broker.EnsureTopic("not-readable");
        await Assert.ThrowsAsync<ArgumentException>(() => _connection.PeekAsync(new TopicRef("not-readable"), MessageSource.Active, 0, 10));
    }
}
