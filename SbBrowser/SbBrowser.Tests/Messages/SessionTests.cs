using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public sealed class SessionTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private NamespaceConnection _connection = null!;
    private ServiceBusClient _client = null!;

    public Task InitializeAsync()
    {
        _connection = NamespaceConnection.Open(
            new ConnectionProfile("test", "Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
                _broker.HttpPort, _broker.Port),
            new NamespaceConnectionOptions { NextSessionWait = TimeSpan.FromSeconds(2) });
        _client = _broker.CreateServiceBusClient();
        _broker.EnsureQueue("carts", requiresSession: true);
        _broker.EnsureQueue("plain");
        _broker.EnsureTopic("cart-events");
        _broker.EnsureSubscription("cart-events", "by-cart", requiresSession: true);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _connection.DisposeAsync();
        _broker.Dispose();
    }

    /// <summary>Sends steps 0..perSession-1 for each cart, interleaved across sessions.</summary>
    private async Task SendCartsAsync(string entity, int sessions, int perSession)
    {
        await using var sender = _client.CreateSender(entity);
        for (int step = 0; step < perSession; step++)
            for (int cart = 0; cart < sessions; cart++)
                await sender.SendMessageAsync(new ServiceBusMessage($"cart-{cart} step {step}") { SessionId = $"cart-{cart}", MessageId = $"cart-{cart}-{step}" });
    }

    [Fact]
    public async Task PeekBySessionId_ReturnsOnlyThatSession_InOrder()
    {
        await SendCartsAsync("carts", sessions: 3, perSession: 4);

        var page = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 50, SessionChoice.Id("cart-1"));

        Assert.Equal("cart-1", page.SessionId);
        Assert.Equal(Enumerable.Range(0, 4).Select(s => $"cart-1-{s}"), page.Messages.Select(m => m.MessageId));
        Assert.All(page.Messages, m => Assert.Equal("cart-1", m.SessionId));
        Assert.Null(page.NextFromSequenceNumber);
    }

    [Fact]
    public async Task PeekBySessionId_PagesWithinTheSession()
    {
        await SendCartsAsync("carts", sessions: 2, perSession: 5);
        var choice = SessionChoice.Id("cart-0");

        var first = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 3, choice);
        var second = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, first.NextFromSequenceNumber!.Value, 3, choice);

        Assert.Equal(["cart-0-0", "cart-0-1", "cart-0-2"], first.Messages.Select(m => m.MessageId));
        Assert.Equal(["cart-0-3", "cart-0-4"], second.Messages.Select(m => m.MessageId));
        Assert.Null(second.NextFromSequenceNumber);
    }

    [Fact]
    public async Task NextAvailableSession_PeeksOneSession_AndSaysWhichItWas()
    {
        await SendCartsAsync("carts", sessions: 3, perSession: 2);

        var page = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 50, SessionChoice.NextAvailable);

        Assert.NotNull(page.SessionId);
        Assert.Equal(2, page.Messages.Count);
        Assert.All(page.Messages, m => Assert.Equal(page.SessionId, m.SessionId));
    }

    [Fact]
    public async Task PagingAfterNextAvailable_StaysInTheSessionTheFirstPageRead()
    {
        await SendCartsAsync("carts", sessions: 3, perSession: 4);
        var choice = SessionChoice.NextAvailable;

        var first = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 2, choice);
        choice = choice.After(first);
        var second = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, first.NextFromSequenceNumber!.Value, 2, choice);

        Assert.Equal(first.SessionId, choice.SessionId);
        Assert.Equal(first.SessionId, second.SessionId);
        Assert.All(first.Messages.Concat(second.Messages), m => Assert.Equal(first.SessionId, m.SessionId));
        Assert.Equal(4, first.Messages.Count + second.Messages.Count);
    }

    [Fact]
    public void After_KeepsAChosenSession_AndKeepsNextAvailableWhenNoSessionWasRead()
    {
        var page = new MessagePage([], null, SessionId: "s1");
        Assert.Equal("s1", SessionChoice.NextAvailable.After(page).SessionId);
        Assert.Equal("mine", SessionChoice.Id("mine").After(page).SessionId);
        Assert.Null(SessionChoice.NextAvailable.After(new MessagePage([], null)).SessionId);
    }

    [Fact]
    public async Task NextAvailableSession_OnASubscription_Works()
    {
        await SendCartsAsync("cart-events", sessions: 1, perSession: 3);

        var page = await _connection.PeekAsync(new SubscriptionRef("cart-events", "by-cart"), MessageSource.Active, 0, 50, SessionChoice.NextAvailable);

        Assert.Equal("cart-0", page.SessionId);
        Assert.Equal(3, page.Messages.Count);
    }

    [Fact]
    public async Task NoSessionAvailable_GivesAnEmptyPage_Promptly()
    {
        var started = DateTime.UtcNow;
        var page = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 10, SessionChoice.NextAvailable);

        Assert.Empty(page.Messages);
        Assert.Null(page.SessionId);
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(10), $"took {DateTime.UtcNow - started}");
    }

    [Fact]
    public async Task ASessionLockedByAConsumer_IsReportedClearly()
    {
        await SendCartsAsync("carts", sessions: 1, perSession: 1);
        await using var consumer = await _client.AcceptSessionAsync("carts", "cart-0");

        var ex = await Assert.ThrowsAsync<MessageOperationException>(() =>
            _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 10, SessionChoice.Id("cart-0")));
        Assert.Contains("locked", ex.Message);
    }

    [Fact]
    public async Task ASessionEntity_WithoutASessionChoice_IsRejectedClearly()
    {
        var ex = await Assert.ThrowsAsync<MessageOperationException>(() =>
            _connection.PeekAsync(new QueueRef("carts"), MessageSource.Active, 0, 10));
        Assert.Contains("session-enabled", ex.Message);
    }

    [Fact]
    public async Task ASessionChoice_OnAPlainEntity_IsRejectedClearly()
    {
        var ex = await Assert.ThrowsAsync<MessageOperationException>(() =>
            _connection.PeekAsync(new QueueRef("plain"), MessageSource.Active, 0, 10, SessionChoice.Id("x")));
        Assert.Contains("not session-enabled", ex.Message);
    }

    [Fact]
    public async Task TheDeadLetterQueueOfASessionEntity_NeedsNoSession()
    {
        await SendCartsAsync("carts", sessions: 1, perSession: 2);
        await using (var receiver = await _client.AcceptSessionAsync("carts", "cart-0"))
            foreach (var m in await receiver.ReceiveMessagesAsync(2, TimeSpan.FromSeconds(10)))
                await receiver.DeadLetterMessageAsync(m, "Broken", "test");

        var page = await _connection.PeekAsync(new QueueRef("carts"), MessageSource.DeadLetter, 0, 10);

        Assert.Equal(2, page.Messages.Count);
        Assert.All(page.Messages, m => Assert.Equal("Broken", m.DeadLetterReason));
    }
}
