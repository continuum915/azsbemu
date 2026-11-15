using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public sealed class ReceiveAndDeleteTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private NamespaceConnection _connection = null!;
    private ServiceBusClient _client = null!;

    public Task InitializeAsync()
    {
        _connection = NamespaceConnection.Open(new ConnectionProfile("test",
                "Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
                _broker.HttpPort, _broker.Port),
            new NamespaceConnectionOptions { ReceiveWait = TimeSpan.FromSeconds(1), NextSessionWait = TimeSpan.FromSeconds(2) });
        _client = _broker.CreateServiceBusClient();
        _broker.EnsureQueue("work");
        _broker.EnsureQueue("carts", requiresSession: true);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _connection.DisposeAsync();
        _broker.Dispose();
    }

    private async Task SendAsync(string entity, int count, Func<int, string?>? sessionOf = null)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessagesAsync(Enumerable.Range(0, count)
            .Select(i => new ServiceBusMessage($"m{i}") { MessageId = $"id-{i}", SessionId = sessionOf?.Invoke(i) }));
    }

    /// <summary>Counts settle as the broker processes removals; wait briefly for the expected value.</summary>
    private async Task<long> ActiveCountAsync(EntityRef entity, long expected)
    {
        long active = -1;
        for (int i = 0; i < 20 && active != expected; i++)
        {
            active = (await _connection.GetCountsAsync(entity)).Active;
            if (active != expected) await Task.Delay(100);
        }
        return active;
    }

    [Fact]
    public async Task RemovesExactlyN_WhenMoreAreAvailable_AndTheCountDrops()
    {
        await SendAsync("work", 10);

        var removed = await _connection.ReceiveAndDeleteAsync(new QueueRef("work"), MessageSource.Active, 4);

        Assert.Equal(["id-0", "id-1", "id-2", "id-3"], removed.Select(m => m.MessageId));
        Assert.Equal(6, await ActiveCountAsync(new QueueRef("work"), 6));
    }

    [Fact]
    public async Task ReturnsWhatExists_WhenFewerAreAvailable()
    {
        await SendAsync("work", 3);

        var removed = await _connection.ReceiveAndDeleteAsync(new QueueRef("work"), MessageSource.Active, 50);

        Assert.Equal(3, removed.Count);
        Assert.Equal(0, await ActiveCountAsync(new QueueRef("work"), 0));
    }

    [Fact]
    public async Task AnEmptyEntity_ReturnsPromptly()
    {
        var started = DateTime.UtcNow;
        var removed = await _connection.ReceiveAndDeleteAsync(new QueueRef("work"), MessageSource.Active, 10);

        Assert.Empty(removed);
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(10), $"took {DateTime.UtcNow - started}");
    }

    [Fact]
    public async Task WorksOnTheDeadLetterQueue()
    {
        await SendAsync("work", 3);
        await using (var receiver = _client.CreateReceiver("work"))
            foreach (var m in await receiver.ReceiveMessagesAsync(3, TimeSpan.FromSeconds(10)))
                await receiver.DeadLetterMessageAsync(m, "Broken", "test");

        var removed = await _connection.ReceiveAndDeleteAsync(new QueueRef("work"), MessageSource.DeadLetter, 2);

        Assert.Equal(2, removed.Count);
        Assert.All(removed, m => Assert.Equal("Broken", m.DeadLetterReason));
        Assert.Single(await _connection.ReceiveAndDeleteAsync(new QueueRef("work"), MessageSource.DeadLetter, 10));
    }

    [Fact]
    public async Task BySessionId_RemovesOnlyThatSession()
    {
        await SendAsync("carts", 6, i => $"cart-{i % 2}");

        var removed = await _connection.ReceiveAndDeleteAsync(new QueueRef("carts"), MessageSource.Active, 10, SessionChoice.Id("cart-1"));

        Assert.Equal(["id-1", "id-3", "id-5"], removed.Select(m => m.MessageId));
        Assert.Equal(3, await ActiveCountAsync(new QueueRef("carts"), 3));
    }

    [Fact]
    public async Task ByNextAvailableSession_RemovesFromOneSession()
    {
        await SendAsync("carts", 6, i => $"cart-{i % 2}");

        var removed = await _connection.ReceiveAndDeleteAsync(new QueueRef("carts"), MessageSource.Active, 10, SessionChoice.NextAvailable);

        Assert.Equal(3, removed.Count);
        Assert.Single(removed.Select(m => m.SessionId).Distinct());
    }

    [Fact]
    public async Task ASessionEntity_WithoutASessionChoice_IsRejectedClearly()
    {
        var ex = await Assert.ThrowsAsync<MessageOperationException>(() =>
            _connection.ReceiveAndDeleteAsync(new QueueRef("carts"), MessageSource.Active, 1));
        Assert.Contains("session-enabled", ex.Message);
    }
}
