using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public sealed class PurgeTests : IAsyncLifetime
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
        for (int start = 0; start < count; start += 500)
            await sender.SendMessagesAsync(Enumerable.Range(start, Math.Min(500, count - start))
                .Select(i => new ServiceBusMessage($"m{i}") { SessionId = sessionOf?.Invoke(i) }));
    }

    /// <summary>Reports on the calling thread, unlike <see cref="Progress{T}"/>.</summary>
    private sealed class Recorder(Action<PurgeProgress>? onReport = null) : IProgress<PurgeProgress>
    {
        public List<long> Reports { get; } = [];
        public void Report(PurgeProgress value)
        {
            lock (Reports) Reports.Add(value.Removed);
            onReport?.Invoke(value);
        }
    }

    [Fact]
    public async Task Purges10000Messages_AndReportsTheTotal()
    {
        await SendAsync("work", 10_000);
        var progress = new Recorder();

        var result = await _connection.PurgeAsync(new QueueRef("work"), MessageSource.Active, progress);

        Assert.Equal(new PurgeResult(10_000, 0, 0, false), result);
        Assert.Equal(10_000, progress.Reports[^1]);
        Assert.True(progress.Reports.Count > 1, "progress was reported once");
        Assert.Equal(progress.Reports.Order(), progress.Reports);
    }

    [Fact]
    public async Task Cancelling_StopsPromptly_AndReportsWhatWasRemoved()
    {
        await SendAsync("work", 5_000);
        using var cts = new CancellationTokenSource();
        var progress = new Recorder(p => { if (p.Removed >= 1_000) cts.Cancel(); });

        var started = DateTime.UtcNow;
        var result = await _connection.PurgeAsync(new QueueRef("work"), MessageSource.Active, progress, cts.Token);

        Assert.True(result.Cancelled);
        Assert.InRange(result.Removed, 1_000, 4_999);
        Assert.Equal(progress.Reports[^1], result.Removed);
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(20), $"took {DateTime.UtcNow - started}");
    }

    [Fact]
    public async Task ScheduledMessages_AreLeftAlone_AndReported()
    {
        await SendAsync("work", 5);
        await using (var sender = _client.CreateSender("work"))
            await sender.ScheduleMessagesAsync([new ServiceBusMessage("later"), new ServiceBusMessage("later")], DateTimeOffset.UtcNow.AddHours(1));

        var result = await _connection.PurgeAsync(new QueueRef("work"), MessageSource.Active);

        Assert.Equal(new PurgeResult(5, 2, 0, false), result);
    }

    [Fact]
    public async Task DeferredMessages_AreReportedAsRemaining()
    {
        await SendAsync("work", 4);
        await using (var receiver = _client.CreateReceiver("work"))
        {
            var m = (await receiver.ReceiveMessagesAsync(1, TimeSpan.FromSeconds(10))).Single();
            await receiver.DeferMessageAsync(m);
        }

        var result = await _connection.PurgeAsync(new QueueRef("work"), MessageSource.Active);

        Assert.Equal(new PurgeResult(3, 0, 1, false), result);
    }

    [Fact]
    public async Task PurgingASessionEntity_RemovesEverySession()
    {
        await SendAsync("carts", 60, i => $"cart-{i % 6}");

        var result = await _connection.PurgeAsync(new QueueRef("carts"), MessageSource.Active);

        Assert.Equal(new PurgeResult(60, 0, 0, false), result);
    }

    [Fact]
    public async Task PurgingTheDeadLetterQueue_LeavesActiveMessages()
    {
        await SendAsync("work", 5);
        await using (var receiver = _client.CreateReceiver("work"))
            foreach (var m in await receiver.ReceiveMessagesAsync(3, TimeSpan.FromSeconds(10)))
                await receiver.DeadLetterMessageAsync(m, "Broken", "test");

        var result = await _connection.PurgeAsync(new QueueRef("work"), MessageSource.DeadLetter);

        Assert.Equal(new PurgeResult(3, 0, 0, false), result);
        Assert.Equal(2, (await _connection.GetCountsAsync(new QueueRef("work"))).Active);
    }
}
