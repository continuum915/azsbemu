using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// A receiver that attaches to a session-enabled entity without naming a session is given the next session that has
/// messages and isn't locked, as Service Bus does for <c>AcceptNextSessionAsync</c> and the session processor.
/// </summary>
public class AcceptNextSessionTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusClient _client;

    public AcceptNextSessionTests()
    {
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task AcceptNextSession_ReturnsASessionWithMessages_AndReceivesItsMessages()
    {
        _broker.EnsureQueue("next-session", requiresSession: true);
        await SendAsync("next-session", ("s1", "a"), ("s1", "b"));

        await using var session = await _client.AcceptNextSessionAsync("next-session");

        Assert.Equal("s1", session.SessionId);
        var first = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var second = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { "a", "b" }, new[] { first!.Body.ToString(), second!.Body.ToString() });
        await session.CompleteMessageAsync(first);
        await session.CompleteMessageAsync(second);
    }

    [Fact]
    public async Task AcceptNextSession_SkipsSessionsHeldByAnotherReceiver()
    {
        _broker.EnsureQueue("next-session-skip", requiresSession: true);
        await SendAsync("next-session-skip", ("held", "x"), ("free", "y"));

        await using var holder = await _client.AcceptSessionAsync("next-session-skip", "held");
        await using var next = await _client.AcceptNextSessionAsync("next-session-skip");

        Assert.Equal("free", next.SessionId);
        Assert.Equal("y", (await next.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!.Body.ToString());
    }

    [Fact]
    public async Task ConcurrentAcceptNextSession_GivesEachReceiverADifferentSession()
    {
        _broker.EnsureQueue("next-session-concurrent", requiresSession: true);
        var sessionIds = Enumerable.Range(0, 5).Select(i => $"s{i}").ToArray();
        await SendAsync("next-session-concurrent", sessionIds.Select(id => (id, id)).ToArray());

        var receivers = await Task.WhenAll(sessionIds.Select(_ => _client.AcceptNextSessionAsync("next-session-concurrent")));
        try
        {
            Assert.Equal(sessionIds, receivers.Select(r => r.SessionId).OrderBy(id => id));
        }
        finally
        {
            foreach (var receiver in receivers) await receiver.DisposeAsync();
        }
    }

    [Fact]
    public async Task AcceptNextSession_WhenNoSessionIsAvailable_TimesOut()
    {
        _broker.EnsureQueue("next-session-none", requiresSession: true);
        await SendAsync("next-session-none", ("held", "x"));
        await using var holder = await _client.AcceptSessionAsync("next-session-none", "held");

        await using var client = new ServiceBusClient(_broker.ConnectionString, new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions { TryTimeout = TimeSpan.FromSeconds(2), MaxRetries = 0 }
        });
        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => client.AcceptNextSessionAsync("next-session-none"));
        Assert.Equal(ServiceBusFailureReason.ServiceTimeout, ex.Reason);
    }

    [Fact]
    public async Task AcceptNextSession_WaitsForASessionToArrive()
    {
        _broker.EnsureQueue("next-session-arrives", requiresSession: true);

        var accepting = _client.AcceptNextSessionAsync("next-session-arrives");
        await Task.Delay(500);
        Assert.False(accepting.IsCompleted);
        await SendAsync("next-session-arrives", ("late", "z"));

        await using var session = await accepting.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("late", session.SessionId);
    }

    [Fact]
    public async Task CancellingAWaitingAcceptNextSession_LeavesTheConnectionsOtherReceiversWorking()
    {
        _broker.EnsureQueue("next-session-cancel", requiresSession: true);
        await SendAsync("next-session-cancel", ("held", "x"));
        await using var holder = await _client.AcceptSessionAsync("next-session-cancel", "held");
        var held = await holder.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        // Nothing else is available, so this attach waits until the client gives up on it.
        using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _client.AcceptNextSessionAsync("next-session-cancel", cancellationToken: cancel.Token));
        await Task.Delay(500);

        // Same connection: the held message's lock survives, and the session is still free of other receivers.
        await holder.CompleteMessageAsync(held!);
        await SendAsync("next-session-cancel", ("other", "y"));
        await using var next = await _client.AcceptNextSessionAsync("next-session-cancel");
        Assert.Equal("other", next.SessionId);
    }

    [Fact]
    public async Task AnAcceptedSessionsLock_CanBeRenewed_AndKeepsOtherReceiversOut()
    {
        _broker.EnsureQueue("next-session-renew", requiresSession: true, lockDuration: TimeSpan.FromSeconds(6));
        await SendAsync("next-session-renew", ("s", "x"));

        await using var session = await _client.AcceptNextSessionAsync("next-session-renew");
        var originalExpiry = session.SessionLockedUntil;
        Assert.True(originalExpiry > DateTimeOffset.UtcNow, $"SessionLockedUntil was {originalExpiry}");

        // Renew shortly before the original lock expires, then look for a session after it would have expired but well
        // before the renewed one does. Timed from the lock itself, so a slow machine shifts the steps, not the margins.
        await DelayUntilAsync(originalExpiry - TimeSpan.FromSeconds(2));
        await session.RenewSessionLockAsync();
        Assert.True(DateTimeOffset.UtcNow < originalExpiry, "the renewal finished after the original lock had expired; the machine is too slow for this test");
        Assert.True(session.SessionLockedUntil > originalExpiry + TimeSpan.FromSeconds(3), $"renewed until {session.SessionLockedUntil:O}, originally {originalExpiry:O}");
        await DelayUntilAsync(originalExpiry + TimeSpan.FromSeconds(0.5));
        await using var other = new ServiceBusClient(_broker.ConnectionString, new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions { TryTimeout = TimeSpan.FromSeconds(1), MaxRetries = 0 }
        });
        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => other.AcceptNextSessionAsync("next-session-renew"));
        Assert.Equal(ServiceBusFailureReason.ServiceTimeout, ex.Reason);
    }

    [Fact]
    public async Task ASessionWhoseOnlyMessageWasAbandoned_IsAvailableAgain()
    {
        _broker.EnsureQueue("next-session-abandon", requiresSession: true);
        await SendAsync("next-session-abandon", ("s", "only"));

        await using (var first = await _client.AcceptNextSessionAsync("next-session-abandon"))
        {
            var message = await first.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await first.AbandonMessageAsync(message!);
        }

        await using var again = await _client.AcceptNextSessionAsync("next-session-abandon");
        Assert.Equal("s", again.SessionId);
        var redelivered = await again.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("only", redelivered!.Body.ToString());
        Assert.Equal(2, redelivered.DeliveryCount);
    }

    [Fact]
    public async Task AcceptNextSession_WorksOnASessionSubscription()
    {
        _broker.EnsureTopic("next-session-topic");
        _broker.EnsureSubscription("next-session-topic", "sub", requiresSession: true);
        await SendAsync("next-session-topic", ("t", "published"));

        await using var session = await _client.AcceptNextSessionAsync("next-session-topic", "sub");
        Assert.Equal("t", session.SessionId);
        Assert.Equal("published", (await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!.Body.ToString());
    }

    [Fact]
    public async Task SessionProcessor_ProcessesSeveralSessionsConcurrently_InOrderWithinEachSession()
    {
        _broker.EnsureQueue("next-session-processor", requiresSession: true);
        var sessionIds = new[] { "a", "b", "c" };
        await SendAsync("next-session-processor",
            sessionIds.SelectMany(id => Enumerable.Range(0, 3).Select(i => (id, $"{id}{i}"))).ToArray());

        var received = new System.Collections.Concurrent.ConcurrentDictionary<string, List<string>>();
        int active = 0, maxActive = 0;
        var allDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var processor = _client.CreateSessionProcessor("next-session-processor", new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 3,
            SessionIdleTimeout = TimeSpan.FromSeconds(1),
        });
        processor.ProcessMessageAsync += async args =>
        {
            int now = Interlocked.Increment(ref active);
            InterlockedMax(ref maxActive, now);
            await Task.Delay(200); // long enough for the sessions' handlers to overlap
            var list = received.GetOrAdd(args.SessionId, _ => new List<string>());
            lock (list) list.Add(args.Message.Body.ToString());
            Interlocked.Decrement(ref active);
            if (received.Values.Sum(l => { lock (l) return l.Count; }) == 9) allDone.TrySetResult();
        };
        processor.ProcessErrorAsync += _ => Task.CompletedTask;

        await processor.StartProcessingAsync();
        await allDone.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await processor.StopProcessingAsync();

        foreach (var id in sessionIds)
            Assert.Equal(new[] { $"{id}0", $"{id}1", $"{id}2" }, received[id]);
        Assert.True(maxActive > 1, $"sessions were processed one at a time (max concurrent handlers {maxActive})");
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current) { }
    }

    private static Task DelayUntilAsync(DateTimeOffset when)
    {
        var wait = when - DateTimeOffset.UtcNow;
        return wait > TimeSpan.Zero ? Task.Delay(wait) : Task.CompletedTask;
    }

    private async Task SendAsync(string entity, params (string SessionId, string Body)[] messages)
    {
        await using var sender = _client.CreateSender(entity);
        foreach (var (sessionId, body) in messages)
            await sender.SendMessageAsync(new ServiceBusMessage(body) { SessionId = sessionId });
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
