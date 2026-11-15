using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Peeking through a session receiver, and the error for a session that is already locked.</summary>
public sealed class SessionPeekAndLockTests : IDisposable
{
    private readonly BrokerFixture _broker = new();

    public void Dispose() => _broker.Dispose();

    [Fact]
    public async Task PeekingThroughASessionReceiver_ReturnsOnlyThatSessionsMessages()
    {
        _broker.EnsureQueue("peek-sessions", requiresSession: true);
        await using var client = _broker.CreateServiceBusClient();
        await using (var sender = client.CreateSender("peek-sessions"))
            for (int step = 0; step < 3; step++)
                foreach (var session in new[] { "a", "b", "c" })
                    await sender.SendMessageAsync(new ServiceBusMessage($"{session}{step}") { SessionId = session });

        await using var receiver = await client.AcceptSessionAsync("peek-sessions", "b");
        var peeked = await receiver.PeekMessagesAsync(10);

        Assert.Equal(["b0", "b1", "b2"], peeked.Select(m => m.Body.ToString()));
        // Paging from past the first message stays within the session.
        var rest = await receiver.PeekMessagesAsync(10, peeked[0].SequenceNumber + 1);
        Assert.Equal(["b1", "b2"], rest.Select(m => m.Body.ToString()));
    }

    [Fact]
    public async Task AcceptingALockedSession_FailsWithSessionCannotBeLocked()
    {
        _broker.EnsureQueue("locked-sessions", requiresSession: true);
        await using var client = _broker.CreateServiceBusClient();
        await using (var sender = client.CreateSender("locked-sessions"))
            await sender.SendMessageAsync(new ServiceBusMessage("x") { SessionId = "held" });
        await using var holder = await client.AcceptSessionAsync("locked-sessions", "held");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => client.AcceptSessionAsync("locked-sessions", "held"));

        Assert.Equal(ServiceBusFailureReason.SessionCannotBeLocked, ex.Reason);
    }
}
