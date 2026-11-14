using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Abandoned and lock-expired messages on session entities go back to their session, ahead of the session's later
/// messages, as Service Bus does.
/// </summary>
public class SessionRedeliveryTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusClient _client;

    public SessionRedeliveryTests()
    {
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task AnAbandonedSessionMessage_ComesBackToItsSession_AheadOfLaterMessages()
    {
        _broker.EnsureQueue("sessions-abandon", requiresSession: true);
        await SendAsync("sessions-abandon", "first", "second");

        await using var session = await _client.AcceptSessionAsync("sessions-abandon", "s");
        var first = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("first", first!.Body.ToString());
        await session.AbandonMessageAsync(first, new Dictionary<string, object> { ["stage"] = "abandoned" });

        var again = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("first", again!.Body.ToString());
        Assert.Equal(2, again.DeliveryCount);
        Assert.Equal("abandoned", again.ApplicationProperties["stage"]);
        await session.CompleteMessageAsync(again);

        var second = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("second", second!.Body.ToString());
        await session.CompleteMessageAsync(second);
    }

    [Fact]
    public async Task AnAbandonedSessionMessage_IsAvailableToTheNextReceiverOfTheSession()
    {
        _broker.EnsureQueue("sessions-handover", requiresSession: true);
        await SendAsync("sessions-handover", "only");

        await using (var firstReceiver = await _client.AcceptSessionAsync("sessions-handover", "s"))
        {
            var message = await firstReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await firstReceiver.AbandonMessageAsync(message!);
        }

        await using var next = await _client.AcceptSessionAsync("sessions-handover", "s");
        var again = await next.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("only", again!.Body.ToString());
        Assert.Equal(2, again.DeliveryCount);
    }

    [Fact]
    public async Task ASessionMessageWhoseLockExpires_IsRedeliveredToTheSession()
    {
        _broker.EnsureQueue("sessions-lock-expiry", requiresSession: true, lockDuration: TimeSpan.FromSeconds(2));
        await SendAsync("sessions-lock-expiry", "held", "later");

        await using var session = await _client.AcceptSessionAsync("sessions-lock-expiry", "s");
        var held = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("held", held!.Body.ToString());
        // Not settled: the lock expires and the message returns to the session.
        await Task.Delay(TimeSpan.FromSeconds(4));

        var again = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("held", again!.Body.ToString());
        Assert.Equal(2, again.DeliveryCount);
    }

    [Fact]
    public async Task RepeatedlyAbandoningASessionMessage_DeadLettersItAtMaxDeliveryCount()
    {
        _broker.EnsureQueue("sessions-max-delivery", maxDeliveryCount: 2, requiresSession: true);
        await SendAsync("sessions-max-delivery", "poison", "next");

        await using var session = await _client.AcceptSessionAsync("sessions-max-delivery", "s");
        for (int attempt = 1; attempt <= 2; attempt++)
        {
            var poison = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("poison", poison!.Body.ToString());
            await session.AbandonMessageAsync(poison);
        }

        var next = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("next", next!.Body.ToString());

        await using var dlq = _client.CreateReceiver("sessions-max-delivery", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var deadLettered = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("poison", deadLettered!.Body.ToString());
        Assert.Equal("MaxDeliveryCountExceeded", deadLettered.DeadLetterReason);
    }

    [Fact]
    public async Task CompletingOneSessionsMessage_DoesNotLoseAnEarlierMessageOfAnotherSession()
    {
        _broker.EnsureQueue("sessions-truncation", requiresSession: true);
        await using (var sender = _client.CreateSender("sessions-truncation"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("a1") { SessionId = "a" });
            await sender.SendMessageAsync(new ServiceBusMessage("b1") { SessionId = "b" });
        }

        await using (var sessionB = await _client.AcceptSessionAsync("sessions-truncation", "b"))
        {
            var b1 = await sessionB.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await sessionB.CompleteMessageAsync(b1!);
        }

        await using var sessionA = await _client.AcceptSessionAsync("sessions-truncation", "a");
        var a1 = await sessionA.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("a1", a1?.Body.ToString());
    }

    [Fact]
    public async Task CompletingOneSessionsMessage_DoesNotLoseAnEarlierMessageOfAnotherSession_AcrossARestart()
    {
        _broker.EnsureQueue("sessions-truncation-restart", requiresSession: true);
        await using (var sender = _client.CreateSender("sessions-truncation-restart"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("a1") { SessionId = "a" });
            await sender.SendMessageAsync(new ServiceBusMessage("b1") { SessionId = "b" });
        }
        await using (var sessionB = await _client.AcceptSessionAsync("sessions-truncation-restart", "b"))
        {
            var b1 = await sessionB.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await sessionB.CompleteMessageAsync(b1!);
        }
        await _client.DisposeAsync();

        var dataDir = _broker.TakeDataDir();
        _broker.Dispose();
        using var restarted = new BrokerFixture(dataDir);
        await using var client = restarted.CreateServiceBusClient();

        await using var sessionA = await client.AcceptSessionAsync("sessions-truncation-restart", "a");
        var a1 = await sessionA.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("a1", a1?.Body.ToString());
    }

    [Fact]
    public async Task AnAbandonedMessageOnASessionSubscription_ComesBackToItsSession()
    {
        _broker.EnsureTopic("session-topic");
        _broker.EnsureSubscription("session-topic", "sub", requiresSession: true);
        await using (var sender = _client.CreateSender("session-topic"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("first") { SessionId = "s" });
            await sender.SendMessageAsync(new ServiceBusMessage("second") { SessionId = "s" });
        }

        await using var session = await _client.AcceptSessionAsync("session-topic", "sub", "s");
        var first = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        await session.AbandonMessageAsync(first!);

        var again = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("first", again!.Body.ToString());
        Assert.Equal(2, again.DeliveryCount);
    }

    private async Task SendAsync(string queue, params string[] bodies)
    {
        await using var sender = _client.CreateSender(queue);
        foreach (var body in bodies)
            await sender.SendMessageAsync(new ServiceBusMessage(body) { SessionId = "s" });
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
