using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Entities with <c>ForwardDeadLetteredMessagesTo</c> send their dead-lettered messages to that target instead of their own DLQ.</summary>
public class ForwardDeadLetteredMessagesTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ForwardDeadLetteredMessagesTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task AnExplicitlyDeadLetteredMessage_ArrivesAtTheTarget_WithItsReason_AndTheSourceDlqStaysEmpty()
    {
        await _admin.CreateQueueAsync("central-dlq");
        await _admin.CreateQueueAsync(new CreateQueueOptions("work") { ForwardDeadLetteredMessagesTo = "central-dlq" });
        await SendAsync("work", "poison");

        await using (var receiver = _client.CreateReceiver("work"))
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeadLetterMessageAsync(message!, "BadPayload", "could not parse");
        }

        var forwarded = await ReceiveAsync("central-dlq");
        Assert.Equal("poison", forwarded?.Body.ToString());
        Assert.Equal("BadPayload", forwarded!.ApplicationProperties["DeadLetterReason"]);
        Assert.Equal("could not parse", forwarded.ApplicationProperties["DeadLetterErrorDescription"]);
        Assert.Equal("work", forwarded.ApplicationProperties["DeadLetterSource"]);
        Assert.Null(await ReceiveAsync("work", deadLetter: true, wait: TimeSpan.FromSeconds(1)));
        Assert.Equal(0, _broker.GetQueue("work").MessageCount);
        Assert.Equal(0, _broker.GetQueue("work").DeadLetterMessageCount);
    }

    [Fact]
    public async Task MessagesExceedingMaxDeliveryCountOrExpiring_ArriveAtTheTarget()
    {
        await _admin.CreateQueueAsync("central-dlq");
        await _admin.CreateQueueAsync(new CreateQueueOptions("flaky")
        {
            ForwardDeadLetteredMessagesTo = "central-dlq",
            MaxDeliveryCount = 1,
            DefaultMessageTimeToLive = TimeSpan.FromSeconds(1),
            DeadLetteringOnMessageExpiration = true,
        });

        await SendAsync("flaky", "abandoned");
        await using (var receiver = _client.CreateReceiver("flaky"))
        {
            await receiver.AbandonMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!);
            // The emulator applies MaxDeliveryCount when it next tries to deliver the message.
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        }
        var maxed = await ReceiveAsync("central-dlq");
        Assert.Equal("abandoned", maxed?.Body.ToString());
        Assert.Equal("MaxDeliveryCountExceeded", maxed!.ApplicationProperties["DeadLetterReason"]);

        await SendAsync("flaky", "expired");
        await Task.Delay(TimeSpan.FromSeconds(2));
        await using (var receiver = _client.CreateReceiver("flaky"))
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1))); // expiry is noticed at delivery
        var expired = await ReceiveAsync("central-dlq", wait: TimeSpan.FromSeconds(15));
        Assert.Equal("expired", expired?.Body.ToString());
        Assert.Equal("TTLExpiredException", expired!.ApplicationProperties["DeadLetterReason"]);

        Assert.Null(await ReceiveAsync("flaky", deadLetter: true, wait: TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task ASubscription_ForwardsItsDeadLetteredMessages()
    {
        await _admin.CreateQueueAsync("central-dlq");
        await _admin.CreateTopicAsync("events");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("events", "s") { ForwardDeadLetteredMessagesTo = "central-dlq" });
        await SendAsync("events", "event");

        await using (var receiver = _client.CreateReceiver("events", "s"))
            await receiver.DeadLetterMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!, "Rejected", "by test");

        Assert.Equal("Rejected", (await ReceiveAsync("central-dlq"))?.ApplicationProperties["DeadLetterReason"]);
    }

    [Fact]
    public async Task InvalidTargets_AreRejectedOnCreateAndUpdate()
    {
        await AssertBadRequestAsync(() => _admin.CreateQueueAsync(new CreateQueueOptions("q") { ForwardDeadLetteredMessagesTo = "missing" }));
        Assert.False((await _admin.QueueExistsAsync("q")).Value);

        // The SDK refuses a self-target before sending, so send that request directly.
        using (var http = new HttpClient())
        {
            var body = new StringContent(
                """<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><ForwardDeadLetteredMessagesTo>http://localhost/Self</ForwardDeadLetteredMessagesTo></QueueDescription></content></entry>""",
                System.Text.Encoding.UTF8, "application/atom+xml");
            var response = await http.PutAsync($"http://127.0.0.1:{_broker.HttpPort}/self?api-version=2021-05", body);
            Assert.Equal(System.Net.HttpStatusCode.BadRequest, response.StatusCode);
        }

        QueueProperties queue = await _admin.CreateQueueAsync("valid");
        queue.ForwardDeadLetteredMessagesTo = "missing";
        await AssertBadRequestAsync(() => _admin.UpdateQueueAsync(queue));
        Assert.Null((await _admin.GetQueueAsync("valid")).Value.ForwardDeadLetteredMessagesTo);
    }

    [Fact]
    public async Task DeadLetterForwarding_StillAppliesAfterARestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateQueueAsync("central-dlq");
            await admin.CreateQueueAsync(new CreateQueueOptions("work") { ForwardDeadLetteredMessagesTo = "central-dlq" });
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            await using var client = restarted.CreateServiceBusClient();
            await using (var sender = client.CreateSender("work"))
                await sender.SendMessageAsync(new ServiceBusMessage("after-restart"));
            await using (var receiver = client.CreateReceiver("work"))
                await receiver.DeadLetterMessageAsync((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))!);
            await using var target = client.CreateReceiver("central-dlq");
            Assert.Equal("after-restart", (await target.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
            Assert.EndsWith("/central-dlq", (await restarted.CreateAdministrationClient().GetQueueAsync("work")).Value.ForwardDeadLetteredMessagesTo);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task SubscriptionTargets_AreValidatedOnCreateAndUpdate()
    {
        await _admin.CreateTopicAsync("t");
        await AssertBadRequestAsync(() => _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("t", "s") { ForwardDeadLetteredMessagesTo = "missing" }));

        using (var http = new HttpClient())
        {
            var body = new StringContent(
                """<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><ForwardDeadLetteredMessagesTo>http://localhost/t</ForwardDeadLetteredMessagesTo></SubscriptionDescription></content></entry>""",
                System.Text.Encoding.UTF8, "application/atom+xml");
            var response = await http.PutAsync($"http://127.0.0.1:{_broker.HttpPort}/t/Subscriptions/own-topic?api-version=2021-05", body);
            Assert.Equal(System.Net.HttpStatusCode.BadRequest, response.StatusCode);
        }

        SubscriptionProperties sub = await _admin.CreateSubscriptionAsync("t", "valid");
        sub.ForwardDeadLetteredMessagesTo = "missing";
        await AssertBadRequestAsync(() => _admin.UpdateSubscriptionAsync(sub));
        Assert.False((await _admin.SubscriptionExistsAsync("t", "s")).Value);
        Assert.False((await _admin.SubscriptionExistsAsync("t", "own-topic")).Value);
    }

    [Fact]
    public async Task DeadLetterForwardingLoops_StopAtTheHopLimit()
    {
        // a and b forward their dead-letters to each other; every delivery attempt exceeds MaxDeliveryCount = 1.
        await _admin.CreateQueueAsync(new CreateQueueOptions("loop-b") { MaxDeliveryCount = 1 });
        await _admin.CreateQueueAsync(new CreateQueueOptions("loop-a") { MaxDeliveryCount = 1, ForwardDeadLetteredMessagesTo = "loop-b" });
        var b = (await _admin.GetQueueAsync("loop-b")).Value;
        b.ForwardDeadLetteredMessagesTo = "loop-a";
        await _admin.UpdateQueueAsync(b);
        await SendAsync("loop-a", "bouncing");

        for (int round = 0; round < 8; round++)
        {
            foreach (var queue in new[] { "loop-a", "loop-b" })
            {
                await using var receiver = _client.CreateReceiver(queue);
                if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(300)) is { } message)
                {
                    await receiver.AbandonMessageAsync(message);
                    await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(300)); // triggers the dead-letter
                }
            }
        }

        Assert.Equal(0, _broker.GetQueue("loop-a").MessageCount + _broker.GetQueue("loop-b").MessageCount);
        Assert.Equal(1, _broker.GetQueue("loop-a").DeadLetterMessageCount + _broker.GetQueue("loop-b").DeadLetterMessageCount);
    }

    /// <summary>The SDK surfaces an HTTP 400 from the management API as an ArgumentException.</summary>
    private static async Task AssertBadRequestAsync(Func<Task> call)
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(call);
        Assert.Equal(400, Assert.IsType<Azure.RequestFailedException>(ex.InnerException).Status);
    }

    private async Task SendAsync(string entity, string body)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessageAsync(new ServiceBusMessage(body));
    }

    private async Task<ServiceBusReceivedMessage?> ReceiveAsync(string queue, bool deadLetter = false, TimeSpan? wait = null)
    {
        await using var receiver = _client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            SubQueue = deadLetter ? SubQueue.DeadLetter : SubQueue.None,
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
        });
        return await receiver.ReceiveMessageAsync(wait ?? TimeSpan.FromSeconds(5));
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
