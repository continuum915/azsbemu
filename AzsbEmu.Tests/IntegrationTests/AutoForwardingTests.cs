using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Entities configured with <c>ForwardTo</c> pass every arriving message straight to the target.</summary>
public class AutoForwardingTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public AutoForwardingTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task MessagePublishedToATopic_ReachesTheQueueItsSubscriptionForwardsTo_Unchanged()
    {
        // MassTransit's topology: one topic per message type, one forwarding subscription per consuming endpoint.
        await _admin.CreateQueueAsync("order-service");
        await _admin.CreateTopicAsync("myapp.contracts/ordersubmitted");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("myapp.contracts/ordersubmitted", "order-service") { ForwardTo = "order-service" });

        var sent = new ServiceBusMessage("order-42")
        {
            MessageId = "m-42",
            CorrelationId = "c-42",
            Subject = "OrderSubmitted",
            ContentType = "application/json",
            TimeToLive = TimeSpan.FromHours(1),
            ApplicationProperties = { ["MT-MessageType"] = "urn:message:MyApp.Contracts:OrderSubmitted" },
        };
        await using (var sender = _client.CreateSender("myapp.contracts/ordersubmitted"))
            await sender.SendMessageAsync(sent);

        await using var receiver = _client.CreateReceiver("order-service");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("order-42", received.Body.ToString());
        Assert.Equal("m-42", received.MessageId);
        Assert.Equal("c-42", received.CorrelationId);
        Assert.Equal("OrderSubmitted", received.Subject);
        Assert.Equal("application/json", received.ContentType);
        Assert.Equal(TimeSpan.FromHours(1), received.TimeToLive);
        Assert.Equal("urn:message:MyApp.Contracts:OrderSubmitted", received.ApplicationProperties["MT-MessageType"]);
    }

    [Fact]
    public async Task QueueForwardingToAQueue_DeliversThereAndKeepsNothing()
    {
        await _admin.CreateQueueAsync("destination");
        await _admin.CreateQueueAsync(new CreateQueueOptions("forwarder") { ForwardTo = "destination" });
        // Give the destination its own history so a copied sequence number would be detectable.
        await SendAsync("destination", "already-there");

        await SendAsync("forwarder", "forwarded");

        await using (var forwarderReceiver = _client.CreateReceiver("forwarder"))
            Assert.Null(await forwarderReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal(0, _broker.GetQueue("forwarder").MessageCount);

        await using var receiver = _client.CreateReceiver("destination");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("already-there", first?.Body.ToString());
        Assert.Equal("forwarded", second?.Body.ToString());
        Assert.Equal(first!.SequenceNumber + 1, second!.SequenceNumber);
        Assert.True(second.EnqueuedTime >= first.EnqueuedTime);
    }

    [Fact]
    public async Task QueueForwardingToATopic_ReachesTheTopicsSubscriptions()
    {
        await _admin.CreateTopicAsync("fan-out");
        await _admin.CreateSubscriptionAsync("fan-out", "a");
        await _admin.CreateSubscriptionAsync("fan-out", "b");
        await _admin.CreateQueueAsync(new CreateQueueOptions("into-topic") { ForwardTo = "fan-out" });

        await SendAsync("into-topic", "broadcast");

        foreach (var sub in new[] { "a", "b" })
        {
            await using var receiver = _client.CreateReceiver("fan-out", sub);
            Assert.Equal("broadcast", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
        }
    }

    [Fact]
    public async Task ForwardTargets_ThatAreMissingASubscriptionOrTheSubscriptionsOwnTopic_AreRejectedByTheServer()
    {
        await _admin.CreateTopicAsync("t");
        await _admin.CreateSubscriptionAsync("t", "s");

        await AssertBadRequestAsync(() => _admin.CreateQueueAsync(new CreateQueueOptions("nowhere") { ForwardTo = "missing" }));
        await AssertBadRequestAsync(() => _admin.CreateQueueAsync(new CreateQueueOptions("to-sub") { ForwardTo = "t/Subscriptions/s" }));
        await AssertBadRequestAsync(() => _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("t", "bad") { ForwardTo = "missing" }));
        // Forwarding back into its own topic would fan out to itself forever. The SDK refuses this before sending,
        // so send the request directly.
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, await PutAsync("t/Subscriptions/own-topic", "SubscriptionDescription", "http://localhost/T"));

        Assert.False((await _admin.QueueExistsAsync("nowhere")).Value);
        Assert.False((await _admin.QueueExistsAsync("to-sub")).Value);
        Assert.False((await _admin.SubscriptionExistsAsync("t", "bad")).Value);
        Assert.False((await _admin.SubscriptionExistsAsync("t", "own-topic")).Value);
    }

    [Fact]
    public async Task ForwardingToItself_IsRejectedByTheServer_InAnyCasingOrAddressForm()
    {
        // The SDK refuses self-forwarding before sending, so send the request directly.
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, await PutAsync("loop", "QueueDescription", "http://localhost/LOOP"));
        Assert.False((await _admin.QueueExistsAsync("loop")).Value);
    }

    [Fact]
    public async Task AForwardingLoop_CreatedByRecreatingATarget_DeadLettersInsteadOfLoopingForever()
    {
        // Validation can't see loops formed later: b, then a → b, then b is recreated as b → a.
        await _admin.CreateQueueAsync("b");
        await _admin.CreateQueueAsync(new CreateQueueOptions("a") { ForwardTo = "b" });
        await _admin.DeleteQueueAsync("b");
        await _admin.CreateQueueAsync(new CreateQueueOptions("b") { ForwardTo = "a" });

        await SendAsync("a", "looping");

        Assert.Equal(1, _broker.GetQueue("a").DeadLetterMessageCount + _broker.GetQueue("b").DeadLetterMessageCount);
        Assert.Equal(0, _broker.GetQueue("a").MessageCount + _broker.GetQueue("b").MessageCount);
    }

    /// <summary>Creates an entity through a raw management API request, for forms the SDK won't send.</summary>
    private async Task<System.Net.HttpStatusCode> PutAsync(string path, string descriptionElement, string forwardTo)
    {
        using var http = new HttpClient();
        var body = new StringContent(
            $"""<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><{descriptionElement} xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><ForwardTo>{forwardTo}</ForwardTo></{descriptionElement}></content></entry>""",
            System.Text.Encoding.UTF8, "application/atom+xml");
        return (await http.PutAsync($"http://127.0.0.1:{_broker.HttpPort}/{path}?api-version=2021-05", body)).StatusCode;
    }

    [Fact]
    public async Task ForwardTargets_GivenAsABareNameOrAnAbsoluteUri_BothWork_AndComeBackAsSupplied()
    {
        await _admin.CreateQueueAsync("destination");

        // The SDK always sends an absolute URI, so send the bare-name form as a raw management API request.
        Assert.Equal(System.Net.HttpStatusCode.Created, await PutAsync("by-name", "QueueDescription", "destination"));
        await _admin.CreateQueueAsync(new CreateQueueOptions("by-uri") { ForwardTo = "sb://my-namespace.servicebus.windows.net/destination" });

        Assert.Equal("destination", (await _admin.GetQueueAsync("by-name")).Value.ForwardTo);
        Assert.Equal("sb://my-namespace.servicebus.windows.net/destination", (await _admin.GetQueueAsync("by-uri")).Value.ForwardTo);

        await SendAsync("by-name", "one");
        await SendAsync("by-uri", "two");
        await using var receiver = _client.CreateReceiver("destination");
        var bodies = new[] { await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)), await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)) }
            .Select(m => m?.Body.ToString()).Order();
        Assert.Equal(new[] { "one", "two" }, bodies);
    }

    [Fact]
    public async Task Forwarding_StillAppliesAfterARestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateQueueAsync("destination");
            await admin.CreateTopicAsync("events");
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("events", "to-destination") { ForwardTo = "destination" });
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            await using var client = restarted.CreateServiceBusClient();
            await using (var sender = client.CreateSender("events"))
                await sender.SendMessageAsync(new ServiceBusMessage("after-restart"));
            await using var receiver = client.CreateReceiver("destination");
            Assert.Equal("after-restart", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    /// <summary>The SDK surfaces an HTTP 400 from the management API as an ArgumentException.</summary>
    private static async Task AssertBadRequestAsync(Func<Task> create)
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(create);
        Assert.Equal(400, Assert.IsType<Azure.RequestFailedException>(ex.InnerException).Status);
    }

    private async Task SendAsync(string entity, string body)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessageAsync(new ServiceBusMessage(body));
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
