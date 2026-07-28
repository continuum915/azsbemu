using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests.IntegrationTests;

public class TopicTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string TopicName = "integration-test-topic";

    public TopicTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureTopic(TopicName);
        _broker.EnsureSubscription(TopicName, "sub1");
        _broker.EnsureSubscription(TopicName, "sub2");
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task Topic_PublishToTopic_AllSubscriptionsReceiveIndependentMessages()
    {
        await using var sender = _client.CreateSender(TopicName);
        await using var receiver1 = _client.CreateReceiver(TopicName, "sub1");
        await using var receiver2 = _client.CreateReceiver(TopicName, "sub2");

        await sender.SendMessageAsync(new ServiceBusMessage("topic-msg"));

        // Both subscriptions should receive the message
        var m1 = await receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var m2 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(m1);
        Assert.NotNull(m2);
        Assert.Equal("topic-msg", m1.Body.ToString());
        Assert.Equal("topic-msg", m2.Body.ToString());

        // Verify independent sequence numbering starting from 1
        Assert.True(m1.SequenceNumber >= 1);
        Assert.True(m2.SequenceNumber >= 1);

        await receiver1.CompleteMessageAsync(m1);
        await receiver2.CompleteMessageAsync(m2);

        // Verify count is decremented correctly to 0 for both subscriptions
        var s1 = _broker.GetSubscription(TopicName, "sub1");
        var s2 = _broker.GetSubscription(TopicName, "sub2");
        Assert.Equal(0, s1.MessageCount);
        Assert.Equal(0, s2.MessageCount);
    }

    [Fact]
    public async Task Topic_SubscriptionAddressing_IsCaseInsensitive()
    {
        await using var sender = _client.CreateSender(TopicName);
        
        // Use non-standard casing in the entity path to attach a receiver
        await using var receiverUpper = _client.CreateReceiver(TopicName.ToUpperInvariant(), "SUB1");
        
        await sender.SendMessageAsync(new ServiceBusMessage("casing-msg"));

        var msg = await receiverUpper.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("casing-msg", msg.Body.ToString());
        await receiverUpper.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task Topic_NonMatchingRule_IsFiltered()
    {
        _broker.EnsureTopic("filtered-topic");
        _broker.EnsureSubscription("filtered-topic", "sub-matched");
        _broker.EnsureSubscription("filtered-topic", "sub-unmatched");

        // Get the subscription object in memory and modify its rule to False filter
        var subUnmatched = _broker.GetSubscription("filtered-topic", "sub-unmatched");
        lock (subUnmatched.Rules)
        {
            subUnmatched.Rules.Clear();
            subUnmatched.Rules.Add(new RuleInfo { Name = "FalseRule", FilterType = "False" });
        }

        await using var sender = _client.CreateSender("filtered-topic");
        await using var receiverMatched = _client.CreateReceiver("filtered-topic", "sub-matched");
        await using var receiverUnmatched = _client.CreateReceiver("filtered-topic", "sub-unmatched");

        await sender.SendMessageAsync(new ServiceBusMessage("filtered-message"));

        // Matched subscription receives it
        var msgMatched = await receiverMatched.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msgMatched);
        Assert.Equal("filtered-message", msgMatched.Body.ToString());
        await receiverMatched.CompleteMessageAsync(msgMatched);

        // Unmatched subscription does NOT receive it
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await receiverUnmatched.ReceiveMessageAsync(cancellationToken: cts.Token));
    }

    [Fact]
    public async Task Topic_SqlRule_FiltersMessages()
    {
        const string sqlTopic = "sql-filtered-topic";
        const string sqlSub = "sql-sub";

        _broker.EnsureTopic(sqlTopic);
        _broker.EnsureSubscription(sqlTopic, sqlSub);

        var subscription = _broker.GetSubscription(sqlTopic, sqlSub);
        lock (subscription.Rules)
        {
            subscription.Rules.Clear();
            subscription.Rules.Add(new RuleInfo
            {
                Name = "SqlOnly",
                FilterType = "Sql",
                FilterExpression = "sys.Label = 'match' AND Color = 'Blue' AND Price >= 10 AND Name LIKE 'A%' AND Size IN ('L', 'XL') AND Missing IS NULL AND NOT (IsArchived = TRUE)"
            });
        }

        await using var sender = _client.CreateSender(sqlTopic);
        await using var receiver = _client.CreateReceiver(sqlTopic, sqlSub);

        var matching = new ServiceBusMessage("sql-match") { Subject = "match" };
        matching.ApplicationProperties["Color"] = "Blue";
        matching.ApplicationProperties["Price"] = 12;
        matching.ApplicationProperties["Name"] = "Alpha";
        matching.ApplicationProperties["Size"] = "XL";
        matching.ApplicationProperties["IsArchived"] = false;

        var wrongSubject = new ServiceBusMessage("wrong-subject") { Subject = "other" };
        wrongSubject.ApplicationProperties["Color"] = "Blue";
        wrongSubject.ApplicationProperties["Price"] = 12;
        wrongSubject.ApplicationProperties["Name"] = "Alpha";
        wrongSubject.ApplicationProperties["Size"] = "XL";
        wrongSubject.ApplicationProperties["IsArchived"] = false;

        var wrongPattern = new ServiceBusMessage("wrong-pattern") { Subject = "match" };
        wrongPattern.ApplicationProperties["Color"] = "Blue";
        wrongPattern.ApplicationProperties["Price"] = 12;
        wrongPattern.ApplicationProperties["Name"] = "Beta";
        wrongPattern.ApplicationProperties["Size"] = "XL";
        wrongPattern.ApplicationProperties["IsArchived"] = false;

        await sender.SendMessageAsync(matching);
        await sender.SendMessageAsync(wrongSubject);
        await sender.SendMessageAsync(wrongPattern);

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("sql-match", received.Body.ToString());
        await receiver.CompleteMessageAsync(received);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await receiver.ReceiveMessageAsync(cancellationToken: cts.Token));
    }

    [Fact]
    public async Task Subscription_DeferMessage_ThenReceiveBySequenceNumber_CanComplete()
    {
        await using var sender = _client.CreateSender(TopicName);
        await using var receiver = _client.CreateReceiver(TopicName, "sub1");

        await sender.SendMessageAsync(new ServiceBusMessage("sub-deferred"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        long seqNum = msg.SequenceNumber;
        await receiver.DeferMessageAsync(msg);

        var notVisible = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(notVisible);

        var deferred = await receiver.ReceiveDeferredMessageAsync(seqNum);
        Assert.NotNull(deferred);
        Assert.Equal("sub-deferred", deferred.Body.ToString());
        Assert.Equal(seqNum, deferred.SequenceNumber);

        await receiver.CompleteMessageAsync(deferred);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
