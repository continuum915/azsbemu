using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Topic and subscription management through <see cref="ServiceBusAdministrationClient"/>.</summary>
public class ManagementHttpTopicTests : IDisposable
{
    private const string MassTransitStyleTopic = "myapp.contracts/ordersubmitted";

    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ManagementHttpTopicTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task Topic_CanBeCreatedFetchedPublishedToAndDeleted()
    {
        TopicProperties created = await _admin.CreateTopicAsync(MassTransitStyleTopic);
        Assert.Equal(MassTransitStyleTopic, created.Name);
        Assert.True((await _admin.TopicExistsAsync(MassTransitStyleTopic)).Value);
        Assert.Equal(MassTransitStyleTopic, (await _admin.GetTopicAsync(MassTransitStyleTopic)).Value.Name);

        _broker.EnsureSubscription(MassTransitStyleTopic, "listener");
        await using (var sender = _client.CreateSender(MassTransitStyleTopic))
            await sender.SendMessageAsync(new ServiceBusMessage("published"));
        await using (var receiver = _client.CreateReceiver(MassTransitStyleTopic, "listener"))
            Assert.Equal("published", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());

        var duplicate = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateTopicAsync(MassTransitStyleTopic));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, duplicate.Reason);

        await _admin.DeleteTopicAsync(MassTransitStyleTopic);
        Assert.False((await _admin.TopicExistsAsync(MassTransitStyleTopic)).Value);
        var missing = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.GetTopicAsync(MassTransitStyleTopic));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, missing.Reason);
    }

    [Fact]
    public async Task Subscription_OnATopicWithSlashesInItsName_CanBeCreatedFetchedReceivedFromAndDeleted()
    {
        await _admin.CreateTopicAsync(MassTransitStyleTopic);

        SubscriptionProperties created = await _admin.CreateSubscriptionAsync(MassTransitStyleTopic, "order-service");
        Assert.Equal(MassTransitStyleTopic, created.TopicName);
        Assert.Equal("order-service", created.SubscriptionName);
        Assert.True((await _admin.SubscriptionExistsAsync(MassTransitStyleTopic, "order-service")).Value);
        Assert.Equal("order-service", (await _admin.GetSubscriptionAsync(MassTransitStyleTopic, "order-service")).Value.SubscriptionName);

        await using (var sender = _client.CreateSender(MassTransitStyleTopic))
            await sender.SendMessageAsync(new ServiceBusMessage("to-subscription"));
        await using (var receiver = _client.CreateReceiver(MassTransitStyleTopic, "order-service"))
            Assert.Equal("to-subscription", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());

        var duplicate = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateSubscriptionAsync(MassTransitStyleTopic, "order-service"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, duplicate.Reason);

        await _admin.DeleteSubscriptionAsync(MassTransitStyleTopic, "order-service");
        Assert.False((await _admin.SubscriptionExistsAsync(MassTransitStyleTopic, "order-service")).Value);
        var missing = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.GetSubscriptionAsync(MassTransitStyleTopic, "order-service"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, missing.Reason);
    }

    [Fact]
    public async Task Subscription_OnAMissingTopic_CannotBeCreated()
    {
        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateSubscriptionAsync("no-such-topic", "s"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task Subscription_CreatedWithADefaultRule_ReceivesOnlyMatchingMessages()
    {
        await _admin.CreateTopicAsync("colours");
        await _admin.CreateSubscriptionAsync(
            new CreateSubscriptionOptions("colours", "reds"),
            new CreateRuleOptions("$Default", new SqlRuleFilter("colour = 'red'")));
        var correlation = new CorrelationRuleFilter { Subject = "paint" };
        correlation.ApplicationProperties["colour"] = "blue";
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("colours", "blue-paint"), new CreateRuleOptions("blue-paint", correlation));

        await using (var sender = _client.CreateSender("colours"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("red") { ApplicationProperties = { ["colour"] = "red" } });
            await sender.SendMessageAsync(new ServiceBusMessage("blue-paint") { Subject = "paint", ApplicationProperties = { ["colour"] = "blue" } });
            await sender.SendMessageAsync(new ServiceBusMessage("blue-ink") { Subject = "ink", ApplicationProperties = { ["colour"] = "blue" } });
        }

        Assert.Equal(new[] { "red" }, await DrainAsync("colours", "reds"));
        Assert.Equal(new[] { "blue-paint" }, await DrainAsync("colours", "blue-paint"));
    }

    [Fact]
    public async Task EntitiesWhoseNamesMerelyContainTheWordRules_AreServedNormally()
    {
        await _admin.CreateTopicAsync("myapp.contracts/rulesevaluated");
        await _admin.CreateSubscriptionAsync("myapp.contracts/rulesevaluated", "RulesEngine");

        Assert.True((await _admin.TopicExistsAsync("myapp.contracts/rulesevaluated")).Value);
        Assert.True((await _admin.SubscriptionExistsAsync("myapp.contracts/rulesevaluated", "RulesEngine")).Value);
        await _admin.DeleteSubscriptionAsync("myapp.contracts/rulesevaluated", "RulesEngine");
        await _admin.DeleteTopicAsync("myapp.contracts/rulesevaluated");
    }

    [Fact]
    public async Task DefaultRules_IncludingFalseFilters_SurviveRestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateTopicAsync("filtered");
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("filtered", "reds"), new CreateRuleOptions("$Default", new SqlRuleFilter("colour = 'red'")));
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("filtered", "nothing"), new CreateRuleOptions("$Default", new FalseRuleFilter()));
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            await using var client = restarted.CreateServiceBusClient();
            await using (var sender = client.CreateSender("filtered"))
            {
                await sender.SendMessageAsync(new ServiceBusMessage("red") { ApplicationProperties = { ["colour"] = "red" } });
                await sender.SendMessageAsync(new ServiceBusMessage("green") { ApplicationProperties = { ["colour"] = "green" } });
            }

            await using var reds = client.CreateReceiver("filtered", "reds");
            Assert.Equal("red", (await reds.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
            Assert.Null(await reds.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
            await using var nothing = client.CreateReceiver("filtered", "nothing");
            Assert.Null(await nothing.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task QueuesAndTopics_ShareOneNamespace()
    {
        await _admin.CreateQueueAsync("shared-name");

        var asTopic = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.GetTopicAsync("shared-name"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, asTopic.Reason);
        Assert.False((await _admin.TopicExistsAsync("shared-name")).Value);

        var create = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateTopicAsync("SHARED-NAME"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, create.Reason);
        Assert.Null(_broker.EntityManager.GetTopic("shared-name"));
    }

    [Fact]
    public async Task CreateTopic_WithASubscriptionsSegmentInItsName_IsRejected()
    {
        await Assert.ThrowsAnyAsync<Exception>(() => _admin.CreateTopicAsync("orders/Subscriptions/x"));
        Assert.Null(_broker.EntityManager.GetTopic("orders/Subscriptions/x"));
    }

    [Fact]
    public async Task TopicAndSubscriptionProperties_RoundTripAndSurviveRestart()
    {
        var topicOptions = new CreateTopicOptions("Orders.Topic")
        {
            AutoDeleteOnIdle = TimeSpan.FromDays(3),
            DefaultMessageTimeToLive = TimeSpan.FromMinutes(90),
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(4),
            EnableBatchedOperations = false,
            EnablePartitioning = true,
            MaxSizeInMegabytes = 2048,
            RequiresDuplicateDetection = true,
            Status = EntityStatus.SendDisabled,
            SupportOrdering = false,
            UserMetadata = "topic-meta",
            MaxMessageSizeInKilobytes = 2048,
        };
        var subOptions = new CreateSubscriptionOptions("Orders.Topic", "Billing-Sub")
        {
            AutoDeleteOnIdle = TimeSpan.FromHours(30),
            DefaultMessageTimeToLive = TimeSpan.FromMinutes(45),
            DeadLetteringOnMessageExpiration = true,
            EnableDeadLetteringOnFilterEvaluationExceptions = false,
            EnableBatchedOperations = false,
            ForwardDeadLetteredMessagesTo = "billing-errors",
            ForwardTo = "billing-queue",
            LockDuration = TimeSpan.FromMilliseconds(2500),
            MaxDeliveryCount = 6,
            RequiresSession = true,
            Status = EntityStatus.ReceiveDisabled,
            UserMetadata = "sub-meta",
        };

        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateQueueAsync("billing-queue");
            await admin.CreateQueueAsync("billing-errors");
            AssertMatches(topicOptions, await admin.CreateTopicAsync(topicOptions));
            AssertMatches(subOptions, await admin.CreateSubscriptionAsync(subOptions));
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            var admin = restarted.CreateAdministrationClient();
            AssertMatches(topicOptions, await admin.GetTopicAsync("Orders.Topic"));
            AssertMatches(subOptions, await admin.GetSubscriptionAsync("Orders.Topic", "Billing-Sub"));

            // Lookups are case-insensitive. (The SDK reports the name it was asked for; the stored casing the API
            // returns becomes observable through the listing operations.)
            Assert.True((await admin.TopicExistsAsync("orders.topic")).Value);
            Assert.True((await admin.SubscriptionExistsAsync("orders.topic", "billing-sub")).Value);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    private static void AssertMatches(CreateTopicOptions expected, TopicProperties actual)
    {
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.AutoDeleteOnIdle, actual.AutoDeleteOnIdle);
        Assert.Equal(expected.DefaultMessageTimeToLive, actual.DefaultMessageTimeToLive);
        Assert.Equal(expected.DuplicateDetectionHistoryTimeWindow, actual.DuplicateDetectionHistoryTimeWindow);
        Assert.Equal(expected.EnableBatchedOperations, actual.EnableBatchedOperations);
        Assert.Equal(expected.EnablePartitioning, actual.EnablePartitioning);
        Assert.Equal(expected.MaxSizeInMegabytes, actual.MaxSizeInMegabytes);
        Assert.Equal(expected.RequiresDuplicateDetection, actual.RequiresDuplicateDetection);
        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.SupportOrdering, actual.SupportOrdering);
        Assert.Equal(expected.UserMetadata, actual.UserMetadata);
        Assert.Equal(expected.MaxMessageSizeInKilobytes, actual.MaxMessageSizeInKilobytes);
    }

    private static void AssertMatches(CreateSubscriptionOptions expected, SubscriptionProperties actual)
    {
        Assert.Equal(expected.TopicName, actual.TopicName);
        Assert.Equal(expected.SubscriptionName, actual.SubscriptionName);
        Assert.Equal(expected.AutoDeleteOnIdle, actual.AutoDeleteOnIdle);
        Assert.Equal(expected.DefaultMessageTimeToLive, actual.DefaultMessageTimeToLive);
        Assert.Equal(expected.DeadLetteringOnMessageExpiration, actual.DeadLetteringOnMessageExpiration);
        Assert.Equal(expected.EnableDeadLetteringOnFilterEvaluationExceptions, actual.EnableDeadLetteringOnFilterEvaluationExceptions);
        Assert.Equal(expected.EnableBatchedOperations, actual.EnableBatchedOperations);
        // The SDK sends forward targets as absolute URIs (http://host/name); the API returns them as received.
        Assert.EndsWith("/" + expected.ForwardDeadLetteredMessagesTo, actual.ForwardDeadLetteredMessagesTo);
        Assert.EndsWith("/" + expected.ForwardTo, actual.ForwardTo);
        Assert.Equal(expected.LockDuration, actual.LockDuration);
        Assert.Equal(expected.MaxDeliveryCount, actual.MaxDeliveryCount);
        Assert.Equal(expected.RequiresSession, actual.RequiresSession);
        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.UserMetadata, actual.UserMetadata);
    }

    private async Task<List<string>> DrainAsync(string topic, string subscription)
    {
        await using var receiver = _client.CreateReceiver(topic, subscription);
        var bodies = new List<string>();
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)) is { } msg)
        {
            bodies.Add(msg.Body.ToString());
            await receiver.CompleteMessageAsync(msg);
        }
        return bodies;
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
