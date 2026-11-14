using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Deleting an entity removes everything it owns, so it can be recreated cleanly and stays gone across restarts.</summary>
public class EntityDeletionTests
{
    [Fact]
    public async Task Subscription_DeletedAndRecreated_StartsWithOnlyItsNewRule()
    {
        using var broker = new BrokerFixture();
        var admin = broker.CreateAdministrationClient();
        await admin.CreateTopicAsync("t");
        await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("t", "s"), new CreateRuleOptions("$Default", new SqlRuleFilter("colour = 'red'")));

        await admin.DeleteSubscriptionAsync("t", "s");
        await admin.CreateSubscriptionAsync("t", "s");

        await using var client = broker.CreateServiceBusClient();
        await using (var sender = client.CreateSender("t"))
            await sender.SendMessageAsync(new ServiceBusMessage("green") { ApplicationProperties = { ["colour"] = "green" } });
        await using var receiver = client.CreateReceiver("t", "s");
        Assert.Equal("green", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString());
    }

    [Fact]
    public async Task Topic_DeletedAndRecreated_DoesNotGetItsOldSubscriptionsBack()
    {
        await WithRestartAsync(
            async admin =>
            {
                await admin.CreateTopicAsync("t");
                await admin.CreateSubscriptionAsync("t", "old-sub");
                await admin.DeleteTopicAsync("t");
                await admin.CreateTopicAsync("t");
                await admin.CreateSubscriptionAsync("t", "old-sub"); // the name is free again
                await admin.DeleteSubscriptionAsync("t", "old-sub");
            },
            async admin => Assert.False((await admin.SubscriptionExistsAsync("t", "old-sub")).Value));
    }

    [Fact]
    public async Task Entities_DeletedUsingDifferentCasing_StayDeletedAfterRestart()
    {
        await WithRestartAsync(
            async admin =>
            {
                await admin.CreateQueueAsync("Mixed-Queue");
                await admin.CreateTopicAsync("Mixed-Topic");
                await admin.CreateSubscriptionAsync("Mixed-Topic", "Mixed-Sub");
                await admin.CreateTopicAsync("Other-Topic");
                await admin.CreateSubscriptionAsync("Other-Topic", "Kept-Sub");

                await admin.DeleteQueueAsync("mixed-queue");
                await admin.DeleteSubscriptionAsync("other-topic", "kept-sub");
                await admin.DeleteTopicAsync("mixed-topic");
            },
            async admin =>
            {
                Assert.False((await admin.QueueExistsAsync("Mixed-Queue")).Value);
                Assert.False((await admin.TopicExistsAsync("Mixed-Topic")).Value);
                Assert.False((await admin.SubscriptionExistsAsync("Other-Topic", "Kept-Sub")).Value);
                Assert.True((await admin.TopicExistsAsync("Other-Topic")).Value);
            });
    }

    private static async Task WithRestartAsync(Func<ServiceBusAdministrationClient, Task> before, Func<ServiceBusAdministrationClient, Task> after)
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            await before(broker.CreateAdministrationClient());
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            await after(restarted.CreateAdministrationClient());
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }
}
