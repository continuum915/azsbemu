using Azure.Messaging.ServiceBus.Administration;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;

namespace SbBrowser.Tests.Entities;

/// <summary>What the tree says about routing: subscription rules, forward targets and who feeds an entity.</summary>
public sealed class RoutingTreeTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private NamespaceConnection _connection = null!;

    public Task InitializeAsync()
    {
        _connection = NamespaceConnection.Open(new ConnectionProfile("test",
            "Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
            _broker.HttpPort, _broker.Port));
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
        _broker.Dispose();
    }

    [Fact]
    public async Task Subscriptions_CarryTheirRules()
    {
        _broker.EnsureTopic("orders");
        _broker.EnsureSubscription("orders", "all");
        var admin = _broker.CreateAdministrationClient();
        await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("orders", "eu"),
            new CreateRuleOptions("eu-only", new SqlRuleFilter("Region = 'EU'")) { Action = new SqlRuleAction("SET Seen = TRUE") });
        await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("orders", "shipped"),
            new CreateRuleOptions("by-subject", new CorrelationRuleFilter { Subject = "Shipped" }));

        var topic = Assert.Single((await _connection.GetTreeAsync()).Topics);

        var all = topic.Subscriptions.Single(s => s.Ref.Name == "all");
        Assert.True(all.ReceivesAll);
        Assert.Equal("$Default", Assert.Single(all.Rules).Name);

        var eu = topic.Subscriptions.Single(s => s.Ref.Name == "eu");
        Assert.False(eu.ReceivesAll);
        var euRule = Assert.Single(eu.Rules);
        Assert.Equal(new RuleView("eu-only", RuleFilterKind.Sql, "Region = 'EU'", "SET Seen = TRUE"), euRule);

        var shipped = Assert.Single(topic.Subscriptions.Single(s => s.Ref.Name == "shipped").Rules);
        Assert.Equal(RuleFilterKind.Correlation, shipped.Kind);
        Assert.Equal("Subject = 'Shipped'", shipped.Filter);
    }

    [Fact]
    public async Task FeedersOf_ListsWhatForwardsIntoAnEntity()
    {
        _broker.EnsureQueue("billing");
        _broker.EnsureQueue("billing_error");
        _broker.EnsureTopic("orders");
        var admin = _broker.CreateAdministrationClient();
        await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("orders", "billing") { ForwardTo = "billing" });
        await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("orders", "audit") { ForwardDeadLetteredMessagesTo = "billing_error" });

        var tree = await _connection.GetTreeAsync();

        Assert.Equal("billing", tree.FindSubscription("orders", "billing")!.Settings.ForwardTo);
        Assert.Equal([new Feeder(new SubscriptionRef("orders", "billing"), DeadLetters: false)], tree.FeedersOf("billing"));
        Assert.Equal([new Feeder(new SubscriptionRef("orders", "audit"), DeadLetters: true)], tree.FeedersOf("BILLING_ERROR"));
        Assert.Empty(tree.FeedersOf("orders"));
        Assert.IsType<QueueRef>(tree.FindQueueOrTopic("billing"));
        Assert.IsType<TopicRef>(tree.FindQueueOrTopic("orders"));
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("billing", "billing")]
    [InlineData("sb://ns.servicebus.windows.net/billing", "billing")]
    [InlineData("https://ns.servicebus.windows.net/myapp.contracts/ordersubmitted", "myapp.contracts/ordersubmitted")]
    public void ForwardTargets_AreReducedToEntityNames(string? value, string? expected) =>
        Assert.Equal(expected, RuleMapper.ForwardName(value));
}
