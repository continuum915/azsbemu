using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>The SDK's own AMQP rule manager (<see cref="ServiceBusClient.CreateRuleManager"/>), and its view of HTTP-created rules.</summary>
public class AmqpRuleManagerTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public AmqpRuleManagerTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task RulesCreatedOverHttpAndThroughTheAmqpRuleManager_AreVisibleToBoth()
    {
        await _admin.CreateTopicAsync("both");
        await _admin.CreateSubscriptionAsync("both", "s");
        await _admin.CreateRuleAsync("both", "s", new CreateRuleOptions("from-http", new SqlRuleFilter("source = 'http'")));

        await using (var rules = _client.CreateRuleManager("both", "s"))
        {
            var corr = new CorrelationRuleFilter { CorrelationId = "c-1" };
            corr.ApplicationProperties["n"] = 7;
            await rules.CreateRuleAsync(new CreateRuleOptions("from-amqp", corr) { Action = new SqlRuleAction("SET via = 'amqp'") });
            await rules.DeleteRuleAsync("$Default");

            var seenOverAmqp = new List<RuleProperties>();
            await foreach (var r in rules.GetRulesAsync()) seenOverAmqp.Add(r);
            Assert.Equal(new[] { "from-amqp", "from-http" }, seenOverAmqp.Select(r => r.Name).Order());
            Assert.Equal("source = 'http'", Assert.IsType<SqlRuleFilter>(seenOverAmqp.Single(r => r.Name == "from-http").Filter).SqlExpression);
        }

        RuleProperties seenOverHttp = await _admin.GetRuleAsync("both", "s", "from-amqp");
        var filter = Assert.IsType<CorrelationRuleFilter>(seenOverHttp.Filter);
        Assert.Equal("c-1", filter.CorrelationId);
        Assert.Equal(7, filter.ApplicationProperties["n"]);
        Assert.Equal("SET via = 'amqp'", Assert.IsType<SqlRuleAction>(seenOverHttp.Action).SqlExpression);

        await SendAsync("both", new ServiceBusMessage("amqp-routed") { CorrelationId = "c-1", ApplicationProperties = { ["n"] = 7 } });
        await using var receiver = _client.CreateReceiver("both", "s");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("amqp", received?.ApplicationProperties["via"]);
    }

    [Fact]
    public async Task DuplicateRules_AreRejected_AndASubscriptionWithoutRulesListsNone()
    {
        await _admin.CreateTopicAsync("dup");
        await _admin.CreateSubscriptionAsync("dup", "s");
        await using var rules = _client.CreateRuleManager("dup", "s");

        await rules.CreateRuleAsync("r", new SqlRuleFilter("n = 1"));
        var duplicate = await Assert.ThrowsAsync<ServiceBusException>(() => rules.CreateRuleAsync("R", new SqlRuleFilter("n = 2")));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, duplicate.Reason);

        await rules.DeleteRuleAsync("R"); // any casing
        await rules.DeleteRuleAsync("$Default");
        var remaining = new List<RuleProperties>();
        await foreach (var r in rules.GetRulesAsync()) remaining.Add(r);
        Assert.Empty(remaining);
    }

    [Fact]
    public async Task TrueAndFalseFiltersCreatedOverAmqp_ReadBackAsTrueAndFalse()
    {
        await _admin.CreateTopicAsync("tf");
        await _admin.CreateSubscriptionAsync("tf", "s");
        await using (var rules = _client.CreateRuleManager("tf", "s"))
        {
            await rules.CreateRuleAsync("yes", new TrueRuleFilter());
            await rules.CreateRuleAsync("no", new FalseRuleFilter());
        }

        Assert.IsType<TrueRuleFilter>((await _admin.GetRuleAsync("tf", "s", "yes")).Value.Filter);
        Assert.IsType<FalseRuleFilter>((await _admin.GetRuleAsync("tf", "s", "no")).Value.Filter);
    }

    private async Task SendAsync(string topic, params ServiceBusMessage[] messages)
    {
        await using var sender = _client.CreateSender(topic);
        foreach (var message in messages) await sender.SendMessageAsync(message);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
