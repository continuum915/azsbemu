using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Subscription rules managed through the HTTP management API, and through the SDK's AMQP rule manager.</summary>
public class ManagementHttpRuleTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public ManagementHttpRuleTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task CreateRule_WithASqlFilter_RoutesOnlyMatchingMessages_AndCanBeFetched()
    {
        await _admin.CreateTopicAsync("orders");
        await _admin.CreateSubscriptionAsync("orders", "eu");
        await _admin.DeleteRuleAsync("orders", "eu", "$Default");

        RuleProperties created = await _admin.CreateRuleAsync("orders", "eu", new CreateRuleOptions("eu-only", new SqlRuleFilter("region = 'eu'")));
        Assert.Equal("eu-only", created.Name);

        await SendAsync("orders", Message("eu-order", ("region", "eu")), Message("us-order", ("region", "us")));
        Assert.Equal(new[] { "eu-order" }, await DrainAsync("orders", "eu"));

        RuleProperties fetched = await _admin.GetRuleAsync("orders", "eu", "eu-only");
        Assert.Equal("region = 'eu'", Assert.IsType<SqlRuleFilter>(fetched.Filter).SqlExpression);
    }

    [Fact]
    public async Task CorrelationFiltersWithTypedPropertiesAndSqlActions_RouteAndRoundTrip()
    {
        await _admin.CreateTopicAsync("events");
        await _admin.CreateSubscriptionAsync("events", "big-paint");
        await _admin.DeleteRuleAsync("events", "big-paint", "$Default");
        var filter = new CorrelationRuleFilter { Subject = "paint" };
        filter.ApplicationProperties["size"] = 3;
        filter.ApplicationProperties["colour"] = "blue";
        await _admin.CreateRuleAsync("events", "big-paint", new CreateRuleOptions("blue-3", filter) { Action = new SqlRuleAction("SET matched = 'blue-3'") });

        await SendAsync("events",
            Message("match", ("size", 3), ("colour", "blue")).WithSubject("paint"),
            Message("wrong-size", ("size", 4), ("colour", "blue")).WithSubject("paint"),
            Message("wrong-subject", ("size", 3), ("colour", "blue")).WithSubject("ink"));
        await using (var receiver = _client.CreateReceiver("events", "big-paint"))
        {
            var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("match", received?.Body.ToString());
            Assert.Equal("blue-3", received!.ApplicationProperties["matched"]);
            await receiver.CompleteMessageAsync(received);
            Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        }

        var rules = await ToListAsync(_admin.GetRulesAsync("events", "big-paint"));
        var rule = Assert.Single(rules);
        var fetched = Assert.IsType<CorrelationRuleFilter>(rule.Filter);
        Assert.Equal("paint", fetched.Subject);
        Assert.Equal(3, fetched.ApplicationProperties["size"]);
        Assert.Equal("blue", fetched.ApplicationProperties["colour"]);
        Assert.Equal("SET matched = 'blue-3'", Assert.IsType<SqlRuleAction>(rule.Action).SqlExpression);
    }

    [Fact]
    public async Task UpdatingAndDeletingRules_ChangesRoutingImmediately_AndConflictsAndMissesAreReported()
    {
        await _admin.CreateTopicAsync("t");
        await _admin.CreateSubscriptionAsync("t", "s");
        await _admin.DeleteRuleAsync("t", "s", "$Default");
        RuleProperties rule = await _admin.CreateRuleAsync("t", "s", new CreateRuleOptions("r", new SqlRuleFilter("n = 1")));

        var duplicate = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.CreateRuleAsync("t", "s", new CreateRuleOptions("R", new TrueRuleFilter())));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, duplicate.Reason);
        var missing = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.GetRuleAsync("t", "s", "nope"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, missing.Reason);

        rule.Filter = new SqlRuleFilter("n = 2");
        await _admin.UpdateRuleAsync("t", "s", rule);
        await SendAsync("t", Message("one", ("n", 1)), Message("two", ("n", 2)));
        Assert.Equal(new[] { "two" }, await DrainAsync("t", "s"));

        await _admin.DeleteRuleAsync("t", "s", "R"); // any casing
        await SendAsync("t", Message("none", ("n", 2)));
        Assert.Empty(await DrainAsync("t", "s"));
        var gone = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.DeleteRuleAsync("t", "s", "r"));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, gone.Reason);
        Assert.Empty(await ToListAsync(_admin.GetRulesAsync("t", "s")));
    }

    [Fact]
    public async Task Rules_SurviveRestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateTopicAsync("t");
            await admin.CreateSubscriptionAsync("t", "s");
            var filter = new CorrelationRuleFilter();
            filter.ApplicationProperties["size"] = 3;
            await admin.CreateRuleAsync("t", "s", new CreateRuleOptions("sized", filter));
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            var rules = await ToListAsync(restarted.CreateAdministrationClient().GetRulesAsync("t", "s"));
            Assert.Equal(new[] { "$Default", "sized" }, rules.Select(r => r.Name).Order());
            Assert.Equal(3, Assert.IsType<CorrelationRuleFilter>(rules.Single(r => r.Name == "sized").Filter).ApplicationProperties["size"]);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task CorrelationPropertyTypes_SurviveRestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateTopicAsync("t");
            await admin.CreateSubscriptionAsync("t", "s");
            var filter = new CorrelationRuleFilter();
            filter.ApplicationProperties["i"] = 3;
            filter.ApplicationProperties["l"] = 5L;
            filter.ApplicationProperties["d"] = 2.0;
            filter.ApplicationProperties["b"] = true;
            filter.ApplicationProperties["str"] = "x";
            await admin.CreateRuleAsync("t", "s", new CreateRuleOptions("typed", filter));
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            var props = Assert.IsType<CorrelationRuleFilter>((await restarted.CreateAdministrationClient().GetRuleAsync("t", "s", "typed")).Value.Filter).ApplicationProperties;
            Assert.IsType<int>(props["i"]);
            Assert.IsType<long>(props["l"]);
            Assert.IsType<double>(props["d"]);
            Assert.IsType<bool>(props["b"]);
            Assert.IsType<string>(props["str"]);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task TrueAndFalseFilters_RouteAndReadBackAsTrueAndFalse()
    {
        await _admin.CreateTopicAsync("tf");
        await _admin.CreateSubscriptionAsync("tf", "all");
        await _admin.CreateSubscriptionAsync("tf", "none");
        await _admin.DeleteRuleAsync("tf", "all", "$Default");
        await _admin.DeleteRuleAsync("tf", "none", "$Default");
        await _admin.CreateRuleAsync("tf", "all", new CreateRuleOptions("everything", new TrueRuleFilter()));
        await _admin.CreateRuleAsync("tf", "none", new CreateRuleOptions("nothing", new FalseRuleFilter()));
        await _admin.CreateRuleAsync("tf", "none", new CreateRuleOptions("as-sql", new SqlRuleFilter("1=0")));

        await SendAsync("tf", Message("m"));
        Assert.Equal(new[] { "m" }, await DrainAsync("tf", "all"));
        Assert.Empty(await DrainAsync("tf", "none"));

        Assert.IsType<TrueRuleFilter>((await _admin.GetRuleAsync("tf", "all", "everything")).Value.Filter);
        Assert.IsType<FalseRuleFilter>((await _admin.GetRuleAsync("tf", "none", "nothing")).Value.Filter);
        Assert.IsType<FalseRuleFilter>((await _admin.GetRuleAsync("tf", "none", "as-sql")).Value.Filter);
    }

    [Fact]
    public async Task InvalidRuleSqlAndUpdatesOfMissingRules_AreRejected()
    {
        await _admin.CreateTopicAsync("v");
        await _admin.CreateSubscriptionAsync("v", "s");

        await Assert.ThrowsAsync<ArgumentException>(() => _admin.CreateRuleAsync("v", "s", new CreateRuleOptions("bad-filter", new SqlRuleFilter("region ==== 'eu'"))));
        RuleProperties rule = await _admin.CreateRuleAsync("v", "s", new CreateRuleOptions("ok", new SqlRuleFilter("n = 1")));
        rule.Action = new SqlRuleAction("UPDATE everything");
        await Assert.ThrowsAsync<ArgumentException>(() => _admin.UpdateRuleAsync("v", "s", rule));

        await _admin.DeleteRuleAsync("v", "s", "ok");
        rule.Action = null;
        var missing = await Assert.ThrowsAsync<ServiceBusException>(() => _admin.UpdateRuleAsync("v", "s", rule));
        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, missing.Reason);
        Assert.DoesNotContain("ok", (await ToListAsync(_admin.GetRulesAsync("v", "s"))).Select(r => r.Name));
    }

    [Fact]
    public async Task ARuleDeletedInDifferentCasing_StaysDeletedAfterRestart()
    {
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            var admin = broker.CreateAdministrationClient();
            await admin.CreateTopicAsync("t");
            await admin.CreateSubscriptionAsync("t", "s");
            await admin.CreateRuleAsync("t", "s", new CreateRuleOptions("Mixed-Rule", new SqlRuleFilter("n = 1")));
            await admin.DeleteRuleAsync("t", "s", "mixed-rule");
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            var names = (await ToListAsync(restarted.CreateAdministrationClient().GetRulesAsync("t", "s"))).Select(r => r.Name);
            Assert.DoesNotContain("Mixed-Rule", names);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source) list.Add(item);
        return list;
    }

    private static ServiceBusMessage Message(string body, params (string Key, object Value)[] properties)
    {
        var message = new ServiceBusMessage(body);
        foreach (var (key, value) in properties) message.ApplicationProperties[key] = value;
        return message;
    }

    private async Task SendAsync(string topic, params ServiceBusMessage[] messages)
    {
        await using var sender = _client.CreateSender(topic);
        foreach (var message in messages) await sender.SendMessageAsync(message);
    }

    private async Task<List<string>> DrainAsync(string topic, string subscription)
    {
        await using var receiver = _client.CreateReceiver(topic, subscription, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var bodies = new List<string>();
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)) is { } msg) bodies.Add(msg.Body.ToString());
        return bodies;
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}

internal static class ServiceBusMessageTestExtensions
{
    public static ServiceBusMessage WithSubject(this ServiceBusMessage message, string subject)
    {
        message.Subject = subject;
        return message;
    }
}
