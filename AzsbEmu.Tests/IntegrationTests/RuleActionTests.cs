using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>SQL rule actions rewrite a subscription's copy of a matching message, whichever API created the rule.</summary>
public class RuleActionTests : IDisposable
{
    private readonly BrokerFixture _broker = new();
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusClient _client;

    public RuleActionTests()
    {
        _admin = _broker.CreateAdministrationClient();
        _client = _broker.CreateServiceBusClient();
    }

    [Fact]
    public async Task ARuleAction_ChangesOnlyItsOwnSubscriptionsCopy()
    {
        await _admin.CreateTopicAsync("shared");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("shared", "rewrites"),
            new CreateRuleOptions("$Default", new TrueRuleFilter()) { Action = new SqlRuleAction("SET sys.Label = 'rewritten'") });
        await _admin.CreateSubscriptionAsync("shared", "plain");

        await SendAsync("shared", new ServiceBusMessage("m") { Subject = "original" });

        Assert.Equal("rewritten", (await ReceiveAsync("shared", "rewrites"))?.Subject);
        Assert.Equal("original", (await ReceiveAsync("shared", "plain"))?.Subject);
    }

    [Theory]
    [InlineData("UPDATE everything")]
    [InlineData("SET a = b + 1")]
    [InlineData("SET sys.DeliveryCount = 5")] // not a settable system property
    public async Task ARuleWithAnInvalidAction_IsRejectedAtCreate(string action)
    {
        await _admin.CreateTopicAsync("t");

        await Assert.ThrowsAnyAsync<Exception>(() => _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("t", "s"),
            new CreateRuleOptions("$Default", new TrueRuleFilter()) { Action = new SqlRuleAction(action) }));
        Assert.False((await _admin.SubscriptionExistsAsync("t", "s")).Value);
    }

    private async Task SendAsync(string entity, ServiceBusMessage message)
    {
        await using var sender = _client.CreateSender(entity);
        await sender.SendMessageAsync(message);
    }

    private async Task<ServiceBusReceivedMessage?> ReceiveAsync(string topic, string subscription)
    {
        await using var receiver = _client.CreateReceiver(topic, subscription, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        return await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
