using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests.IntegrationTests;

public class ReceiveAndDeleteTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string QueueName = "rad-test-queue";

    public ReceiveAndDeleteTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureQueue(QueueName);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task ReceiveAndDelete_ConsumesMessageImmediately()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });

        await sender.SendMessageAsync(new ServiceBusMessage("rad-msg"));

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("rad-msg", received.Body.ToString());

        // Second receive should return nothing because the first was ReceiveAndDelete
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(second);

        // Verify queue count is zero
        var info = _broker.GetQueue(QueueName);
        Assert.Equal(0, info.MessageCount);
    }

    [Fact]
    public async Task ReceiveAndDelete_MultipleMessages_AreConsumedCorrectly()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });

        await sender.SendMessageAsync(new ServiceBusMessage("msg-1"));
        await sender.SendMessageAsync(new ServiceBusMessage("msg-2"));

        var m1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var m2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(m1);
        Assert.NotNull(m2);
        Assert.Equal("msg-1", m1.Body.ToString());
        Assert.Equal("msg-2", m2.Body.ToString());

        var info = _broker.GetQueue(QueueName);
        Assert.Equal(0, info.MessageCount);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
