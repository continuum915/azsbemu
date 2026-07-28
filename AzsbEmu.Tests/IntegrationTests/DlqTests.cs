using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests.IntegrationTests;

public class DlqTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string QueueName = "dlq-test-queue";

    public DlqTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureQueue(QueueName);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task SendToDlq_CanReceiveFromDlq()
    {
        // Azure SDK doesn't support direct send to DLQ easily via ServiceBusSender,
        // but it's possible via AMQP if we use the address.
        // For this test, we'll use the internal broker to inject a message into the DLQ
        // and then use the SDK to receive it.
        
        var queue = _broker.GetQueue(QueueName);
        Assert.NotNull(queue.DlqMessageStore);
        
        var header = new MessageHeader { SequenceNumber = 1, EnqueueTimeUtcTicks = DateTime.UtcNow.Ticks, IsDeadLettered = true };
        var amqpMessage = new Amqp.Message { BodySection = new Amqp.Framing.Data { Binary = System.Text.Encoding.UTF8.GetBytes("dead-letter-payload") } };
        byte[] data = MessageSerializer.Encode(amqpMessage, header);
        queue.DlqMessageStore!.Append(data);

        // Now receive from DLQ using SDK
        string dlqPath = QueueName + "/$DeadLetterQueue";
        await using var receiver = _client.CreateReceiver(dlqPath);
        
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("dead-letter-payload", received.Body.ToString());
        
        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task DlqAddress_IsCaseInsensitive()
    {
        var queue = _broker.GetQueue(QueueName);
        var header = new MessageHeader { SequenceNumber = 1, EnqueueTimeUtcTicks = DateTime.UtcNow.Ticks, IsDeadLettered = true };
        var amqpMessage = new Amqp.Message { BodySection = new Amqp.Framing.Data { Binary = System.Text.Encoding.UTF8.GetBytes("case-test") } };
        byte[] data = MessageSerializer.Encode(amqpMessage, header);
        queue.DlqMessageStore!.Append(data);

        // Try different casings
        string[] variations = [
            QueueName + "/$deadletterqueue",
            QueueName + "/$DeadLetterQueue",
            QueueName + "/$DEADLETTERQUEUE"
        ];

        foreach (var path in variations)
        {
            await using var receiver = _client.CreateReceiver(path);
            var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(received);
            Assert.Equal("case-test", received.Body.ToString());
            await receiver.CompleteMessageAsync(received);
            
            // Re-inject for next variation
            queue.DlqMessageStore!.Append(data);
        }
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
