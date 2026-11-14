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

    [Fact]
    public async Task DeadLetterMessage_AppliesPropertiesToModify_AndUsesOnlyAGivenReason()
    {
        _broker.EnsureQueue("dlq-props");
        await using (var sender = _client.CreateSender("dlq-props"))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("with-reason") { ApplicationProperties = { ["kept"] = "yes" } });
            await sender.SendMessageAsync(new ServiceBusMessage("no-reason"));
        }

        await using (var receiver = _client.CreateReceiver("dlq-props"))
        {
            var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeadLetterMessageAsync(first!, new Dictionary<string, object> { ["fault"] = "boom", ["attempts"] = 3 }, "Broken", "it broke");
            var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeadLetterMessageAsync(second!);
        }

        await using var dlq = _client.CreateReceiver("dlq-props", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var withReason = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("Broken", withReason!.DeadLetterReason);
        Assert.Equal("it broke", withReason.DeadLetterErrorDescription);
        Assert.Equal("boom", withReason.ApplicationProperties["fault"]);
        Assert.Equal(3, withReason.ApplicationProperties["attempts"]);
        Assert.Equal("yes", withReason.ApplicationProperties["kept"]);

        var noReason = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("no-reason", noReason?.Body.ToString());
        Assert.Null(noReason!.DeadLetterReason);
    }

    [Fact]
    public async Task TransactionalDeadLetter_KeepsReasonAndPropertiesToModify()
    {
        _broker.EnsureQueue("dlq-txn");
        await using (var sender = _client.CreateSender("dlq-txn"))
            await sender.SendMessageAsync(new ServiceBusMessage("in-txn"));

        await using (var receiver = _client.CreateReceiver("dlq-txn"))
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            using var scope = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled);
            await receiver.DeadLetterMessageAsync(message!, new Dictionary<string, object> { ["fault"] = "txn" }, "TxnReason", "txn description");
            scope.Complete();
        }

        await using var dlq = _client.CreateReceiver("dlq-txn", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var deadLettered = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("TxnReason", deadLettered!.DeadLetterReason);
        Assert.Equal("txn description", deadLettered.DeadLetterErrorDescription);
        Assert.Equal("txn", deadLettered.ApplicationProperties["fault"]);
    }

    [Fact]
    public async Task DeadLetteringADeferredMessage_KeepsReasonAndPropertiesToModify()
    {
        // Deferred messages are settled through the management link (update-disposition).
        _broker.EnsureQueue("dlq-deferred");
        await using (var sender = _client.CreateSender("dlq-deferred"))
            await sender.SendMessageAsync(new ServiceBusMessage("deferred"));

        await using (var receiver = _client.CreateReceiver("dlq-deferred"))
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeferMessageAsync(message!);
            var deferred = await receiver.ReceiveDeferredMessageAsync(message!.SequenceNumber);
            await receiver.DeadLetterMessageAsync(deferred, new Dictionary<string, object> { ["fault"] = "deferred" }, "DeferredReason", "deferred description");
        }

        await using var dlq = _client.CreateReceiver("dlq-deferred", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var deadLettered = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("DeferredReason", deadLettered!.DeadLetterReason);
        Assert.Equal("deferred", deadLettered.ApplicationProperties["fault"]);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
