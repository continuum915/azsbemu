using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Integration tests for Task 1.10: persistence recovery on startup.</summary>
public class StartupRecoveryTests
{
    [Fact]
    public async Task Restart_PendingMessage_IsRedeliveredWithPreservedMetadata()
    {
        const string queueName = "recovery-queue";
        const string messageBody = "survive-restart";
        long originalSeqNum;
        string dataDir;

        // Phase 1: send a message then stop cleanly (Dispose flushes the commit loop)
        {
            using var broker1 = new BrokerFixture();
            dataDir = broker1.TakeDataDir(); // we own cleanup
            broker1.EnsureQueue(queueName);

            await using var client1 = broker1.CreateServiceBusClient();
            await using var sender = client1.CreateSender(queueName);
            await sender.SendMessageAsync(new ServiceBusMessage(messageBody));

            var q = broker1.GetQueue(queueName);
            Assert.Equal(1, q.MessageCount);
            originalSeqNum = q.NextSequenceNumber;
        }
        // broker1.Dispose() ran: broker stopped, MessageStore flushed to disk

        try
        {
            // Phase 2: restart against the same data directory
            using var broker2 = new BrokerFixture(dataDir);

            var q2 = broker2.GetQueue(queueName);
            Assert.Equal(1, q2.MessageCount);
            Assert.Equal(originalSeqNum, q2.NextSequenceNumber);

            await using var client2 = broker2.CreateServiceBusClient();
            await using var receiver = client2.CreateReceiver(queueName);

            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            Assert.Equal(messageBody, msg.Body.ToString());
            Assert.Equal(originalSeqNum, msg.SequenceNumber);

            await receiver.CompleteMessageAsync(msg);

            Assert.Equal(0, broker2.GetQueue(queueName).MessageCount);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Restart_SequenceNumbersContinueFromPreviousMax()
    {
        const string queueName = "seqnum-recovery-queue";
        long maxSeqBeforeRestart;
        string dataDir;

        {
            using var broker1 = new BrokerFixture();
            dataDir = broker1.TakeDataDir();
            broker1.EnsureQueue(queueName);

            await using var client1 = broker1.CreateServiceBusClient();
            await using var sender = client1.CreateSender(queueName);
            await sender.SendMessageAsync(new ServiceBusMessage("msg-1"));
            await sender.SendMessageAsync(new ServiceBusMessage("msg-2"));

            // Complete the first message to advance BeginAddress past it
            await using var receiver = client1.CreateReceiver(queueName);
            var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(first);
            await receiver.CompleteMessageAsync(first);

            maxSeqBeforeRestart = broker1.GetQueue(queueName).NextSequenceNumber; // 2
        }

        try
        {
            using var broker2 = new BrokerFixture(dataDir);

            var q2 = broker2.GetQueue(queueName);
            // Startup scan should find msg-2 (seq 2), so NextSequenceNumber == 2
            Assert.Equal(maxSeqBeforeRestart, q2.NextSequenceNumber);
            // msg-1 was completed (truncated), msg-2 is still present
            Assert.Equal(1, q2.MessageCount);

            await using var client2 = broker2.CreateServiceBusClient();
            await using var sender2 = client2.CreateSender(queueName);
            await sender2.SendMessageAsync(new ServiceBusMessage("msg-3"));

            // msg-3 should receive a sequence number > 2
            await using var receiver2 = client2.CreateReceiver(queueName);
            // drain msg-2 first
            var msg2 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg2);
            await receiver2.CompleteMessageAsync(msg2);
            // now msg-3
            var msg3 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg3);
            Assert.True(msg3.SequenceNumber > maxSeqBeforeRestart,
                $"msg-3 seq {msg3.SequenceNumber} should be > prior max {maxSeqBeforeRestart}");
            await receiver2.CompleteMessageAsync(msg3);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }
}
