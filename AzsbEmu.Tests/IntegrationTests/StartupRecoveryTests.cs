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

    /// <summary>
    /// Regression: the active and dead-letter logs of an entity used to share one FASTER commit folder, so the
    /// dead-letter log's commits could replace the active log's and a clean restart lost messages (intermittently,
    /// hence several cycles with commit ticks in between).
    /// </summary>
    [Fact]
    public async Task Restart_Repeatedly_NeverLosesPendingMessages()
    {
        int lost = 0;
        for (int cycle = 1; cycle <= 10; cycle++)
        {
            string dataDir;
            using (var broker = new BrokerFixture())
            {
                dataDir = broker.TakeDataDir();
                broker.EnsureQueue("restart-queue");
                await using var client = broker.CreateServiceBusClient();
                await using var sender = client.CreateSender("restart-queue");
                await sender.SendMessageAsync(new ServiceBusMessage($"cycle-{cycle}"));
                await Task.Delay(200); // let periodic commits run before shutdown
            }

            try
            {
                using var restarted = new BrokerFixture(dataDir);
                if (restarted.GetQueue("restart-queue").MessageCount != 1) lost++;
            }
            finally
            {
                try { Directory.Delete(dataDir, recursive: true); } catch { }
            }
        }

        Assert.Equal(0, lost);
    }

    [Fact]
    public async Task Restart_ActiveAndDeadLetterMessages_BothRecover()
    {
        const string queueName = "active-and-dlq-queue";
        string dataDir;
        using (var broker = new BrokerFixture())
        {
            dataDir = broker.TakeDataDir();
            broker.EnsureQueue(queueName);
            await using var client = broker.CreateServiceBusClient();
            await using (var sender = client.CreateSender(queueName))
            {
                foreach (var body in new[] { "keep-1", "keep-2", "keep-3" })
                    await sender.SendMessageAsync(new ServiceBusMessage(body));
            }
            await using (var dlqSender = client.CreateSender($"{queueName}/$deadletterqueue"))
                await dlqSender.SendMessageAsync(new ServiceBusMessage("dead"));
            await Task.Delay(200);
        }

        try
        {
            using var restarted = new BrokerFixture(dataDir);
            Assert.Equal(3, restarted.GetQueue(queueName).MessageCount);
            Assert.Equal(1, restarted.GetQueue(queueName).DeadLetterMessageCount);

            await using var client = restarted.CreateServiceBusClient();
            await using var dlqReceiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
            var dead = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(dead);
            Assert.Equal("dead", dead.Body.ToString());
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
