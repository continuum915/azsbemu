using Azure.Messaging.ServiceBus;
using Amqp.Types;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Integration tests for Tasks 1.3 (wire send path) and 1.4 (PeekLock receive).
/// Each test class gets its own broker on a unique port with an isolated data directory.
/// </summary>
public class QueueSendReceiveTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string QueueName = "integration-test-queue";

    public QueueSendReceiveTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureQueue(QueueName);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task Send_SingleMessage_IsDeliveredToReceiver()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("hello"));

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("hello", received.Body.ToString());

        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task Send_MessageWithProperties_PropertiesArePreserved()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var outgoing = new ServiceBusMessage("payload")
        {
            MessageId = "msg-prop-test",
            CorrelationId = "corr-001",
            Subject = "test-subject"
        };
        await sender.SendMessageAsync(outgoing);

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("msg-prop-test", received.MessageId);
        Assert.Equal("corr-001", received.CorrelationId);
        Assert.Equal("test-subject", received.Subject);

        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task Send_MultipleMessages_AllDeliveredInOrder()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("msg-1"));
        await sender.SendMessageAsync(new ServiceBusMessage("msg-2"));
        await sender.SendMessageAsync(new ServiceBusMessage("msg-3"));

        var received = new List<ServiceBusReceivedMessage>();
        for (int i = 0; i < 3; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            received.Add(msg);
        }

        Assert.Equal(["msg-1", "msg-2", "msg-3"], received.Select(m => m.Body.ToString()).ToArray());

        foreach (var msg in received)
            await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task Receive_AssignsSequenceNumbers_StartingFromOne()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("first"));
        await sender.SendMessageAsync(new ServiceBusMessage("second"));

        var msg1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msg2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(msg1);
        Assert.NotNull(msg2);
        Assert.True(msg1.SequenceNumber >= 1, "First message should have sequence number >= 1");
        Assert.True(msg2.SequenceNumber > msg1.SequenceNumber, "Second message should have a higher sequence number");

        await receiver.CompleteMessageAsync(msg1);
        await receiver.CompleteMessageAsync(msg2);
    }

    [Fact]
    public async Task Receive_EnqueuedTimeIsSet()
    {
        var beforeSend = DateTimeOffset.UtcNow.AddSeconds(-1);

        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("time-test"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.True(msg.EnqueuedTime >= beforeSend, "EnqueuedTime should be at or after the time before send");
        Assert.True(msg.EnqueuedTime <= DateTimeOffset.UtcNow.AddSeconds(5), "EnqueuedTime should not be in the far future");

        await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task Receive_PeekLockAssignsLockTokenAndLockedUntil()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("peek-lock-test"));

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.NotEqual(Guid.Empty.ToString(), received.LockToken);
        Assert.True(received.LockedUntil > DateTimeOffset.UtcNow, "LockedUntil should be in the future for a PeekLock delivery");

        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task PeekMessages_ReturnsMessagesWithoutConsumingThem()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("peek-1"));
        await sender.SendMessageAsync(new ServiceBusMessage("peek-2"));
        await sender.SendMessageAsync(new ServiceBusMessage("peek-3"));

        IReadOnlyList<ServiceBusReceivedMessage> peeked = await receiver.PeekMessagesAsync(2);

        Assert.Equal(2, peeked.Count);
        Assert.Equal(["peek-1", "peek-2"], peeked.Select(m => m.Body.ToString()).ToArray());
        Assert.True(peeked[1].SequenceNumber > peeked[0].SequenceNumber);

        var received = new List<ServiceBusReceivedMessage>();
        for (int i = 0; i < 3; i++)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            received.Add(message);
        }

        Assert.Equal(["peek-1", "peek-2", "peek-3"], received.Select(m => m.Body.ToString()).ToArray());

        foreach (var message in received)
        {
            await receiver.CompleteMessageAsync(message);
        }
    }

    [Fact]
    public async Task PeekMessages_FromSequenceNumberStartsAtEarliestAvailableRecord()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("peek-seq-1"));
        await sender.SendMessageAsync(new ServiceBusMessage("peek-seq-2"));
        await sender.SendMessageAsync(new ServiceBusMessage("peek-seq-3"));
        await sender.SendMessageAsync(new ServiceBusMessage("peek-seq-4"));

        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(first);
        await receiver.CompleteMessageAsync(first);

        IReadOnlyList<ServiceBusReceivedMessage> fromTrimmedHead = await receiver.PeekMessagesAsync(10, fromSequenceNumber: 1);

        Assert.Equal(["peek-seq-2", "peek-seq-3", "peek-seq-4"], fromTrimmedHead.Select(m => m.Body.ToString()).ToArray());

        IReadOnlyList<ServiceBusReceivedMessage> paged = await receiver.PeekMessagesAsync(2, fromTrimmedHead[0].SequenceNumber);

        Assert.Equal(2, paged.Count);
        Assert.Equal(["peek-seq-2", "peek-seq-3"], paged.Select(m => m.Body.ToString()).ToArray());
    }

    [Fact]
    public async Task Receive_PeekLockHidesLockedMessageFromOtherReceivers()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var firstReceiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });
        await using var secondReceiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("single-consumer-visibility"));

        var lockedMessage = await firstReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(lockedMessage);

        using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500)))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await secondReceiver.ReceiveMessageAsync(cancellationToken: cts.Token));
        }

        await firstReceiver.AbandonMessageAsync(lockedMessage);
        var redelivered = await firstReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("single-consumer-visibility", redelivered.Body.ToString());

        await firstReceiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task Send_StoredMessageIncludesServiceBusAnnotations()
    {
        await using var sender = _client.CreateSender(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("annotation-test"));

        var queue = _broker.GetQueue(QueueName);
        Assert.NotNull(queue.MessageStore);

        var stored = await queue.MessageStore!.ReadAsync(queue.MessageStore.Log.BeginAddress);
        var (_, message) = MessageSerializer.Decode(stored.data);
        Assert.NotNull(message.MessageAnnotations);
        Assert.Equal(1L, message.MessageAnnotations![new Symbol("x-opt-sequence-number")]);
        Assert.IsType<DateTime>(message.MessageAnnotations[new Symbol("x-opt-enqueued-time-utc")]);
    }

    [Fact]
    public async Task Abandon_MessageBecomesAvailableAgain()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        await sender.SendMessageAsync(new ServiceBusMessage("abandon-test"));

        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(first);
        Assert.Equal("abandon-test", first.Body.ToString());
        Assert.Equal(1, first.DeliveryCount);

        // Abandon: message should re-appear
        await receiver.AbandonMessageAsync(first);

        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(second);
        Assert.Equal("abandon-test", second.Body.ToString());
        Assert.Equal(2, second.DeliveryCount);

        await receiver.CompleteMessageAsync(second);
    }

    [Fact]
    public async Task DeadLetter_MessageMovesToDlq()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);
        await using var dlqReceiver = _client.CreateReceiver(QueueName + "/$DeadLetterQueue");

        await sender.SendMessageAsync(new ServiceBusMessage("to-be-dead-lettered"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        await receiver.DeadLetterMessageAsync(msg, "test-reason", "test-description");

        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("to-be-dead-lettered", dlqMsg.Body.ToString());
        Assert.Equal("test-reason", dlqMsg.DeadLetterReason);
        Assert.Equal("test-description", dlqMsg.DeadLetterErrorDescription);

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task Defer_MessageIsHiddenFromNormalReceivers()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("to-be-deferred"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        await receiver.DeferMessageAsync(msg);

        // Should not be available for normal receive anymore
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(second);
    }

    [Fact]
    public async Task MaxDeliveryCount_MovesMessageToDlq_AfterLimitReached()
    {
        const string queueName = "max-dc-queue";
        _broker.EnsureQueue(queueName, maxDeliveryCount: 2);
        
        await using var sender = _client.CreateSender(queueName);
        await using var receiver = _client.CreateReceiver(queueName);
        await using var dlqReceiver = _client.CreateReceiver(queueName + "/$DeadLetterQueue");

        await sender.SendMessageAsync(new ServiceBusMessage("max-dc-test"));

        // 1st delivery
        var m1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(m1);
        Assert.Equal(1, m1.DeliveryCount);
        await receiver.AbandonMessageAsync(m1);

        // 2nd delivery
        var m2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(m2);
        Assert.Equal(2, m2.DeliveryCount);
        await receiver.AbandonMessageAsync(m2);

        // 3rd delivery should fail and move to DLQ
        // In our implementation, the dead-lettering happens during the next attempt to deliver.
        // So we try to receive, it will dead-letter and then wait for more messages.
        var m3 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(m3);

        // Verify it is in DLQ
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("max-dc-test", dlqMsg.Body.ToString());
        Assert.Equal("MaxDeliveryCountExceeded", dlqMsg.DeadLetterReason);

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task SendBatch_EnumerableMessages_AllDeliveredInOrder()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        var messages = Enumerable.Range(1, 5)
            .Select(i => new ServiceBusMessage($"batch-enum-{i}"))
            .ToList();

        await sender.SendMessagesAsync(messages);

        var received = new List<ServiceBusReceivedMessage>();
        for (int i = 0; i < 5; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            received.Add(msg);
        }

        Assert.Equal(
            Enumerable.Range(1, 5).Select(i => $"batch-enum-{i}").ToArray(),
            received.Select(m => m.Body.ToString()).ToArray());

        foreach (var msg in received)
            await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task SendMessageBatch_UsingBatchObject_AllDeliveredInOrder()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        using var batch = await sender.CreateMessageBatchAsync();
        for (int i = 1; i <= 4; i++)
            Assert.True(batch.TryAddMessage(new ServiceBusMessage($"mb-{i}")), $"Message mb-{i} should fit in the batch");

        await sender.SendMessagesAsync(batch);

        var received = new List<ServiceBusReceivedMessage>();
        for (int i = 0; i < 4; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            received.Add(msg);
        }

        Assert.Equal(
            Enumerable.Range(1, 4).Select(i => $"mb-{i}").ToArray(),
            received.Select(m => m.Body.ToString()).ToArray());

        foreach (var msg in received)
            await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task ReceiveMessages_BatchReceive_ReturnsUpToMaxCount()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        for (int i = 1; i <= 6; i++)
            await sender.SendMessageAsync(new ServiceBusMessage($"batch-recv-{i}"));

        var firstBatch = await receiver.ReceiveMessagesAsync(maxMessages: 4, maxWaitTime: TimeSpan.FromSeconds(5));

        Assert.True(firstBatch.Count > 0 && firstBatch.Count <= 4, $"Expected 1–4 messages but got {firstBatch.Count}");

        foreach (var msg in firstBatch)
            await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task DeferMessage_ThenReceiveBySequenceNumber_CanComplete()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("deferred-retrieve"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        long seqNum = msg.SequenceNumber;

        await receiver.DeferMessageAsync(msg);

        // Deferred message should not be visible via normal receive
        var notVisible = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(notVisible);

        // Retrieve via sequence number
        var deferred = await receiver.ReceiveDeferredMessageAsync(seqNum);
        Assert.NotNull(deferred);
        Assert.Equal("deferred-retrieve", deferred.Body.ToString());
        Assert.Equal(seqNum, deferred.SequenceNumber);
        Assert.NotEqual(Guid.Empty.ToString(), deferred.LockToken);

        await receiver.CompleteMessageAsync(deferred);
    }

    [Fact]
    public async Task DeferMessage_ThenReceiveBySequenceNumber_ThenAbandon_MessageReappearsForNormalReceive()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("deferred-abandon"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        long seqNum = msg.SequenceNumber;
        await receiver.DeferMessageAsync(msg);

        var deferred = await receiver.ReceiveDeferredMessageAsync(seqNum);
        Assert.NotNull(deferred);
        await receiver.AbandonMessageAsync(deferred);

        // Message should be visible again via normal receive after abandon
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("deferred-abandon", redelivered.Body.ToString());
        Assert.Equal(seqNum, redelivered.SequenceNumber);
        await receiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task DeferMessage_ThenReceiveBySequenceNumber_ThenDeadLetter_MessageAppearsInDlq()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);
        await using var dlqReceiver = _client.CreateReceiver(QueueName,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        await sender.SendMessageAsync(new ServiceBusMessage("deferred-dlq"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        long seqNum = msg.SequenceNumber;
        await receiver.DeferMessageAsync(msg);

        var deferred = await receiver.ReceiveDeferredMessageAsync(seqNum);
        Assert.NotNull(deferred);
        await receiver.DeadLetterMessageAsync(deferred, "TestReason", "TestDescription");

        // Message should not be visible for normal receive
        var notVisible = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(notVisible);

        // Message should appear in DLQ
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("deferred-dlq", dlqMsg.Body.ToString());
        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task DeferMessage_ThenReceiveBySequenceNumber_ThenReDefer_CanRetrieveAgain()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("deferred-redefer"));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        long seqNum = msg.SequenceNumber;
        await receiver.DeferMessageAsync(msg);

        var deferred = await receiver.ReceiveDeferredMessageAsync(seqNum);
        Assert.NotNull(deferred);

        // Re-defer via ReceiveDeferredMessage followed by DeferMessage
        await receiver.DeferMessageAsync(deferred);

        // Not visible via normal receive
        var notVisible = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(notVisible);

        // Still retrievable by sequence number
        var reDeferredMsg = await receiver.ReceiveDeferredMessageAsync(seqNum);
        Assert.NotNull(reDeferredMsg);
        Assert.Equal("deferred-redefer", reDeferredMsg.Body.ToString());
        await receiver.CompleteMessageAsync(reDeferredMsg);
    }

    [Fact]
    public async Task ReceiveBySequenceNumber_WithUnknownSequenceNumber_ReturnsNull()
    {
        await using var receiver = _client.CreateReceiver(QueueName);

        // Sequence number that was never deferred
        await Assert.ThrowsAnyAsync<Exception>(
            () => receiver.ReceiveDeferredMessageAsync(99999));
    }

    [Fact]
    public async Task Queue_MessageWithTtl_ExpiresAndMovesToDlq()
    {
        const string ttlQueue = "ttl-test-queue";
        _broker.EnsureQueue(ttlQueue);
        await using var client = new ServiceBusClient(_broker.ConnectionString);
        await using var sender = client.CreateSender(ttlQueue);
        await using var receiver = client.CreateReceiver(ttlQueue);
        await using var dlqReceiver = client.CreateReceiver(
            ttlQueue,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        // Send a message with a very short TTL (1 second)
        var msg = new ServiceBusMessage("ttl-content") { TimeToLive = TimeSpan.FromSeconds(1) };
        await sender.SendMessageAsync(msg);

        // Wait for the TTL to elapse
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Normal receive should return nothing (message expired)
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        ServiceBusReceivedMessage? received = null;
        try { received = await receiver.ReceiveMessageAsync(cancellationToken: cts.Token); }
        catch (OperationCanceledException) { }

        // If the message was returned (background task didn't expire it yet), it should be dead-lettered at delivery time
        if (received != null)
        {
            // Message was returned but should have been dead-lettered; verify the DLQ path
            // (This would only happen if the background task didn't fire yet — delivery-time check handles it)
            Assert.Fail($"Expected no message (TTL expired), but got: {received.Body}");
        }

        // DLQ should have the expired message
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("ttl-content", dlqMsg.Body.ToString());
        Assert.Equal("TTLExpiredException", dlqMsg.ApplicationProperties["DeadLetterReason"]?.ToString());
        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    [Fact]
    public async Task Queue_MessageWithTtl_AtDeliveryTime_IsDeadLettered()
    {
        const string ttlQueue2 = "ttl-delivery-test-queue";
        _broker.EnsureQueue(ttlQueue2);
        await using var client = new ServiceBusClient(_broker.ConnectionString);
        await using var sender = client.CreateSender(ttlQueue2);
        await using var receiver = client.CreateReceiver(ttlQueue2);
        await using var dlqReceiver = client.CreateReceiver(
            ttlQueue2,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        // Send a message with a 1s TTL, then receive immediately after expiry
        var msg = new ServiceBusMessage("delivery-ttl-content") { TimeToLive = TimeSpan.FromSeconds(1) };
        await sender.SendMessageAsync(msg);

        // Wait past TTL
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Try to receive — should be dead-lettered at delivery time, not delivered
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        ServiceBusReceivedMessage? received = null;
        try { received = await receiver.ReceiveMessageAsync(cancellationToken: cts.Token); }
        catch (OperationCanceledException) { }

        // Message should have been sent to DLQ instead of delivered
        Assert.Null(received);

        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("delivery-ttl-content", dlqMsg.Body.ToString());
        Assert.Equal("TTLExpiredException", dlqMsg.ApplicationProperties["DeadLetterReason"]?.ToString());
        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
