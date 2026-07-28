using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;
using Xunit.Abstractions;

namespace AzsbEmu.Tests.IntegrationTests;

public class TransactionTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private readonly ITestOutputHelper _output;
    private const string QueueName = "txn-test-queue";

    public TransactionTests(ITestOutputHelper output)
    {
        _output = output;
        _broker = new BrokerFixture();
        _broker.EnsureQueue(QueueName);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task TransactionalSend_Commit_ShouldEnqueueMessages()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        // 1. Send messages inside transaction scope
        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("txn-msg-1"));
            await sender.SendMessageAsync(new ServiceBusMessage("txn-msg-2"));

            // 2. Receive should return null before commit
            var msgBeforeCommit = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
            Assert.Null(msgBeforeCommit);

            ts.Complete();
        }

        // 3. Receive after commit should return the messages in order
        var msg1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg1);
        Assert.Equal("txn-msg-1", msg1.Body.ToString());
        await receiver.CompleteMessageAsync(msg1);

        var msg2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg2);
        Assert.Equal("txn-msg-2", msg2.Body.ToString());
        await receiver.CompleteMessageAsync(msg2);
    }

    [Fact]
    public async Task TransactionalSend_Rollback_ShouldDiscardMessages()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        // 1. Send messages inside transaction scope and roll back (omit ts.Complete())
        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("txn-rollback-msg"));
            // Do not call ts.Complete() to simulate rollback
        }

        // 2. Receive should return null (rolled back)
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(msg);
    }

    [Fact]
    public async Task TransactionalSettlement_Commit_ShouldSettleMessage()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        // Send a message normally
        await sender.SendMessageAsync(new ServiceBusMessage("settle-commit-msg"));

        // Receive it to lock it
        var receivedMsg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(receivedMsg);

        // Complete message inside transaction scope
        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.CompleteMessageAsync(receivedMsg);
            ts.Complete();
        }

        // Try to receive again - should be null because it was committed and deleted
        var secondReceive = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(secondReceive);
    }

    [Fact]
    public async Task TransactionalSettlement_Rollback_ShouldKeepMessageLocked()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        // Send a message normally
        await sender.SendMessageAsync(new ServiceBusMessage("settle-rollback-msg"));

        // Receive it to lock it
        var receivedMsg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(receivedMsg);

        // Complete message inside transaction scope and roll back
        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.CompleteMessageAsync(receivedMsg);
            // Omit Complete to rollback
        }

        // Try to receive from a different receiver - should return null because message is still locked on original receiver
        await using var secondClient = new ServiceBusClient(_broker.ConnectionString);
        await using var secondReceiver = secondClient.CreateReceiver(QueueName);
        var secondReceive = await secondReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(secondReceive);

        // Settle outside transaction to clean up
        await receiver.CompleteMessageAsync(receivedMsg);
    }

    [Fact]
    public async Task Transaction_Timeout_ShouldAutoRollback()
    {
        // Expose shorter timeout (3 seconds) for test
        _broker.EntityManager.TransactionTimeout = TimeSpan.FromSeconds(3);

        await using var sender = _client.CreateSender(QueueName);

        // Discharge (commit) should fail because transaction timed out and was rolled back
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await sender.SendMessageAsync(new ServiceBusMessage("timeout-msg"));

                // Wait 5 seconds (expires the 3s timeout)
                await Task.Delay(TimeSpan.FromSeconds(5));

                ts.Complete();
            }
        });
        _output.WriteLine($"Expected exception thrown on timeout commit: {ex.Message}");
    }

    [Fact]
    public async Task Transaction_LimitCount_ShouldBeEnforced()
    {
        await using var sender = _client.CreateSender(QueueName);

        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            // Send 1000 messages (which is allowed)
            for (int i = 0; i < 1000; i++)
            {
                await sender.SendMessageAsync(new ServiceBusMessage($"msg-{i}"));
            }

            // The 1001st message should fail
            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await sender.SendMessageAsync(new ServiceBusMessage("msg-limit-exceeded"));
            });
            
            _output.WriteLine($"Expected exception on limit check: {exception.Message}");
        }
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
