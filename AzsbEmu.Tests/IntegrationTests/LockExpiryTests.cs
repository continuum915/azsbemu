using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace AzsbEmu.Tests.IntegrationTests;

public class LockExpiryTests : IClassFixture<BrokerFixture>
{
    private readonly BrokerFixture _fixture;
    private readonly ITestOutputHelper _output;

    public LockExpiryTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task MessageLock_ShouldExpire_AndBeRedelivered()
    {
        string queueName = "lock-expiry-test-" + Guid.NewGuid();
        // Set lock duration to 2 seconds for quick testing
        _fixture.EnsureQueue(
queueName, lockDuration: TimeSpan.FromSeconds(2));

        var client = _fixture.CreateServiceBusClient();
        var sender = client.CreateSender(queueName);
        var receiver = client.CreateReceiver(queueName);

        string body = "Hello, Expiry!";
        await sender.SendMessageAsync(new ServiceBusMessage(body));

        // 1. Receive message
        var msg1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg1);
        Assert.Equal(body, msg1.Body.ToString());
        Assert.Equal(1, (int)msg1.DeliveryCount);

        // 2. Wait for lock to expire (duration is 2s)
        _output.WriteLine("Waiting for lock to expire...");
        await Task.Delay(TimeSpan.FromSeconds(4));

        // 3. Try to complete the expired lock - should fail (or at least not prevent redelivery)
        // Azure SDK might throw ServiceBusException if it detects expiry locally, but here we want to see server behavior.
        try
        {
            await receiver.CompleteMessageAsync(msg1);
            _output.WriteLine("CompleteMessageAsync succeeded unexpectedly on expired lock");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Expected failure on Complete: {ex.Message}");
        }

        // 4. Receive again - should get the same message
        var msg2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg2);
        Assert.Equal(body, msg2.Body.ToString());
        // Delivery count should be 2 now
        Assert.Equal(2, (int)msg2.DeliveryCount);

        await receiver.CompleteMessageAsync(msg2);
    }

    [Fact]
    public async Task Message_ShouldDeadLetter_AfterMaxLockExpiries()
    {
        string queueName = "lock-deadletter-test-" + Guid.NewGuid();
        // MaxDeliveryCount = 2, LockDuration = 2s
        _fixture.EnsureQueue(
queueName, lockDuration: TimeSpan.FromSeconds(2), maxDeliveryCount: 2);

        var client = _fixture.CreateServiceBusClient();
        var sender = client.CreateSender(queueName);
        var receiver = client.CreateReceiver(queueName);

        await sender.SendMessageAsync(new ServiceBusMessage("Die on expiry"));

        // 1. First delivery
        var msg1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg1);
        Assert.Equal(1, (int)msg1.DeliveryCount);

        // 2. Wait for first expiry
        _output.WriteLine("Waiting for first expiry...");
        await Task.Delay(TimeSpan.FromSeconds(4));

        // 3. Second delivery (DeliveryCount = 2, which is MaxDeliveryCount)
        var msg2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg2);
        Assert.Equal(2, (int)msg2.DeliveryCount);

        // 4. Wait for second expiry -> should dead-letter
        _output.WriteLine("Waiting for second expiry (should dead-letter)...");
        await Task.Delay(TimeSpan.FromSeconds(4));

        // 5. Try to receive from main queue - should be empty
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var msg3 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), cts.Token);
        Assert.Null(msg3);

        // 6. Check DLQ
        var dlqReceiver = client.CreateReceiver(queueName + "/$deadletterqueue");
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("LockLostException", dlqMsg.DeadLetterReason);
    }

    [Fact]
    public async Task MessageLock_ShouldBeRenewable()
    {
        string queueName = "lock-renewal-test-" + Guid.NewGuid();
        // Set lock duration to 5 seconds
        _fixture.EnsureQueue(queueName, lockDuration: TimeSpan.FromSeconds(5));

        var client = _fixture.CreateServiceBusClient();
        var sender = client.CreateSender(queueName);
        var receiver = client.CreateReceiver(queueName);

        string body = "Hello, Renew!";
        await sender.SendMessageAsync(new ServiceBusMessage(body));

        // 1. Receive message
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal(body, msg.Body.ToString());
        
        var originalLockedUntil = msg.LockedUntil;
        _output.WriteLine($"Received message. Lock expires at: {originalLockedUntil}");

        // 2. Wait 2 seconds, then renew lock
        await Task.Delay(TimeSpan.FromSeconds(2));
        _output.WriteLine("Renewing message lock...");
        await receiver.RenewMessageLockAsync(msg);
        
        var newLockedUntil = msg.LockedUntil;
        _output.WriteLine($"Lock renewed. New lock expires at: {newLockedUntil}");
        
        Assert.True(newLockedUntil > originalLockedUntil);

        // 3. Wait another 4 seconds (total 6 seconds since receive).
        // Without renewal, the 5-second lock would have expired.
        // With renewal, it should still be locked.
        await Task.Delay(TimeSpan.FromSeconds(4));

        // Try to receive from another receiver link - should receive null because it's still locked.
        var otherReceiver = client.CreateReceiver(queueName);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var msg2 = await otherReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), cts.Token);
        Assert.Null(msg2);

        // Complete the message using the original receiver
        await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task MessageLock_RenewalShouldFail_IfAlreadyExpired()
    {
        string queueName = "lock-renew-fail-test-" + Guid.NewGuid();
        // Set lock duration to 2 seconds
        _fixture.EnsureQueue(queueName, lockDuration: TimeSpan.FromSeconds(2));

        var client = _fixture.CreateServiceBusClient();
        var sender = client.CreateSender(queueName);
        var receiver = client.CreateReceiver(queueName);

        await sender.SendMessageAsync(new ServiceBusMessage("Too late to renew"));

        // Receive message
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        // Wait 4 seconds (lock duration is 2s, so lock expires)
        await Task.Delay(TimeSpan.FromSeconds(4));

        // Try to renew lock - should fail with ServiceBusException / MessageLockLostException
        var exception = await Assert.ThrowsAsync<ServiceBusException>(async () =>
        {
            await receiver.RenewMessageLockAsync(msg);
        });

        Assert.Equal(ServiceBusFailureReason.MessageLockLost, exception.Reason);
    }
}

