using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace AzsbEmu.Tests.IntegrationTests;

public class ScheduledMessageTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string QueueName = "scheduled-test-queue";
    private const string TopicName = "scheduled-test-topic";
    private const string SubscriptionName = "sub1";

    public ScheduledMessageTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureQueue(QueueName);
        _broker.EnsureTopic(TopicName);
        _broker.EnsureSubscription(TopicName, SubscriptionName);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task ScheduleAndCancel_Queue_Works()
    {
        await using var sender = _client.CreateSender(QueueName);
        var message = new ServiceBusMessage("cancel-me-payload");
        
        // Schedule 10 seconds in the future
        DateTimeOffset scheduledTime = DateTimeOffset.UtcNow.AddSeconds(10);
        long sequenceNumber = await sender.ScheduleMessageAsync(message, scheduledTime);
        
        // Verify ScheduledMessageCount is updated in database
        Assert.Equal(1, _broker.GetScheduledMessageCount(QueueName));

        // Cancel it
        await sender.CancelScheduledMessageAsync(sequenceNumber);
        
        // Verify ScheduledMessageCount drops to 0
        Assert.Equal(0, _broker.GetScheduledMessageCount(QueueName));

        // Try to receive (should timeout and return null since it's canceled)
        await using var receiver = _client.CreateReceiver(QueueName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(received);
    }

    [Fact]
    public async Task ScheduleAndReceive_Queue_Works()
    {
        await using var sender = _client.CreateSender(QueueName);
        var message = new ServiceBusMessage("receive-me-queue-payload");
        
        // Schedule 2 seconds in the future
        DateTimeOffset scheduledTime = DateTimeOffset.UtcNow.AddSeconds(2);
        long sequenceNumber = await sender.ScheduleMessageAsync(message, scheduledTime);
        
        // Verify it was scheduled
        Assert.Equal(1, _broker.GetScheduledMessageCount(QueueName));

        // Wait for it to become due (3 seconds to be safe)
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Receive the message
        await using var receiver = _client.CreateReceiver(QueueName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("receive-me-queue-payload", received.Body.ToString());
        Assert.Equal(sequenceNumber, received.SequenceNumber);

        await receiver.CompleteMessageAsync(received);

        // Verify counts drop to 0
        Assert.Equal(0, _broker.GetScheduledMessageCount(QueueName));
        Assert.Equal(0, _broker.GetQueue(QueueName).MessageCount);
    }

    [Fact]
    public async Task ScheduleAndReceive_Topic_Works()
    {
        await using var sender = _client.CreateSender(TopicName);
        var message = new ServiceBusMessage("receive-me-topic-payload");
        
        // Schedule 2 seconds in the future
        DateTimeOffset scheduledTime = DateTimeOffset.UtcNow.AddSeconds(2);
        long sequenceNumber = await sender.ScheduleMessageAsync(message, scheduledTime);

        // Verify topic reports scheduled message count in database
        Assert.Equal(1, _broker.GetScheduledMessageCount(TopicName));

        // Wait for it to become due
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Receive from subscription
        await using var receiver = _client.CreateReceiver(TopicName, SubscriptionName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("receive-me-topic-payload", received.Body.ToString());

        await receiver.CompleteMessageAsync(received);

        // Verify count is 0
        Assert.Equal(0, _broker.GetScheduledMessageCount(TopicName));
    }

    [Fact]
    public async Task SendScheduled_StandardSend_Works()
    {
        await using var sender = _client.CreateSender(QueueName);
        var message = new ServiceBusMessage("standard-send-scheduled")
        {
            ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(2)
        };
        
        // Send normally
        await sender.SendMessageAsync(message);

        // Verify count
        Assert.Equal(1, _broker.GetScheduledMessageCount(QueueName));

        // Wait for it to become due
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Receive
        await using var receiver = _client.CreateReceiver(QueueName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("standard-send-scheduled", received.Body.ToString());

        await receiver.CompleteMessageAsync(received);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.Dispose();
    }
}
