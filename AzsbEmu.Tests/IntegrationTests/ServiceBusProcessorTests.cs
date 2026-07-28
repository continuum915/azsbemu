using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Tests.IntegrationTests;

public class ServiceBusProcessorTests : IAsyncDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string QueueName = "processor-test-queue";
    private const string TopicName = "processor-test-topic";
    private const string SubName = "processor-sub";

    public ServiceBusProcessorTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureQueue(QueueName);
        _broker.EnsureTopic(TopicName);
        _broker.EnsureSubscription(TopicName, SubName);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task Processor_AutoComplete_ProcessesAllMessages()
    {
        await using var sender = _client.CreateSender(QueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("proc-1"));
        await sender.SendMessageAsync(new ServiceBusMessage("proc-2"));
        await sender.SendMessageAsync(new ServiceBusMessage("proc-3"));

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        await using var processor = _client.CreateProcessor(QueueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = true,
            MaxConcurrentCalls = 1
        });

        processor.ProcessMessageAsync += async args =>
        {
            received.Add(args.Message.Body.ToString());
            if (received.Count >= 3)
                allReceived.TrySetResult();
            await Task.CompletedTask;
        };

        processor.ProcessErrorAsync += args =>
        {
            allReceived.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully, "All 3 messages should have been processed without error");
        Assert.Equal(3, received.Count);
        Assert.Equal(["proc-1", "proc-2", "proc-3"], received.OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Processor_ManualComplete_ConsumesMessage()
    {
        await using var sender = _client.CreateSender(QueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("manual-complete-msg"));

        var tcs = new TaskCompletionSource();

        await using var processor = _client.CreateProcessor(QueueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        });

        processor.ProcessMessageAsync += async args =>
        {
            await args.CompleteMessageAsync(args.Message);
            tcs.TrySetResult();
        };

        processor.ProcessErrorAsync += args =>
        {
            tcs.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        await processor.StopProcessingAsync();

        Assert.True(tcs.Task.IsCompletedSuccessfully, "Handler should have completed the message without error");
    }

    [Fact]
    public async Task Processor_OnTopicSubscription_ReceivesMessages()
    {
        await using var sender = _client.CreateSender(TopicName);
        await sender.SendMessageAsync(new ServiceBusMessage("topic-proc-msg"));

        var tcs = new TaskCompletionSource<string>();

        await using var processor = _client.CreateProcessor(TopicName, SubName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = true,
            MaxConcurrentCalls = 1
        });

        processor.ProcessMessageAsync += async args =>
        {
            tcs.TrySetResult(args.Message.Body.ToString());
            await Task.CompletedTask;
        };

        processor.ProcessErrorAsync += args =>
        {
            tcs.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        await processor.StopProcessingAsync();

        Assert.True(tcs.Task.IsCompletedSuccessfully, "Topic subscription processor should have received the message");
        Assert.Equal("topic-proc-msg", await tcs.Task);
    }

    [Fact]
    public async Task Processor_ThrowInHandler_MessageIsAbandoned_AndRedelivered()
    {
        const string queueName = "processor-abandon-queue";
        _broker.EnsureQueue(queueName, maxDeliveryCount: 5);

        await using var sender = _client.CreateSender(queueName);
        await sender.SendMessageAsync(new ServiceBusMessage("abandon-via-throw"));

        var deliveryCounts = new ConcurrentBag<int>();
        var secondDelivery = new TaskCompletionSource();
        int callCount = 0;

        await using var processor = _client.CreateProcessor(queueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = true,
            MaxConcurrentCalls = 1
        });

        processor.ProcessMessageAsync += async args =>
        {
            deliveryCounts.Add(args.Message.DeliveryCount);
            if (Interlocked.Increment(ref callCount) == 1)
                throw new InvalidOperationException("simulated first-delivery failure");

            secondDelivery.TrySetResult();
            await Task.CompletedTask;
        };

        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();
        await Task.WhenAny(secondDelivery.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(secondDelivery.Task.IsCompletedSuccessfully, "Message should have been redelivered after first-delivery exception");
        Assert.Equal(2, deliveryCounts.Count);
        Assert.Contains(1, deliveryCounts);
        Assert.Contains(2, deliveryCounts);
    }

    [Fact]
    public async Task Processor_MultipleConcurrentCalls_AllMessagesProcessed()
    {
        await using var sender = _client.CreateSender(QueueName);
        for (int i = 1; i <= 10; i++)
            await sender.SendMessageAsync(new ServiceBusMessage($"concurrent-{i}"));

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        await using var processor = _client.CreateProcessor(QueueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = true,
            MaxConcurrentCalls = 4
        });

        processor.ProcessMessageAsync += async args =>
        {
            received.Add(args.Message.Body.ToString());
            if (received.Count >= 10)
                allReceived.TrySetResult();
            await Task.CompletedTask;
        };

        processor.ProcessErrorAsync += args =>
        {
            allReceived.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully, "All 10 messages should have been processed");
        Assert.Equal(10, received.Count);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        _broker.Dispose();
    }
}
