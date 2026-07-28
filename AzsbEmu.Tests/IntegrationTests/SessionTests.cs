using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;
using Xunit;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Integration tests for Task 4.1 (Message Sessions), including send validation,
/// session locking, isolated FIFO delivery, session state durability, and restart recovery.
/// </summary>
[Collection("SharedBrokerCollection")]
public class SessionTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private readonly ServiceBusClient _client;
    private const string SessionQueueName = "session-test-queue";
    private const string NormalQueueName = "normal-test-queue";
    private const string TopicName = "session-test-topic";
    private const string SubName = "session-test-sub";

    public SessionTests(BrokerFixture broker)
    {
        _broker = broker;
        _broker.EnsureQueue(SessionQueueName, requiresSession: true);
        _broker.EnsureQueue(NormalQueueName, requiresSession: false);
        _broker.EnsureTopic(TopicName);
        _broker.EnsureSubscription(TopicName, SubName, requiresSession: true);
        _client = new ServiceBusClient(_broker.ConnectionString);
    }

    [Fact]
    public async Task Send_WithoutSessionId_ToSessionEnabledQueue_ShouldBeRejected()
    {
        await using var sender = _client.CreateSender(SessionQueueName);

        // Sending without a SessionId to a session-enabled queue should fail
        var message = new ServiceBusMessage("no-session");
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => sender.SendMessageAsync(message));
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task SendAndReceive_WithSessionId_ShouldSucceed()
    {
        await using var sender = _client.CreateSender(SessionQueueName);
        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, "session-1");

        // Send message with session ID
        var message = new ServiceBusMessage("hello-session")
        {
            SessionId = "session-1"
        };
        await sender.SendMessageAsync(message);

        // Receive message
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("hello-session", received.Body.ToString());
        Assert.Equal("session-1", received.SessionId);

        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task SessionState_SetAndGet_ShouldBeDurable()
    {
        string sessionId = "session-state-test";
        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);

        // Initially state is null
        var initialState = await receiver.GetSessionStateAsync();
        Assert.Null(initialState);

        // Set state
        byte[] expectedState = new byte[] { 1, 2, 3, 4, 5 };
        await receiver.SetSessionStateAsync(new BinaryData(expectedState));

        // Get state and verify
        var retrievedState = await receiver.GetSessionStateAsync();
        Assert.NotNull(retrievedState);
        Assert.Equal(expectedState, retrievedState.ToArray());

        // Restart broker
        var dataDir = _broker.TakeDataDir();
        _broker.Dispose();

        using var restartedBroker = new BrokerFixture(dataDir);
        var restartedClient = restartedBroker.CreateServiceBusClient();
        await using var restartedReceiver = await restartedClient.AcceptSessionAsync(SessionQueueName, sessionId);

        // Verify state is recovered after broker restart
        var recoveredState = await restartedReceiver.GetSessionStateAsync();
        Assert.NotNull(recoveredState);
        Assert.Equal(expectedState, recoveredState.ToArray());
    }

    [Fact]
    public async Task CompetingConsumers_ShouldReceiveIsolateSessions_StrictFIFO()
    {
        await using var sender = _client.CreateSender(SessionQueueName);

        // Send messages to session-A (A1, A2) and session-B (B1)
        await sender.SendMessageAsync(new ServiceBusMessage("A1") { SessionId = "session-A" });
        await sender.SendMessageAsync(new ServiceBusMessage("B1") { SessionId = "session-B" });
        await sender.SendMessageAsync(new ServiceBusMessage("A2") { SessionId = "session-A" });

        // Receiver A accepts session-A
        await using var receiverA = await _client.AcceptSessionAsync(SessionQueueName, "session-A");

        // Receiver B accepts session-B
        await using var receiverB = await _client.AcceptSessionAsync(SessionQueueName, "session-B");

        // Verify Receiver A only gets session-A messages in strict FIFO order
        var msgA1 = await receiverA.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msgA1);
        Assert.Equal("A1", msgA1.Body.ToString());
        await receiverA.CompleteMessageAsync(msgA1);

        var msgA2 = await receiverA.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msgA2);
        Assert.Equal("A2", msgA2.Body.ToString());
        await receiverA.CompleteMessageAsync(msgA2);

        // Verify Receiver B gets session-B message
        var msgB1 = await receiverB.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msgB1);
        Assert.Equal("B1", msgB1.Body.ToString());
        await receiverB.CompleteMessageAsync(msgB1);
    }

    [Fact]
    public async Task TopicSubscription_WithoutSessionId_ShouldDeadLetterToSubscription()
    {
        await using var sender = _client.CreateSender(TopicName);

        // Send a message without session ID to the topic
        // Subscription requires session, so it should dead-letter it
        var msg = new ServiceBusMessage("missing-session");
        await sender.SendMessageAsync(msg);

        // Wait a small moment for fanout
        await Task.Delay(200);

        // Create a receiver on the subscription's DLQ
        await using var dlqReceiver = _client.CreateReceiver(TopicName, SubName + "/$deadletterqueue");
        var received = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        
        Assert.NotNull(received);
        Assert.Equal("missing-session", received.Body.ToString());
        Assert.Equal("SessionIdIsNull", received.DeadLetterReason);

        await dlqReceiver.CompleteMessageAsync(received);
    }

    public void Dispose()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
