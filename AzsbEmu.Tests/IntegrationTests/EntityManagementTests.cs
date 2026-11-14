using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;
using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;
using Xunit;

namespace AzsbEmu.Tests.IntegrationTests;

public class EntityManagementTests : IDisposable
{
    private readonly BrokerFixture _broker;

    public EntityManagementTests()
    {
        _broker = new BrokerFixture();
    }

    [Fact]
    public async Task EntityManagement_Queue_CreateAndDelete_Operational()
    {
        var address = new Address($"amqp://localhost:{_broker.Port}");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);

        const string queueName = "dynamic-test-queue";

        // 1. Create the Queue via global $management
        var createBody = new Map
        {
            ["entity-name"] = queueName,
            ["requires-session"] = false,
            ["lock-duration"] = 45L, // 45 seconds
            ["max-delivery-count"] = 5,
            ["max-size-in-megabytes"] = 2048L
        };

        var createReq = new Message(createBody)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        createReq.ApplicationProperties["operation"] = "com.microsoft:create-queue";

        var createResp = await SendManagementRequestAsync(session, createReq);
        Assert.Equal(201, createResp.ApplicationProperties?["status-code"]);

        var respMap = Assert.IsType<Map>(createResp.Body);
        Assert.Equal(queueName, respMap["name"]);
        Assert.False((bool)respMap["requires-session"]);
        Assert.Equal(45L, respMap["lock-duration"]);
        Assert.Equal(5, respMap["max-delivery-count"]);

        // Verify it was registered in EntityManager
        var queueInfo = _broker.GetQueue(queueName);
        Assert.NotNull(queueInfo);
        Assert.False(queueInfo.RequiresSession);
        Assert.Equal(TimeSpan.FromSeconds(45), queueInfo.LockDuration);
        Assert.Equal(5, queueInfo.MaxDeliveryCount);

        // 2. Perform message operations
        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(queueName);
        
        await sender.SendMessageAsync(new ServiceBusMessage("dynamic-message"));

        await using var receiver = client.CreateReceiver(queueName);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("dynamic-message", msg.Body.ToString());
        await receiver.CompleteMessageAsync(msg);

        // 3. Delete the queue via global $management
        var deleteBody = new Map
        {
            ["entity-name"] = queueName
        };
        var deleteReq = new Message(deleteBody)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        deleteReq.ApplicationProperties["operation"] = "com.microsoft:delete-queue";

        var deleteResp = await SendManagementRequestAsync(session, deleteReq);
        Assert.Equal(200, deleteResp.ApplicationProperties?["status-code"]);

        // Verify it was removed from EntityManager
        Assert.Throws<InvalidOperationException>(() => _broker.GetQueue(queueName));

        // Verify physical directories are purged
        string entityDir = Path.Combine(_broker.DataDir, "entities", EntityManager.EncodePathSegment(queueName));
        Assert.False(Directory.Exists(entityDir));

        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task EntityManagement_TopicAndSubscription_CreateAndDelete_Operational()
    {
        var address = new Address($"amqp://localhost:{_broker.Port}");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);

        const string topicName = "dynamic-topic";
        const string subName = "dynamic-sub";

        // 1. Create Topic via global $management
        var createTopicBody = new Map
        {
            ["entity-name"] = topicName
        };
        var createTopicReq = new Message(createTopicBody)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        createTopicReq.ApplicationProperties["operation"] = "com.microsoft:create-topic";

        var createTopicResp = await SendManagementRequestAsync(session, createTopicReq);
        Assert.Equal(201, createTopicResp.ApplicationProperties?["status-code"]);

        // 2. Create Subscription under the topic
        var createSubBody = new Map
        {
            ["topic-name"] = topicName,
            ["subscription-name"] = subName,
            ["lock-duration"] = 30L,
            ["max-delivery-count"] = 3
        };
        var createSubReq = new Message(createSubBody)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        createSubReq.ApplicationProperties["operation"] = "com.microsoft:create-subscription";

        var createSubResp = await SendManagementRequestAsync(session, createSubReq);
        Assert.Equal(201, createSubResp.ApplicationProperties?["status-code"]);

        var subMap = Assert.IsType<Map>(createSubResp.Body);
        Assert.Equal(topicName, subMap["topic-name"]);
        Assert.Equal(subName, subMap["subscription-name"]);
        Assert.Equal(30L, subMap["lock-duration"]);
        Assert.Equal(3, subMap["max-delivery-count"]);

        // Verify registered
        var subInfo = _broker.GetSubscription(topicName, subName);
        Assert.NotNull(subInfo);
        Assert.Equal(TimeSpan.FromSeconds(30), subInfo.LockDuration);
        Assert.Equal(3, subInfo.MaxDeliveryCount);

        // Verify default rule $Default is added
        Assert.Single(subInfo.Rules);
        Assert.Equal("$Default", subInfo.Rules[0].Name);

        // 3. Send and receive via Topic/Subscription
        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(topicName);
        await using var receiver = client.CreateReceiver(topicName, subName);

        await sender.SendMessageAsync(new ServiceBusMessage("topic-message"));
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("topic-message", msg.Body.ToString());
        await receiver.CompleteMessageAsync(msg);

        // 4. Delete topic (should cascade and clean up subscription)
        var deleteTopicBody = new Map
        {
            ["entity-name"] = topicName
        };
        var deleteTopicReq = new Message(deleteTopicBody)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        deleteTopicReq.ApplicationProperties["operation"] = "com.microsoft:delete-topic";

        var deleteTopicResp = await SendManagementRequestAsync(session, deleteTopicReq);
        Assert.Equal(200, deleteTopicResp.ApplicationProperties?["status-code"]);

        // Verify topic and subscription are gone from EntityManager
        Assert.Throws<InvalidOperationException>(() => _broker.GetSubscription(topicName, subName));

        // Verify physical directories are purged
        string entityDir = Path.Combine(_broker.DataDir, "entities", EntityManager.EncodePathSegment(topicName));
        Assert.False(Directory.Exists(entityDir));

        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task EntityManagement_DeadLetteringOnMessageExpiration_IsHonoured_ForQueuesAndSubscriptions()
    {
        // Ticket 17: the $management create operations used to ignore this flag (the HTTP API honoured it).
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(new Address($"amqp://localhost:{_broker.Port}"));
        var session = new Session(connection);
        _broker.EnsureTopic("expiry-topic");

        async Task<Map> CreateAsync(string operation, Map body)
        {
            var request = new Message(body) { ApplicationProperties = new ApplicationProperties() };
            request.ApplicationProperties["operation"] = operation;
            var response = await SendManagementRequestAsync(session, request);
            Assert.Equal(201, response.ApplicationProperties?["status-code"]);
            return Assert.IsType<Map>(response.Body);
        }

        var queueResponse = await CreateAsync("com.microsoft:create-queue",
            new Map { ["entity-name"] = "expiry-dlq-queue", ["dead-lettering-on-message-expiration"] = true });
        var subResponse = await CreateAsync("com.microsoft:create-subscription",
            new Map { ["topic-name"] = "expiry-topic", ["subscription-name"] = "expiry-dlq-sub", ["dead-lettering-on-message-expiration"] = true });
        Assert.Equal(true, queueResponse["dead-lettering-on-message-expiration"]);
        Assert.Equal(true, subResponse["dead-lettering-on-message-expiration"]);
        Assert.True(_broker.GetQueue("expiry-dlq-queue").DeadLetteringOnMessageExpiration);
        Assert.True(_broker.GetSubscription("expiry-topic", "expiry-dlq-sub").DeadLetteringOnMessageExpiration);

        await session.CloseAsync();
        await connection.CloseAsync();

        // Expired messages are dead-lettered rather than discarded.
        await using var client = _broker.CreateServiceBusClient();
        await using (var queueSender = client.CreateSender("expiry-dlq-queue"))
            await queueSender.SendMessageAsync(new ServiceBusMessage("expires") { TimeToLive = TimeSpan.FromSeconds(1) });
        await using (var topicSender = client.CreateSender("expiry-topic"))
            await topicSender.SendMessageAsync(new ServiceBusMessage("expires") { TimeToLive = TimeSpan.FromSeconds(1) });

        var deadline = DateTime.UtcNow.AddSeconds(15); // the TTL scan runs every 5 seconds
        while (DateTime.UtcNow < deadline
               && (_broker.GetQueue("expiry-dlq-queue").DeadLetterMessageCount == 0
                   || _broker.GetSubscription("expiry-topic", "expiry-dlq-sub").DeadLetterMessageCount == 0))
            await Task.Delay(200);

        Assert.Equal(1, _broker.GetQueue("expiry-dlq-queue").DeadLetterMessageCount);
        Assert.Equal(1, _broker.GetSubscription("expiry-topic", "expiry-dlq-sub").DeadLetterMessageCount);
    }

    private async Task<Message> SendManagementRequestAsync(Session session, Message request)
    {
        string replyTo = "client-temp-address";
        
        var receiver = new ReceiverLink(session, "mgmt-receiver", new Source { Address = replyTo }, null);
        receiver.SetCredit(1, true);
        var sender = new SenderLink(session, "mgmt-sender", "$management");

        request.Properties ??= new Properties();
        request.Properties.ReplyTo = replyTo;
        request.Properties.MessageId = Guid.NewGuid().ToString();

        await sender.SendAsync(request);
        
        var response = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(response);
        receiver.Accept(response);

        await sender.CloseAsync();
        await receiver.CloseAsync();

        return response;
    }

    public void Dispose()
    {
        _broker.Dispose();
    }

    private class ClientMssbcbsSaslProfile : SaslProfile
    {
        public ClientMssbcbsSaslProfile() : base("MSSBCBS")
        {
        }

        protected override ITransport UpgradeTransport(ITransport transport)
        {
            return transport;
        }

        protected override DescribedList GetStartCommand(string hostname)
        {
            return new SaslInit
            {
                Mechanism = this.Mechanism,
                InitialResponse = Array.Empty<byte>()
            };
        }

#pragma warning disable CS8603
        protected override DescribedList OnCommand(DescribedList command)
        {
            return null;
        }
#pragma warning restore CS8603
    }
}
