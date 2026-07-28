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
        string safeName = queueName.Replace("/", "_");
        string entityDir = Path.Combine(_broker.DataDir, "entities", safeName);
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
        string safeTopicName = topicName.Replace("/", "_");
        string entityDir = Path.Combine(_broker.DataDir, "entities", safeTopicName);
        Assert.False(Directory.Exists(entityDir));

        await session.CloseAsync();
        await connection.CloseAsync();
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
