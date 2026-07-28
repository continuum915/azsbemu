using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;
using Azure.Messaging.ServiceBus;
using AzsbEmu.Server.Core;
using Xunit;

namespace AzsbEmu.Tests.IntegrationTests;

public class RuleManagementTests : IDisposable
{
    private readonly BrokerFixture _broker;
    private const string TopicName = "rule-test-topic";
    private const string SubName = "sub1";

    public RuleManagementTests()
    {
        _broker = new BrokerFixture();
        _broker.EnsureTopic(TopicName);
        _broker.EnsureSubscription(TopicName, SubName);
    }

    [Fact]
    public async Task RuleManagement_AddRemoveAndEnumerateRules_AMQPDirect()
    {
        var address = new Address($"amqp://localhost:{_broker.Port}");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);

        string mgmtPath = $"{TopicName}/subscriptions/{SubName}";

        // 1. Enumerate rules (should have only $Default rule)
        var enumReq1 = new Message
        {
            ApplicationProperties = new ApplicationProperties()
        };
        enumReq1.ApplicationProperties["operation"] = "com.microsoft:enumerate-rules";
        
        var resp1 = await SendManagementRequestAsync(session, mgmtPath, enumReq1);
        Assert.Equal(200, resp1.ApplicationProperties?["status-code"]);

        var bodyMap1 = Assert.IsType<Map>(resp1.Body);
        var rulesList1 = Assert.IsType<List>(bodyMap1["rules"]);
        Assert.Single(rulesList1); // Only $Default

        var ruleDesc1 = Assert.IsType<DescribedValue>(rulesList1[0]);
        Assert.Equal("com.microsoft:rule-description", (ruleDesc1.Descriptor as Symbol)?.ToString());
        var ruleListValues1 = Assert.IsType<List>(ruleDesc1.Value);
        Assert.Equal("$Default", ruleListValues1[2]); // RuleName is index 2

        var filterDesc1 = Assert.IsType<DescribedValue>(ruleListValues1[0]);
        Assert.Equal("com.microsoft:true-filter", (filterDesc1.Descriptor as Symbol)?.ToString());

        // 2. Add a CorrelationFilter rule
        var corrFilterList = new List();
        corrFilterList.Add("corr-id-123"); // CorrelationId (index 0)
        corrFilterList.Add(null);           // MessageId (index 1)
        corrFilterList.Add(null);           // To (index 2)
        corrFilterList.Add(null);           // ReplyTo (index 3)
        corrFilterList.Add("my-label");     // Label/Subject (index 4)
        corrFilterList.Add(null);           // SessionId (index 5)
        corrFilterList.Add(null);           // ReplyToSessionId (index 6)
        corrFilterList.Add(null);           // ContentType (index 7)
        
        var customProps = new Map();
        customProps["Color"] = "Green";
        corrFilterList.Add(customProps);    // Properties (index 8)

        var corrFilterVal = new DescribedValue(new Symbol("com.microsoft:correlation-filter"), corrFilterList);
        var addRuleList = new List
        {
            corrFilterVal, // Filter
            null,          // Action
            "MyCustomRule" // RuleName
        };
        var addRuleDesc = new DescribedValue(new Symbol("com.microsoft:rule-description"), addRuleList);

        var addReq = new Message(addRuleDesc)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        addReq.ApplicationProperties["operation"] = "com.microsoft:add-rule";

        var addResp = await SendManagementRequestAsync(session, mgmtPath, addReq);
        Assert.Equal(200, addResp.ApplicationProperties?["status-code"]);

        // 3. Enumerate rules again (should have both)
        var enumReq2 = new Message
        {
            ApplicationProperties = new ApplicationProperties()
        };
        enumReq2.ApplicationProperties["operation"] = "com.microsoft:enumerate-rules";

        var resp2 = await SendManagementRequestAsync(session, mgmtPath, enumReq2);
        Assert.Equal(200, resp2.ApplicationProperties?["status-code"]);

        var bodyMap2 = Assert.IsType<Map>(resp2.Body);
        var rulesList2 = Assert.IsType<List>(bodyMap2["rules"]);
        Assert.Equal(2, rulesList2.Count);

        var customRuleDesc = rulesList2.Cast<DescribedValue>().FirstOrDefault(r => 
        {
            var l = r.Value as List;
            return l != null && string.Equals(l[2] as string, "MyCustomRule");
        });
        Assert.NotNull(customRuleDesc);
        var customRuleValues = Assert.IsType<List>(customRuleDesc.Value);
        var customFilter = Assert.IsType<DescribedValue>(customRuleValues[0]);
        Assert.Equal("com.microsoft:correlation-filter", (customFilter.Descriptor as Symbol)?.ToString());
        
        var customFilterValues = Assert.IsType<List>(customFilter.Value);
        Assert.Equal("corr-id-123", customFilterValues[0]);
        Assert.Equal("my-label", customFilterValues[4]);
        var customPropsReceived = Assert.IsType<Map>(customFilterValues[8]);
        Assert.Equal("Green", customPropsReceived["Color"]);

        // 4. Remove $Default rule
        var removeReq = new Message("$Default")
        {
            ApplicationProperties = new ApplicationProperties()
        };
        removeReq.ApplicationProperties["operation"] = "com.microsoft:remove-rule";
        removeReq.ApplicationProperties["ruleName"] = "$Default"; // support passing both ways

        var removeResp = await SendManagementRequestAsync(session, mgmtPath, removeReq);
        Assert.Equal(200, removeResp.ApplicationProperties?["status-code"]);

        // 5. Enumerate rules again (should only have MyCustomRule)
        var enumReq3 = new Message
        {
            ApplicationProperties = new ApplicationProperties()
        };
        enumReq3.ApplicationProperties["operation"] = "com.microsoft:enumerate-rules";

        var resp3 = await SendManagementRequestAsync(session, mgmtPath, enumReq3);
        Assert.Equal(200, resp3.ApplicationProperties?["status-code"]);

        var bodyMap3 = Assert.IsType<Map>(resp3.Body);
        var rulesList3 = Assert.IsType<List>(bodyMap3["rules"]);
        Assert.Single(rulesList3);
        var finalRuleList = Assert.IsType<List>((rulesList3[0] as DescribedValue)?.Value);
        Assert.Equal("MyCustomRule", finalRuleList[2]);

        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task RuleManagement_CorrelationFilterRouting_Verification()
    {
        // 1. Manually add a rule via EntityManager to test routing directly
        var sub = _broker.GetSubscription(TopicName, SubName);
        
        var filterDto = new CorrelationFilterDto
        {
            CorrelationId = "test-corr-1",
            Properties = new Dictionary<string, object>
            {
                { "Color", "Blue" }
            }
        };

        var customRule = new RuleInfo
        {
            Name = "BlueCorrelationRule",
            FilterType = "Correlation",
            FilterExpression = System.Text.Json.JsonSerializer.Serialize(filterDto)
        };

        // Replace default rule with our correlation rule
        lock (sub.Rules)
        {
            sub.Rules.Clear();
            sub.Rules.Add(customRule);
        }

        // 2. Publish messages via ServiceBusClient
        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(TopicName);
        await using var receiver = client.CreateReceiver(TopicName, SubName);

        // Msg 1: Matching
        var m1 = new ServiceBusMessage("matching-msg")
        {
            CorrelationId = "test-corr-1"
        };
        m1.ApplicationProperties["Color"] = "Blue";

        // Msg 2: Non-matching correlation id
        var m2 = new ServiceBusMessage("wrong-corr-msg")
        {
            CorrelationId = "test-corr-2"
        };
        m2.ApplicationProperties["Color"] = "Blue";

        // Msg 3: Non-matching property
        var m3 = new ServiceBusMessage("wrong-prop-msg")
        {
            CorrelationId = "test-corr-1"
        };
        m3.ApplicationProperties["Color"] = "Red";

        await sender.SendMessageAsync(m1);
        await sender.SendMessageAsync(m2);
        await sender.SendMessageAsync(m3);

        // 3. Receive message and assert only Msg 1 is delivered
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("matching-msg", received.Body.ToString());
        Assert.Equal("test-corr-1", received.CorrelationId);
        Assert.Equal("Blue", received.ApplicationProperties["Color"]);
        await receiver.CompleteMessageAsync(received);

        // Assert no more messages are present on the subscription
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await receiver.ReceiveMessageAsync(cancellationToken: cts.Token));
    }

    [Fact]
    public async Task RuleManagement_AddSqlRule_AMQPDirect()
    {
        var address = new Address($"amqp://localhost:{_broker.Port}");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);

        string mgmtPath = $"{TopicName}/subscriptions/{SubName}";

        var sqlFilterVal = new DescribedValue(
            new Symbol("com.microsoft:sql-filter"),
            new List { "sys.Label = 'sql-subject' AND Color = 'Blue' AND Price >= 10", 20 });

        var addRuleDesc = new DescribedValue(
            new Symbol("com.microsoft:rule-description"),
            new List { sqlFilterVal, null, "SqlBlueRule" });

        var addReq = new Message(addRuleDesc)
        {
            ApplicationProperties = new ApplicationProperties()
        };
        addReq.ApplicationProperties["operation"] = "com.microsoft:add-rule";

        var addResp = await SendManagementRequestAsync(session, mgmtPath, addReq);
        Assert.Equal(200, addResp.ApplicationProperties?["status-code"]);

        var enumReq = new Message
        {
            ApplicationProperties = new ApplicationProperties()
        };
        enumReq.ApplicationProperties["operation"] = "com.microsoft:get-rules";

        var enumResp = await SendManagementRequestAsync(session, mgmtPath, enumReq);
        Assert.Equal(200, enumResp.ApplicationProperties?["status-code"]);

        var bodyMap = Assert.IsType<Map>(enumResp.Body);
        var rules = Assert.IsType<List>(bodyMap["rules"]);
        var sqlRuleDesc = rules.Cast<DescribedValue>().FirstOrDefault(r =>
        {
            var ruleList = r.Value as List;
            return string.Equals(ruleList?[2] as string, "SqlBlueRule", StringComparison.Ordinal);
        });

        Assert.NotNull(sqlRuleDesc);
        var sqlRuleValues = Assert.IsType<List>(sqlRuleDesc!.Value);
        var sqlRuleFilter = Assert.IsType<DescribedValue>(sqlRuleValues[0]);
        Assert.Equal("com.microsoft:sql-filter", (sqlRuleFilter.Descriptor as Symbol)?.ToString());
        var sqlFilterValues = Assert.IsType<List>(sqlRuleFilter.Value);
        Assert.Equal("sys.Label = 'sql-subject' AND Color = 'Blue' AND Price >= 10", sqlFilterValues[0]);

        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task RuntimeInfo_TopicAndSubscriptions_ReportActiveAndDeadLetterCounts_AMQPDirect()
    {
        const string secondSubName = "sub2";
        _broker.EnsureSubscription(TopicName, secondSubName);

        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(TopicName);
        await using var sub1Receiver = client.CreateReceiver(TopicName, SubName);

        await sender.SendMessageAsync(new ServiceBusMessage("runtime-info-message"));

        var sub1Message = await sub1Receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(sub1Message);
        await sub1Receiver.DeadLetterMessageAsync(sub1Message, "runtime-info", "move to dlq");

        var address = new Address($"amqp://localhost:{_broker.Port}");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);

        var topicResponse = await SendManagementRequestAsync(session, TopicName, CreateRuntimeInfoRequest());
        Assert.Equal(200, topicResponse.ApplicationProperties?["status-code"]);
        var topicBody = Assert.IsType<Map>(topicResponse.Body);
        Assert.Equal(1L, topicBody["MessageCount"]);
        Assert.Equal(1L, topicBody["ActiveMessageCount"]);
        Assert.Equal(1L, topicBody["DeadLetterMessageCount"]);
        Assert.Equal(2L, topicBody["TotalMessageCount"]);
        Assert.Equal(2, topicBody["SubscriptionCount"]);
        Assert.Equal(0L, topicBody["ScheduledMessageCount"]);
        Assert.Equal(0L, topicBody["TransferMessageCount"]);

        var sub1Response = await SendManagementRequestAsync(session, $"{TopicName}/subscriptions/{SubName}", CreateRuntimeInfoRequest());
        Assert.Equal(200, sub1Response.ApplicationProperties?["status-code"]);
        var sub1Body = Assert.IsType<Map>(sub1Response.Body);
        Assert.Equal(0L, sub1Body["MessageCount"]);
        Assert.Equal(0L, sub1Body["ActiveMessageCount"]);
        Assert.Equal(1L, sub1Body["DeadLetterMessageCount"]);
        Assert.Equal(1L, sub1Body["TotalMessageCount"]);
        Assert.Equal(0L, sub1Body["ScheduledMessageCount"]);
        Assert.Equal(0L, sub1Body["TransferMessageCount"]);

        var sub2Response = await SendManagementRequestAsync(session, $"{TopicName}/subscriptions/{secondSubName}", CreateRuntimeInfoRequest());
        Assert.Equal(200, sub2Response.ApplicationProperties?["status-code"]);
        var sub2Body = Assert.IsType<Map>(sub2Response.Body);
        Assert.Equal(1L, sub2Body["MessageCount"]);
        Assert.Equal(1L, sub2Body["ActiveMessageCount"]);
        Assert.Equal(0L, sub2Body["DeadLetterMessageCount"]);
        Assert.Equal(1L, sub2Body["TotalMessageCount"]);
        Assert.Equal(0L, sub2Body["ScheduledMessageCount"]);
        Assert.Equal(0L, sub2Body["TransferMessageCount"]);

        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task RuntimeInfo_QueueReportsDeadLetterCounts_AMQPDirect()
    {
        const string queueName = "runtime-info-queue";
        _broker.EnsureQueue(queueName);

        await using var client = _broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(queueName);
        await using var receiver = client.CreateReceiver(queueName);

        await sender.SendMessageAsync(new ServiceBusMessage("queue-runtime-info"));

        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(message);
        await receiver.DeadLetterMessageAsync(message, "runtime-info", "queue dlq");

        var address = new Address($"amqp://localhost:{_broker.Port}");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = new ClientMssbcbsSaslProfile();
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);

        var queueResponse = await SendManagementRequestAsync(session, queueName, CreateRuntimeInfoRequest());
        Assert.Equal(200, queueResponse.ApplicationProperties?["status-code"]);
        var queueBody = Assert.IsType<Map>(queueResponse.Body);
        Assert.Equal(0L, queueBody["MessageCount"]);
        Assert.Equal(0L, queueBody["ActiveMessageCount"]);
        Assert.Equal(1L, queueBody["DeadLetterMessageCount"]);
        Assert.Equal(1L, queueBody["TotalMessageCount"]);
        Assert.Equal(0L, queueBody["ScheduledMessageCount"]);
        Assert.Equal(0L, queueBody["TransferMessageCount"]);

        await session.CloseAsync();
        await connection.CloseAsync();
    }

    private static Message CreateRuntimeInfoRequest()
    {
        return new Message
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["operation"] = "com.microsoft:get-runtime-info"
            }
        };
    }

    private async Task<Message> SendManagementRequestAsync(Session session, string path, Message request)
    {
        string replyTo = "client-temp-address";
        
        var receiver = new ReceiverLink(session, "mgmt-receiver", new Source { Address = replyTo }, null);
        receiver.SetCredit(1, true);
        var sender = new SenderLink(session, "mgmt-sender", $"{path}/$management");

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
