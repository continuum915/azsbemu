using System.Collections.Concurrent;
using AzsbEmu.Interop.Contracts;
using MassTransit;

namespace AzsbEmu.Interop.Mt7Tests;

/// <summary>
/// A MassTransit 7.3 bus and a MassTransit 8 bus sharing the emulator: every message path in both directions, with a
/// payload chosen to expose differences between Newtonsoft.Json (MassTransit 7) and System.Text.Json (MassTransit 8).
/// </summary>
[Collection(InteropCollection.Name)]
public class Mt7Mt8InteropTests(InteropFixture fixture)
{
    private static string NewKind() => Guid.NewGuid().ToString("N");

    private Task SendToPeerAsync(PeerInstruction instruction) =>
        fixture.Bus.GetSendEndpoint(new Uri($"queue:{InteropFixture.Mt8Endpoint}")).ContinueWith(t => t.Result.Send(instruction)).Unwrap();

    [Fact]
    public async Task Mt7Publish_IsConsumedByMt8_WithThePayloadIntact()
    {
        string kind = NewKind();
        var payload = Payload.Sample("mt7 publish");
        await fixture.Bus.Publish(new InteropEvent { Kind = kind, Origin = "mt7", Payload = payload });

        var seen = await fixture.WaitForAsync(o => o.Side == "mt8" && o.Via == "publish" && o.Kind == kind, "mt8 to consume mt7's event");
        PayloadAssert.Same(payload, seen.Payload);
    }

    [Fact]
    public async Task Mt8Publish_IsConsumedByMt7_WithThePayloadIntact()
    {
        string kind = NewKind();
        var payload = Payload.Sample("mt8 publish");
        await SendToPeerAsync(new PeerInstruction { Kind = kind, Action = "publish", Payload = payload });

        var seen = await fixture.WaitForAsync(o => o.Side == "mt7" && o.Via == "publish" && o.Kind == kind, "mt7 to consume mt8's event");
        PayloadAssert.Same(payload, seen.Payload);
    }

    [Fact]
    public async Task Mt7Send_ToMt8sQueue_IsConsumed_WithThePayloadIntact()
    {
        string kind = NewKind();
        var payload = Payload.Sample("mt7 send");
        var endpoint = await fixture.Bus.GetSendEndpoint(new Uri($"queue:{InteropFixture.Mt8Endpoint}"));
        await endpoint.Send(new InteropCommand { Kind = kind, Origin = "mt7", Payload = payload });

        var seen = await fixture.WaitForAsync(o => o.Side == "mt8" && o.Via == "send" && o.Kind == kind, "mt8 to consume mt7's command");
        PayloadAssert.Same(payload, seen.Payload);
    }

    [Fact]
    public async Task Mt8Send_ToMt7sQueue_IsConsumed_WithThePayloadIntact()
    {
        string kind = NewKind();
        var payload = Payload.Sample("mt8 send");
        await SendToPeerAsync(new PeerInstruction { Kind = kind, Action = "send", Payload = payload });

        var seen = await fixture.WaitForAsync(o => o.Side == "mt7" && o.Via == "send" && o.Kind == kind, "mt7 to consume mt8's command");
        PayloadAssert.Same(payload, seen.Payload);
    }

    [Fact]
    public async Task Mt7Request_IsAnsweredByMt8()
    {
        string kind = NewKind();
        var payload = Payload.Sample("mt7 request");
        var client = fixture.Bus.CreateRequestClient<EchoRequest>(new Uri($"queue:{InteropFixture.Mt8Endpoint}"), RequestTimeout.After(s: 20));

        var response = await client.GetResponse<EchoResponse>(new EchoRequest { Kind = kind, Payload = payload });

        Assert.Equal("mt8", response.Message.RespondedBy);
        Assert.Equal(kind, response.Message.Kind);
        PayloadAssert.Same(payload, response.Message.Payload);
    }

    [Fact]
    public async Task Mt8Request_IsAnsweredByMt7()
    {
        string kind = NewKind();
        var payload = Payload.Sample("mt8 request");
        await SendToPeerAsync(new PeerInstruction { Kind = kind, Action = "request", Payload = payload });

        var seen = await fixture.WaitForAsync(o => o.Side == "mt8" && o.Via == "response" && o.Kind == kind, "mt8 to get mt7's response", seconds: 30);
        Assert.Equal("mt7", seen.Detail);
        PayloadAssert.Same(payload, seen.Payload);
    }

    [Fact]
    public async Task AnMt8ConsumerFault_GoesToItsErrorQueue_AndMt7ConsumesTheFaultEvent()
    {
        string kind = NewKind();
        var endpoint = await fixture.Bus.GetSendEndpoint(new Uri($"queue:{InteropFixture.Mt8Endpoint}"));
        await endpoint.Send(new Poison { Kind = kind });

        var fault = await fixture.WaitForAsync(o => o.Side == "mt7" && o.Via == "fault" && o.Kind == kind, "mt7 to consume mt8's Fault<Poison>");
        Assert.Contains($"mt8 refuses {kind}", fault.Detail);

        var moved = await ErrorQueue.ReceiveAsync(fixture, $"{InteropFixture.Mt8Endpoint}_error", kind);
        Assert.Equal($"mt8 refuses {kind}", moved.ApplicationProperties["MT-Fault-Message"]);
    }

    [Fact]
    public async Task AnMt7ConsumerFault_GoesToItsErrorQueue_AndMt8ConsumesTheFaultEvent()
    {
        string kind = NewKind();
        await SendToPeerAsync(new PeerInstruction { Kind = kind, Action = "poison" });

        var fault = await fixture.WaitForAsync(o => o.Side == "mt8" && o.Via == "fault" && o.Kind == kind, "mt8 to consume mt7's Fault<Poison>");
        Assert.Contains($"mt7 refuses {kind}", fault.Detail);

        var moved = await ErrorQueue.ReceiveAsync(fixture, $"{InteropFixture.Mt7Endpoint}_error", kind);
        Assert.Equal($"mt7 refuses {kind}", moved.ApplicationProperties["MT-Fault-Message"]);
    }

    [Fact]
    public async Task BothVersionsOnOneEndpoint_ShareOneSubscription_AndEachMessageIsConsumedOnce()
    {
        // The peer (MassTransit 8) already deployed shared-endpoint; a MassTransit 7.3 bus now starts the same endpoint.
        var mt7Received = new ConcurrentQueue<SharedEvent>();
        var bus = fixture.CreateBus(cfg =>
            cfg.ReceiveEndpoint(InteropFixture.SharedEndpoint, e => e.Consumer(() => new SharedEventConsumer(mt7Received))));
        await bus.StartAsync(InteropFixture.Timeout(60));
        try
        {
            string kind = NewKind();
            const int count = 20;
            for (int i = 0; i < count; i++) await bus.Publish(new SharedEvent { Kind = kind, Index = i });

            var deadline = DateTime.UtcNow.AddSeconds(20);
            int Total() => mt7Received.Count(m => m.Kind == kind) + fixture.Observations.Count(o => o.Via == "shared" && o.Kind == kind);
            while (Total() < count && DateTime.UtcNow < deadline) await Task.Delay(100);
            await Task.Delay(1000); // let any duplicates arrive

            var indexes = mt7Received.Where(m => m.Kind == kind).Select(m => m.Index)
                .Concat(fixture.Observations.Where(o => o.Via == "shared" && o.Kind == kind).Select(o => int.Parse(o.Detail!)))
                .OrderBy(i => i).ToList();
            Assert.Equal(Enumerable.Range(0, count), indexes);

            var subscriptions = fixture.Broker.EntityManager.GetSubscriptions(TopicName<SharedEvent>());
            Assert.NotNull(subscriptions);
            Assert.Single(subscriptions);
        }
        finally
        {
            await bus.StopAsync(InteropFixture.Timeout(30));
        }
    }

    [Fact]
    public void BothVersions_CreateEndpointQueuesAndSubscriptionsWithTheSameSettings()
    {
        var entities = fixture.Broker.EntityManager;
        var mt7Queue = fixture.Broker.GetQueue(InteropFixture.Mt7Endpoint);
        var mt8Queue = fixture.Broker.GetQueue(InteropFixture.Mt8Endpoint);
        var mt7Sub = Assert.Single(entities.GetSubscriptions(TopicName<EchoRequest>()) ?? [], s => s.ForwardTo?.EndsWith(InteropFixture.Mt7Endpoint) == true);
        var mt8Sub = Assert.Single(entities.GetSubscriptions(TopicName<EchoRequest>()) ?? [], s => s.ForwardTo?.EndsWith(InteropFixture.Mt8Endpoint) == true);

        var differences = new List<string>();
        void Compare<T>(string what, T mt7, T mt8)
        {
            if (!EqualityComparer<T>.Default.Equals(mt7, mt8)) differences.Add($"{what}: mt7={mt7}, mt8={mt8}");
        }
        Compare("queue LockDuration", mt7Queue.LockDuration, mt8Queue.LockDuration);
        Compare("queue MaxDeliveryCount", mt7Queue.MaxDeliveryCount, mt8Queue.MaxDeliveryCount);
        Compare("queue AutoDeleteOnIdle", mt7Queue.AutoDeleteOnIdle, mt8Queue.AutoDeleteOnIdle);
        Compare("queue DefaultMessageTimeToLive", mt7Queue.DefaultMessageTimeToLive, mt8Queue.DefaultMessageTimeToLive);
        Compare("queue DeadLetteringOnMessageExpiration", mt7Queue.DeadLetteringOnMessageExpiration, mt8Queue.DeadLetteringOnMessageExpiration);
        Compare("queue RequiresDuplicateDetection", mt7Queue.RequiresDuplicateDetection, mt8Queue.RequiresDuplicateDetection);
        Compare("queue EnableBatchedOperations", mt7Queue.EnableBatchedOperations, mt8Queue.EnableBatchedOperations);
        Compare("queue MaxSizeInMegabytes", mt7Queue.MaxSizeInMegabytes, mt8Queue.MaxSizeInMegabytes);
        Compare("subscription LockDuration", mt7Sub.LockDuration, mt8Sub.LockDuration);
        Compare("subscription MaxDeliveryCount", mt7Sub.MaxDeliveryCount, mt8Sub.MaxDeliveryCount);
        // Known difference: MassTransit 7.3 leaves forwarding subscriptions at the Service Bus default (never), while
        // MassTransit 8 gives them 427 days, as it does its queues. Whichever version creates a subscription first
        // decides, and neither rewrites an existing one, so mixed fleets just keep the first one's value.
        Assert.Equal(TimeSpan.MaxValue, mt7Sub.AutoDeleteOnIdle);
        Assert.Equal(TimeSpan.FromDays(427), mt8Sub.AutoDeleteOnIdle);
        Compare("subscription DefaultMessageTimeToLive", mt7Sub.DefaultMessageTimeToLive, mt8Sub.DefaultMessageTimeToLive);
        Compare("subscription EnableBatchedOperations", mt7Sub.EnableBatchedOperations, mt8Sub.EnableBatchedOperations);

        Assert.True(differences.Count == 0, "MassTransit 7.3 and 8 created entities differently:\n" + string.Join("\n", differences));
    }

    /// <summary>MassTransit's topic for a message type: <c>Namespace/TypeName</c>.</summary>
    private static string TopicName<T>() => $"{typeof(T).Namespace}/{typeof(T).Name}";
}

public sealed class SharedEventConsumer(ConcurrentQueue<SharedEvent> received) : IConsumer<SharedEvent>
{
    public Task Consume(ConsumeContext<SharedEvent> context)
    {
        received.Enqueue(context.Message);
        return Task.CompletedTask;
    }
}

internal static class ErrorQueue
{
    /// <summary>Receives (and completes) the faulted message for <paramref name="kind"/> from an error queue.</summary>
    public static async Task<Azure.Messaging.ServiceBus.ServiceBusReceivedMessage> ReceiveAsync(InteropFixture fixture, string queue, string kind)
    {
        await using var client = fixture.Broker.CreateServiceBusClient();
        await using var receiver = client.CreateReceiver(queue);
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
            if (message == null) continue;
            await receiver.CompleteMessageAsync(message);
            if (message.Body.ToString().Contains(kind)) return message;
        }
        throw new Xunit.Sdk.XunitException($"No faulted message for {kind} arrived in {queue}.");
    }
}

/// <summary>Compares payloads field by field and reports every difference at once.</summary>
internal static class PayloadAssert
{
    public static void Same(Payload expected, Payload? actual)
    {
        Assert.NotNull(actual);
        var differences = new List<string>();
        void Check(string field, object? e, object? a)
        {
            if (!Equals(e, a)) differences.Add($"{field}: expected {Show(e)}, got {Show(a)}");
        }
        Check(nameof(Payload.Id), expected.Id, actual.Id);
        Check(nameof(Payload.Text), expected.Text, actual.Text);
        Check(nameof(Payload.Count), expected.Count, actual.Count);
        Check(nameof(Payload.Big), expected.Big, actual.Big);
        Check(nameof(Payload.Price), expected.Price, actual.Price);
        Check(nameof(Payload.Ratio), expected.Ratio, actual.Ratio);
        Check(nameof(Payload.Flag), expected.Flag, actual.Flag);
        Check(nameof(Payload.AtUtc), expected.AtUtc, actual.AtUtc);
        Check(nameof(Payload.AtUtc) + ".Kind", expected.AtUtc.Kind, actual.AtUtc.Kind);
        Check(nameof(Payload.AtOffset), expected.AtOffset.ToString("O"), actual.AtOffset.ToString("O"));
        Check(nameof(Payload.Duration), expected.Duration, actual.Duration);
        Check(nameof(Payload.Missing), expected.Missing, actual.Missing);
        Check(nameof(Payload.Present), expected.Present, actual.Present);
        Check(nameof(Payload.Shade), expected.Shade, actual.Shade);
        Check(nameof(Payload.Data), Convert.ToBase64String(expected.Data), actual.Data == null ? null : Convert.ToBase64String(actual.Data));
        Check(nameof(Payload.Tags), string.Join(",", expected.Tags), actual.Tags == null ? null : string.Join(",", actual.Tags));
        Check(nameof(Payload.Counts), string.Join(",", expected.Counts.OrderBy(p => p.Key)), actual.Counts == null ? null : string.Join(",", actual.Counts.OrderBy(p => p.Key)));
        Check(nameof(Payload.Child) + ".Name", expected.Child.Name, actual.Child?.Name);
        Check(nameof(Payload.Child) + ".Values", string.Join(",", expected.Child.Values), actual.Child?.Values == null ? null : string.Join(",", actual.Child.Values));

        Assert.True(differences.Count == 0, "The payload changed between MassTransit versions:\n" + string.Join("\n", differences));
    }

    private static string Show(object? value) => value switch
    {
        null => "null",
        DateTime d => d.ToString("O"),
        double d => d.ToString("R"),
        _ => value.ToString() ?? "",
    };
}
