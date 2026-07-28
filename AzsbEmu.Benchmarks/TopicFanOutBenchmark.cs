using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AzsbEmu.Tests.IntegrationTests;
using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Benchmarks;

public class TopicFanOutBenchmark
{
    private const string TopicName = "perf-topic";
    private const int SubscriptionCount = 10;

    public async Task RunAsync(int messageCount, int concurrency, int batchSize)
    {
        using var broker = new BrokerFixture();
        broker.EnsureTopic(TopicName);
        for (int i = 0; i < SubscriptionCount; i++)
        {
            broker.EnsureSubscription(TopicName, $"perf-sub-{i}");
        }

        await using var client = broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(TopicName);

        // Adjust messages count for topics to prevent runs taking too long with 10 subscriptions
        int sendCount = messageCount / 5;
        if (sendCount < 100) sendCount = 100;
        int totalExpectedDeliveries = sendCount * SubscriptionCount;

        Console.WriteLine($"Running Topic Fan-out Benchmark (Publish {sendCount} messages to topic with {SubscriptionCount} subscriptions):");

        // --- 1. Publish to Topic ---
        var pubLatencies = new ConcurrentBag<double>();
        var totalPubSw = Stopwatch.StartNew();

        int pubConcurrency = concurrency;
        int pubPerTask = sendCount / pubConcurrency;
        var pubTasks = Enumerable.Range(0, pubConcurrency).Select(async t =>
        {
            var body = BinaryData.FromBytes(new byte[100]);
            for (int i = 0; i < pubPerTask; i++)
            {
                var opSw = Stopwatch.StartNew();
                await sender.SendMessageAsync(new ServiceBusMessage(body));
                opSw.Stop();
                pubLatencies.Add(opSw.Elapsed.TotalMilliseconds);
            }
        });

        await Task.WhenAll(pubTasks);
        totalPubSw.Stop();

        Program.PrintStats("Topic Publish", pubLatencies.ToList(), totalPubSw.Elapsed.TotalSeconds, sendCount);

        // --- 2. Fan-out Receive & Complete ---
        var recvLatencies = new ConcurrentBag<double>();
        var totalRecvSw = Stopwatch.StartNew();

        // Spawn a receiver for each subscription
        var recvTasks = Enumerable.Range(0, SubscriptionCount).Select(async s =>
        {
            await using var subClient = broker.CreateServiceBusClient();
            await using var receiver = subClient.CreateReceiver(TopicName, $"perf-sub-{s}");

            int received = 0;
            while (received < sendCount)
            {
                var opSw = Stopwatch.StartNew();
                var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
                if (msg != null)
                {
                    await receiver.CompleteMessageAsync(msg);
                    opSw.Stop();
                    recvLatencies.Add(opSw.Elapsed.TotalMilliseconds);
                    received++;
                }
                else
                {
                    break;
                }
            }
        });

        await Task.WhenAll(recvTasks);
        totalRecvSw.Stop();

        Program.PrintStats($"Subscription Receive & Complete ({SubscriptionCount} Subs)", recvLatencies.ToList(), totalRecvSw.Elapsed.TotalSeconds, totalExpectedDeliveries);
    }
}
