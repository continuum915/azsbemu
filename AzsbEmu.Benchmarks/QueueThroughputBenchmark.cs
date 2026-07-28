using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AzsbEmu.Tests.IntegrationTests;
using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Benchmarks;

public class QueueThroughputBenchmark
{
    private const string QueueName = "perf-queue";

    public async Task RunAsync(int messageCount, int concurrency, int batchSize)
    {
        using var broker = new BrokerFixture();
        broker.EnsureQueue(QueueName);

        await using var client = broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(QueueName);

        Console.WriteLine($"Running Queue Throughput Benchmark with {messageCount} messages (concurrency = {concurrency}, batchSize = {batchSize}):");

        // --- 1. Send Benchmark ---
        var sendLatencies = new ConcurrentBag<double>();
        var totalSendSw = Stopwatch.StartNew();

        int messagesPerTask = messageCount / concurrency;
        var sendTasks = Enumerable.Range(0, concurrency).Select(async t =>
        {
            var msgBytes = new byte[100]; // 100-byte message payload
            Random.Shared.NextBytes(msgBytes);
            var body = BinaryData.FromBytes(msgBytes);

            for (int i = 0; i < messagesPerTask / batchSize; i++)
            {
                var opSw = Stopwatch.StartNew();
                if (batchSize == 1)
                {
                    await sender.SendMessageAsync(new ServiceBusMessage(body));
                }
                else
                {
                    var batch = new List<ServiceBusMessage>();
                    for (int b = 0; b < batchSize; b++)
                    {
                        batch.Add(new ServiceBusMessage(body));
                    }
                    await sender.SendMessagesAsync(batch);
                }
                opSw.Stop();
                sendLatencies.Add(opSw.Elapsed.TotalMilliseconds);
            }
        });

        await Task.WhenAll(sendTasks);
        totalSendSw.Stop();

        Program.PrintStats("Send Path", sendLatencies.ToList(), totalSendSw.Elapsed.TotalSeconds, messageCount);

        // --- 2. Receive and Complete (PeekLock) Benchmark ---
        await using var receiver = client.CreateReceiver(QueueName);
        var recvLatencies = new ConcurrentBag<double>();
        var totalRecvSw = Stopwatch.StartNew();

        var recvTasks = Enumerable.Range(0, concurrency).Select(async t =>
        {
            int received = 0;
            while (received < messagesPerTask)
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

        Program.PrintStats("Receive & Complete (PeekLock)", recvLatencies.ToList(), totalRecvSw.Elapsed.TotalSeconds, messageCount);

        // --- 3. Receive & Delete Benchmark ---
        // Refill queue
        var refillTasks = Enumerable.Range(0, concurrency).Select(async t =>
        {
            var body = BinaryData.FromBytes(new byte[100]);
            for (int i = 0; i < messagesPerTask; i++)
            {
                await sender.SendMessageAsync(new ServiceBusMessage(body));
            }
        });
        await Task.WhenAll(refillTasks);

        // Consume via ReceiveAndDelete
        await using var deleteClient = broker.CreateServiceBusClient();
        await using var deleteReceiver = deleteClient.CreateReceiver(QueueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });

        var delLatencies = new ConcurrentBag<double>();
        var totalDelSw = Stopwatch.StartNew();

        var delTasks = Enumerable.Range(0, concurrency).Select(async t =>
        {
            int received = 0;
            while (received < messagesPerTask)
            {
                var opSw = Stopwatch.StartNew();
                var msg = await deleteReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
                if (msg != null)
                {
                    opSw.Stop();
                    delLatencies.Add(opSw.Elapsed.TotalMilliseconds);
                    received++;
                }
                else
                {
                    break;
                }
            }
        });

        await Task.WhenAll(delTasks);
        totalDelSw.Stop();

        Program.PrintStats("Receive & Delete (ReceiveAndDelete)", delLatencies.ToList(), totalDelSw.Elapsed.TotalSeconds, messageCount);
    }
}
