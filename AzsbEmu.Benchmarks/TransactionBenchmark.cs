using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using AzsbEmu.Tests.IntegrationTests;
using Azure.Messaging.ServiceBus;

namespace AzsbEmu.Benchmarks;

public class TransactionBenchmark
{
    private const string QueueName = "perf-queue-txn";

    public async Task RunAsync(int transactionCount, int concurrency, int batchSize)
    {
        using var broker = new BrokerFixture();
        broker.EnsureQueue(QueueName);

        await using var client = broker.CreateServiceBusClient();
        await using var sender = client.CreateSender(QueueName);

        // Adjust transactionCount so it doesn't take too long
        int runCount = transactionCount / 10;
        if (runCount < 50) runCount = 50;

        Console.WriteLine($"Running Transaction Benchmark ({runCount} transactions, concurrency = {concurrency}, sends per txn = {batchSize}):");

        var txnLatencies = new ConcurrentBag<double>();
        var totalSw = Stopwatch.StartNew();

        int txnsPerTask = runCount / concurrency;
        var tasks = Enumerable.Range(0, concurrency).Select(async t =>
        {
            var body = BinaryData.FromBytes(new byte[100]);
            for (int i = 0; i < txnsPerTask; i++)
            {
                var opSw = Stopwatch.StartNew();
                using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    for (int b = 0; b < batchSize; b++)
                    {
                        await sender.SendMessageAsync(new ServiceBusMessage(body));
                    }
                    ts.Complete();
                }
                opSw.Stop();
                txnLatencies.Add(opSw.Elapsed.TotalMilliseconds);
            }
        });

        await Task.WhenAll(tasks);
        totalSw.Stop();

        Program.PrintStats("Transactional Scope (Declare + Send + Commit)", txnLatencies.ToList(), totalSw.Elapsed.TotalSeconds, runCount);
    }
}
