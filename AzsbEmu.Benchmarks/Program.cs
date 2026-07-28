using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace AzsbEmu.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("=================================================================");
        Console.WriteLine("                AZSBEmu Performance Benchmark Suite              ");
        Console.WriteLine("=================================================================");
        Console.ResetColor();

        string benchmark = "all";
        int messages = 5000;
        int concurrency = 4;
        int batchSize = 1;
        int entities = 500;
        int producers = 16;
        int consumers = 16;
        int messageSize = 1024;
        int receiveTimeoutSeconds = 30;
        string mode = "backlog";

        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--benchmark" && i + 1 < args.Length)
                benchmark = args[++i].ToLowerInvariant();
            else if (args[i] == "--messages" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedMessages))
            {
                messages = parsedMessages;
                i++;
            }
            else if (args[i] == "--concurrency" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedConcurrency))
            {
                concurrency = parsedConcurrency;
                i++;
            }
            else if (args[i] == "--batch-size" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedBatchSize))
            {
                batchSize = parsedBatchSize;
                i++;
            }
            else if (args[i] == "--entities" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedEntities))
            {
                entities = parsedEntities;
                i++;
            }
            else if (args[i] == "--producers" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedProducers))
            {
                producers = parsedProducers;
                i++;
            }
            else if (args[i] == "--consumers" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedConsumers))
            {
                consumers = parsedConsumers;
                i++;
            }
            else if (args[i] == "--message-size" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedMessageSize))
            {
                messageSize = parsedMessageSize;
                i++;
            }
            else if (args[i] == "--mode" && i + 1 < args.Length)
            {
                mode = args[++i].ToLowerInvariant();
            }
            else if (args[i] == "--receive-timeout-seconds" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedReceiveTimeout))
            {
                receiveTimeoutSeconds = parsedReceiveTimeout;
                i++;
            }
        }

        Console.WriteLine("Configured Run Parameters:");
        Console.WriteLine($"  - Benchmark Target: {benchmark.ToUpperInvariant()}");
        Console.WriteLine($"  - Message Count:    {messages}");
        Console.WriteLine($"  - Concurrency:      {concurrency}");
        Console.WriteLine($"  - Batch Size:       {batchSize}");
        Console.WriteLine($"  - Entities:         {entities}");
        Console.WriteLine($"  - Producers:        {producers}");
        Console.WriteLine($"  - Consumers:        {consumers}");
        Console.WriteLine($"  - Message Size:     {messageSize} bytes");
        Console.WriteLine($"  - Load Mode:        {mode}");
        Console.WriteLine("-----------------------------------------------------------------\n");

        try
        {
            if (benchmark == "all" || benchmark == "scalability")
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(">>> Running Entity Scalability Benchmark...");
                Console.ResetColor();
                var scalabilityBenchmark = new EntityScalabilityBenchmark();
                await scalabilityBenchmark.RunAsync(100, concurrency, batchSize);
                Console.WriteLine();
            }

            if (benchmark == "all" || benchmark == "queue")
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(">>> Running Queue Throughput Benchmark...");
                Console.ResetColor();
                var queueBenchmark = new QueueThroughputBenchmark();
                await queueBenchmark.RunAsync(messages, concurrency, batchSize);
                Console.WriteLine();
            }

            if (benchmark == "all" || benchmark == "topic")
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(">>> Running Topic Fan-out Benchmark...");
                Console.ResetColor();
                var topicBenchmark = new TopicFanOutBenchmark();
                await topicBenchmark.RunAsync(messages, concurrency, batchSize);
                Console.WriteLine();
            }

            if (benchmark == "all" || benchmark == "concurrent")
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(">>> Running Concurrent Load Benchmark...");
                Console.ResetColor();
                var concurrentBenchmark = new ConcurrentLoadBenchmark();
                await concurrentBenchmark.RunAsync(entities, messages, producers, consumers, messageSize, TimeSpan.FromSeconds(receiveTimeoutSeconds), mode == "backlog");
                Console.WriteLine();
            }

            if (benchmark == "all" || benchmark == "transaction")
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(">>> Running Transaction Controller Benchmark...");
                Console.ResetColor();
                var transactionBenchmark = new TransactionBenchmark();
                await transactionBenchmark.RunAsync(messages, concurrency, batchSize);
                Console.WriteLine();
            }
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Benchmark run encountered an error: {ex}");
            Console.ResetColor();
        }

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("=================================================================");
        Console.WriteLine("                  Benchmark Suite Completed!                     ");
        Console.WriteLine("=================================================================");
        Console.ResetColor();
    }

    public static void PrintStats(string label, List<double> latenciesMs, double totalDurationSeconds, int operationCount)
    {
        if (latenciesMs.Count == 0)
        {
            Console.WriteLine($"  {label}: No operations recorded.");
            return;
        }

        latenciesMs.Sort();
        double avg = latenciesMs.Average();
        double min = latenciesMs[0];
        double max = latenciesMs[^1];
        double p50 = latenciesMs[(int)(latenciesMs.Count * 0.5)];
        double p95 = latenciesMs[(int)(latenciesMs.Count * 0.95)];
        double p99 = latenciesMs[(int)(latenciesMs.Count * 0.99)];
        double throughput = operationCount / totalDurationSeconds;

        Console.WriteLine($"  {label} Results:");
        Console.WriteLine($"    - Throughput: {throughput:F2} ops/sec");
        Console.WriteLine($"    - Latency (Avg): {avg:F2} ms");
        Console.WriteLine($"    - Latency (Min / Max): {min:F2} ms / {max:F2} ms");
        Console.WriteLine($"    - Latency (P50 / P95 / P99): {p50:F2} ms / {p95:F2} ms / {p99:F2} ms");
    }
}