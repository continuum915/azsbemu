using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using AzsbEmu.Server.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzsbEmu.Benchmarks;

public class EntityScalabilityBenchmark
{
    public async Task RunAsync(int entityCount, int concurrency, int batchSize)
    {
        // We will run this with entityCount (e.g. 100 or 500)
        string tempDir = Path.Combine(Path.GetTempPath(), $"azsb-emu-perf-scale-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            Console.WriteLine($"Running Scalability Benchmark with {entityCount} entities:");
            var loggerFactory = NullLoggerFactory.Instance;

            // 1. Initial Creation
            var sw = Stopwatch.StartNew();
            long initialMemory = GC.GetTotalMemory(true);

            var entityManager = new EntityManager(tempDir, loggerFactory);
            
            // Create queues
            for (int i = 0; i < entityCount; i++)
            {
                entityManager.CreateQueue(new QueueInfo
                {
                    Name = $"perf-queue-{i}",
                    MaxDeliveryCount = 10,
                    LockDuration = TimeSpan.FromSeconds(60)
                });
            }

            // Create topic and subscriptions
            entityManager.CreateTopic("perf-topic");
            for (int i = 0; i < entityCount; i++)
            {
                entityManager.CreateSubscription("perf-topic", new SubscriptionInfo
                {
                    Name = $"perf-sub-{i}",
                    TopicName = "perf-topic"
                });
            }

            sw.Stop();
            long peakMemory = GC.GetTotalMemory(true);
            long memoryDelta = peakMemory - initialMemory;

            // Measure SQLite db file size
            string dbPath = Path.Combine(tempDir, "metadata.db");
            long dbSize = File.Exists(dbPath) ? new FileInfo(dbPath).Length : 0;

            Console.WriteLine($"  Entity Creation ({entityCount} queues, 1 topic, {entityCount} subscriptions):");
            Console.WriteLine($"    - Duration:    {sw.ElapsedMilliseconds} ms");
            Console.WriteLine($"    - SQLite Size: {dbSize / 1024.0 / 1024.0:F2} MB");
            Console.WriteLine($"    - GC Heap:     {memoryDelta / 1024.0 / 1024.0:F2} MB delta");

            // 2. Startup Recovery time
            // Dispose first
            entityManager.Dispose();

            sw.Restart();
            var recoveryManager = new EntityManager(tempDir, loggerFactory);
            sw.Stop();

            Console.WriteLine($"  Startup Recovery Scan ({entityCount * 2} active entities):");
            Console.WriteLine($"    - Duration:    {sw.ElapsedMilliseconds} ms");

            recoveryManager.Dispose();
        }
        finally
        {
            try
            {
                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, recursive: true);
                }
            }
            catch
            {
                // Ignore cleanup error
            }
        }
    }
}
