using System.Collections.Concurrent;
using System.Diagnostics;
using AzsbEmu.Server.Core;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzsbEmu.Benchmarks;

public sealed class ConcurrentLoadBenchmark
{
    public async Task RunAsync(
        int entityCount,
        int messageCount,
        int producerCount,
        int consumerCount,
        int messageSize,
        TimeSpan receiveTimeout,
        bool drainBacklog)
    {
        ValidateArguments(entityCount, messageCount, producerCount, consumerCount, messageSize);

        string dataDirectory = Path.Combine(Path.GetTempPath(), $"azsb-emu-perf-concurrent-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dataDirectory);

        EntityManager? entityManager = null;
        Broker? broker = null;
        var startMeasuredPhase = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var warmupReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        List<ProducerWorker>? producers = null;
        try
        {
            entityManager = new EntityManager(dataDirectory, NullLoggerFactory.Instance);
            for (int index = 0; index < entityCount; index++)
            {
                entityManager.CreateQueue(new QueueInfo { Name = QueueName(index) });
            }

            int port = FindFreePort();
            broker = new Broker($"amqp://localhost:{port}", entityManager, NullLoggerFactory.Instance);
            broker.Start();

            string connectionString = $"Endpoint=sb://localhost:{port}/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true";
            int[] expectedPerQueue = CalculateExpectedMessages(entityCount, messageCount, producerCount);
            producers = CreateProducerWorkers(connectionString, entityCount, messageCount, producerCount, messageSize);
            int[] warmupPerQueue = CalculateWarmupMessagesPerQueue(entityCount, producers);
            int activeQueueCount = expectedPerQueue.Count(count => count > 0);
            TimeSpan warmupTimeout = TimeSpan.FromSeconds(Math.Max(120, receiveTimeout.TotalSeconds));
            int readyCount = 0;
            var sendLatencies = new ConcurrentBag<double>();
            var receiveLatencies = new ConcurrentBag<double>();
            var endToEndLatencies = new ConcurrentBag<double>();
            var sentAtTicks = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
            var receivedMessageIds = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
            long duplicateCount = 0;

            BrokerProfiler.Enabled = true;
            string benchmarkMode = drainBacklog ? "backlog-drain" : "streaming";
            Console.WriteLine($"Running {benchmarkMode} concurrent load benchmark with {entityCount} queues, {messageCount} messages, {producerCount} producers, and {consumerCount} consumers.");
            Console.WriteLine($"Warming {producers.Sum(producer => producer.QueueIndexes.Count)} producer links and {activeQueueCount} consumer links.");
            var warmupStopwatch = Stopwatch.StartNew();

            // Populate every queue's warm-up markers before receivers begin draining them.
            // This avoids racing a receiver against producers still opening other links.
            await Task.WhenAll(producers.Select(producer => producer.WarmAsync()));

            Task[] consumerTasks = StartConsumersAsync(
                connectionString,
                expectedPerQueue,
                warmupPerQueue,
                consumerCount,
                warmupTimeout,
                receiveTimeout,
                startMeasuredPhase.Task,
                () =>
                {
                    if (Interlocked.Increment(ref readyCount) == activeQueueCount)
                    {
                        warmupReady.TrySetResult();
                    }
                },
                warmupReady,
                receiveLatencies,
                endToEndLatencies,
                sentAtTicks,
                receivedMessageIds,
                () => Interlocked.Increment(ref duplicateCount));

            await Task.WhenAll(producers.Select(producer => producer.WarmAsync()));
            await warmupReady.Task.WaitAsync(warmupTimeout);
            warmupStopwatch.Stop();
            Console.WriteLine($"Warm-up complete in {warmupStopwatch.Elapsed.TotalSeconds:F2} s; starting steady-state measurement.");

            var sendStopwatch = Stopwatch.StartNew();
            Stopwatch totalStopwatch;
            long initialWorkingSet;
            long initialManagedMemory;
            var process = Process.GetCurrentProcess();

            if (drainBacklog)
            {
                await Task.WhenAll(producers.Select(producer => producer.SendMeasuredAsync(sendLatencies, sentAtTicks)));
                sendStopwatch.Stop();
                Console.WriteLine("Backlog published; starting timed drain measurement.");
                BrokerProfiler.Reset();
                process.Refresh();
                initialWorkingSet = process.WorkingSet64;
                initialManagedMemory = GC.GetTotalMemory(forceFullCollection: true);
                totalStopwatch = Stopwatch.StartNew();
                startMeasuredPhase.TrySetResult();
            }
            else
            {
                BrokerProfiler.Reset();
                process.Refresh();
                initialWorkingSet = process.WorkingSet64;
                initialManagedMemory = GC.GetTotalMemory(forceFullCollection: true);
                totalStopwatch = Stopwatch.StartNew();
                startMeasuredPhase.TrySetResult();
                await Task.WhenAll(producers.Select(producer => producer.SendMeasuredAsync(sendLatencies, sentAtTicks)));
                sendStopwatch.Stop();
            }

            await Task.WhenAll(consumerTasks);
            totalStopwatch.Stop();

            process.Refresh();
            long finalWorkingSet = process.WorkingSet64;
            long finalManagedMemory = GC.GetTotalMemory(forceFullCollection: true);
            long dataSize = GetDirectorySize(dataDirectory);
            BrokerProfileSnapshot profile = BrokerProfiler.Snapshot();

            if (receivedMessageIds.Count != messageCount || duplicateCount != 0 || sentAtTicks.Count != 0)
            {
                throw new InvalidOperationException(
                    $"Delivery validation failed. Expected {messageCount} unique messages, received {receivedMessageIds.Count}, duplicates {duplicateCount}, undelivered {sentAtTicks.Count}.");
            }

            Program.PrintStats(drainBacklog ? "Backlog Publish" : "Steady-State Send", sendLatencies.ToList(), sendStopwatch.Elapsed.TotalSeconds, messageCount);
            if (drainBacklog)
                Program.PrintStats("Backlog Drain Receive & Complete", receiveLatencies.ToList(), totalStopwatch.Elapsed.TotalSeconds, messageCount);
            else
            {
                Program.PrintStats("End-to-End Receive & Complete", endToEndLatencies.ToList(), totalStopwatch.Elapsed.TotalSeconds, messageCount);
                Program.PrintStats("Receiver Demand Wait & Complete", receiveLatencies.ToList(), totalStopwatch.Elapsed.TotalSeconds, messageCount);
            }
            Console.WriteLine("  Broker profile (average per operation):");
            foreach (BrokerProfileStage stage in profile.Stages.Where(stage => stage.Count > 0))
                Console.WriteLine($"    - {stage.Name}: {stage.AverageMilliseconds:F3} ms ({stage.Count:N0} operations)");
            Console.WriteLine("  Validation:");
            Console.WriteLine($"    - Unique messages received: {receivedMessageIds.Count}/{messageCount}");
            Console.WriteLine($"    - Duplicate deliveries:      {duplicateCount}");
            Console.WriteLine("  Steady-state resource usage:");
            Console.WriteLine($"    - Working set delta: {(finalWorkingSet - initialWorkingSet) / 1024.0 / 1024.0:F2} MB");
            Console.WriteLine($"    - Managed heap delta: {(finalManagedMemory - initialManagedMemory) / 1024.0 / 1024.0:F2} MB");
            Console.WriteLine($"    - Data directory size: {dataSize / 1024.0 / 1024.0:F2} MB");
        }
        finally
        {
            BrokerProfiler.Enabled = false;
            startMeasuredPhase.TrySetCanceled();
            warmupReady.TrySetCanceled();

            if (producers != null)
            {
                foreach (ProducerWorker producer in producers)
                {
                    await producer.DisposeAsync();
                }
            }

            broker?.Stop();
            entityManager?.Dispose();
            TryDelete(dataDirectory);
        }
    }

    private static List<ProducerWorker> CreateProducerWorkers(string connectionString, int entityCount, int messageCount, int producerCount, int messageSize)
    {
        int baseMessagesPerProducer = messageCount / producerCount;
        int remainingMessages = messageCount % producerCount;
        return Enumerable.Range(0, producerCount)
            .Select(producerIndex => new ProducerWorker(
                connectionString,
                producerIndex,
                baseMessagesPerProducer + (producerIndex < remainingMessages ? 1 : 0),
                entityCount,
                producerCount,
                messageSize))
            .ToList();
    }

    private static Task[] StartConsumersAsync(
        string connectionString,
        int[] expectedPerQueue,
        int[] warmupPerQueue,
        int consumerCount,
        TimeSpan warmupTimeout,
        TimeSpan receiveTimeout,
        Task startMeasuredPhase,
        Action receiverReady,
        TaskCompletionSource warmupReady,
        ConcurrentBag<double> latencies,
        ConcurrentBag<double> endToEndLatencies,
        ConcurrentDictionary<string, long> sentAtTicks,
        ConcurrentDictionary<string, byte> receivedMessageIds,
        Action duplicateDetected)
    {
        return Enumerable.Range(0, consumerCount).Select(async consumerIndex =>
        {
            await using var client = new ServiceBusClient(connectionString);
            Task[] receiverTasks = Enumerable.Range(0, expectedPerQueue.Length)
                .Where(queueIndex => queueIndex % consumerCount == consumerIndex && expectedPerQueue[queueIndex] > 0)
                .Select(queueIndex => ReceiveQueueAsync(
                    client,
                    QueueName(queueIndex),
                    warmupPerQueue[queueIndex],
                    expectedPerQueue[queueIndex],
                    warmupTimeout,
                    receiveTimeout,
                    startMeasuredPhase,
                    receiverReady,
                    warmupReady,
                    latencies,
                    endToEndLatencies,
                    sentAtTicks,
                    receivedMessageIds,
                    duplicateDetected))
                .ToArray();
            await Task.WhenAll(receiverTasks);
        }).ToArray();
    }

    private static async Task ReceiveQueueAsync(
        ServiceBusClient client,
        string queueName,
        int warmupCount,
        int expectedCount,
        TimeSpan warmupTimeout,
        TimeSpan receiveTimeout,
        Task startMeasuredPhase,
        Action receiverReady,
        TaskCompletionSource warmupReady,
        ConcurrentBag<double> latencies,
        ConcurrentBag<double> endToEndLatencies,
        ConcurrentDictionary<string, long> sentAtTicks,
        ConcurrentDictionary<string, byte> receivedMessageIds,
        Action duplicateDetected)
    {
        try
        {
            await using var receiver = client.CreateReceiver(queueName);
            for (int warmupIndex = 0; warmupIndex < warmupCount; warmupIndex++)
            {
                ServiceBusReceivedMessage warmupMessage = await ReceiveRequiredAsync(receiver, warmupTimeout, queueName, "warm-up");
                if (!warmupMessage.ApplicationProperties.TryGetValue("benchmark-warmup", out object? marker) || marker is not true)
                {
                    throw new InvalidOperationException($"Expected a warm-up marker on queue '{queueName}'.");
                }

                await receiver.CompleteMessageAsync(warmupMessage);
            }

            receiverReady();
            await startMeasuredPhase;

            int receivedCount = 0;
            while (receivedCount < expectedCount)
            {
                var stopwatch = Stopwatch.StartNew();
                ServiceBusReceivedMessage message = await ReceiveRequiredAsync(receiver, receiveTimeout, queueName, "measured");
                await receiver.CompleteMessageAsync(message);
                stopwatch.Stop();

                // A marker may still be pending after its readiness marker was consumed.
                // It is only used to open a producer link and is not part of the workload.
                if (message.ApplicationProperties.TryGetValue("benchmark-warmup", out object? marker) && marker is true)
                {
                    continue;
                }

                latencies.Add(stopwatch.Elapsed.TotalMilliseconds);
                receivedCount++;

                if (!receivedMessageIds.TryAdd(message.MessageId, 0))
                {
                    duplicateDetected();
                    continue;
                }

                if (!sentAtTicks.TryRemove(message.MessageId, out long sentTicks))
                {
                    throw new InvalidOperationException($"Message '{message.MessageId}' was received without a send timestamp.");
                }

                endToEndLatencies.Add((Stopwatch.GetTimestamp() - sentTicks) * 1000d / Stopwatch.Frequency);
            }
        }
        catch (Exception ex)
        {
            warmupReady.TrySetException(ex);
            throw;
        }
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveRequiredAsync(ServiceBusReceiver receiver, TimeSpan timeout, string queueName, string phase)
    {
        ServiceBusReceivedMessage? message = await receiver.ReceiveMessageAsync(timeout);
        return message ?? throw new TimeoutException($"Timed out waiting for a {phase} message on queue '{queueName}'.");
    }

    private static int[] CalculateExpectedMessages(int entityCount, int messageCount, int producerCount)
    {
        var expectedPerQueue = new int[entityCount];
        int baseMessagesPerProducer = messageCount / producerCount;
        int remainingMessages = messageCount % producerCount;
        for (int producerIndex = 0; producerIndex < producerCount; producerIndex++)
        {
            int messagesForProducer = baseMessagesPerProducer + (producerIndex < remainingMessages ? 1 : 0);
            for (int ordinal = 0; ordinal < messagesForProducer; ordinal++)
            {
                expectedPerQueue[GetQueueIndex(producerIndex, ordinal, producerCount, entityCount)]++;
            }
        }
        return expectedPerQueue;
    }

    private static int[] CalculateWarmupMessagesPerQueue(int entityCount, IEnumerable<ProducerWorker> producers)
    {
        var warmupPerQueue = new int[entityCount];
        foreach (ProducerWorker producer in producers)
        {
            foreach (int queueIndex in producer.QueueIndexes)
            {
                warmupPerQueue[queueIndex]++;
            }
        }
        return warmupPerQueue;
    }

    private static int GetQueueIndex(int producerIndex, int ordinal, int producerCount, int entityCount)
        => (producerIndex + ordinal * producerCount) % entityCount;

    private static string QueueName(int index) => $"concurrent-perf-queue-{index}";

    private static void ValidateArguments(int entityCount, int messageCount, int producerCount, int consumerCount, int messageSize)
    {
        if (entityCount <= 0 || messageCount <= 0 || producerCount <= 0 || consumerCount <= 0 || messageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(entityCount), "All concurrent benchmark arguments must be positive.");
        }
    }

    private static int FindFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        int port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static long GetDirectorySize(string path)
        => Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories).Sum(file => new FileInfo(file).Length);

    private static void TryDelete(string path)
    {
        try { Directory.Delete(path, recursive: true); }
        catch { }
    }

    private sealed class ProducerWorker : IAsyncDisposable
    {
        private readonly ServiceBusClient _client;
        private readonly Dictionary<int, ServiceBusSender> _senders;
        private readonly int _producerIndex;
        private readonly int _messageCount;
        private readonly int _entityCount;
        private readonly int _producerCount;
        private readonly byte[] _payload;

        public ProducerWorker(string connectionString, int producerIndex, int messageCount, int entityCount, int producerCount, int messageSize)
        {
            _client = new ServiceBusClient(connectionString);
            _producerIndex = producerIndex;
            _messageCount = messageCount;
            _entityCount = entityCount;
            _producerCount = producerCount;
            _payload = new byte[messageSize];
            Random.Shared.NextBytes(_payload);

            QueueIndexes = Enumerable.Range(0, messageCount)
                .Select(ordinal => GetQueueIndex(producerIndex, ordinal, producerCount, entityCount))
                .Distinct()
                .ToArray();
            _senders = QueueIndexes.ToDictionary(queueIndex => queueIndex, queueIndex => _client.CreateSender(QueueName(queueIndex)));
        }

        public IReadOnlyList<int> QueueIndexes { get; }

        public async Task WarmAsync()
        {
            foreach (int queueIndex in QueueIndexes)
            {
                var marker = new ServiceBusMessage(BinaryData.FromBytes(Array.Empty<byte>()))
                {
                    MessageId = $"warmup-{_producerIndex}-{queueIndex}"
                };
                marker.ApplicationProperties["benchmark-warmup"] = true;
                await _senders[queueIndex].SendMessageAsync(marker);
            }
        }

        public async Task SendMeasuredAsync(ConcurrentBag<double> latencies, ConcurrentDictionary<string, long> sentAtTicks)
        {
            for (int ordinal = 0; ordinal < _messageCount; ordinal++)
            {
                int queueIndex = GetQueueIndex(_producerIndex, ordinal, _producerCount, _entityCount);
                string messageId = $"{_producerIndex}-{ordinal}";
                var message = new ServiceBusMessage(BinaryData.FromBytes(_payload))
                {
                    MessageId = messageId
                };
                sentAtTicks[messageId] = Stopwatch.GetTimestamp();
                var stopwatch = Stopwatch.StartNew();
                await _senders[queueIndex].SendMessageAsync(message);
                stopwatch.Stop();
                latencies.Add(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (ServiceBusSender sender in _senders.Values)
            {
                await sender.DisposeAsync();
            }
            await _client.DisposeAsync();
        }
    }
}