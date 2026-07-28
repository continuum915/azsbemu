using System.Net;
using System.Net.Sockets;
using AzsbEmu.Server.Core;
using AzsbEmu.Server.Protocol;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>
/// Spins up a real Broker on a random free port for integration tests.
/// Each instance gets an isolated temp directory cleaned up on disposal.
/// Pass an existing data directory to the second constructor to simulate a broker restart.
/// </summary>
public sealed class BrokerFixture : IDisposable
{
    public int Port { get; }
    public string ConnectionString { get; }
    public string DataDir => _dataDir;
    public EntityManager EntityManager => _entityManager;

    private readonly string _dataDir;
    private readonly EntityManager _entityManager;
    private readonly Broker _broker;
    private bool _ownedDataDir;
    private bool _disposed;

    // xUnit IClassFixture requires exactly one public constructor.
    // Tests that need a restart pass the existing data directory via TakeDataDir() +
    // BrokerFixture(dataDir) from inside the test body (not as a fixture).
    public BrokerFixture() : this(existingDataDir: null) { }

    // Called explicitly from test bodies for restart scenarios — NOT a fixture constructor.
    internal BrokerFixture(string? existingDataDir)
    {
        Amqp.Trace.TraceLevel = Amqp.TraceLevel.Frame | Amqp.TraceLevel.Information;
        Amqp.Trace.TraceListener = (level, format, args) => Console.WriteLine(format, args);

        if (existingDataDir == null)
        {
            _dataDir = Path.Combine(Path.GetTempPath(), $"azsb-emu-test-{Guid.NewGuid():N}");
            _ownedDataDir = true;
        }
        else
        {
            _dataDir = existingDataDir;
            _ownedDataDir = false;
        }

        Port = FindFreePort();

        var loggerFactory = LoggerFactory.Create(b => b
            .AddConsole()
            .SetMinimumLevel(LogLevel.Debug));

        _entityManager = new EntityManager(_dataDir, loggerFactory);
        _broker = new Broker($"amqp://localhost:{Port}", _entityManager, loggerFactory);
        _broker.Start();

        ConnectionString = $"Endpoint=sb://localhost:{Port}/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true";
    }

    /// <summary>
    /// Returns the data directory path and marks it as unowned so Dispose will not delete it.
    /// The caller is responsible for cleanup.
    /// </summary>
    public string TakeDataDir()
    {
        _ownedDataDir = false;
        return _dataDir;
    }

    public void EnsureQueue(string name, int maxDeliveryCount = 10, TimeSpan? lockDuration = null, bool requiresSession = false)
    {
        _entityManager.CreateQueue(new QueueInfo
        {
            Name = name,
            MaxDeliveryCount = maxDeliveryCount,
            LockDuration = lockDuration ?? TimeSpan.FromSeconds(60),
            RequiresSession = requiresSession
        });
    }

    public void EnsureTopic(string name)
    {
        _entityManager.CreateTopic(name);
    }

    public void EnsureSubscription(string topicName, string subName, int maxDeliveryCount = 10, TimeSpan? lockDuration = null, bool requiresSession = false)
    {
        _entityManager.CreateSubscription(topicName, new SubscriptionInfo
        {
            Name = subName,
            TopicName = topicName,
            MaxDeliveryCount = maxDeliveryCount,
            LockDuration = lockDuration ?? TimeSpan.FromSeconds(60),
            RequiresSession = requiresSession
        });
    }

    public Azure.Messaging.ServiceBus.ServiceBusClient CreateServiceBusClient()
        => new(ConnectionString);

    public QueueInfo GetQueue(string name)
        => _entityManager.GetQueue(name)
            ?? throw new InvalidOperationException($"Queue '{name}' was not found.");

    public SubscriptionInfo GetSubscription(string topicName, string subName)
    {
        var topic = _entityManager.GetTopic(topicName)
            ?? throw new InvalidOperationException($"Topic '{topicName}' was not found.");
        return topic.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Subscription '{subName}' was not found on topic '{topicName}'.");
    }

    public int GetScheduledMessageCount(string entityName)
    {
        return _entityManager.GetScheduledMessageCount(entityName);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _broker.Stop();
        _entityManager.Dispose();

        if (_ownedDataDir)
            try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    private static int FindFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

[Xunit.CollectionDefinition("SharedBrokerCollection")]
public class SharedBrokerCollection : Xunit.ICollectionFixture<BrokerFixture>
{
    // This class has no code, and is just used to associate the collection with the fixture.
}
