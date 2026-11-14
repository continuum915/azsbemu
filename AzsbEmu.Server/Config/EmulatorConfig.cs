namespace AzsbEmu.Server.Config;

public class EmulatorConfig
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    /// <summary>
    /// Port of the HTTP management API used by ServiceBusAdministrationClient. The SDK's admin client always
    /// connects to port 80 when UseDevelopmentEmulator=true, so only change this behind a port mapping.
    /// </summary>
    public int HttpPort { get; set; } = 80;
    public string DataDirectory { get; set; } = "";
    public List<QueueConfig> Queues { get; set; } = new();
    public List<TopicConfig> Topics { get; set; } = new();
}

public class QueueConfig
{
    public string Name { get; set; } = string.Empty;
    public bool RequiresSession { get; set; }
    public int LockDurationSeconds { get; set; } = 60;
    public long MaxSizeInMegabytes { get; set; } = 1024;
    public int MaxDeliveryCount { get; set; } = 10;
    /// <summary>Dead-letter expired messages instead of discarding them (Service Bus default: false).</summary>
    public bool DeadLetteringOnMessageExpiration { get; set; }
}

public class TopicConfig
{
    public string Name { get; set; } = string.Empty;
    public List<SubscriptionConfig> Subscriptions { get; set; } = new();
}

public class SubscriptionConfig
{
    public string Name { get; set; } = string.Empty;
    public bool RequiresSession { get; set; }
    /// <summary>Dead-letter expired messages instead of discarding them (Service Bus default: false).</summary>
    public bool DeadLetteringOnMessageExpiration { get; set; }
}
