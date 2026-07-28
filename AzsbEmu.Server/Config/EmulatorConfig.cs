namespace AzsbEmu.Server.Config;

public class EmulatorConfig
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
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
}
