using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace AzsbEmu.Server.Core;

public class SessionLockState
{
    public string SessionId { get; set; } = string.Empty;
    public string LockedByLink { get; set; } = string.Empty;
    public DateTime LockedUntil { get; set; }
    public object? ActiveLink { get; set; }
}

public interface IMessageEntity
{
    string Name { get; }
    /// <summary>Entity that messages arriving here are forwarded to instead of being stored, or null.</summary>
    string? ForwardToEntityName { get; }
    /// <summary>Entity that this entity's dead-lettered messages are forwarded to instead of its own DLQ, or null.</summary>
    string? ForwardDeadLetteredMessagesToEntityName { get; }
    TimeSpan LockDuration { get; }
    int MaxDeliveryCount { get; }
    TimeSpan DefaultMessageTimeToLive { get; }
    /// <summary>Whether expired messages are dead-lettered (true) or discarded (false, the Service Bus default).</summary>
    bool DeadLetteringOnMessageExpiration { get; }
    long MessageCount { get; }
    long DeadLetterMessageCount { get; }
    
    long AssignNextSequenceNumber();
    void UpdateNextSequenceNumber(long seqNum);
    void IncrementMessageCount();
    void DecrementMessageCount();
    void IncrementDeadLetterMessageCount();
    void DecrementDeadLetterMessageCount();
    
    MessageStore? MessageStore { get; }
    Dispatcher? Dispatcher { get; }
    MessageStore? DlqMessageStore { get; }
    Dispatcher? DlqDispatcher { get; }
    ConcurrentDictionary<string, SessionMessageQueue> SessionMessageAddresses { get; }
    bool RequiresSession { get; }
    EntityActivity Activity { get; }
}

public class RuleInfo
{
    /// <summary>The rule every subscription starts with unless one is supplied: <c>$Default</c>, matching everything.</summary>
    public static RuleInfo CreateDefault() => new() { Name = "$Default", FilterType = "True" };

    /// <summary>
    /// Stores the SQL forms of the true and false filters (<c>1=1</c>, <c>1=0</c>, which is how the SDK sends them over
    /// AMQP) as true/false filters, so every API reads them back as TrueRuleFilter/FalseRuleFilter.
    /// </summary>
    public void NormalizeFilter()
    {
        if (!string.Equals(FilterType, "Sql", StringComparison.OrdinalIgnoreCase)) return;
        string compact = string.Concat((FilterExpression ?? "").Where(c => !char.IsWhiteSpace(c)));
        if (compact == "1=1") { FilterType = "True"; FilterExpression = ""; }
        else if (compact == "1=0") { FilterType = "False"; FilterExpression = ""; }
    }

    public string Name { get; set; } = string.Empty;
    public string FilterType { get; set; } = "True"; // True, False, Correlation, Sql
    public string FilterExpression { get; set; } = string.Empty;
    public string ActionExpression { get; set; } = string.Empty;
}

public class CorrelationFilterDto
{
    public string? CorrelationId { get; set; }
    public string? MessageId { get; set; }
    public string? To { get; set; }
    public string? ReplyTo { get; set; }
    public string? Label { get; set; }
    public string? SessionId { get; set; }
    public string? ReplyToSessionId { get; set; }
    public string? ContentType { get; set; }
    public Dictionary<string, object>? Properties { get; set; }

    /// <summary>
    /// The original type of each property ("int", "long", "double", "boolean", "string"). Properties are stored as
    /// JSON, which keeps only "a number"; without this, 5L or 2.0 would read back as int after a restart.
    /// </summary>
    public Dictionary<string, string>? PropertyTypes { get; set; }

    public void SetProperty(string key, object value)
    {
        (Properties ??= new Dictionary<string, object>())[key] = value;
        (PropertyTypes ??= new Dictionary<string, string>())[key] = value switch
        {
            int or short or byte or sbyte or ushort => "int",
            long or uint => "long",
            double or float or decimal => "double",
            bool => "boolean",
            _ => "string",
        };
    }

    /// <summary>The properties with their original types restored (values read back from JSON are JsonElements).</summary>
    public IEnumerable<KeyValuePair<string, object>> TypedProperties()
    {
        foreach (var (key, value) in Properties ?? new Dictionary<string, object>())
        {
            string? type = PropertyTypes != null && PropertyTypes.TryGetValue(key, out var t) ? t : null;
            yield return new(key, value is System.Text.Json.JsonElement json ? FromJson(json, type) : value);
        }
    }

    private static object FromJson(System.Text.Json.JsonElement json, string? type) => (type, json.ValueKind) switch
    {
        ("int", _) => json.GetInt32(),
        ("long", _) => json.GetInt64(),
        ("double", _) => json.GetDouble(),
        ("boolean", _) => json.GetBoolean(),
        ("string", _) => json.ValueKind == System.Text.Json.JsonValueKind.String ? json.GetString()! : json.ToString(),
        // Rows written before types were recorded: infer.
        (_, System.Text.Json.JsonValueKind.Number) when json.TryGetInt32(out var i) => i,
        (_, System.Text.Json.JsonValueKind.Number) when json.TryGetInt64(out var l) => l,
        (_, System.Text.Json.JsonValueKind.Number) => json.GetDouble(),
        (_, System.Text.Json.JsonValueKind.True) => true,
        (_, System.Text.Json.JsonValueKind.False) => false,
        _ => json.ToString(),
    };

    /// <summary>Reads a stored correlation filter; an empty or malformed expression reads as an empty filter.</summary>
    public static CorrelationFilterDto Parse(string? json)
    {
        if (string.IsNullOrWhiteSpace(json)) return new CorrelationFilterDto();
        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<CorrelationFilterDto>(json) ?? new CorrelationFilterDto();
        }
        catch (System.Text.Json.JsonException)
        {
            return new CorrelationFilterDto();
        }
    }
}

public class QueueInfo : IMessageEntity
{
    public string Name { get; set; } = string.Empty;
    public bool RequiresSession { get; set; }
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(60);
    public long MaxSizeInMegabytes { get; set; } = 1024;
    public int MaxDeliveryCount { get; set; } = 10;
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(1);
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public bool EnableBatchedOperations { get; set; } = true;
    public string Status { get; set; } = "Active";
    private string? _forwardTo;

    /// <summary>Forward target exactly as the client supplied it (the SDK sends an absolute URI).</summary>
    public string? ForwardTo
    {
        get => _forwardTo;
        set { _forwardTo = value; ForwardToEntityName = ForwardTarget.ToEntityName(value); }
    }

    public string? ForwardToEntityName { get; private set; }
    private string? _forwardDeadLetteredMessagesTo;

    /// <summary>Dead-letter forward target exactly as the client supplied it.</summary>
    public string? ForwardDeadLetteredMessagesTo
    {
        get => _forwardDeadLetteredMessagesTo;
        set { _forwardDeadLetteredMessagesTo = value; ForwardDeadLetteredMessagesToEntityName = ForwardTarget.ToEntityName(value); }
    }

    public string? ForwardDeadLetteredMessagesToEntityName { get; private set; }
    public string? UserMetadata { get; set; }
    public TimeSpan AutoDeleteOnIdle { get; set; } = TimeSpan.MaxValue;
    public bool EnablePartitioning { get; set; }
    public long? MaxMessageSizeInKilobytes { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }

    /// <summary>Copies every client-configurable setting from <paramref name="source"/>; name, timestamps and runtime state are kept.</summary>
    public void ApplyConfigurationFrom(QueueInfo source)
    {
        RequiresSession = source.RequiresSession;
        LockDuration = source.LockDuration;
        MaxSizeInMegabytes = source.MaxSizeInMegabytes;
        MaxDeliveryCount = source.MaxDeliveryCount;
        DefaultMessageTimeToLive = source.DefaultMessageTimeToLive;
        RequiresDuplicateDetection = source.RequiresDuplicateDetection;
        DuplicateDetectionHistoryTimeWindow = source.DuplicateDetectionHistoryTimeWindow;
        DeadLetteringOnMessageExpiration = source.DeadLetteringOnMessageExpiration;
        EnableBatchedOperations = source.EnableBatchedOperations;
        Status = source.Status;
        ForwardTo = source.ForwardTo;
        ForwardDeadLetteredMessagesTo = source.ForwardDeadLetteredMessagesTo;
        UserMetadata = source.UserMetadata;
        AutoDeleteOnIdle = source.AutoDeleteOnIdle;
        EnablePartitioning = source.EnablePartitioning;
        MaxMessageSizeInKilobytes = source.MaxMessageSizeInKilobytes;
    }

    /// <summary>A detached copy of this queue's identity and timestamps with <paramref name="source"/>'s settings.</summary>
    public QueueInfo WithConfigurationFrom(QueueInfo source)
    {
        var copy = new QueueInfo { Name = Name, CreatedAt = CreatedAt, UpdatedAt = UpdatedAt };
        copy.ApplyConfigurationFrom(source);
        return copy;
    }

    private long _messageCount;
    private long _deadLetterMessageCount;
    private long _nextSequenceNumber;

    public long MessageCount
    {
        get => Volatile.Read(ref _messageCount);
        set => Volatile.Write(ref _messageCount, value);
    }

    public long NextSequenceNumber
    {
        get => Volatile.Read(ref _nextSequenceNumber);
        set => Volatile.Write(ref _nextSequenceNumber, value);
    }

    public long DeadLetterMessageCount
    {
        get => Volatile.Read(ref _deadLetterMessageCount);
        set => Volatile.Write(ref _deadLetterMessageCount, value);
    }

    public long AssignNextSequenceNumber() => Interlocked.Increment(ref _nextSequenceNumber);
    public void UpdateNextSequenceNumber(long seqNum)
    {
        long currentNext = Volatile.Read(ref _nextSequenceNumber);
        while (seqNum >= currentNext)
        {
            if (Interlocked.CompareExchange(ref _nextSequenceNumber, seqNum + 1, currentNext) == currentNext)
                break;
            currentNext = Volatile.Read(ref _nextSequenceNumber);
        }
    }
    public void IncrementMessageCount() => Interlocked.Increment(ref _messageCount);
    public void DecrementMessageCount() => Interlocked.Decrement(ref _messageCount);
    public void IncrementDeadLetterMessageCount() => Interlocked.Increment(ref _deadLetterMessageCount);
    public void DecrementDeadLetterMessageCount() => Interlocked.Decrement(ref _deadLetterMessageCount);

    // Runtime state
    public MessageStore? MessageStore { get; set; }
    public Dispatcher? Dispatcher { get; set; }
    public MessageStore? DlqMessageStore { get; set; }
    public Dispatcher? DlqDispatcher { get; set; }
    public ConcurrentDictionary<string, SessionMessageQueue> SessionMessageAddresses { get; } = new(StringComparer.OrdinalIgnoreCase);
    public EntityActivity Activity { get; } = new();
    public DuplicateDetectionHistory DuplicateHistory { get; } = new();
}

public class TopicInfo
{
    public string Name { get; set; } = string.Empty;
    public List<SubscriptionInfo> Subscriptions { get; set; } = new();

    // Settings the management API stores and returns. Duplicate detection and AutoDeleteOnIdle are enforced; size,
    // partitioning, ordering and status are not.
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;
    public long MaxSizeInMegabytes { get; set; } = 1024;
    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(1);
    public bool EnableBatchedOperations { get; set; } = true;
    public bool EnablePartitioning { get; set; }
    public bool SupportOrdering { get; set; } = true;
    public TimeSpan AutoDeleteOnIdle { get; set; } = TimeSpan.MaxValue;
    public string Status { get; set; } = "Active";
    public string? UserMetadata { get; set; }
    public long? MaxMessageSizeInKilobytes { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public EntityActivity Activity { get; } = new();
    public DuplicateDetectionHistory DuplicateHistory { get; } = new();

    /// <summary>Copies every client-configurable setting from <paramref name="source"/>; name, subscriptions and timestamps are kept.</summary>
    public void ApplyConfigurationFrom(TopicInfo source)
    {
        DefaultMessageTimeToLive = source.DefaultMessageTimeToLive;
        MaxSizeInMegabytes = source.MaxSizeInMegabytes;
        RequiresDuplicateDetection = source.RequiresDuplicateDetection;
        DuplicateDetectionHistoryTimeWindow = source.DuplicateDetectionHistoryTimeWindow;
        EnableBatchedOperations = source.EnableBatchedOperations;
        EnablePartitioning = source.EnablePartitioning;
        SupportOrdering = source.SupportOrdering;
        AutoDeleteOnIdle = source.AutoDeleteOnIdle;
        Status = source.Status;
        UserMetadata = source.UserMetadata;
        MaxMessageSizeInKilobytes = source.MaxMessageSizeInKilobytes;
    }

    /// <summary>A detached copy of this topic's identity and timestamps with <paramref name="source"/>'s settings.</summary>
    public TopicInfo WithConfigurationFrom(TopicInfo source)
    {
        var copy = new TopicInfo { Name = Name, CreatedAt = CreatedAt, UpdatedAt = UpdatedAt };
        copy.ApplyConfigurationFrom(source);
        return copy;
    }
}

public class SubscriptionInfo : IMessageEntity
{
    public string Name { get; set; } = string.Empty;
    public string TopicName { get; set; } = string.Empty;
    public bool RequiresSession { get; set; }
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(60);
    public int MaxDeliveryCount { get; set; } = 10;
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;
    public List<RuleInfo> Rules { get; } = new();

    // Settings the management API stores and returns; DeadLetteringOnFilterEvaluationExceptions, batching and status
    // are not enforced.
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public bool DeadLetteringOnFilterEvaluationExceptions { get; set; } = true;
    public bool EnableBatchedOperations { get; set; } = true;
    public string Status { get; set; } = "Active";
    private string? _forwardTo;

    /// <summary>Forward target exactly as the client supplied it (the SDK sends an absolute URI).</summary>
    public string? ForwardTo
    {
        get => _forwardTo;
        set { _forwardTo = value; ForwardToEntityName = ForwardTarget.ToEntityName(value); }
    }

    public string? ForwardToEntityName { get; private set; }
    private string? _forwardDeadLetteredMessagesTo;

    /// <summary>Dead-letter forward target exactly as the client supplied it.</summary>
    public string? ForwardDeadLetteredMessagesTo
    {
        get => _forwardDeadLetteredMessagesTo;
        set { _forwardDeadLetteredMessagesTo = value; ForwardDeadLetteredMessagesToEntityName = ForwardTarget.ToEntityName(value); }
    }

    public string? ForwardDeadLetteredMessagesToEntityName { get; private set; }
    public string? UserMetadata { get; set; }
    public TimeSpan AutoDeleteOnIdle { get; set; } = TimeSpan.MaxValue;
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }

    /// <summary>Copies every client-configurable setting from <paramref name="source"/>; names, rules, timestamps and runtime state are kept.</summary>
    public void ApplyConfigurationFrom(SubscriptionInfo source)
    {
        RequiresSession = source.RequiresSession;
        LockDuration = source.LockDuration;
        MaxDeliveryCount = source.MaxDeliveryCount;
        DefaultMessageTimeToLive = source.DefaultMessageTimeToLive;
        DeadLetteringOnMessageExpiration = source.DeadLetteringOnMessageExpiration;
        DeadLetteringOnFilterEvaluationExceptions = source.DeadLetteringOnFilterEvaluationExceptions;
        EnableBatchedOperations = source.EnableBatchedOperations;
        Status = source.Status;
        ForwardTo = source.ForwardTo;
        ForwardDeadLetteredMessagesTo = source.ForwardDeadLetteredMessagesTo;
        UserMetadata = source.UserMetadata;
        AutoDeleteOnIdle = source.AutoDeleteOnIdle;
    }

    /// <summary>A detached copy of this subscription's identity and timestamps with <paramref name="source"/>'s settings.</summary>
    public SubscriptionInfo WithConfigurationFrom(SubscriptionInfo source)
    {
        var copy = new SubscriptionInfo { Name = Name, TopicName = TopicName, CreatedAt = CreatedAt, UpdatedAt = UpdatedAt };
        copy.ApplyConfigurationFrom(source);
        return copy;
    }

    private long _messageCount;
    private long _deadLetterMessageCount;
    private long _nextSequenceNumber;

    public long MessageCount
    {
        get => Volatile.Read(ref _messageCount);
        set => Volatile.Write(ref _messageCount, value);
    }

    public long NextSequenceNumber
    {
        get => Volatile.Read(ref _nextSequenceNumber);
        set => Volatile.Write(ref _nextSequenceNumber, value);
    }

    public long DeadLetterMessageCount
    {
        get => Volatile.Read(ref _deadLetterMessageCount);
        set => Volatile.Write(ref _deadLetterMessageCount, value);
    }

    public long AssignNextSequenceNumber() => Interlocked.Increment(ref _nextSequenceNumber);
    public void UpdateNextSequenceNumber(long seqNum)
    {
        long currentNext = Volatile.Read(ref _nextSequenceNumber);
        while (seqNum >= currentNext)
        {
            if (Interlocked.CompareExchange(ref _nextSequenceNumber, seqNum + 1, currentNext) == currentNext)
                break;
            currentNext = Volatile.Read(ref _nextSequenceNumber);
        }
    }
    public void IncrementMessageCount() => Interlocked.Increment(ref _messageCount);
    public void DecrementMessageCount() => Interlocked.Decrement(ref _messageCount);
    public void IncrementDeadLetterMessageCount() => Interlocked.Increment(ref _deadLetterMessageCount);
    public void DecrementDeadLetterMessageCount() => Interlocked.Decrement(ref _deadLetterMessageCount);

    // Runtime state
    public MessageStore? MessageStore { get; set; }
    public Dispatcher? Dispatcher { get; set; }
    public MessageStore? DlqMessageStore { get; set; }
    public Dispatcher? DlqDispatcher { get; set; }
    public ConcurrentDictionary<string, SessionMessageQueue> SessionMessageAddresses { get; } = new(StringComparer.OrdinalIgnoreCase);
    public EntityActivity Activity { get; } = new();
}


