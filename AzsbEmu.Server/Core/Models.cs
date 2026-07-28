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
    TimeSpan LockDuration { get; }
    int MaxDeliveryCount { get; }
    TimeSpan DefaultMessageTimeToLive { get; }
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
    ConcurrentDictionary<string, ConcurrentQueue<long>> SessionMessageAddresses { get; }
    bool RequiresSession { get; }
}

public class RuleInfo
{
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
}

public class QueueInfo : IMessageEntity
{
    public string Name { get; set; } = string.Empty;
    public bool RequiresSession { get; set; }
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(60);
    public long MaxSizeInMegabytes { get; set; } = 1024;
    public int MaxDeliveryCount { get; set; } = 10;
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

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
    public ConcurrentDictionary<string, ConcurrentQueue<long>> SessionMessageAddresses { get; } = new(StringComparer.OrdinalIgnoreCase);
}

public class TopicInfo
{
    public string Name { get; set; } = string.Empty;
    public List<SubscriptionInfo> Subscriptions { get; set; } = new();
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
    public ConcurrentDictionary<string, ConcurrentQueue<long>> SessionMessageAddresses { get; } = new(StringComparer.OrdinalIgnoreCase);
}


