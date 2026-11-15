namespace SbBrowser.Core.Entities;

/// <summary>Identifies a queue, topic or subscription. <see cref="Path"/> is the entity path the SDK uses.</summary>
public abstract record EntityRef(string Path)
{
    /// <summary>The name shown to users: the queue or topic name, or the subscription's own name.</summary>
    public abstract string DisplayName { get; }
}

public sealed record QueueRef(string Name) : EntityRef(Name)
{
    public override string DisplayName => Name;
}

public sealed record TopicRef(string Name) : EntityRef(Name)
{
    public override string DisplayName => Name;
}

public sealed record SubscriptionRef(string Topic, string Name) : EntityRef($"{Topic}/Subscriptions/{Name}")
{
    public override string DisplayName => Name;
}

/// <summary>
/// Message counts. Subscriptions report no scheduled count: scheduled messages wait on their topic until enqueued.
/// </summary>
public sealed record EntityCounts(long Active, long DeadLetter, long Scheduled, long Transfer, long TransferDeadLetter)
{
    public static readonly EntityCounts None = new(0, 0, 0, 0, 0);

    public long Total => Active + DeadLetter + Scheduled + Transfer + TransferDeadLetter;

    /// <summary>Dead letters, including messages that failed to forward.</summary>
    public long AllDeadLetters => DeadLetter + TransferDeadLetter;
}

/// <summary>
/// Settings shown alongside a queue or subscription. <see cref="ForwardTo"/> and
/// <see cref="ForwardDeadLetteredMessagesTo"/> are entity names (a full endpoint URL is reduced to its path).
/// </summary>
public sealed record ReceiverSettings(
    bool RequiresSession,
    int MaxDeliveryCount,
    TimeSpan LockDuration,
    TimeSpan DefaultMessageTimeToLive,
    bool DeadLetteringOnMessageExpiration,
    string? ForwardTo,
    string? ForwardDeadLetteredMessagesTo,
    string Status);

/// <summary>What a subscription rule's filter is.</summary>
public enum RuleFilterKind
{
    /// <summary>Every message matches (a TrueFilter, or the SQL filter <c>1=1</c>).</summary>
    True,

    /// <summary>No message matches.</summary>
    False,

    Sql,
    Correlation,
}

/// <summary>
/// A subscription rule: <see cref="Filter"/> is the filter as text (an SQL expression, or the correlation filter's
/// properties as <c>Name = 'value'</c> pairs); <see cref="Action"/> is the SQL action, if any.
/// </summary>
public sealed record RuleView(string Name, RuleFilterKind Kind, string Filter, string? Action)
{
    public bool MatchesAll => Kind == RuleFilterKind.True;
}

public sealed record QueueNode(QueueRef Ref, EntityCounts Counts, ReceiverSettings Settings);

/// <summary>A subscription with its rules. A message reaches it when any rule matches.</summary>
public sealed record SubscriptionNode(SubscriptionRef Ref, EntityCounts Counts, ReceiverSettings Settings, IReadOnlyList<RuleView> Rules)
{
    /// <summary>True when some rule lets every message in.</summary>
    public bool ReceivesAll => Rules.Any(r => r.MatchesAll);
}

public sealed record TopicNode(TopicRef Ref, long ScheduledCount, string Status, IReadOnlyList<SubscriptionNode> Subscriptions);

/// <summary>
/// An entity that forwards into another: its messages (<see cref="DeadLetters"/> false) or its dead letters
/// (<see cref="DeadLetters"/> true).
/// </summary>
public sealed record Feeder(EntityRef Source, bool DeadLetters);

/// <summary>Every queue, topic and subscription in a namespace, sorted by name.</summary>
public sealed record NamespaceTree(IReadOnlyList<QueueNode> Queues, IReadOnlyList<TopicNode> Topics)
{
    private static readonly StringComparer Names = StringComparer.OrdinalIgnoreCase;

    public static readonly NamespaceTree Empty = new([], []);

    public IEnumerable<SubscriptionNode> Subscriptions => Topics.SelectMany(t => t.Subscriptions);

    public QueueNode? FindQueue(string name) => Queues.FirstOrDefault(q => Names.Equals(q.Ref.Name, name));

    public TopicNode? FindTopic(string name) => Topics.FirstOrDefault(t => Names.Equals(t.Ref.Name, name));

    public SubscriptionNode? FindSubscription(string topic, string name) =>
        FindTopic(topic)?.Subscriptions.FirstOrDefault(s => Names.Equals(s.Ref.Name, name));

    /// <summary>The queue or topic named <paramref name="name"/>, or null when there is none.</summary>
    public EntityRef? FindQueueOrTopic(string name) =>
        (EntityRef?)FindQueue(name)?.Ref ?? FindTopic(name)?.Ref;

    /// <summary>
    /// This tree with <paramref name="entity"/>'s counts replaced (for a topic, its scheduled count). Unchanged when the
    /// entity is not in the tree.
    /// </summary>
    public NamespaceTree WithCounts(EntityRef entity, EntityCounts counts) => entity switch
    {
        QueueRef q => this with
        {
            Queues = Queues.Select(n => Names.Equals(n.Ref.Name, q.Name) ? n with { Counts = counts } : n).ToList(),
        },
        SubscriptionRef s => this with
        {
            Topics = Topics.Select(t => !Names.Equals(t.Ref.Name, s.Topic) ? t : t with
            {
                Subscriptions = t.Subscriptions.Select(n => Names.Equals(n.Ref.Name, s.Name) ? n with { Counts = counts } : n).ToList(),
            }).ToList(),
        },
        TopicRef topic => this with
        {
            Topics = Topics.Select(t => Names.Equals(t.Ref.Name, topic.Name) ? t with { ScheduledCount = counts.Scheduled } : t).ToList(),
        },
        _ => this,
    };

    /// <summary>The queues and subscriptions that forward messages or dead letters to <paramref name="entityName"/>.</summary>
    public IReadOnlyList<Feeder> FeedersOf(string entityName)
    {
        var feeders = new List<Feeder>();
        foreach (var q in Queues) AddFeeders(feeders, q.Ref, q.Settings, entityName);
        foreach (var s in Subscriptions) AddFeeders(feeders, s.Ref, s.Settings, entityName);
        return feeders;
    }

    private static void AddFeeders(List<Feeder> feeders, EntityRef source, ReceiverSettings settings, string target)
    {
        if (Names.Equals(settings.ForwardTo, target)) feeders.Add(new Feeder(source, DeadLetters: false));
        if (Names.Equals(settings.ForwardDeadLetteredMessagesTo, target)) feeders.Add(new Feeder(source, DeadLetters: true));
    }
}
