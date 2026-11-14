namespace AzsbEmu.Server.Protocol.Http;

internal enum ManagementPathKind
{
    /// <summary>A queue or topic: <c>{name}</c>, where the name may itself contain '/'.</summary>
    Entity,
    /// <summary><c>{topic}/Subscriptions/{subscription}</c>.</summary>
    Subscription,
    /// <summary><c>{topic}/Subscriptions/{subscription}/Rules/{rule}</c>.</summary>
    Rule,
    /// <summary><c>$Resources/queues</c>.</summary>
    QueueList,
    /// <summary><c>$Resources/topics</c>.</summary>
    TopicList,
    /// <summary><c>{topic}/Subscriptions</c>.</summary>
    SubscriptionList,
    /// <summary><c>{topic}/Subscriptions/{subscription}/Rules</c>.</summary>
    RuleList,
    /// <summary>A reserved path the API does not serve (other <c>$</c>-prefixed resources).</summary>
    NotSupported,
}

/// <summary>
/// A management API request path. Entity names may contain '/', so the path is parsed from the right, segment by
/// segment: a trailing <c>Subscriptions/{name}</c> or <c>Subscriptions/{name}/Rules/{rule}</c> (or the listing forms
/// without the last name) belongs to the topic named by everything before it.
/// </summary>
/// <param name="EntityName">The queue or topic name; for subscriptions and rules, the topic's name.</param>
internal readonly record struct ManagementPath(ManagementPathKind Kind, string EntityName, string SubscriptionName = "", string RuleName = "")
{
    private const string Subscriptions = "Subscriptions";
    private const string Rules = "Rules";

    public static ManagementPath Parse(string path)
    {
        if (path.Equals("$Resources/queues", StringComparison.OrdinalIgnoreCase))
            return new ManagementPath(ManagementPathKind.QueueList, "");
        if (path.Equals("$Resources/topics", StringComparison.OrdinalIgnoreCase))
            return new ManagementPath(ManagementPathKind.TopicList, "");
        if (path.StartsWith('$'))
            return new ManagementPath(ManagementPathKind.NotSupported, "");

        var segments = path.Split('/');
        int n = segments.Length;
        string Topic(int segmentsFromEnd) => string.Join('/', segments[..(n - segmentsFromEnd)]);

        if (n >= 5 && Is(segments[n - 2], Rules) && Is(segments[n - 4], Subscriptions))   // {topic}/Subscriptions/{sub}/Rules/{rule}
            return new ManagementPath(ManagementPathKind.Rule, Topic(4), segments[n - 3], segments[n - 1]);
        if (n >= 4 && Is(segments[n - 1], Rules) && Is(segments[n - 3], Subscriptions))   // {topic}/Subscriptions/{sub}/Rules
            return new ManagementPath(ManagementPathKind.RuleList, Topic(3), segments[n - 2]);
        if (n >= 2 && Is(segments[n - 1], Subscriptions))                          // {topic}/Subscriptions
            return new ManagementPath(ManagementPathKind.SubscriptionList, Topic(1));
        if (n >= 3 && Is(segments[n - 2], Subscriptions))                          // {topic}/Subscriptions/{sub}
            return new ManagementPath(ManagementPathKind.Subscription, Topic(2), segments[n - 1]);

        return new ManagementPath(ManagementPathKind.Entity, path);
    }

    /// <summary>
    /// Why <paramref name="name"/> cannot be created as a queue or topic, or null if it can: names the parser would
    /// read as something else (empty, '$'-prefixed, or containing a whole <c>Subscriptions</c>/<c>Rules</c> segment).
    /// </summary>
    public static string? InvalidEntityNameReason(string name)
    {
        if (name.Length == 0) return "An entity name is required.";
        if (name.StartsWith('$')) return $"Entity names cannot start with '$': '{name}'.";
        foreach (var segment in name.Split('/'))
        {
            if (Is(segment, Subscriptions) || Is(segment, Rules))
                return $"Entity names cannot contain a '{segment}' path segment: '{name}'.";
        }
        return null;
    }

    private static bool Is(string segment, string keyword) => segment.Equals(keyword, StringComparison.OrdinalIgnoreCase);

    public override string ToString() => Kind switch
    {
        ManagementPathKind.Subscription => $"{EntityName}/{Subscriptions}/{SubscriptionName}",
        ManagementPathKind.Rule => $"{EntityName}/{Subscriptions}/{SubscriptionName}/{Rules}/{RuleName}",
        _ => EntityName,
    };
}
