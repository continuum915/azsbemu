using System.Xml;
using System.Xml.Linq;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Server.Protocol.Http;

/// <summary>Message counts reported in an entity's runtime properties.</summary>
internal readonly record struct EntityCounts(long Active, long DeadLetter, long Scheduled)
{
    public long Total => Active + DeadLetter + Scheduled;
}

/// <summary>
/// Reads and writes the Atom documents the Service Bus management API (and so <c>ServiceBusAdministrationClient</c>)
/// exchanges: an Atom <c>entry</c> whose <c>content</c> holds a <c>QueueDescription</c>.
/// Reading is by element name, so element order does not matter; writing follows the service's order.
/// </summary>
internal static class AtomSerializer
{
    public static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";
    public static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    public static readonly XNamespace Xsi = "http://www.w3.org/2001/XMLSchema-instance";
    /// <summary>Namespace of the runtime <c>CountDetails</c> children.</summary>
    public static readonly XNamespace Counts = "http://schemas.microsoft.com/netservices/2011/06/servicebus";

    /// <summary>Returns the description element inside an entry body (e.g. <c>QueueDescription</c>), or null.</summary>
    public static XElement? ReadDescription(string body)
    {
        var entry = XDocument.Parse(body).Root;
        return entry?.Element(Atom + "content")?.Elements().FirstOrDefault();
    }

    public static QueueInfo ReadQueue(string name, XElement d)
    {
        var defaults = new QueueInfo();
        return new QueueInfo
        {
            Name = name,
            LockDuration = Duration(d, "LockDuration") ?? defaults.LockDuration,
            MaxSizeInMegabytes = Long(d, "MaxSizeInMegabytes") ?? defaults.MaxSizeInMegabytes,
            RequiresDuplicateDetection = Bool(d, "RequiresDuplicateDetection") ?? defaults.RequiresDuplicateDetection,
            RequiresSession = Bool(d, "RequiresSession") ?? defaults.RequiresSession,
            DefaultMessageTimeToLive = Duration(d, "DefaultMessageTimeToLive") ?? defaults.DefaultMessageTimeToLive,
            DeadLetteringOnMessageExpiration = Bool(d, "DeadLetteringOnMessageExpiration") ?? defaults.DeadLetteringOnMessageExpiration,
            DuplicateDetectionHistoryTimeWindow = Duration(d, "DuplicateDetectionHistoryTimeWindow") ?? defaults.DuplicateDetectionHistoryTimeWindow,
            MaxDeliveryCount = (int)(Long(d, "MaxDeliveryCount") ?? defaults.MaxDeliveryCount),
            EnableBatchedOperations = Bool(d, "EnableBatchedOperations") ?? defaults.EnableBatchedOperations,
            Status = Text(d, "Status") is { Length: > 0 } status ? status : defaults.Status,
            ForwardTo = NonEmpty(Text(d, "ForwardTo")),
            ForwardDeadLetteredMessagesTo = NonEmpty(Text(d, "ForwardDeadLetteredMessagesTo")),
            UserMetadata = NonEmpty(Text(d, "UserMetadata")),
            AutoDeleteOnIdle = Duration(d, "AutoDeleteOnIdle") ?? defaults.AutoDeleteOnIdle,
            EnablePartitioning = Bool(d, "EnablePartitioning") ?? defaults.EnablePartitioning,
            MaxMessageSizeInKilobytes = Long(d, "MaxMessageSizeInKilobytes"),
        };
    }

    public static string WriteQueueEntry(QueueInfo q, EntityCounts counts, string baseAddress)
    {
        var description = new XElement(Sb + "QueueDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            new XElement(Sb + "LockDuration", XmlConvert.ToString(q.LockDuration)),
            new XElement(Sb + "MaxSizeInMegabytes", q.MaxSizeInMegabytes),
            new XElement(Sb + "RequiresDuplicateDetection", q.RequiresDuplicateDetection),
            new XElement(Sb + "RequiresSession", q.RequiresSession),
            new XElement(Sb + "DefaultMessageTimeToLive", XmlConvert.ToString(q.DefaultMessageTimeToLive)),
            new XElement(Sb + "DeadLetteringOnMessageExpiration", q.DeadLetteringOnMessageExpiration),
            new XElement(Sb + "DuplicateDetectionHistoryTimeWindow", XmlConvert.ToString(q.DuplicateDetectionHistoryTimeWindow)),
            new XElement(Sb + "MaxDeliveryCount", q.MaxDeliveryCount),
            new XElement(Sb + "EnableBatchedOperations", q.EnableBatchedOperations),
            new XElement(Sb + "SizeInBytes", 0),
            new XElement(Sb + "MessageCount", counts.Total),
            new XElement(Sb + "Status", q.Status),
            Optional("ForwardTo", q.ForwardTo),
            new XElement(Sb + "CreatedAt", Timestamp(q.CreatedAt)),
            new XElement(Sb + "UpdatedAt", Timestamp(q.UpdatedAt)),
            new XElement(Sb + "AccessedAt", Timestamp(q.Activity.LastActivityUtc)),
            CountDetails(counts),
            new XElement(Sb + "AutoDeleteOnIdle", XmlConvert.ToString(q.AutoDeleteOnIdle)),
            new XElement(Sb + "EnablePartitioning", q.EnablePartitioning),
            Optional("ForwardDeadLetteredMessagesTo", q.ForwardDeadLetteredMessagesTo),
            Optional("UserMetadata", q.UserMetadata),
            q.MaxMessageSizeInKilobytes is long kb ? new XElement(Sb + "MaxMessageSizeInKilobytes", kb) : null);

        return WriteEntry(q.Name, baseAddress, description);
    }

    public static TopicInfo ReadTopic(string name, XElement d)
    {
        var defaults = new TopicInfo();
        return new TopicInfo
        {
            Name = name,
            DefaultMessageTimeToLive = Duration(d, "DefaultMessageTimeToLive") ?? defaults.DefaultMessageTimeToLive,
            MaxSizeInMegabytes = Long(d, "MaxSizeInMegabytes") ?? defaults.MaxSizeInMegabytes,
            RequiresDuplicateDetection = Bool(d, "RequiresDuplicateDetection") ?? defaults.RequiresDuplicateDetection,
            DuplicateDetectionHistoryTimeWindow = Duration(d, "DuplicateDetectionHistoryTimeWindow") ?? defaults.DuplicateDetectionHistoryTimeWindow,
            EnableBatchedOperations = Bool(d, "EnableBatchedOperations") ?? defaults.EnableBatchedOperations,
            EnablePartitioning = Bool(d, "EnablePartitioning") ?? defaults.EnablePartitioning,
            SupportOrdering = Bool(d, "SupportOrdering") ?? defaults.SupportOrdering,
            AutoDeleteOnIdle = Duration(d, "AutoDeleteOnIdle") ?? defaults.AutoDeleteOnIdle,
            Status = NonEmpty(Text(d, "Status")) ?? defaults.Status,
            UserMetadata = NonEmpty(Text(d, "UserMetadata")),
            MaxMessageSizeInKilobytes = Long(d, "MaxMessageSizeInKilobytes"),
        };
    }

    public static string WriteTopicEntry(TopicInfo t, EntityCounts counts, int subscriptionCount, string baseAddress)
    {
        var description = new XElement(Sb + "TopicDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            new XElement(Sb + "DefaultMessageTimeToLive", XmlConvert.ToString(t.DefaultMessageTimeToLive)),
            new XElement(Sb + "MaxSizeInMegabytes", t.MaxSizeInMegabytes),
            new XElement(Sb + "RequiresDuplicateDetection", t.RequiresDuplicateDetection),
            new XElement(Sb + "DuplicateDetectionHistoryTimeWindow", XmlConvert.ToString(t.DuplicateDetectionHistoryTimeWindow)),
            new XElement(Sb + "EnableBatchedOperations", t.EnableBatchedOperations),
            new XElement(Sb + "SizeInBytes", 0),
            new XElement(Sb + "Status", t.Status),
            new XElement(Sb + "CreatedAt", Timestamp(t.CreatedAt)),
            new XElement(Sb + "UpdatedAt", Timestamp(t.UpdatedAt)),
            new XElement(Sb + "AccessedAt", Timestamp(t.Activity.LastActivityUtc)),
            new XElement(Sb + "SupportOrdering", t.SupportOrdering),
            CountDetails(counts),
            new XElement(Sb + "SubscriptionCount", subscriptionCount),
            new XElement(Sb + "AutoDeleteOnIdle", XmlConvert.ToString(t.AutoDeleteOnIdle)),
            new XElement(Sb + "EnablePartitioning", t.EnablePartitioning),
            Optional("UserMetadata", t.UserMetadata),
            t.MaxMessageSizeInKilobytes is long kb ? new XElement(Sb + "MaxMessageSizeInKilobytes", kb) : null);

        return WriteEntry(t.Name, baseAddress, description);
    }

    public static SubscriptionInfo ReadSubscription(string topicName, string name, XElement d)
    {
        var defaults = new SubscriptionInfo();
        return new SubscriptionInfo
        {
            TopicName = topicName,
            Name = name,
            LockDuration = Duration(d, "LockDuration") ?? defaults.LockDuration,
            RequiresSession = Bool(d, "RequiresSession") ?? defaults.RequiresSession,
            DefaultMessageTimeToLive = Duration(d, "DefaultMessageTimeToLive") ?? defaults.DefaultMessageTimeToLive,
            DeadLetteringOnMessageExpiration = Bool(d, "DeadLetteringOnMessageExpiration") ?? defaults.DeadLetteringOnMessageExpiration,
            DeadLetteringOnFilterEvaluationExceptions = Bool(d, "DeadLetteringOnFilterEvaluationExceptions") ?? defaults.DeadLetteringOnFilterEvaluationExceptions,
            MaxDeliveryCount = (int)(Long(d, "MaxDeliveryCount") ?? defaults.MaxDeliveryCount),
            EnableBatchedOperations = Bool(d, "EnableBatchedOperations") ?? defaults.EnableBatchedOperations,
            Status = NonEmpty(Text(d, "Status")) ?? defaults.Status,
            ForwardTo = NonEmpty(Text(d, "ForwardTo")),
            ForwardDeadLetteredMessagesTo = NonEmpty(Text(d, "ForwardDeadLetteredMessagesTo")),
            UserMetadata = NonEmpty(Text(d, "UserMetadata")),
            AutoDeleteOnIdle = Duration(d, "AutoDeleteOnIdle") ?? defaults.AutoDeleteOnIdle,
        };
    }

    public static string WriteSubscriptionEntry(SubscriptionInfo s, EntityCounts counts, string baseAddress)
    {
        var description = new XElement(Sb + "SubscriptionDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            new XElement(Sb + "LockDuration", XmlConvert.ToString(s.LockDuration)),
            new XElement(Sb + "RequiresSession", s.RequiresSession),
            new XElement(Sb + "DefaultMessageTimeToLive", XmlConvert.ToString(s.DefaultMessageTimeToLive)),
            new XElement(Sb + "DeadLetteringOnMessageExpiration", s.DeadLetteringOnMessageExpiration),
            new XElement(Sb + "DeadLetteringOnFilterEvaluationExceptions", s.DeadLetteringOnFilterEvaluationExceptions),
            new XElement(Sb + "MessageCount", counts.Total),
            new XElement(Sb + "MaxDeliveryCount", s.MaxDeliveryCount),
            new XElement(Sb + "EnableBatchedOperations", s.EnableBatchedOperations),
            new XElement(Sb + "Status", s.Status),
            Optional("ForwardTo", s.ForwardTo),
            new XElement(Sb + "CreatedAt", Timestamp(s.CreatedAt)),
            new XElement(Sb + "UpdatedAt", Timestamp(s.UpdatedAt)),
            new XElement(Sb + "AccessedAt", Timestamp(s.Activity.LastActivityUtc)),
            CountDetails(counts),
            Optional("UserMetadata", s.UserMetadata),
            Optional("ForwardDeadLetteredMessagesTo", s.ForwardDeadLetteredMessagesTo),
            new XElement(Sb + "AutoDeleteOnIdle", XmlConvert.ToString(s.AutoDeleteOnIdle)));

        return WriteEntry($"{s.TopicName}/Subscriptions/{s.Name}", baseAddress, description, title: s.Name);
    }

    /// <summary>
    /// Maps a <c>RuleDescription</c> (also used as a subscription's <c>DefaultRuleDescription</c>) onto the rule model
    /// the AMQP rule operations use: SQL filters keep their expression, correlation filters become a serialized
    /// <see cref="CorrelationFilterDto"/>, and a SQL rule action keeps its expression.
    /// </summary>
    public static RuleInfo ReadRule(XElement d)
    {
        var rule = new RuleInfo { Name = NonEmpty(Text(d, "Name")) ?? "$Default" };

        var filter = d.Element(Sb + "Filter");
        switch (XsiType(filter))
        {
            case "SqlFilter":
                rule.FilterType = "Sql";
                rule.FilterExpression = Text(filter!, "SqlExpression") ?? "";
                rule.NormalizeFilter(); // "1=1" / "1=0" are the true and false filters
                if (rule.FilterType == "Sql")
                    RethrowAsFormatException(() => SqlFilterEvaluator.Validate(rule.FilterExpression), "SQL filter");
                break;
            case "CorrelationFilter":
                rule.FilterType = "Correlation";
                rule.FilterExpression = System.Text.Json.JsonSerializer.Serialize(ReadCorrelationFilter(filter!));
                break;
            case "FalseFilter":
                rule.FilterType = "False";
                break;
            case "TrueFilter" or null:
                rule.FilterType = "True";
                break;
            case var other:
                throw new FormatException($"Unsupported rule filter type '{other}'.");
        }

        var action = d.Element(Sb + "Action");
        if (XsiType(action) == "SqlRuleAction")
        {
            rule.ActionExpression = Text(action!, "SqlExpression") ?? "";
            RethrowAsFormatException(() => SqlFilterEvaluator.ValidateAction(rule.ActionExpression), "SQL rule action");
        }

        return rule;
    }

    public static string WriteRuleEntry(RuleInfo rule, string topicName, string subscriptionName, string baseAddress)
    {
        var description = new XElement(Sb + "RuleDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            WriteFilter(rule),
            string.IsNullOrWhiteSpace(rule.ActionExpression)
                ? new XElement(Sb + "Action", new XAttribute(Xsi + "type", "EmptyRuleAction"))
                : new XElement(Sb + "Action", new XAttribute(Xsi + "type", "SqlRuleAction"),
                    new XElement(Sb + "SqlExpression", rule.ActionExpression),
                    new XElement(Sb + "CompatibilityLevel", 20)),
            new XElement(Sb + "Name", rule.Name));

        return WriteEntry($"{topicName}/Subscriptions/{subscriptionName}/Rules/{rule.Name}", baseAddress, description, title: rule.Name);
    }

    private static XElement WriteFilter(RuleInfo rule)
    {
        switch (rule.FilterType.ToLowerInvariant())
        {
            case "sql":
                return SqlFilterElement("SqlFilter", rule.FilterExpression);
            case "false":
                return SqlFilterElement("FalseFilter", "1=0");
            case "correlation":
                var dto = CorrelationFilterDto.Parse(rule.FilterExpression);
                return new XElement(Sb + "Filter", new XAttribute(Xsi + "type", "CorrelationFilter"),
                    Optional("CorrelationId", dto.CorrelationId),
                    Optional("MessageId", dto.MessageId),
                    Optional("To", dto.To),
                    Optional("ReplyTo", dto.ReplyTo),
                    Optional("Label", dto.Label),
                    Optional("SessionId", dto.SessionId),
                    Optional("ReplyToSessionId", dto.ReplyToSessionId),
                    Optional("ContentType", dto.ContentType),
                    new XElement(Sb + "Properties", dto.TypedProperties().Select(kv => WriteProperty(kv.Key, kv.Value))));
            default:
                return SqlFilterElement("TrueFilter", "1=1");
        }
    }

    private static XElement SqlFilterElement(string type, string expression) =>
        new(Sb + "Filter", new XAttribute(Xsi + "type", type),
            new XElement(Sb + "SqlExpression", expression),
            new XElement(Sb + "CompatibilityLevel", 20));

    private static readonly XNamespace Xsd = "http://www.w3.org/2001/XMLSchema";

    /// <summary>A correlation property with its xsd type, so the SDK reads back the type it sent.</summary>
    private static XElement WriteProperty(string key, object? value)
    {
        (string type, string text) = value switch
        {
            int i => ("int", XmlConvert.ToString(i)),
            long l => ("long", XmlConvert.ToString(l)),
            double d => ("double", XmlConvert.ToString(d)),
            bool b => ("boolean", XmlConvert.ToString(b)),
            _ => ("string", value?.ToString() ?? ""),
        };

        return new XElement(Sb + "KeyValueOfstringanyType",
            new XElement(Sb + "Key", key),
            new XElement(Sb + "Value", new XAttribute(XNamespace.Xmlns + "d5p1", Xsd), new XAttribute(Xsi + "type", $"d5p1:{type}"), text));
    }

    /// <summary>Runs a validator, turning its ArgumentException into the FormatException the API maps to 400.</summary>
    private static void RethrowAsFormatException(Action validate, string what)
    {
        try { validate(); }
        catch (ArgumentException ex) { throw new FormatException($"Invalid {what}: {ex.Message}", ex); }
    }

    private static CorrelationFilterDto ReadCorrelationFilter(XElement f)
    {
        var dto = new CorrelationFilterDto
        {
            CorrelationId = NonEmpty(Text(f, "CorrelationId")),
            MessageId = NonEmpty(Text(f, "MessageId")),
            To = NonEmpty(Text(f, "To")),
            ReplyTo = NonEmpty(Text(f, "ReplyTo")),
            Label = NonEmpty(Text(f, "Label")),
            SessionId = NonEmpty(Text(f, "SessionId")),
            ReplyToSessionId = NonEmpty(Text(f, "ReplyToSessionId")),
            ContentType = NonEmpty(Text(f, "ContentType")),
        };

        var properties = f.Element(Sb + "Properties")?.Elements().ToList();
        if (properties is { Count: > 0 })
        {
            dto.Properties = new Dictionary<string, object>();
            foreach (var kv in properties)
            {
                string? key = kv.Element(Sb + "Key")?.Value;
                var value = kv.Element(Sb + "Value");
                if (key is null || value is null) continue;
                dto.SetProperty(key, XsiType(value) switch
                {
                    "int" => int.Parse(value.Value),
                    "long" => long.Parse(value.Value),
                    "double" => XmlConvert.ToDouble(value.Value),
                    "boolean" => XmlConvert.ToBoolean(value.Value),
                    _ => value.Value,
                });
            }
        }

        return dto;
    }

    /// <summary>The local part of an element's <c>xsi:type</c> ("d6p1:string" → "string").</summary>
    private static string? XsiType(XElement? element)
    {
        string? type = element?.Attribute(Xsi + "type")?.Value;
        return type is null ? null : type[(type.IndexOf(':') + 1)..];
    }

    private static XElement CountDetails(EntityCounts counts) =>
        new(Sb + "CountDetails",
            new XAttribute(XNamespace.Xmlns + "d2p1", Counts),
            new XElement(Counts + "ActiveMessageCount", counts.Active),
            new XElement(Counts + "DeadLetterMessageCount", counts.DeadLetter),
            new XElement(Counts + "ScheduledMessageCount", counts.Scheduled),
            new XElement(Counts + "TransferMessageCount", 0),
            new XElement(Counts + "TransferDeadLetterMessageCount", 0));

    private static XElement? Optional(string name, string? value) => value is null ? null : new XElement(Sb + name, value);

    private static string Timestamp(DateTime utc) => XmlConvert.ToString(utc, XmlDateTimeSerializationMode.Utc);

    private static string? NonEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    private static string WriteEntry(string path, string baseAddress, XElement description, string? title = null)
    {
        string self = $"{baseAddress.TrimEnd('/')}/{path}?api-version=2021-05";
        string now = XmlConvert.ToString(DateTime.UtcNow, XmlDateTimeSerializationMode.Utc);
        var entry = new XElement(Atom + "entry",
            new XElement(Atom + "id", self),
            new XElement(Atom + "title", new XAttribute("type", "text"), title ?? path),
            new XElement(Atom + "published", now),
            new XElement(Atom + "updated", now),
            new XElement(Atom + "link", new XAttribute("rel", "self"), new XAttribute("href", self)),
            new XElement(Atom + "content", new XAttribute("type", "application/xml"), description));
        return entry.ToString(SaveOptions.DisableFormatting);
    }

    /// <summary>Wraps entries (as produced by the Write*Entry methods) in an Atom feed, as listings return them.</summary>
    public static string WriteFeed(string title, string selfUrl, IEnumerable<string> entries)
    {
        var feed = new XElement(Atom + "feed",
            new XElement(Atom + "title", new XAttribute("type", "text"), title),
            new XElement(Atom + "id", selfUrl),
            new XElement(Atom + "updated", XmlConvert.ToString(DateTime.UtcNow, XmlDateTimeSerializationMode.Utc)),
            new XElement(Atom + "link", new XAttribute("rel", "self"), new XAttribute("href", selfUrl)),
            entries.Select(XElement.Parse));
        return feed.ToString(SaveOptions.DisableFormatting);
    }

    public static string WriteError(int code, string detail) =>
        new XElement("Error", new XElement("Code", code), new XElement("Detail", detail)).ToString(SaveOptions.DisableFormatting);

    private static string? Text(XElement description, string name) => description.Element(Sb + name)?.Value;

    private static TimeSpan? Duration(XElement d, string name) => Text(d, name) is { Length: > 0 } v ? XmlConvert.ToTimeSpan(v) : null;
    private static long? Long(XElement d, string name) => Text(d, name) is { Length: > 0 } v ? long.Parse(v) : null;
    private static bool? Bool(XElement d, string name) => Text(d, name) is { Length: > 0 } v ? XmlConvert.ToBoolean(v) : null;
}
