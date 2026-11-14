using Microsoft.Data.Sqlite;

namespace AzsbEmu.Server.Core;

// Mapping between TopicInfo / SubscriptionInfo and their tables, following the same rules as EntityManager.QueueRows:
// added columns are additive and nullable (NULL reads back as the property default), durations are stored as ticks,
// and the original whole-second subscription columns are still written and read only for older rows.
public partial class EntityManager
{
    private static readonly (string Name, string Type)[] AddedTopicColumns =
    [
        ("DefaultTtlTicks", "INTEGER"),
        ("MaxSizeInMegabytes", "INTEGER"),
        ("RequiresDuplicateDetection", "INTEGER"),
        ("DuplicateDetectionWindowTicks", "INTEGER"),
        ("EnableBatchedOperations", "INTEGER"),
        ("EnablePartitioning", "INTEGER"),
        ("SupportOrdering", "INTEGER"),
        ("AutoDeleteOnIdleTicks", "INTEGER"),
        ("Status", "TEXT"),
        ("UserMetadata", "TEXT"),
        ("MaxMessageSizeInKilobytes", "INTEGER"),
        ("CreatedAtTicks", "INTEGER"),
        ("UpdatedAtTicks", "INTEGER"),
        ("LastActivityTicks", "INTEGER"),
    ];

    private static readonly (string Name, string Type)[] AddedSubscriptionColumns =
    [
        ("LockDurationTicks", "INTEGER"),
        ("DefaultTtlTicks", "INTEGER"),
        ("DeadLetteringOnExpiration", "INTEGER"),
        ("DeadLetteringOnFilterExceptions", "INTEGER"),
        ("EnableBatchedOperations", "INTEGER"),
        ("Status", "TEXT"),
        ("ForwardTo", "TEXT"),
        ("ForwardDeadLetteredMessagesTo", "TEXT"),
        ("UserMetadata", "TEXT"),
        ("AutoDeleteOnIdleTicks", "INTEGER"),
        ("CreatedAtTicks", "INTEGER"),
        ("UpdatedAtTicks", "INTEGER"),
        ("LastActivityTicks", "INTEGER"),
    ];

    private static void AddColumns(SqliteConnection connection, string table, (string Name, string Type)[] columns)
    {
        foreach (var (name, type) in columns)
        {
            if (ColumnExists(connection, table, name)) continue;
            var alter = connection.CreateCommand();
            alter.CommandText = $"ALTER TABLE {table} ADD COLUMN {name} {type}";
            alter.ExecuteNonQuery();
        }
    }

    // ---------------- Topics ----------------

    private static (string Column, object? Value)[] TopicKey(TopicInfo t) => [("Name", t.Name)];

    private static (string Column, object? Value)[] TopicColumns(TopicInfo t) =>
    [
        ("DefaultTtlTicks", t.DefaultMessageTimeToLive.Ticks),
        ("MaxSizeInMegabytes", t.MaxSizeInMegabytes),
        ("RequiresDuplicateDetection", t.RequiresDuplicateDetection ? 1 : 0),
        ("DuplicateDetectionWindowTicks", t.DuplicateDetectionHistoryTimeWindow.Ticks),
        ("EnableBatchedOperations", t.EnableBatchedOperations ? 1 : 0),
        ("EnablePartitioning", t.EnablePartitioning ? 1 : 0),
        ("SupportOrdering", t.SupportOrdering ? 1 : 0),
        ("AutoDeleteOnIdleTicks", t.AutoDeleteOnIdle.Ticks),
        ("Status", t.Status),
        ("UserMetadata", t.UserMetadata),
        ("MaxMessageSizeInKilobytes", t.MaxMessageSizeInKilobytes),
        ("CreatedAtTicks", t.CreatedAt.Ticks),
        ("UpdatedAtTicks", t.UpdatedAt.Ticks),
    ];

    private static TopicInfo ReadTopicRow(SqliteDataReader r)
    {
        var defaults = new TopicInfo();
        var loadedAt = DateTime.UtcNow; // stable value for rows that predate the timestamp columns
        var topic = new TopicInfo
        {
            Name = r.GetString(r.GetOrdinal("Name")),
            DefaultMessageTimeToLive = Long(r, "DefaultTtlTicks") is long ttl ? TimeSpan.FromTicks(ttl) : defaults.DefaultMessageTimeToLive,
            MaxSizeInMegabytes = Long(r, "MaxSizeInMegabytes") ?? defaults.MaxSizeInMegabytes,
            RequiresDuplicateDetection = Long(r, "RequiresDuplicateDetection") == 1,
            DuplicateDetectionHistoryTimeWindow = Long(r, "DuplicateDetectionWindowTicks") is long w ? TimeSpan.FromTicks(w) : defaults.DuplicateDetectionHistoryTimeWindow,
            EnableBatchedOperations = Long(r, "EnableBatchedOperations") is long b ? b == 1 : defaults.EnableBatchedOperations,
            EnablePartitioning = Long(r, "EnablePartitioning") == 1,
            SupportOrdering = Long(r, "SupportOrdering") is long o ? o == 1 : defaults.SupportOrdering,
            AutoDeleteOnIdle = Long(r, "AutoDeleteOnIdleTicks") is long a ? TimeSpan.FromTicks(a) : defaults.AutoDeleteOnIdle,
            Status = Text(r, "Status") ?? defaults.Status,
            UserMetadata = Text(r, "UserMetadata"),
            MaxMessageSizeInKilobytes = Long(r, "MaxMessageSizeInKilobytes"),
            CreatedAt = Long(r, "CreatedAtTicks") is long c ? new DateTime(c, DateTimeKind.Utc) : loadedAt,
            UpdatedAt = Long(r, "UpdatedAtTicks") is long u ? new DateTime(u, DateTimeKind.Utc) : loadedAt,
        };
        RestoreActivity(r, topic.Activity, topic.UpdatedAt);
        return topic;
    }

    // ---------------- Subscriptions ----------------

    private static (string Column, object? Value)[] SubscriptionKey(SubscriptionInfo s) => [("TopicName", s.TopicName), ("Name", s.Name)];

    private static (string Column, object? Value)[] SubscriptionColumns(SubscriptionInfo s) =>
    [
        ("RequiresSession", s.RequiresSession ? 1 : 0),
        ("LockDurationSeconds", (int)s.LockDuration.TotalSeconds),
        ("MaxDeliveryCount", s.MaxDeliveryCount),
        ("DefaultTtlSeconds", s.DefaultMessageTimeToLive == TimeSpan.MaxValue ? -1 : (long)s.DefaultMessageTimeToLive.TotalSeconds),
        ("LockDurationTicks", s.LockDuration.Ticks),
        ("DefaultTtlTicks", s.DefaultMessageTimeToLive.Ticks),
        ("DeadLetteringOnExpiration", s.DeadLetteringOnMessageExpiration ? 1 : 0),
        ("DeadLetteringOnFilterExceptions", s.DeadLetteringOnFilterEvaluationExceptions ? 1 : 0),
        ("EnableBatchedOperations", s.EnableBatchedOperations ? 1 : 0),
        ("Status", s.Status),
        ("ForwardTo", s.ForwardTo),
        ("ForwardDeadLetteredMessagesTo", s.ForwardDeadLetteredMessagesTo),
        ("UserMetadata", s.UserMetadata),
        ("AutoDeleteOnIdleTicks", s.AutoDeleteOnIdle.Ticks),
        ("CreatedAtTicks", s.CreatedAt.Ticks),
        ("UpdatedAtTicks", s.UpdatedAt.Ticks),
    ];

    private static SubscriptionInfo ReadSubscriptionRow(SqliteDataReader r)
    {
        var defaults = new SubscriptionInfo();
        var loadedAt = DateTime.UtcNow;
        var sub = new SubscriptionInfo
        {
            TopicName = r.GetString(r.GetOrdinal("TopicName")),
            Name = r.GetString(r.GetOrdinal("Name")),
            RequiresSession = Long(r, "RequiresSession") == 1,
            LockDuration = Long(r, "LockDurationTicks") is long lockTicks ? TimeSpan.FromTicks(lockTicks)
                : Long(r, "LockDurationSeconds") is long lockSeconds ? TimeSpan.FromSeconds(lockSeconds)
                : defaults.LockDuration,
            MaxDeliveryCount = (int)(Long(r, "MaxDeliveryCount") ?? defaults.MaxDeliveryCount),
            DefaultMessageTimeToLive = Long(r, "DefaultTtlTicks") is long ttlTicks ? TimeSpan.FromTicks(ttlTicks)
                : Long(r, "DefaultTtlSeconds") is > 0 and long ttlSeconds ? TimeSpan.FromSeconds(ttlSeconds)
                : defaults.DefaultMessageTimeToLive,
            DeadLetteringOnMessageExpiration = Long(r, "DeadLetteringOnExpiration") == 1,
            DeadLetteringOnFilterEvaluationExceptions = Long(r, "DeadLetteringOnFilterExceptions") is long f ? f == 1 : defaults.DeadLetteringOnFilterEvaluationExceptions,
            EnableBatchedOperations = Long(r, "EnableBatchedOperations") is long b ? b == 1 : defaults.EnableBatchedOperations,
            Status = Text(r, "Status") ?? defaults.Status,
            ForwardTo = Text(r, "ForwardTo"),
            ForwardDeadLetteredMessagesTo = Text(r, "ForwardDeadLetteredMessagesTo"),
            UserMetadata = Text(r, "UserMetadata"),
            AutoDeleteOnIdle = Long(r, "AutoDeleteOnIdleTicks") is long a ? TimeSpan.FromTicks(a) : defaults.AutoDeleteOnIdle,
            CreatedAt = Long(r, "CreatedAtTicks") is long c ? new DateTime(c, DateTimeKind.Utc) : loadedAt,
            UpdatedAt = Long(r, "UpdatedAtTicks") is long u ? new DateTime(u, DateTimeKind.Utc) : loadedAt,
        };
        RestoreActivity(r, sub.Activity, sub.UpdatedAt);
        return sub;
    }
}
