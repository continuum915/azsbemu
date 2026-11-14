using Microsoft.Data.Sqlite;

namespace AzsbEmu.Server.Core;

// Mapping between QueueInfo and the Queues table. Columns added after the original schema are created by
// AddQueueColumns (additive, nullable), so older databases upgrade in place; NULL reads back as the property default.
// Durations are stored as ticks; the original whole-second columns are still written and are read only for rows
// that predate the tick columns.
public partial class EntityManager
{
    private static readonly (string Name, string Type)[] AddedQueueColumns =
    [
        ("LockDurationTicks", "INTEGER"),
        ("DefaultTtlTicks", "INTEGER"),
        ("RequiresDuplicateDetection", "INTEGER"),
        ("DuplicateDetectionWindowTicks", "INTEGER"),
        ("DeadLetteringOnExpiration", "INTEGER"),
        ("EnableBatchedOperations", "INTEGER"),
        ("Status", "TEXT"),
        ("ForwardTo", "TEXT"),
        ("ForwardDeadLetteredMessagesTo", "TEXT"),
        ("UserMetadata", "TEXT"),
        ("AutoDeleteOnIdleTicks", "INTEGER"),
        ("EnablePartitioning", "INTEGER"),
        ("MaxMessageSizeInKilobytes", "INTEGER"),
        ("CreatedAtTicks", "INTEGER"),
        ("UpdatedAtTicks", "INTEGER"),
        ("LastActivityTicks", "INTEGER"),
    ];

    private static void AddQueueColumns(SqliteConnection connection) => AddColumns(connection, "Queues", AddedQueueColumns);

    private static (string Column, object? Value)[] QueueKey(QueueInfo q) => [("Name", q.Name)];

    private static (string Column, object? Value)[] QueueColumns(QueueInfo q) =>
    [
        ("RequiresSession", q.RequiresSession ? 1 : 0),
        ("LockDurationSeconds", (int)q.LockDuration.TotalSeconds),
        ("MaxSizeInMegabytes", q.MaxSizeInMegabytes),
        ("MaxDeliveryCount", q.MaxDeliveryCount),
        ("DefaultTtlSeconds", q.DefaultMessageTimeToLive == TimeSpan.MaxValue ? -1 : (long)q.DefaultMessageTimeToLive.TotalSeconds),
        ("LockDurationTicks", q.LockDuration.Ticks),
        ("DefaultTtlTicks", q.DefaultMessageTimeToLive.Ticks),
        ("RequiresDuplicateDetection", q.RequiresDuplicateDetection ? 1 : 0),
        ("DuplicateDetectionWindowTicks", q.DuplicateDetectionHistoryTimeWindow.Ticks),
        ("DeadLetteringOnExpiration", q.DeadLetteringOnMessageExpiration ? 1 : 0),
        ("EnableBatchedOperations", q.EnableBatchedOperations ? 1 : 0),
        ("Status", q.Status),
        ("ForwardTo", q.ForwardTo),
        ("ForwardDeadLetteredMessagesTo", q.ForwardDeadLetteredMessagesTo),
        ("UserMetadata", q.UserMetadata),
        ("AutoDeleteOnIdleTicks", q.AutoDeleteOnIdle.Ticks),
        ("EnablePartitioning", q.EnablePartitioning ? 1 : 0),
        ("MaxMessageSizeInKilobytes", q.MaxMessageSizeInKilobytes),
        ("CreatedAtTicks", q.CreatedAt.Ticks),
        ("UpdatedAtTicks", q.UpdatedAt.Ticks),
    ];

    /// <summary>INSERTs (or UPDATEs, matching <paramref name="key"/>) one row; returns the number of rows affected.</summary>
    private static int WriteRow(SqliteConnection connection, SqliteTransaction? transaction, string table, bool insert,
        (string Column, object? Value)[] key, (string Column, object? Value)[] values)
    {
        var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        var all = insert ? key.Concat(values).ToArray() : values.Concat(key).ToArray();
        for (int i = 0; i < all.Length; i++)
            cmd.Parameters.AddWithValue($"$p{i}", all[i].Value ?? DBNull.Value);

        if (insert)
        {
            cmd.CommandText = $"INSERT INTO {table} ({string.Join(", ", all.Select(c => c.Column))}) " +
                              $"VALUES ({string.Join(", ", all.Select((_, i) => $"$p{i}"))})";
        }
        else
        {
            var set = values.Select((c, i) => $"{c.Column} = $p{i}");
            var where = key.Select((c, i) => $"{c.Column} = $p{values.Length + i}");
            cmd.CommandText = $"UPDATE {table} SET {string.Join(", ", set)} WHERE {string.Join(" AND ", where)}";
        }
        return cmd.ExecuteNonQuery();
    }

    private const string SelectQueueRows = "SELECT * FROM Queues";

    private static QueueInfo ReadQueueRow(SqliteDataReader r)
    {
        var defaults = new QueueInfo();
        // Rows written before the timestamp columns existed get a stable value for this process's lifetime.
        var loadedAt = DateTime.UtcNow;
        var queue = new QueueInfo
        {
            Name = r.GetString(r.GetOrdinal("Name")),
            RequiresSession = Long(r, "RequiresSession") == 1,
            LockDuration = Long(r, "LockDurationTicks") is long lockTicks ? TimeSpan.FromTicks(lockTicks)
                : Long(r, "LockDurationSeconds") is long lockSeconds ? TimeSpan.FromSeconds(lockSeconds)
                : defaults.LockDuration,
            MaxSizeInMegabytes = Long(r, "MaxSizeInMegabytes") ?? defaults.MaxSizeInMegabytes,
            MaxDeliveryCount = (int)(Long(r, "MaxDeliveryCount") ?? defaults.MaxDeliveryCount),
            DefaultMessageTimeToLive = Long(r, "DefaultTtlTicks") is long ttlTicks ? TimeSpan.FromTicks(ttlTicks)
                : Long(r, "DefaultTtlSeconds") is > 0 and long ttlSeconds ? TimeSpan.FromSeconds(ttlSeconds)
                : defaults.DefaultMessageTimeToLive,
            RequiresDuplicateDetection = Long(r, "RequiresDuplicateDetection") == 1,
            DuplicateDetectionHistoryTimeWindow = Long(r, "DuplicateDetectionWindowTicks") is long w ? TimeSpan.FromTicks(w) : defaults.DuplicateDetectionHistoryTimeWindow,
            DeadLetteringOnMessageExpiration = Long(r, "DeadLetteringOnExpiration") == 1,
            EnableBatchedOperations = Long(r, "EnableBatchedOperations") is long b ? b == 1 : defaults.EnableBatchedOperations,
            Status = Text(r, "Status") ?? defaults.Status,
            ForwardTo = Text(r, "ForwardTo"),
            ForwardDeadLetteredMessagesTo = Text(r, "ForwardDeadLetteredMessagesTo"),
            UserMetadata = Text(r, "UserMetadata"),
            AutoDeleteOnIdle = Long(r, "AutoDeleteOnIdleTicks") is long a ? TimeSpan.FromTicks(a) : defaults.AutoDeleteOnIdle,
            EnablePartitioning = Long(r, "EnablePartitioning") == 1,
            MaxMessageSizeInKilobytes = Long(r, "MaxMessageSizeInKilobytes"),
            CreatedAt = Long(r, "CreatedAtTicks") is long c ? new DateTime(c, DateTimeKind.Utc) : loadedAt,
            UpdatedAt = Long(r, "UpdatedAtTicks") is long u ? new DateTime(u, DateTimeKind.Utc) : loadedAt,
        };
        RestoreActivity(r, queue.Activity, queue.UpdatedAt);
        return queue;
    }

    private static long? Long(SqliteDataReader r, string column)
    {
        int i = r.GetOrdinal(column);
        return r.IsDBNull(i) ? null : r.GetInt64(i);
    }

    private static string? Text(SqliteDataReader r, string column)
    {
        int i = r.GetOrdinal(column);
        return r.IsDBNull(i) ? null : r.GetString(i);
    }
}
