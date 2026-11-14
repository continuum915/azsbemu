using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Core;

// AutoDeleteOnIdle: queues, topics and subscriptions unused for their AutoDeleteOnIdle are deleted, as Service Bus does.
// Activity is tracked in memory (EntityActivity) and stored in SQLite by this background loop, never on the message
// hot path, so a restart remembers how long an entity had been idle (to within ActivityPersistInterval after a crash).
public partial class EntityManager
{
    private readonly DateTime _startedAt = DateTime.UtcNow;

    /// <summary>How often idle entities are looked for.</summary>
    public TimeSpan AutoDeleteScanInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// After startup, no entity is auto-deleted for this long (or for its AutoDeleteOnIdle, if shorter), so clients
    /// reconnecting after a restart get to use entities that went idle while the emulator was stopped.
    /// </summary>
    public TimeSpan StartupGracePeriod { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How often changed activity times are written to SQLite; they are also written on shutdown.</summary>
    public TimeSpan ActivityPersistInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Records activity on the queue, subscription (<c>topic/subscriptions/sub</c>) or topic at <paramref name="entityPath"/>, if it exists.</summary>
    public void RecordActivity(string entityPath)
    {
        const string dlqSuffix = "/$deadletterqueue";
        entityPath = entityPath.TrimStart('/');
        if (entityPath.EndsWith(dlqSuffix, StringComparison.OrdinalIgnoreCase)) entityPath = entityPath[..^dlqSuffix.Length];
        if (entityPath.Length == 0) return;

        if (GetQueue(entityPath) is { } queue) queue.Activity.Record();
        else if (FindSubscription(entityPath) is { } sub) sub.Activity.Record();
        else if (GetTopic(entityPath) is { } topic) topic.Activity.Record();
    }

    private static void RestoreActivity(SqliteDataReader r, EntityActivity activity, DateTime updatedAt) =>
        // Rows from before activity was stored count as last used when they were last changed.
        activity.Restore(Long(r, "LastActivityTicks") is long ticks ? new DateTime(ticks, DateTimeKind.Utc) : updatedAt);

    private async Task RunAutoDeleteLoopAsync(CancellationToken ct)
    {
        var lastPersisted = DateTime.UtcNow;
        try
        {
            while (true)
            {
                await Task.Delay(AutoDeleteScanInterval, ct).ConfigureAwait(false);
                var now = DateTime.UtcNow;
                try
                {
                    DeleteIdleEntities(now);
                    if (now - lastPersisted >= ActivityPersistInterval)
                    {
                        PersistActivity();
                        lastPersisted = now;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while auto-deleting idle entities");
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private bool IsIdle(TimeSpan autoDeleteOnIdle, EntityActivity activity, DateTime now)
    {
        if (autoDeleteOnIdle == TimeSpan.MaxValue || autoDeleteOnIdle <= TimeSpan.Zero) return false;
        var grace = autoDeleteOnIdle < StartupGracePeriod ? autoDeleteOnIdle : StartupGracePeriod;
        if (now - _startedAt < grace) return false;
        if (now - activity.LastActivityUtc < autoDeleteOnIdle) return false;
        return !activity.HasOpenReceivers();
    }

    private void DeleteIdleEntities(DateTime now)
    {
        foreach (var queue in _queues.Values)
        {
            if (!IsIdle(queue.AutoDeleteOnIdle, queue.Activity, now)) continue;
            lock (_createLock)
            {
                // Re-checked under the lock: the queue may have been deleted, replaced or used meanwhile.
                if (!ReferenceEquals(GetQueue(queue.Name), queue) || !IsIdle(queue.AutoDeleteOnIdle, queue.Activity, DateTime.UtcNow)) continue;
                _logger.LogInformation("Queue {Name} has been idle for {Idle} (AutoDeleteOnIdle); deleting it.", queue.Name, queue.AutoDeleteOnIdle);
                DeleteQueueLocked(queue.Name);
            }
        }

        foreach (var topic in _topics.Values)
        {
            if (IsIdle(topic.AutoDeleteOnIdle, topic.Activity, now))
            {
                lock (_createLock)
                {
                    if (ReferenceEquals(GetTopic(topic.Name), topic) && IsIdle(topic.AutoDeleteOnIdle, topic.Activity, DateTime.UtcNow))
                    {
                        _logger.LogInformation("Topic {Name} has been idle for {Idle} (AutoDeleteOnIdle); deleting it and its subscriptions.", topic.Name, topic.AutoDeleteOnIdle);
                        DeleteTopicLocked(topic.Name);
                        continue;
                    }
                }
            }

            List<SubscriptionInfo> subs;
            lock (topic.Subscriptions) subs = new List<SubscriptionInfo>(topic.Subscriptions);
            foreach (var sub in subs)
            {
                if (!IsIdle(sub.AutoDeleteOnIdle, sub.Activity, now)) continue;
                lock (_createLock)
                {
                    if (!ReferenceEquals(GetSubscription(topic.Name, sub.Name), sub) || !IsIdle(sub.AutoDeleteOnIdle, sub.Activity, DateTime.UtcNow)) continue;
                    _logger.LogInformation("Subscription {Topic}/{Name} has been idle for {Idle} (AutoDeleteOnIdle); deleting it.", topic.Name, sub.Name, sub.AutoDeleteOnIdle);
                    DeleteSubscriptionLocked(topic.Name, sub.Name);
                }
            }
        }
    }

    /// <summary>Stores the activity times that changed since they were last stored, for entities that can auto-delete.</summary>
    private void PersistActivity()
    {
        var pending = new List<(string Sql, (string Name, object Value)[] Key, EntityActivity Activity, long Ticks)>();
        foreach (var queue in _queues.Values)
        {
            if (queue.AutoDeleteOnIdle != TimeSpan.MaxValue && queue.Activity.TryGetUnpersisted(out long ticks))
                pending.Add(("UPDATE Queues SET LastActivityTicks = $ticks WHERE Name = $name", [("$name", queue.Name)], queue.Activity, ticks));
        }
        foreach (var topic in _topics.Values)
        {
            if (topic.AutoDeleteOnIdle != TimeSpan.MaxValue && topic.Activity.TryGetUnpersisted(out long ticks))
                pending.Add(("UPDATE Topics SET LastActivityTicks = $ticks WHERE Name = $name", [("$name", topic.Name)], topic.Activity, ticks));

            List<SubscriptionInfo> subs;
            lock (topic.Subscriptions) subs = new List<SubscriptionInfo>(topic.Subscriptions);
            foreach (var sub in subs)
            {
                if (sub.AutoDeleteOnIdle != TimeSpan.MaxValue && sub.Activity.TryGetUnpersisted(out long subTicks))
                    pending.Add(("UPDATE Subscriptions SET LastActivityTicks = $ticks WHERE TopicName = $topic AND Name = $name",
                        [("$topic", sub.TopicName), ("$name", sub.Name)], sub.Activity, subTicks));
            }
        }
        if (pending.Count == 0) return;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        using var transaction = connection.BeginTransaction();
        foreach (var (sql, key, _, ticks) in pending)
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("$ticks", ticks);
            foreach (var (name, value) in key) cmd.Parameters.AddWithValue(name, value);
            cmd.ExecuteNonQuery();
        }
        transaction.Commit();

        foreach (var (_, _, activity, ticks) in pending) activity.MarkPersisted(ticks);
    }
}
