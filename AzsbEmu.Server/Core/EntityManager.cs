using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Core;

public partial class EntityManager : IDisposable
{
    private readonly string _connectionString;
    private readonly string _dataDir;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<EntityManager> _logger;
    private readonly ConcurrentDictionary<string, QueueInfo> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicInfo> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly CancellationTokenSource _ttlCts = new();
    private readonly Task _ttlTask;
    private readonly CancellationTokenSource _schedulerCts = new();
    private readonly Task _schedulerTask;
    private readonly ConcurrentDictionary<(string entityPath, string sessionId), SessionLockState> _sessionLocks = new();
    private readonly ConcurrentDictionary<Guid, MessageLockState> _activeLocks = new();
    private readonly ConcurrentDictionary<string, TransactionState> _transactions = new(StringComparer.OrdinalIgnoreCase);
    private readonly CancellationTokenSource _transactionCts = new();
    private readonly Task _transactionTask;
    private readonly CancellationTokenSource _autoDeleteCts = new();
    private readonly Task _autoDeleteTask;

    public event Action<string>? ManagementAddressAdded;

    public EntityManager(string dataDir, ILoggerFactory loggerFactory)
    {
        _dataDir = dataDir;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<EntityManager>();
        
        string dbPath = Path.Combine(dataDir, "metadata.db");
        _connectionString = $"Data Source={dbPath}";
        
        Directory.CreateDirectory(dataDir);
        InitializeDatabase();
        LoadEntities();
        _ttlTask = RunTtlScanLoopAsync(_ttlCts.Token);
        _schedulerTask = RunSchedulerScanLoopAsync(_schedulerCts.Token);
        _transactionTask = RunTransactionTimeoutScanLoopAsync(_transactionCts.Token);
        _autoDeleteTask = RunAutoDeleteLoopAsync(_autoDeleteCts.Token);
    }

    private void InitializeDatabase()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS Queues (
                Name TEXT PRIMARY KEY,
                RequiresSession INTEGER,
                LockDurationSeconds INTEGER,
                MaxSizeInMegabytes INTEGER,
                MaxDeliveryCount INTEGER DEFAULT 10
            );
            CREATE TABLE IF NOT EXISTS Topics (
                Name TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS Subscriptions (
                TopicName TEXT,
                Name TEXT,
                RequiresSession INTEGER,
                LockDurationSeconds INTEGER DEFAULT 60,
                MaxDeliveryCount INTEGER DEFAULT 10,
                PRIMARY KEY (TopicName, Name),
                FOREIGN KEY (TopicName) REFERENCES Topics(Name) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS Rules (
                TopicName TEXT,
                SubscriptionName TEXT,
                RuleName TEXT,
                FilterType TEXT,
                FilterExpression TEXT,
                ActionExpression TEXT,
                PRIMARY KEY (TopicName, SubscriptionName, RuleName),
                FOREIGN KEY (TopicName, SubscriptionName) REFERENCES Subscriptions(TopicName, Name) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS ScheduledMessages (
                SequenceNumber INTEGER PRIMARY KEY AUTOINCREMENT,
                EntityName TEXT NOT NULL,
                ScheduledEnqueueTimeUtcTicks INTEGER NOT NULL,
                Payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS SessionStates (
                EntityName TEXT NOT NULL,
                SessionId TEXT NOT NULL,
                State BLOB,
                PRIMARY KEY (EntityName, SessionId)
            );
        ";
        command.ExecuteNonQuery();

        // Schema migrations for Subscriptions table
        if (!ColumnExists(connection, "Subscriptions", "LockDurationSeconds"))
        {
            var alterCmd = connection.CreateCommand();
            alterCmd.CommandText = "ALTER TABLE Subscriptions ADD COLUMN LockDurationSeconds INTEGER DEFAULT 60";
            alterCmd.ExecuteNonQuery();
        }

        if (!ColumnExists(connection, "Subscriptions", "MaxDeliveryCount"))
        {
            var alterCmd = connection.CreateCommand();
            alterCmd.CommandText = "ALTER TABLE Subscriptions ADD COLUMN MaxDeliveryCount INTEGER DEFAULT 10";
            alterCmd.ExecuteNonQuery();
        }

        // Schema migration for Queues table
        if (!ColumnExists(connection, "Queues", "MaxDeliveryCount"))
        {
            var alterCmd = connection.CreateCommand();
            alterCmd.CommandText = "ALTER TABLE Queues ADD COLUMN MaxDeliveryCount INTEGER DEFAULT 10";
            alterCmd.ExecuteNonQuery();
        }

        if (!ColumnExists(connection, "Queues", "DefaultTtlSeconds"))
        {
            var alterCmd = connection.CreateCommand();
            alterCmd.CommandText = "ALTER TABLE Queues ADD COLUMN DefaultTtlSeconds INTEGER DEFAULT -1";
            alterCmd.ExecuteNonQuery();
        }

        if (!ColumnExists(connection, "Subscriptions", "DefaultTtlSeconds"))
        {
            var alterCmd = connection.CreateCommand();
            alterCmd.CommandText = "ALTER TABLE Subscriptions ADD COLUMN DefaultTtlSeconds INTEGER DEFAULT -1";
            alterCmd.ExecuteNonQuery();
        }

        AddQueueColumns(connection);
        AddColumns(connection, "Topics", AddedTopicColumns);
        AddColumns(connection, "Subscriptions", AddedSubscriptionColumns);
    }

    private static bool ColumnExists(SqliteConnection connection, string tableName, string columnName)
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({tableName})";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(1); // Column name is the second field in PRAGMA table_info
            if (string.Equals(name, columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private void LoadEntities()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        MigrateLegacyStorageLayout(connection);

        var qCmd = connection.CreateCommand();
        qCmd.CommandText = SelectQueueRows;
        using (var reader = qCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var q = ReadQueueRow(reader);
                InitializeEntityStorage(q);
                _queues[q.Name] = q;
            }
        }

        var tCmd = connection.CreateCommand();
        tCmd.CommandText = "SELECT * FROM Topics";
        using (var reader = tCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var t = ReadTopicRow(reader);
                _topics[t.Name] = t;
            }
        }

        var sCmd = connection.CreateCommand();
        sCmd.CommandText = "SELECT * FROM Subscriptions";
        using (var reader = sCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var sub = ReadSubscriptionRow(reader);
                if (_topics.TryGetValue(sub.TopicName, out var topic))
                {
                    InitializeEntityStorage(sub);
                    topic.Subscriptions.Add(sub);
                }
            }
        }

        var rCmd = connection.CreateCommand();
        rCmd.CommandText = "SELECT TopicName, SubscriptionName, RuleName, FilterType, FilterExpression, ActionExpression FROM Rules";
        using (var reader = rCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var topicName = reader.GetString(0);
                var subName = reader.GetString(1);
                var rule = new RuleInfo
                {
                    Name = reader.GetString(2),
                    FilterType = reader.IsDBNull(3) ? "True" : reader.GetString(3),
                    FilterExpression = reader.IsDBNull(4) ? "" : reader.GetString(4),
                    ActionExpression = reader.IsDBNull(5) ? "" : reader.GetString(5)
                };
                if (_topics.TryGetValue(topicName, out var topic))
                {
                    var sub = topic.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                    if (sub != null)
                    {
                        sub.Rules.Add(rule);
                    }
                }
            }
        }

        // Resilient fallback: ensure loaded subscriptions always have at least one rule ($Default TrueFilter)
        foreach (var topic in _topics.Values)
        {
            foreach (var sub in topic.Subscriptions)
            {
                if (sub.Rules.Count == 0)
                {
                    sub.Rules.Add(RuleInfo.CreateDefault());
                }
            }
        }
        
        _logger.LogInformation("Loaded {QueueCount} queues and {TopicCount} topics from database.", _queues.Count, _topics.Count);
    }

    private void InitializeEntityStorage(QueueInfo queue)
    {
        string entityDir = QueueDirectory(queue.Name);
        Directory.CreateDirectory(entityDir);

        string path = Path.Combine(entityDir, "messages.log");
        queue.MessageStore = new MessageStore(path, _loggerFactory.CreateLogger($"Store:{queue.Name}"));

        string dlqPath = Path.Combine(entityDir, "dlq.log");
        queue.DlqMessageStore = new MessageStore(dlqPath, _loggerFactory.CreateLogger($"Store:{queue.Name}/$dlq"), DeadLetterCommitDirectory(entityDir));

        var (seqMax, count) = ScanLogForStartupState(queue.MessageStore, queue.Name, queue.RequiresSession ? queue.SessionMessageAddresses : null);
        var (dlqSeqMax, dlqCount) = ScanLogForStartupState(queue.DlqMessageStore, $"{queue.Name}/$dlq", null);
        queue.NextSequenceNumber = Math.Max(seqMax, dlqSeqMax);
        queue.MessageCount = count;
        queue.DeadLetterMessageCount = dlqCount;

        queue.Dispatcher = new Dispatcher(queue.MessageStore, _loggerFactory.CreateLogger($"Dispatcher:{queue.Name}"),
            queue.RequiresSession ? queue.SessionMessageAddresses : null);
        queue.Dispatcher.StartLockMonitor(queue.LockDuration, (addr, token) => HandleLockExpiryAsync(queue, addr, token));
        queue.DlqDispatcher = new Dispatcher(queue.DlqMessageStore, _loggerFactory.CreateLogger($"Dispatcher:{queue.Name}/$dlq"));
    }

    private void InitializeEntityStorage(SubscriptionInfo sub)
    {
        string subDir = SubscriptionDirectory(sub.TopicName, sub.Name);
        Directory.CreateDirectory(subDir);

        string path = Path.Combine(subDir, "messages.log");
        sub.MessageStore = new MessageStore(path, _loggerFactory.CreateLogger($"Store:{sub.TopicName}/Sub:{sub.Name}"));

        string dlqPath = Path.Combine(subDir, "dlq.log");
        sub.DlqMessageStore = new MessageStore(dlqPath, _loggerFactory.CreateLogger($"Store:{sub.TopicName}/Sub:{sub.Name}/$dlq"), DeadLetterCommitDirectory(subDir));

        var (seqMax, count) = ScanLogForStartupState(sub.MessageStore, $"{sub.TopicName}/Sub:{sub.Name}", sub.RequiresSession ? sub.SessionMessageAddresses : null);
        var (dlqSeqMax, dlqCount) = ScanLogForStartupState(sub.DlqMessageStore, $"{sub.TopicName}/Sub:{sub.Name}/$dlq", null);
        sub.NextSequenceNumber = Math.Max(seqMax, dlqSeqMax);
        sub.MessageCount = count;
        sub.DeadLetterMessageCount = dlqCount;

        sub.Dispatcher = new Dispatcher(sub.MessageStore, _loggerFactory.CreateLogger($"Dispatcher:{sub.TopicName}/Sub:{sub.Name}"),
            sub.RequiresSession ? sub.SessionMessageAddresses : null);
        sub.Dispatcher.StartLockMonitor(sub.LockDuration, (addr, token) => HandleLockExpiryAsync(sub, addr, token));
        sub.DlqDispatcher = new Dispatcher(sub.DlqMessageStore, _loggerFactory.CreateLogger($"Dispatcher:{sub.TopicName}/Sub:{sub.Name}/$dlq"));
    }

    private (long seqMax, long count) ScanLogForStartupState(
        MessageStore store, 
        string entityName, 
        ConcurrentDictionary<string, SessionMessageQueue>? sessionQueue = null)
    {
        var log = store.Log;
        long snapshotTail = log.TailAddress;
        if (snapshotTail <= log.BeginAddress)
        {
            _logger.LogDebug("Empty log for entity {Entity}, skipping startup scan", entityName);
            return (0, 0);
        }

        return Task.Run(async () =>
        {
            long seqMax = 0;
            long count = 0;
            using var scanner = log.Scan(log.BeginAddress, snapshotTail);
            while (await scanner.WaitAsync().ConfigureAwait(false))
            {
                while (scanner.GetNext(out byte[] data, out _, out long address, out _))
                {
                    var header = MessageSerializer.DecodeHeader(data);
                    if (header.SequenceNumber > seqMax) seqMax = header.SequenceNumber;
                    count++;

                    if (sessionQueue != null)
                    {
                        try
                        {
                            var (_, message) = MessageSerializer.Decode(data);
                            string? sessionId = message.Properties?.GroupId;
                            if (!string.IsNullOrEmpty(sessionId))
                            {
                                var q = sessionQueue.GetOrAdd(sessionId, _ => new SessionMessageQueue());
                                q.Enqueue(address);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to decode session ID during startup scan for entity {Entity}", entityName);
                        }
                    }
                }
            }
            _logger.LogInformation("Startup scan {Entity}: {Count} messages, max seq {SeqMax}", entityName, count, seqMax);
            return (seqMax, count);
        }).GetAwaiter().GetResult();
    }

    private async Task HandleLockExpiryAsync(QueueInfo queue, long address, Guid token)
    {
        _logger.LogDebug("Lock expired for address {Address} on queue {QueueName}", address, queue.Name);
        
        try
        {
            TryRemoveLock(token, out _);
            var dataResult = await queue.MessageStore!.ReadAsync(address);
            var (header, _) = MessageSerializer.Decode(dataResult.data);
            int overlayCount = queue.Dispatcher!.IncrementDeliveryCount(address);
            int totalCount = header.DeliveryCount + overlayCount;

            if (totalCount >= queue.MaxDeliveryCount)
            {
                _logger.LogInformation("Message at {Address} on queue {QueueName} reached MaxDeliveryCount ({MaxDC}) via lock expiry, dead-lettering", 
                    address, queue.Name, queue.MaxDeliveryCount);
                await DeadLetterMessageAsync(queue, address, "LockLostException", "The message lock has expired; it has reached the maximum delivery count.");
            }
            else
            {
                _logger.LogDebug("Returning message at {Address} on queue {QueueName} to redelivery queue (delivery count: {Count})", address, queue.Name, totalCount);
                queue.Dispatcher!.Redeliver(address);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling lock expiry for address {Address} on queue {QueueName}", address, queue.Name);
            // On failure, try to redeliver anyway to avoid losing the message
            queue.Dispatcher?.Redeliver(address);
        }
    }

    private async Task HandleLockExpiryAsync(SubscriptionInfo sub, long address, Guid token)
    {
        _logger.LogDebug("Lock expired for address {Address} on subscription {Topic}/{Sub}", address, sub.TopicName, sub.Name);
        
        try
        {
            TryRemoveLock(token, out _);
            var dataResult = await sub.MessageStore!.ReadAsync(address);
            var (header, _) = MessageSerializer.Decode(dataResult.data);
            int overlayCount = sub.Dispatcher!.IncrementDeliveryCount(address);
            int totalCount = header.DeliveryCount + overlayCount;

            if (totalCount >= sub.MaxDeliveryCount)
            {
                _logger.LogInformation("Message at {Address} on subscription {Topic}/{Sub} reached MaxDeliveryCount ({MaxDC}) via lock expiry, dead-lettering", 
                    address, sub.TopicName, sub.Name, sub.MaxDeliveryCount);
                await DeadLetterMessageAsync(sub, address, "LockLostException", "The message lock has expired; it has reached the maximum delivery count.");
            }
            else
            {
                _logger.LogDebug("Returning message at {Address} on subscription {Topic}/{Sub} to redelivery queue", address, sub.TopicName, sub.Name);
                sub.Dispatcher!.Redeliver(address);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling lock expiry for address {Address} on subscription {Topic}/{Sub}", address, sub.TopicName, sub.Name);
            sub.Dispatcher?.Redeliver(address);
        }
    }

    /// <summary>
    /// Retires a message found expired when it was about to be delivered: dead-lettered (TTLExpiredException) if the
    /// entity has DeadLetteringOnMessageExpiration, otherwise discarded.
    /// </summary>
    public async Task ExpireMessageAsync(IMessageEntity entity, long address, long sequenceNumber)
    {
        LogExpiry(entity, sequenceNumber);
        if (entity.DeadLetteringOnMessageExpiration)
        {
            await DeadLetterMessageAsync(entity, address, DeadLetterDetails.TtlExpired.Reason, DeadLetterDetails.TtlExpired.Description)
                .ConfigureAwait(false);
            return;
        }

        if (entity.Dispatcher is { } dispatcher)
        {
            dispatcher.ForceComplete(address);
            entity.DecrementMessageCount();
        }
    }

    public Task DeadLetterMessageAsync(IMessageEntity entity, long address, string? reason, string? description) =>
        DeadLetterMessageAsync(entity, address, DeadLetterDetails.WithReason(reason, description));

    public async Task DeadLetterMessageAsync(IMessageEntity entity, long address, DeadLetterDetails details)
    {
        var dispatcher = entity.Dispatcher;
        var store = entity.MessageStore;

        if (dispatcher == null || store == null || entity.DlqMessageStore == null) return;

        try
        {
            var dataResult = await store.ReadAsync(address);
            var (oldHeader, message) = MessageSerializer.Decode(dataResult.data);

            var header = oldHeader with
            {
                DeliveryCount = oldHeader.DeliveryCount + dispatcher.GetDeliveryCount(address)
            };

            dispatcher.ApplyModifiedProperties(address, message); // from earlier abandons and defers
            PropertiesToModify.ApplyTo(details.PropertiesToModify, message);
            message.ApplicationProperties ??= new ApplicationProperties();
            if (details.Reason != null) message.ApplicationProperties[DeadLetterDetails.ReasonProperty] = details.Reason;
            if (details.Description != null) message.ApplicationProperties[DeadLetterDetails.DescriptionProperty] = details.Description;

            await MoveToDeadLetterLogAsync(entity, header, message);
            dispatcher.ForceComplete(address);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error dead-lettering message at address {Address} for entity {EntityName}", address, entity.Name);
        }
    }

    private readonly object _createLock = new();
    private const int SqliteConstraintViolation = 19; // SQLITE_CONSTRAINT

    /// <summary>A snapshot of a topic's subscriptions, or null if the topic does not exist.</summary>
    public IReadOnlyList<SubscriptionInfo>? GetSubscriptions(string topicName)
    {
        var topic = GetTopic(topicName);
        if (topic == null) return null;
        lock (topic.Subscriptions) return topic.Subscriptions.ToList();
    }

    /// <summary>The number of messages scheduled for <paramref name="entity"/> (a queue or subscription) or a topic.</summary>
    public int GetScheduledMessageCount(IMessageEntity entity) =>
        GetScheduledMessageCount(entity is SubscriptionInfo sub ? $"{sub.TopicName}/subscriptions/{sub.Name}" : entity.Name);

    /// <summary>Finds a subscription by topic and subscription name, both case-insensitively.</summary>
    public SubscriptionInfo? GetSubscription(string topicName, string subscriptionName)
    {
        var topic = GetTopic(topicName);
        if (topic == null) return null;
        lock (topic.Subscriptions)
        {
            return topic.Subscriptions.Find(s => string.Equals(s.Name, subscriptionName, StringComparison.OrdinalIgnoreCase));
        }
    }

    /// <summary>
    /// Creates a queue. Returns false if a queue or topic of that name (in any casing) already exists; the check and
    /// the insert happen under one lock so concurrent creates of the same name produce exactly one queue.
    /// </summary>
    public bool CreateQueue(QueueInfo queue)
    {
        lock (_createLock)
        {
            if (_queues.ContainsKey(queue.Name) || _topics.ContainsKey(queue.Name)) return false;

            using var connection = new SqliteConnection(_connectionString);
            connection.Open();
            if (queue.CreatedAt == default) queue.CreatedAt = DateTime.UtcNow;
            if (queue.UpdatedAt == default) queue.UpdatedAt = queue.CreatedAt;

            try
            {
                WriteRow(connection, null, "Queues", insert: true, QueueKey(queue), QueueColumns(queue));
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == SqliteConstraintViolation) // row already exists
            {
                return false;
            }

            try
            {
                InitializeEntityStorage(queue);
            }
            catch
            {
                DisposeEntityStorage(queue);
                var rollback = connection.CreateCommand();
                rollback.CommandText = "DELETE FROM Queues WHERE Name = $name";
                rollback.Parameters.AddWithValue("$name", queue.Name);
                rollback.ExecuteNonQuery();
                throw;
            }

            _queues[queue.Name] = queue;
        }

        ManagementAddressAdded?.Invoke($"{queue.Name}/$management");
        return true;
    }

    public QueueInfo? GetQueue(string name) => _queues.TryGetValue(name, out var q) ? q : null;
    
    public IEnumerable<QueueInfo> GetQueues() => _queues.Values;

    public IEnumerable<TopicInfo> GetTopics() => _topics.Values;

    public bool CreateTopic(string name) => CreateTopic(new TopicInfo { Name = name });

    /// <summary>
    /// Creates a topic. Returns false if a queue or topic of that name (in any casing) already exists; shares the
    /// queue-creation lock so the two kinds cannot race into the same name.
    /// </summary>
    public bool CreateTopic(TopicInfo topic)
    {
        lock (_createLock)
        {
            if (_queues.ContainsKey(topic.Name) || _topics.ContainsKey(topic.Name)) return false;

            using var connection = new SqliteConnection(_connectionString);
            connection.Open();
            if (topic.CreatedAt == default) topic.CreatedAt = DateTime.UtcNow;
            if (topic.UpdatedAt == default) topic.UpdatedAt = topic.CreatedAt;
            try
            {
                WriteRow(connection, null, "Topics", insert: true, TopicKey(topic), TopicColumns(topic));
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == SqliteConstraintViolation) // row already exists
            {
                return false;
            }

            _topics[topic.Name] = topic;
        }

        ManagementAddressAdded?.Invoke($"{topic.Name}/$management");
        return true;
    }

    public TopicInfo? GetTopic(string name) => _topics.TryGetValue(name, out var t) ? t : null;

    /// <summary>
    /// Creates a subscription with <paramref name="defaultRule"/>, or the <c>$Default</c> true-filter rule if none is
    /// given. Returns false if the topic does not exist or already has a subscription of that name (in any casing);
    /// check and insert happen under the creation lock.
    /// </summary>
    public bool CreateSubscription(string topicName, SubscriptionInfo sub, RuleInfo? defaultRule = null)
    {
        var rule = defaultRule ?? RuleInfo.CreateDefault();
        rule.NormalizeFilter();
        TopicInfo? topic;
        lock (_createLock)
        {
            if (!_topics.TryGetValue(topicName, out topic)) return false;
            lock (topic.Subscriptions)
            {
                if (topic.Subscriptions.Exists(s => string.Equals(s.Name, sub.Name, StringComparison.OrdinalIgnoreCase))) return false;
            }

            using var connection = new SqliteConnection(_connectionString);
            connection.Open();
            using var transaction = connection.BeginTransaction();
            if (sub.CreatedAt == default) sub.CreatedAt = DateTime.UtcNow;
            if (sub.UpdatedAt == default) sub.UpdatedAt = sub.CreatedAt;
            try
            {
                sub.TopicName = topic.Name;
                WriteRow(connection, transaction, "Subscriptions", insert: true, SubscriptionKey(sub), SubscriptionColumns(sub));
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == SqliteConstraintViolation) // row already exists
            {
                return false;
            }

            var ruleCmd = connection.CreateCommand();
            ruleCmd.Transaction = transaction;
            ruleCmd.CommandText = "INSERT INTO Rules (TopicName, SubscriptionName, RuleName, FilterType, FilterExpression, ActionExpression) VALUES ($tname, $sname, $rname, $ftype, $fexpr, $aexpr)";
            ruleCmd.Parameters.AddWithValue("$tname", topic.Name);
            ruleCmd.Parameters.AddWithValue("$sname", sub.Name);
            ruleCmd.Parameters.AddWithValue("$rname", rule.Name);
            ruleCmd.Parameters.AddWithValue("$ftype", rule.FilterType);
            ruleCmd.Parameters.AddWithValue("$fexpr", rule.FilterExpression ?? "");
            ruleCmd.Parameters.AddWithValue("$aexpr", rule.ActionExpression ?? "");
            ruleCmd.ExecuteNonQuery();

            // Storage paths derive from the stored topic name, not the casing the caller used.
            sub.TopicName = topic.Name;
            try
            {
                InitializeEntityStorage(sub);
                transaction.Commit();
            }
            catch
            {
                DisposeEntityStorage(sub);
                throw;
            }

            sub.Rules.Add(rule);
            lock (topic.Subscriptions)
            {
                topic.Subscriptions.Add(sub);
            }
            topic.Activity.Record();
        }

        ManagementAddressAdded?.Invoke($"{topic.Name}/subscriptions/{sub.Name}/$management");
        return true;
    }

    /// <summary>Result of an update: <see cref="Found"/> is false when the entity does not exist; otherwise
    /// <see cref="Error"/> explains a rejected change, or is null when the update was applied.</summary>
    public readonly record struct UpdateOutcome(bool Found, string? Error)
    {
        public static readonly UpdateOutcome NotFound = new(false, null);
        public static readonly UpdateOutcome Applied = new(true, null);
        public static UpdateOutcome Rejected(string error) => new(true, error);
    }

    /// <summary>
    /// Applies <paramref name="desired"/>'s settings to the existing queue of that name: validated, persisted, then
    /// applied to the running queue (lock duration, delivery count, TTL and forwarding take effect immediately).
    /// </summary>
    public UpdateOutcome UpdateQueue(QueueInfo desired)
    {
        lock (_createLock)
        {
            if (GetQueue(desired.Name) is not { } queue) return UpdateOutcome.NotFound;
            if (desired.RequiresSession != queue.RequiresSession) return UpdateOutcome.Rejected("RequiresSession cannot be changed.");
            if (desired.RequiresDuplicateDetection != queue.RequiresDuplicateDetection) return UpdateOutcome.Rejected("RequiresDuplicateDetection cannot be changed.");
            if (desired.EnablePartitioning != queue.EnablePartitioning) return UpdateOutcome.Rejected("EnablePartitioning cannot be changed.");
            if (ValidateForwardTargets(queue.Name, desired.ForwardTo, desired.ForwardDeadLetteredMessagesTo) is { } forwardError) return UpdateOutcome.Rejected(forwardError);

            var merged = queue.WithConfigurationFrom(desired);
            merged.UpdatedAt = DateTime.UtcNow;
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                if (WriteRow(connection, null, "Queues", insert: false, QueueKey(merged), QueueColumns(merged)) == 0) return UpdateOutcome.NotFound;
            }

            queue.ApplyConfigurationFrom(merged);
            queue.UpdatedAt = merged.UpdatedAt;
            queue.Activity.Record();
            if (queue.Dispatcher != null) queue.Dispatcher.LockDuration = queue.LockDuration;
            return UpdateOutcome.Applied;
        }
    }

    /// <summary>Applies <paramref name="desired"/>'s settings to the existing topic of that name.</summary>
    public UpdateOutcome UpdateTopic(TopicInfo desired)
    {
        lock (_createLock)
        {
            if (GetTopic(desired.Name) is not { } topic) return UpdateOutcome.NotFound;
            if (desired.EnablePartitioning != topic.EnablePartitioning) return UpdateOutcome.Rejected("EnablePartitioning cannot be changed.");
            if (desired.RequiresDuplicateDetection != topic.RequiresDuplicateDetection) return UpdateOutcome.Rejected("RequiresDuplicateDetection cannot be changed.");

            var merged = topic.WithConfigurationFrom(desired);
            merged.UpdatedAt = DateTime.UtcNow;
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                if (WriteRow(connection, null, "Topics", insert: false, TopicKey(merged), TopicColumns(merged)) == 0) return UpdateOutcome.NotFound;
            }

            topic.ApplyConfigurationFrom(merged);
            topic.UpdatedAt = merged.UpdatedAt;
            topic.Activity.Record();
            return UpdateOutcome.Applied;
        }
    }

    /// <summary>Applies <paramref name="desired"/>'s settings to the existing subscription of that topic and name.</summary>
    public UpdateOutcome UpdateSubscription(string topicName, SubscriptionInfo desired)
    {
        lock (_createLock)
        {
            if (GetSubscription(topicName, desired.Name) is not { } sub) return UpdateOutcome.NotFound;
            if (desired.RequiresSession != sub.RequiresSession) return UpdateOutcome.Rejected("RequiresSession cannot be changed.");
            if (ValidateForwardTargets($"{sub.TopicName}/Subscriptions/{sub.Name}", desired.ForwardTo, desired.ForwardDeadLetteredMessagesTo, owningTopic: sub.TopicName) is { } forwardError)
                return UpdateOutcome.Rejected(forwardError);

            var merged = sub.WithConfigurationFrom(desired);
            merged.UpdatedAt = DateTime.UtcNow;
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                if (WriteRow(connection, null, "Subscriptions", insert: false, SubscriptionKey(merged), SubscriptionColumns(merged)) == 0) return UpdateOutcome.NotFound;
            }

            sub.ApplyConfigurationFrom(merged);
            sub.UpdatedAt = merged.UpdatedAt;
            sub.Activity.Record();
            GetTopic(sub.TopicName)?.Activity.Record();
            if (sub.Dispatcher != null) sub.Dispatcher.LockDuration = sub.LockDuration;
            return UpdateOutcome.Applied;
        }
    }

    private void DisposeEntityStorage(IMessageEntity entity)
    {
        try { entity.Dispatcher?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing dispatcher for {Name}", entity.Name); }
        try { entity.DlqDispatcher?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing DLQ dispatcher for {Name}", entity.Name); }
        try { entity.MessageStore?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing store for {Name}", entity.Name); }
        try { entity.DlqMessageStore?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing DLQ store for {Name}", entity.Name); }
    }

    public bool DeleteQueue(string name)
    {
        lock (_createLock) return DeleteQueueLocked(name);
    }

    private bool DeleteQueueLocked(string name)
    {
        if (!_queues.TryRemove(name, out var queue)) return false;
        name = queue.Name; // the stored casing: SQLite compares names case-sensitively

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        try
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DELETE FROM Queues WHERE Name = $name";
            cmd.Parameters.AddWithValue("$name", name);
            cmd.ExecuteNonQuery();

            var schedCmd = connection.CreateCommand();
            schedCmd.Transaction = transaction;
            schedCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $name COLLATE NOCASE OR EntityName = $dlqName COLLATE NOCASE";
            schedCmd.Parameters.AddWithValue("$name", name);
            schedCmd.Parameters.AddWithValue("$dlqName", $"{name}/$deadletterqueue");
            schedCmd.ExecuteNonQuery();

            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            _logger.LogError(ex, "Failed to delete queue {Name} from database", name);
            _queues[name] = queue;
            return false;
        }

        try
        {
            queue.Dispatcher?.Dispose();
            queue.DlqDispatcher?.Dispose();
            queue.MessageStore?.Dispose();
            queue.DlqMessageStore?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing runtime structures for queue {Name}", name);
        }

        string entityDir = QueueDirectory(queue.Name);
        if (Directory.Exists(entityDir))
        {
            try
            {
                DeleteDirectoryWithRetry(entityDir);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to purge storage directory {Dir} for queue {Name}", entityDir, name);
            }
        }

        _logger.LogInformation("Deleted queue {Name} and purged its storage.", name);
        return true;
    }

    public bool DeleteTopic(string name)
    {
        lock (_createLock) return DeleteTopicLocked(name);
    }

    private bool DeleteTopicLocked(string name)
    {
        if (!_topics.TryRemove(name, out var topic)) return false;
        name = topic.Name; // the stored casing: SQLite compares names case-sensitively

        List<SubscriptionInfo> subsToDispose;
        lock (topic.Subscriptions)
        {
            subsToDispose = new List<SubscriptionInfo>(topic.Subscriptions);
        }

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        try
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DELETE FROM Topics WHERE Name = $name";
            cmd.Parameters.AddWithValue("$name", name);
            cmd.ExecuteNonQuery();

            var schedCmd = connection.CreateCommand();
            schedCmd.Transaction = transaction;
            schedCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $name COLLATE NOCASE";
            schedCmd.Parameters.AddWithValue("$name", name);
            schedCmd.ExecuteNonQuery();

            foreach (var sub in subsToDispose)
            {
                string subPath = $"{name}/subscriptions/{sub.Name}";
                var schedSubCmd = connection.CreateCommand();
                schedSubCmd.Transaction = transaction;
                schedSubCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $subPath COLLATE NOCASE OR EntityName = $dlqPath COLLATE NOCASE";
                schedSubCmd.Parameters.AddWithValue("$subPath", subPath);
                schedSubCmd.Parameters.AddWithValue("$dlqPath", $"{subPath}/$deadletterqueue");
                schedSubCmd.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            _logger.LogError(ex, "Failed to delete topic {Name} from database", name);
            _topics[name] = topic;
            return false;
        }

        foreach (var sub in subsToDispose)
        {
            try
            {
                sub.Dispatcher?.Dispose();
                sub.DlqDispatcher?.Dispose();
                sub.MessageStore?.Dispose();
                sub.DlqMessageStore?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing subscription runtime structures for {Topic}/{Sub}", name, sub.Name);
            }
        }

        string entityDir = TopicDirectory(name);
        if (Directory.Exists(entityDir))
        {
            try
            {
                DeleteDirectoryWithRetry(entityDir);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to purge storage directory {Dir} for topic {Name}", entityDir, name);
            }
        }

        _logger.LogInformation("Deleted topic {Name} and purged its storage.", name);
        return true;
    }

    public bool DeleteSubscription(string topicName, string name)
    {
        lock (_createLock)
        {
            GetTopic(topicName)?.Activity.Record();
            return DeleteSubscriptionLocked(topicName, name);
        }
    }

    private bool DeleteSubscriptionLocked(string topicName, string name)
    {
        if (!_topics.TryGetValue(topicName, out var topic)) return false;

        SubscriptionInfo? subscription = null;
        lock (topic.Subscriptions)
        {
            subscription = topic.Subscriptions.Find(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase));
            if (subscription == null) return false;
            topic.Subscriptions.Remove(subscription);
        }
        topicName = topic.Name; // the stored casing: SQLite compares names case-sensitively
        name = subscription.Name;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        try
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DELETE FROM Subscriptions WHERE TopicName = $topicName AND Name = $name";
            cmd.Parameters.AddWithValue("$topicName", topicName);
            cmd.Parameters.AddWithValue("$name", name);
            cmd.ExecuteNonQuery();

            string subPath = $"{topicName}/subscriptions/{name}";
            var schedCmd = connection.CreateCommand();
            schedCmd.Transaction = transaction;
            schedCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $subPath COLLATE NOCASE OR EntityName = $dlqPath COLLATE NOCASE";
            schedCmd.Parameters.AddWithValue("$subPath", subPath);
            schedCmd.Parameters.AddWithValue("$dlqPath", $"{subPath}/$deadletterqueue");
            schedCmd.ExecuteNonQuery();

            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            _logger.LogError(ex, "Failed to delete subscription {Topic}/{Sub} from database", topicName, name);
            lock (topic.Subscriptions)
            {
                topic.Subscriptions.Add(subscription);
            }
            return false;
        }

        try
        {
            subscription.Dispatcher?.Dispose();
            subscription.DlqDispatcher?.Dispose();
            subscription.MessageStore?.Dispose();
            subscription.DlqMessageStore?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing runtime structures for subscription {Topic}/{Sub}", topicName, name);
        }

        string subDir = SubscriptionDirectory(topicName, name);
        if (Directory.Exists(subDir))
        {
            try
            {
                DeleteDirectoryWithRetry(subDir);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to purge storage directory {Dir} for subscription {Topic}/{Sub}", subDir, topicName, name);
            }
        }

        _logger.LogInformation("Deleted subscription {Topic}/{Sub} and purged its storage.", topicName, name);
        return true;
    }

    private static void DeleteDirectoryWithRetry(string path)
    {
        for (int i = 0; i < 5; i++)
        {
            try
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, true);
                }
                return;
            }
            catch (IOException)
            {
                if (i == 4) throw;
                Thread.Sleep(100);
            }
        }
    }

    public bool SaveRule(string topicName, string subName, RuleInfo rule)
    {
        rule.NormalizeFilter();
        if (!_topics.TryGetValue(topicName, out var topic)) return false;
        var sub = topic.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
        if (sub == null) return false;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        try
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = @"
                INSERT INTO Rules (TopicName, SubscriptionName, RuleName, FilterType, FilterExpression, ActionExpression)
                VALUES ($topicName, $subName, $ruleName, $filterType, $filterExpression, $actionExpression)
                ON CONFLICT(TopicName, SubscriptionName, RuleName) DO UPDATE SET
                    FilterType = excluded.FilterType,
                    FilterExpression = excluded.FilterExpression,
                    ActionExpression = excluded.ActionExpression;
            ";
            cmd.Parameters.AddWithValue("$topicName", topic.Name);
            cmd.Parameters.AddWithValue("$subName", sub.Name);
            lock (sub.Rules)
            {
                if (sub.Rules.Find(r => string.Equals(r.Name, rule.Name, StringComparison.OrdinalIgnoreCase)) is { } stored)
                    rule.Name = stored.Name; // SQLite compares names case-sensitively
            }
            cmd.Parameters.AddWithValue("$ruleName", rule.Name);
            cmd.Parameters.AddWithValue("$filterType", rule.FilterType);
            cmd.Parameters.AddWithValue("$filterExpression", rule.FilterExpression ?? "");
            cmd.Parameters.AddWithValue("$actionExpression", rule.ActionExpression ?? "");
            cmd.ExecuteNonQuery();

            transaction.Commit();

            lock (sub.Rules)
            {
                var existingIndex = sub.Rules.FindIndex(r => string.Equals(r.Name, rule.Name, StringComparison.OrdinalIgnoreCase));
                if (existingIndex >= 0)
                {
                    sub.Rules[existingIndex] = rule;
                }
                else
                {
                    sub.Rules.Add(rule);
                }
            }

            sub.Activity.Record();
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save rule {RuleName} for subscription {TopicName}/{SubName}", rule.Name, topicName, subName);
            return false;
        }
    }

    /// <summary>A rule of a subscription by name (case-insensitively), or null if the subscription or rule does not exist.</summary>
    public RuleInfo? GetRule(string topicName, string subName, string ruleName)
    {
        var sub = GetSubscription(topicName, subName);
        if (sub == null) return null;
        lock (sub.Rules) return sub.Rules.Find(r => string.Equals(r.Name, ruleName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>A snapshot of a subscription's rules, or null if the subscription does not exist.</summary>
    public IReadOnlyList<RuleInfo>? GetRules(string topicName, string subName)
    {
        var sub = GetSubscription(topicName, subName);
        if (sub == null) return null;
        lock (sub.Rules) return sub.Rules.ToList();
    }

    /// <summary>Adds a rule. NotFound: no such subscription; Rejected: a rule of that name exists.</summary>
    public UpdateOutcome CreateRule(string topicName, string subName, RuleInfo rule)
    {
        lock (_createLock)
        {
            if (GetSubscription(topicName, subName) is null) return UpdateOutcome.NotFound;
            if (GetRule(topicName, subName, rule.Name) is not null) return UpdateOutcome.Rejected($"Rule '{rule.Name}' already exists.");
            return SaveRule(topicName, subName, rule) ? UpdateOutcome.Applied : throw new InvalidOperationException($"Failed to persist rule '{rule.Name}'.");
        }
    }

    /// <summary>Replaces an existing rule's filter and action. NotFound if the subscription or rule does not exist.</summary>
    public UpdateOutcome UpdateRule(string topicName, string subName, RuleInfo rule)
    {
        lock (_createLock)
        {
            if (GetRule(topicName, subName, rule.Name) is null) return UpdateOutcome.NotFound;
            return SaveRule(topicName, subName, rule) ? UpdateOutcome.Applied : throw new InvalidOperationException($"Failed to persist rule '{rule.Name}'.");
        }
    }

    public bool DeleteRule(string topicName, string subName, string ruleName)
    {
        if (!_topics.TryGetValue(topicName, out var topic)) return false;
        var sub = topic.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
        if (sub == null) return false;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        try
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DELETE FROM Rules WHERE TopicName = $topicName AND SubscriptionName = $subName AND RuleName = $ruleName COLLATE NOCASE";
            cmd.Parameters.AddWithValue("$topicName", topic.Name);
            cmd.Parameters.AddWithValue("$subName", sub.Name);
            cmd.Parameters.AddWithValue("$ruleName", ruleName);
            int rows = cmd.ExecuteNonQuery();

            transaction.Commit();

            if (rows > 0)
            {
                lock (sub.Rules)
                {
                    sub.Rules.RemoveAll(r => string.Equals(r.Name, ruleName, StringComparison.OrdinalIgnoreCase));
                }
                sub.Activity.Record();
                return true;
            }
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete rule {RuleName} for subscription {TopicName}/{SubName}", ruleName, topicName, subName);
            return false;
        }
    }

    private async Task RunTtlScanLoopAsync(CancellationToken ct)
    {
        _logger.LogInformation("Starting TTL scan loop");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                var now = DateTime.UtcNow;
                foreach (var queue in _queues.Values)
                    await ScanEntityForExpiredMessagesAsync(queue, now, ct).ConfigureAwait(false);

                foreach (var topic in _topics.Values)
                {
                    List<SubscriptionInfo> subs;
                    lock (topic.Subscriptions) { subs = new List<SubscriptionInfo>(topic.Subscriptions); }
                    foreach (var sub in subs)
                        await ScanEntityForExpiredMessagesAsync(sub, now, ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
        _logger.LogInformation("TTL scan loop stopped");
    }

    private async Task ScanEntityForExpiredMessagesAsync(IMessageEntity entity, DateTime now, CancellationToken ct)
    {
        var store = entity.MessageStore;
        var dispatcher = entity.Dispatcher;
        if (store == null || dispatcher == null) return;

        var log = store.Log;
        long snapshotTail = log.TailAddress;
        if (snapshotTail <= log.BeginAddress) return;

        try
        {
            using var scanner = log.Scan(log.BeginAddress, snapshotTail);
            while (await scanner.WaitAsync(ct).ConfigureAwait(false))
            {
                while (scanner.GetNext(out byte[] data, out _, out long address, out _))
                {
                    ct.ThrowIfCancellationRequested();

                    var header = MessageSerializer.DecodeHeader(data);

                    // Determine effective TTL: per-message TTL if set, else entity default
                    long effectiveTtlTicks = header.TimeToLiveTicks;
                    if (effectiveTtlTicks == 0 && entity.DefaultMessageTimeToLive != TimeSpan.MaxValue)
                        effectiveTtlTicks = entity.DefaultMessageTimeToLive.Ticks;

                    if (effectiveTtlTicks == 0) continue;

                    long expiryTicks = header.EnqueueTimeUtcTicks + effectiveTtlTicks;
                    if (now.Ticks <= expiryTicks) continue;

                    // Only messages still waiting for their first delivery: in-flight ones are checked at delivery,
                    // and records the delivery loop has passed (completed, or already retired) are not messages any more.
                    if (!dispatcher.IsAwaitingFirstDelivery(address)) continue;

                    LogExpiry(entity, header.SequenceNumber);
                    await RetireExpiredUndeliveredAsync(entity, address, data).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TTL scan for entity {EntityName}", entity.Name);
        }
    }

    private void LogExpiry(IMessageEntity entity, long sequenceNumber) =>
        _logger.LogInformation("Message seq={SeqNum} has expired (TTL) on entity {EntityName}; {Action}",
            sequenceNumber, entity.Name, entity.DeadLetteringOnMessageExpiration ? "dead-lettering" : "discarding");

    /// <summary>
    /// Retires an expired message the delivery loop has not reached yet: dead-lettered if the entity has
    /// DeadLetteringOnMessageExpiration, otherwise discarded. Either way the address is marked so the loop skips it.
    /// </summary>
    private async Task RetireExpiredUndeliveredAsync(IMessageEntity entity, long address, byte[] data)
    {
        var dispatcher = entity.Dispatcher;
        if (dispatcher == null) return;

        if (!entity.DeadLetteringOnMessageExpiration)
        {
            dispatcher.MarkTtlExpired(address);
            entity.DecrementMessageCount();
            return;
        }

        if (entity.DlqMessageStore == null) return;
        try
        {
            var (header, message) = MessageSerializer.Decode(data);

            message.ApplicationProperties ??= new ApplicationProperties();
            message.ApplicationProperties[DeadLetterDetails.ReasonProperty] = DeadLetterDetails.TtlExpired.Reason;
            message.ApplicationProperties[DeadLetterDetails.DescriptionProperty] = DeadLetterDetails.TtlExpired.Description;

            await MoveToDeadLetterLogAsync(entity, header, message).ConfigureAwait(false);

            // Mark the address so the iterator skips it when it reaches this position.
            dispatcher.MarkTtlExpired(address);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error dead-lettering expired message at {Address} for entity {EntityName}", address, entity.Name);
        }
    }

    public void Dispose()
    {
        _ttlCts.Cancel();
        _schedulerCts.Cancel();
        _transactionCts.Cancel();
        _autoDeleteCts.Cancel();
        try { _autoDeleteTask.GetAwaiter().GetResult(); } catch { }
        try { PersistActivity(); } catch (Exception ex) { _logger.LogWarning(ex, "Failed to store entity activity times on shutdown"); }
        try { _ttlTask.GetAwaiter().GetResult(); } catch { }
        try { _schedulerTask.GetAwaiter().GetResult(); } catch { }
        try { _transactionTask.GetAwaiter().GetResult(); } catch { }
        _ttlCts.Dispose();
        _schedulerCts.Dispose();
        _transactionCts.Dispose();
        _autoDeleteCts.Dispose();

        foreach (var queue in _queues.Values)
        {
            try { queue.Dispatcher?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing dispatcher for queue {Name}", queue.Name); }
            try { queue.DlqDispatcher?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing DLQ dispatcher for queue {Name}", queue.Name); }
            try { queue.MessageStore?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing store for queue {Name}", queue.Name); }
            try { queue.DlqMessageStore?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing DLQ store for queue {Name}", queue.Name); }
        }
        foreach (var topic in _topics.Values)
        {
            foreach (var sub in topic.Subscriptions)
            {
                try { sub.Dispatcher?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing dispatcher for subscription {Topic}/{Name}", sub.TopicName, sub.Name); }
                try { sub.DlqDispatcher?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing DLQ dispatcher for subscription {Topic}/{Name}", sub.TopicName, sub.Name); }
                try { sub.MessageStore?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing store for subscription {Topic}/{Name}", sub.TopicName, sub.Name); }
                try { sub.DlqMessageStore?.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Error disposing DLQ store for subscription {Topic}/{Name}", sub.TopicName, sub.Name); }
            }
        }
    }

    // ---------------- Scheduled Messages Repository & Worker ----------------

    public List<long> ScheduleMessages(string entityName, List<(byte[] payload, DateTime scheduledEnqueueTimeUtc)> messages)
    {
        RecordActivity(entityName);
        var seqNumbers = new List<long>();
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        try
        {
            foreach (var (payload, time) in messages)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = @"
                    INSERT INTO ScheduledMessages (EntityName, ScheduledEnqueueTimeUtcTicks, Payload)
                    VALUES (@entityName, @timeTicks, @payload);
                    SELECT last_insert_rowid();
                ";
                command.Parameters.AddWithValue("@entityName", entityName);
                command.Parameters.AddWithValue("@timeTicks", time.Ticks);
                command.Parameters.AddWithValue("@payload", payload);

                long seqNum = (long)command.ExecuteScalar()!;
                seqNumbers.Add(seqNum);
            }
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }

        return seqNumbers;
    }

    public void CancelScheduledMessages(List<long> sequenceNumbers)
    {
        if (sequenceNumbers == null || sequenceNumbers.Count == 0) return;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        try
        {
            foreach (var seqNum in sequenceNumbers)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = "DELETE FROM ScheduledMessages WHERE SequenceNumber = @seqNum";
                command.Parameters.AddWithValue("@seqNum", seqNum);
                command.ExecuteNonQuery();
            }
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    public int GetScheduledMessageCount(string entityName)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM ScheduledMessages WHERE EntityName = @entityName COLLATE NOCASE";
        command.Parameters.AddWithValue("@entityName", entityName);

        return Convert.ToInt32(command.ExecuteScalar());
    }

    private sealed record DueScheduledMessage(long SequenceNumber, string EntityName, byte[] Payload);

    private List<DueScheduledMessage> GetDueScheduledMessagesBatch(long maxTicks, int limit)
    {
        var list = new List<DueScheduledMessage>();
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT SequenceNumber, EntityName, Payload 
            FROM ScheduledMessages 
            WHERE ScheduledEnqueueTimeUtcTicks <= @maxTicks 
            ORDER BY SequenceNumber ASC
            LIMIT @limit";
        command.Parameters.AddWithValue("@maxTicks", maxTicks);
        command.Parameters.AddWithValue("@limit", limit);

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            long seqNum = reader.GetInt64(0);
            string entityName = reader.GetString(1);
            byte[] payload = (byte[])reader.GetValue(2);
            list.Add(new DueScheduledMessage(seqNum, entityName, payload));
        }

        return list;
    }

    private void DeleteScheduledMessage(long seqNum)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM ScheduledMessages WHERE SequenceNumber = @seqNum";
        command.Parameters.AddWithValue("@seqNum", seqNum);
        command.ExecuteNonQuery();
    }

    private async Task RunSchedulerScanLoopAsync(CancellationToken ct)
    {
        _logger.LogInformation("Starting scheduled messages scan loop");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                await ProcessDueScheduledMessagesAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        _logger.LogInformation("Scheduled messages scan loop stopped");
    }

    private async Task ProcessDueScheduledMessagesAsync(CancellationToken ct)
    {
        var nowTicks = DateTime.UtcNow.Ticks;
        const int BatchSize = 100;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var dueMessages = GetDueScheduledMessagesBatch(nowTicks, limit: BatchSize);
            if (dueMessages.Count == 0)
            {
                break;
            }

            foreach (var msg in dueMessages)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    bool success = EnqueueScheduledMessage(msg.EntityName, msg.SequenceNumber, msg.Payload);
                    if (success)
                    {
                        DeleteScheduledMessage(msg.SequenceNumber);
                        _logger.LogInformation("Successfully enqueued scheduled message seq={SeqNum} to entity {EntityName}", msg.SequenceNumber, msg.EntityName);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process due scheduled message seq={SeqNum} for entity {EntityName}", msg.SequenceNumber, msg.EntityName);
                }
            }
        }
    }

    private bool EnqueueScheduledMessage(string entityName, long seqNum, byte[] payload)
    {
        var message = Message.Decode(new ByteBuffer(payload, 0, payload.Length, payload.Length));

        // Queues keep the sequence number handed out at schedule time; topic fan-out assigns per-subscription numbers.
        // Flush before returning: the caller deletes the scheduled row next.
        if (Route(entityName, message, new RouteOptions(SequenceNumber: seqNum, Flush: true)))
            return true;

        _logger.LogWarning("Scheduled message target entity {EntityName} not found. Leaving in scheduled state.", entityName);
        return false;
    }

    // ---------------- Session State & Locks Registry ----------------

    public byte[]? GetSessionState(string entityName, string sessionId)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT State FROM SessionStates WHERE EntityName = @entityName AND SessionId = @sessionId";
        cmd.Parameters.AddWithValue("@entityName", entityName);
        cmd.Parameters.AddWithValue("@sessionId", sessionId);
        var val = cmd.ExecuteScalar();
        return val is byte[] bytes ? bytes : null;
    }

    public void SetSessionState(string entityName, string sessionId, byte[]? state)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var cmd = connection.CreateCommand();
        if (state == null)
        {
            cmd.CommandText = "DELETE FROM SessionStates WHERE EntityName = @entityName AND SessionId = @sessionId";
        }
        else
        {
            cmd.CommandText = @"
                INSERT INTO SessionStates (EntityName, SessionId, State)
                VALUES (@entityName, @sessionId, @state)
                ON CONFLICT(EntityName, SessionId) DO UPDATE SET State = excluded.State;
            ";
            cmd.Parameters.AddWithValue("@state", state);
        }
        cmd.Parameters.AddWithValue("@entityName", entityName);
        cmd.Parameters.AddWithValue("@sessionId", sessionId);
        cmd.ExecuteNonQuery();
    }

    public SessionLockState? TryLockSession(string entityPath, string sessionId, string linkName, TimeSpan lockDuration, object? activeLink = null)
    {
        while (true)
        {
            if (_sessionLocks.TryGetValue((entityPath, sessionId), out var lockState)
                && IsHeldByAnotherLink(lockState, linkName, DateTime.UtcNow))
            {
                return null;
            }
            var claimed = TryClaimSession(entityPath, sessionId, linkName, lockDuration, activeLink ?? lockState?.ActiveLink);
            if (claimed != null) return claimed;
            // Another receiver changed the lock between the read and the claim: look again.
        }
    }

    public SessionLockState? TryLockNextAvailableSession(string entityPath, string linkName, TimeSpan lockDuration, object? activeLink = null)
    {
        IMessageEntity? entity = GetQueue(entityPath);
        if (entity == null)
        {
            int subIndex = entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
            if (subIndex > 0)
            {
                string topicName = entityPath[..subIndex];
                string subName = entityPath[(subIndex + "/subscriptions/".Length)..];
                var topic = GetTopic(topicName);
                entity = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
            }
        }

        if (entity == null) return null;

        var now = DateTime.UtcNow;
        foreach (var kvp in entity.SessionMessageAddresses)
        {
            string sessionId = kvp.Key;
            var q = kvp.Value;

            if (q.IsEmpty) continue;

            if (_sessionLocks.TryGetValue((entityPath, sessionId), out var lockState) && IsHeldByAnotherLink(lockState, linkName, now)) continue;
            var claimed = TryClaimSession(entityPath, sessionId, linkName, lockDuration, activeLink);
            if (claimed != null) return claimed;
        }

        return null;
    }

    /// <summary>A lock keeps other receivers out while it hasn't expired and its receive link is open.</summary>
    private static bool IsHeldByAnotherLink(SessionLockState lockState, string linkName, DateTime now) =>
        lockState.LockedUntil > now
        && lockState.ActiveLink is Amqp.Listener.ListenerLink link && !link.IsClosed
        && !string.Equals(lockState.LockedByLink, linkName, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Replaces the session's lock with one for <paramref name="linkName"/>, but only if nobody changed it since the
    /// caller looked, so two receivers claiming the same session at once can't both win. Returns null if that happened.
    /// </summary>
    private SessionLockState? TryClaimSession(string entityPath, string sessionId, string linkName, TimeSpan lockDuration, object? activeLink)
    {
        var key = (entityPath, sessionId);
        var newLock = new SessionLockState
        {
            SessionId = sessionId,
            LockedByLink = linkName,
            LockedUntil = DateTime.UtcNow + lockDuration,
            ActiveLink = activeLink
        };
        bool claimed = _sessionLocks.TryGetValue(key, out var current)
            ? !IsHeldByAnotherLink(current, linkName, DateTime.UtcNow) && _sessionLocks.TryUpdate(key, newLock, current)
            : _sessionLocks.TryAdd(key, newLock);
        return claimed ? newLock : null;
    }

    public bool ReleaseSessionLock(string entityPath, string sessionId, string linkName)
    {
        var key = (entityPath, sessionId);
        if (_sessionLocks.TryGetValue(key, out var lockState))
        {
            if (string.Equals(lockState.LockedByLink, linkName, StringComparison.OrdinalIgnoreCase))
            {
                return _sessionLocks.TryRemove(key, out _);
            }
        }
        return false;
    }

    /// <summary>
    /// Extends a session lock that hasn't expired, whichever receiver holds it: renewals arrive on the management
    /// link, which can't say which receive link it speaks for. Returns the new expiry, or null if the lock is gone.
    /// </summary>
    public DateTime? RenewSessionLock(string entityPath, string sessionId, TimeSpan lockDuration)
    {
        if (_sessionLocks.TryGetValue((entityPath, sessionId), out var lockState) && lockState.LockedUntil > DateTime.UtcNow)
        {
            lockState.LockedUntil = DateTime.UtcNow + lockDuration;
            return lockState.LockedUntil;
        }
        return null;
    }

    public IEnumerable<string> GetActiveSessions(string entityPath)
    {
        var now = DateTime.UtcNow;
        return _sessionLocks
            .Where(kvp => string.Equals(kvp.Key.entityPath, entityPath, StringComparison.OrdinalIgnoreCase) && kvp.Value.LockedUntil > now)
            .Select(kvp => kvp.Value.SessionId)
            .Distinct(StringComparer.OrdinalIgnoreCase);
    }

    // ---------------- Message Locks Registry ----------------

    public void RegisterLock(Guid lockToken, long sequenceNumber, long logAddress, DateTimeOffset lockedUntil, string entityPath, bool isDlq, string linkName)
    {
        var lockState = new MessageLockState(lockToken, sequenceNumber, logAddress, lockedUntil, entityPath, isDlq, linkName);
        _activeLocks[lockToken] = lockState;
        _logger.LogDebug("Registered lock {LockToken} for entity {EntityPath} (seq={SequenceNumber}, address={LogAddress}, expires={LockedUntil})", 
            lockToken, entityPath, sequenceNumber, logAddress, lockedUntil);
    }

    public bool TryGetLock(Guid lockToken, [NotNullWhen(true)] out MessageLockState? lockState)
    {
        return _activeLocks.TryGetValue(lockToken, out lockState);
    }

    public bool TryRemoveLock(Guid lockToken, [NotNullWhen(true)] out MessageLockState? lockState)
    {
        if (_activeLocks.TryRemove(lockToken, out lockState))
        {
            _logger.LogDebug("Removed lock {LockToken} for entity {EntityPath}", lockToken, lockState.EntityPath);
            return true;
        }
        return false;
    }

    public bool RenewLock(Guid lockToken, TimeSpan duration, string entityPath, out DateTimeOffset newLockedUntil)
    {
        newLockedUntil = default;
        if (!_activeLocks.TryGetValue(lockToken, out var lockState))
        {
            _logger.LogWarning("Lock token {LockToken} not found in active locks", lockToken);
            return false;
        }

        // Validate that the request path matches the locked entity path (case-insensitive)
        var normalizedRequestPath = entityPath.Trim('/').ToLowerInvariant();
        var normalizedLockPath = lockState.EntityPath.Trim('/').ToLowerInvariant();

        if (normalizedRequestPath != normalizedLockPath)
        {
            _logger.LogWarning("Lock renewal request path '{RequestPath}' does not match locked entity path '{LockPath}'", 
                entityPath, lockState.EntityPath);
            return false;
        }

        // Validate that the lock has not already expired
        if (lockState.LockedUntil <= DateTimeOffset.UtcNow)
        {
            _logger.LogWarning("Lock token {LockToken} has already expired (expired at {ExpiredAt})", lockToken, lockState.LockedUntil);
            return false;
        }

        // Resolve the dispatcher for the entity (and its DLQ if applicable)
        Dispatcher? dispatcher = null;
        if (lockState.IsDlq)
        {
            var queue = GetQueue(lockState.EntityPath);
            if (queue != null) dispatcher = queue.DlqDispatcher;
            else
            {
                int subIndex = lockState.EntityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
                if (subIndex > 0)
                {
                    string topicName = lockState.EntityPath[..subIndex];
                    string subName = lockState.EntityPath[(subIndex + "/subscriptions/".Length)..];
                    var topic = GetTopic(topicName);
                    var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                    if (sub != null) dispatcher = sub.DlqDispatcher;
                }
            }
        }
        else
        {
            var queue = GetQueue(lockState.EntityPath);
            if (queue != null) dispatcher = queue.Dispatcher;
            else
            {
                int subIndex = lockState.EntityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
                if (subIndex > 0)
                {
                    string topicName = lockState.EntityPath[..subIndex];
                    string subName = lockState.EntityPath[(subIndex + "/subscriptions/".Length)..];
                    var topic = GetTopic(topicName);
                    var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                    if (sub != null) dispatcher = sub.Dispatcher;
                }
            }
        }

        if (dispatcher == null)
        {
            _logger.LogWarning("Dispatcher not found for lock token {LockToken} on entity {EntityPath}", lockToken, lockState.EntityPath);
            return false;
        }

        newLockedUntil = DateTimeOffset.UtcNow + duration;

        // Try to renew the lock in the dispatcher's active lock dictionary
        if (dispatcher.TryRenewLock(lockState.LogAddress, lockToken, newLockedUntil))
        {
            lockState.LockedUntil = newLockedUntil;
            _logger.LogDebug("Renewed lock {LockToken} for entity {EntityPath} until {NewExpiry}", lockToken, lockState.EntityPath, newLockedUntil);
            return true;
        }

        _logger.LogWarning("Dispatcher failed to renew lock {LockToken} at address {Address}", lockToken, lockState.LogAddress);
        return false;
    }

    // ---------------- Transactions Management ----------------

    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromMinutes(2);

    public byte[] DeclareTransaction()
    {
        byte[] txnId = Guid.NewGuid().ToByteArray();
        var key = Convert.ToHexString(txnId);
        _transactions[key] = new TransactionState(txnId);
        _logger.LogDebug("Declared transaction {TxnId}", key);
        return txnId;
    }

    public void BufferSend(byte[] txnId, string entityPath, Message message, bool isDlq)
    {
        var key = Convert.ToHexString(txnId);
        if (!_transactions.TryGetValue(key, out var state))
            throw new AmqpException(new Error(ErrorCode.NotFound) { Description = "Transaction not found." });

        if (DateTime.UtcNow - state.LastActive > TransactionTimeout)
        {
            RollbackTransaction(txnId);
            throw new AmqpException(new Error(ErrorCode.TransactionTimeout) { Description = "Transaction timed out due to inactivity." });
        }

        int size = message.Encode().Length;

        if (state.Sends.Count + state.Settlements.Count >= 1000)
            throw new AmqpException(new Error(ErrorCode.ResourceLimitExceeded) { Description = "Transaction operation count limit (1000) exceeded." });

        if (state.TotalSize + size > 16 * 1024 * 1024)
            throw new AmqpException(new Error(ErrorCode.ResourceLimitExceeded) { Description = "Transaction size limit (16MB) exceeded." });

        state.Sends.Add(new BufferedSend(entityPath, message, isDlq, size));
        state.TotalSize += size;
        state.LastActive = DateTime.UtcNow;
        _logger.LogDebug("Buffered send on transaction {TxnId} for {EntityPath} (size={Size} bytes)", key, entityPath, size);
    }

    public void BufferSettlement(byte[] txnId, Guid lockToken, object outcome, DeadLetterDetails? deadLetter = null)
    {
        var key = Convert.ToHexString(txnId);
        if (!_transactions.TryGetValue(key, out var state))
            throw new AmqpException(new Error(ErrorCode.NotFound) { Description = "Transaction not found." });

        if (DateTime.UtcNow - state.LastActive > TransactionTimeout)
        {
            RollbackTransaction(txnId);
            throw new AmqpException(new Error(ErrorCode.TransactionTimeout) { Description = "Transaction timed out due to inactivity." });
        }

        if (state.Sends.Count + state.Settlements.Count >= 1000)
            throw new AmqpException(new Error(ErrorCode.ResourceLimitExceeded) { Description = "Transaction operation count limit (1000) exceeded." });

        state.Settlements.Add(new BufferedSettlement(lockToken, outcome, deadLetter));
        state.LastActive = DateTime.UtcNow;
        _logger.LogDebug("Buffered settlement on transaction {TxnId} for lock {LockToken}", key, lockToken);
    }

    public void CommitTransaction(byte[] txnId)
    {
        var key = Convert.ToHexString(txnId);
        if (!_transactions.TryGetValue(key, out var state))
        {
            _logger.LogWarning("Cannot commit: transaction {TxnId} not found or already completed", key);
            throw new AmqpException(new Error(ErrorCode.NotFound) { Description = "Transaction not found or already completed." });
        }

        if (DateTime.UtcNow - state.LastActive > TransactionTimeout)
        {
            RollbackTransaction(txnId);
            throw new AmqpException(new Error(ErrorCode.TransactionTimeout) { Description = "Transaction timed out due to inactivity." });
        }

        if (!_transactions.TryRemove(key, out state))
        {
            _logger.LogWarning("Cannot commit: transaction {TxnId} not found or already completed", key);
            throw new AmqpException(new Error(ErrorCode.NotFound) { Description = "Transaction not found or already completed." });
        }

        _logger.LogInformation("Committing transaction {TxnId} with {SendCount} sends and {SettleCount} settlements", 
            key, state.Sends.Count, state.Settlements.Count);

        // 1. Process sends
        foreach (var send in state.Sends)
        {
            if (!Route(send.EntityPath, send.Message, new RouteOptions(ToDeadLetter: send.IsDlq)))
            {
                _logger.LogWarning("Entity {EntityPath} not found for buffered send during commit", send.EntityPath);
            }
        }

        // 2. Process settlements
        foreach (var settle in state.Settlements)
        {
            ApplySettlement(settle.LockToken, settle.Outcome, settle.DeadLetter);
        }
    }

    public void RollbackTransaction(byte[] txnId)
    {
        var key = Convert.ToHexString(txnId);
        if (_transactions.TryRemove(key, out _))
        {
            _logger.LogInformation("Rolled back transaction {TxnId}", key);
        }
    }

    private void ApplySettlement(Guid lockToken, object outcome, DeadLetterDetails? deadLetter)
    {
        if (!TryRemoveLock(lockToken, out var lockState))
        {
            _logger.LogWarning("Lock token {LockToken} not found in active locks during commit", lockToken);
            return;
        }

        IMessageEntity? entity = null;
        Dispatcher? dispatcher = null;

        var queue = GetQueue(lockState.EntityPath);
        if (queue != null)
        {
            entity = queue;
            dispatcher = lockState.IsDlq ? queue.DlqDispatcher : queue.Dispatcher;
        }
        else
        {
            int subIndex = lockState.EntityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
            if (subIndex > 0)
            {
                string topicName = lockState.EntityPath[..subIndex];
                string subName = lockState.EntityPath[(subIndex + "/subscriptions/".Length)..];
                var topic = GetTopic(topicName);
                var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                if (sub != null)
                {
                    entity = sub;
                    dispatcher = lockState.IsDlq ? sub.DlqDispatcher : sub.Dispatcher;
                }
            }
        }

        if (entity == null || dispatcher == null)
        {
            _logger.LogWarning("Entity or dispatcher not found for lock token {LockToken} (path: {EntityPath}) during commit", 
                lockToken, lockState.EntityPath);
            return;
        }

        string disposition = "";
        if (outcome is string s)
        {
            disposition = s.ToLowerInvariant();
        }
        else if (outcome is Amqp.Framing.Accepted)
        {
            disposition = "completed";
        }
        else if (outcome is Amqp.Framing.Released or Amqp.Framing.Modified)
        {
            bool undeliverableHere = false;
            if (outcome is Amqp.Framing.Modified modified)
            {
                undeliverableHere = modified.UndeliverableHere;
            }
            disposition = undeliverableHere ? "defered" : "abandoned";
        }
        else if (outcome is Amqp.Framing.Rejected)
        {
            disposition = "suspended";
        }

        switch (disposition)
        {
            case "completed":
                if (dispatcher.Complete(lockState.LogAddress, lockToken))
                {
                    if (lockState.IsDlq) entity.DecrementDeadLetterMessageCount();
                    else entity.DecrementMessageCount();
                }
                break;
            case "abandoned":
                dispatcher.IncrementDeliveryCount(lockState.LogAddress);
                dispatcher.Abandon(lockState.LogAddress, lockToken, SettlementProperties(outcome, deadLetter));
                break;
            case "suspended":
                // A transactional link disposition carries its details in the Rejected outcome; management supplies them.
                var details = outcome is Amqp.Framing.Rejected rejected ? DeadLetterDetails.FromRejected(rejected) : deadLetter ?? DeadLetterDetails.None;
                DeadLetterMessageAsync(entity, lockState.LogAddress, details).GetAwaiter().GetResult();
                break;
            case "defered":
                dispatcher.Defer(lockState.LogAddress, lockToken, lockState.SequenceNumber, SettlementProperties(outcome, deadLetter));
                break;
        }
    }

    /// <summary>A transactional link settlement carries properties to modify in its Modified outcome; management supplies them.</summary>
    private static IReadOnlyDictionary<string, object?> SettlementProperties(object outcome, DeadLetterDetails? managementDetails) =>
        outcome is Amqp.Framing.Modified modified
            ? PropertiesToModify.FromModified(modified)
            : managementDetails?.PropertiesToModify ?? PropertiesToModify.None;

    private async Task RunTransactionTimeoutScanLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Transaction timeout scan loop started");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

                var now = DateTime.UtcNow;
                var timedOutKeys = new List<string>();
                foreach (var kvp in _transactions)
                {
                    if (now - kvp.Value.LastActive > TransactionTimeout)
                    {
                        timedOutKeys.Add(kvp.Key);
                    }
                }

                foreach (var key in timedOutKeys)
                {
                    if (_transactions.TryGetValue(key, out var state))
                    {
                        _logger.LogInformation("Transaction {TxnId} timed out due to inactivity, rolling back", key);
                        RollbackTransaction(state.TxnId);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in transaction timeout scan loop");
        }
        _logger.LogInformation("Transaction timeout scan loop stopped");
    }
}
