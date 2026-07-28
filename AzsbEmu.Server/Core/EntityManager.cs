using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Core;

public class EntityManager : IDisposable
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

        var qCmd = connection.CreateCommand();
        qCmd.CommandText = "SELECT Name, RequiresSession, LockDurationSeconds, MaxSizeInMegabytes, MaxDeliveryCount, DefaultTtlSeconds FROM Queues";
        using (var reader = qCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                long ttlSeconds = reader.IsDBNull(5) ? -1 : reader.GetInt64(5);
                var q = new QueueInfo
                {
                    Name = reader.GetString(0),
                    RequiresSession = reader.GetInt32(1) != 0,
                    LockDuration = TimeSpan.FromSeconds(reader.GetInt32(2)),
                    MaxSizeInMegabytes = reader.GetInt64(3),
                    MaxDeliveryCount = reader.IsDBNull(4) ? 10 : reader.GetInt32(4),
                    DefaultMessageTimeToLive = ttlSeconds > 0 ? TimeSpan.FromSeconds(ttlSeconds) : TimeSpan.MaxValue
                };
                InitializeEntityStorage(q);
                _queues[q.Name] = q;
            }
        }

        var tCmd = connection.CreateCommand();
        tCmd.CommandText = "SELECT Name FROM Topics";
        using (var reader = tCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var t = new TopicInfo { Name = reader.GetString(0) };
                _topics[t.Name] = t;
            }
        }

        var sCmd = connection.CreateCommand();
        sCmd.CommandText = "SELECT TopicName, Name, RequiresSession, LockDurationSeconds, MaxDeliveryCount, DefaultTtlSeconds FROM Subscriptions";
        using (var reader = sCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var topicName = reader.GetString(0);
                long ttlSeconds = reader.IsDBNull(5) ? -1 : reader.GetInt64(5);
                var sub = new SubscriptionInfo
                {
                    TopicName = topicName,
                    Name = reader.GetString(1),
                    RequiresSession = reader.GetInt32(2) != 0,
                    LockDuration = TimeSpan.FromSeconds(reader.IsDBNull(3) ? 60 : reader.GetInt32(3)),
                    MaxDeliveryCount = reader.IsDBNull(4) ? 10 : reader.GetInt32(4),
                    DefaultMessageTimeToLive = ttlSeconds > 0 ? TimeSpan.FromSeconds(ttlSeconds) : TimeSpan.MaxValue
                };
                if (_topics.TryGetValue(topicName, out var topic))
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
                    sub.Rules.Add(new RuleInfo { Name = "$Default", FilterType = "True" });
                }
            }
        }
        
        _logger.LogInformation("Loaded {QueueCount} queues and {TopicCount} topics from database.", _queues.Count, _topics.Count);
    }

    private void InitializeEntityStorage(QueueInfo queue)
    {
        string safeName = queue.Name.Replace("/", "_");
        string entityDir = Path.Combine(_dataDir, "entities", safeName);
        Directory.CreateDirectory(entityDir);

        string path = Path.Combine(entityDir, "messages.log");
        queue.MessageStore = new MessageStore(path, _loggerFactory.CreateLogger($"Store:{queue.Name}"));

        string dlqPath = Path.Combine(entityDir, "dlq.log");
        queue.DlqMessageStore = new MessageStore(dlqPath, _loggerFactory.CreateLogger($"Store:{queue.Name}/$dlq"));

        var (seqMax, count) = ScanLogForStartupState(queue.MessageStore, queue.Name, queue.RequiresSession ? queue.SessionMessageAddresses : null);
        var (dlqSeqMax, dlqCount) = ScanLogForStartupState(queue.DlqMessageStore, $"{queue.Name}/$dlq", null);
        queue.NextSequenceNumber = Math.Max(seqMax, dlqSeqMax);
        queue.MessageCount = count;
        queue.DeadLetterMessageCount = dlqCount;

        queue.Dispatcher = new Dispatcher(queue.MessageStore, _loggerFactory.CreateLogger($"Dispatcher:{queue.Name}"));
        queue.Dispatcher.StartLockMonitor(queue.LockDuration, (addr, token) => HandleLockExpiryAsync(queue, addr, token));
        queue.DlqDispatcher = new Dispatcher(queue.DlqMessageStore, _loggerFactory.CreateLogger($"Dispatcher:{queue.Name}/$dlq"));
    }

    private void InitializeEntityStorage(SubscriptionInfo sub)
    {
        string safeTopic = sub.TopicName.Replace("/", "_");
        string safeSub = sub.Name.Replace("/", "_");
        string subDir = Path.Combine(_dataDir, "entities", safeTopic, "subscriptions", safeSub);
        Directory.CreateDirectory(subDir);

        string path = Path.Combine(subDir, "messages.log");
        sub.MessageStore = new MessageStore(path, _loggerFactory.CreateLogger($"Store:{sub.TopicName}/Sub:{sub.Name}"));

        string dlqPath = Path.Combine(subDir, "dlq.log");
        sub.DlqMessageStore = new MessageStore(dlqPath, _loggerFactory.CreateLogger($"Store:{sub.TopicName}/Sub:{sub.Name}/$dlq"));

        var (seqMax, count) = ScanLogForStartupState(sub.MessageStore, $"{sub.TopicName}/Sub:{sub.Name}", sub.RequiresSession ? sub.SessionMessageAddresses : null);
        var (dlqSeqMax, dlqCount) = ScanLogForStartupState(sub.DlqMessageStore, $"{sub.TopicName}/Sub:{sub.Name}/$dlq", null);
        sub.NextSequenceNumber = Math.Max(seqMax, dlqSeqMax);
        sub.MessageCount = count;
        sub.DeadLetterMessageCount = dlqCount;

        sub.Dispatcher = new Dispatcher(sub.MessageStore, _loggerFactory.CreateLogger($"Dispatcher:{sub.TopicName}/Sub:{sub.Name}"));
        sub.Dispatcher.StartLockMonitor(sub.LockDuration, (addr, token) => HandleLockExpiryAsync(sub, addr, token));
        sub.DlqDispatcher = new Dispatcher(sub.DlqMessageStore, _loggerFactory.CreateLogger($"Dispatcher:{sub.TopicName}/Sub:{sub.Name}/$dlq"));
    }

    private (long seqMax, long count) ScanLogForStartupState(
        MessageStore store, 
        string entityName, 
        ConcurrentDictionary<string, ConcurrentQueue<long>>? sessionQueue = null)
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
                                var q = sessionQueue.GetOrAdd(sessionId, _ => new ConcurrentQueue<long>());
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

    public async Task DeadLetterMessageAsync(IMessageEntity entity, long address, string? reason, string? description)
    {
        var dispatcher = entity.Dispatcher;
        var store = entity.MessageStore;
        var dlqStore = entity.DlqMessageStore;
        
        if (dispatcher == null || store == null || dlqStore == null) return;

        try
        {
            var dataResult = await store.ReadAsync(address);
            var (oldHeader, message) = MessageSerializer.Decode(dataResult.data);
            
            var header = oldHeader with
            {
                IsDeadLettered = true,
                DeliveryCount = oldHeader.DeliveryCount + dispatcher.GetDeliveryCount(address)
            };

            message.ApplicationProperties ??= new ApplicationProperties();
            if (reason != null) message.ApplicationProperties["DeadLetterReason"] = reason;
            if (description != null) message.ApplicationProperties["DeadLetterErrorDescription"] = description;

            byte[] encoded = MessageSerializer.Encode(message, header);
            await dlqStore.AppendAsync(encoded);
             
            dispatcher.ForceComplete(address);
            entity.DecrementMessageCount();
            entity.IncrementDeadLetterMessageCount();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error dead-lettering message at address {Address} for entity {EntityName}", address, entity.Name);
        }
    }

    public bool CreateQueue(QueueInfo queue)
    {
        if (_queues.ContainsKey(queue.Name)) return false;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO Queues (Name, RequiresSession, LockDurationSeconds, MaxSizeInMegabytes, MaxDeliveryCount, DefaultTtlSeconds) VALUES ($name, $session, $lock, $size, $maxdc, $ttl)";
        cmd.Parameters.AddWithValue("$name", queue.Name);
        cmd.Parameters.AddWithValue("$session", queue.RequiresSession ? 1 : 0);
        cmd.Parameters.AddWithValue("$lock", (int)queue.LockDuration.TotalSeconds);
        cmd.Parameters.AddWithValue("$size", queue.MaxSizeInMegabytes);
        cmd.Parameters.AddWithValue("$maxdc", queue.MaxDeliveryCount);
        cmd.Parameters.AddWithValue("$ttl", queue.DefaultMessageTimeToLive == TimeSpan.MaxValue ? -1 : (long)queue.DefaultMessageTimeToLive.TotalSeconds);
        
        try {
            cmd.ExecuteNonQuery();
            InitializeEntityStorage(queue);
            _queues[queue.Name] = queue;
            ManagementAddressAdded?.Invoke($"{queue.Name}/$management");
            return true;
        } catch {
            return false;
        }
    }

    public QueueInfo? GetQueue(string name) => _queues.TryGetValue(name, out var q) ? q : null;
    
    public IEnumerable<QueueInfo> GetQueues() => _queues.Values;

    public IEnumerable<TopicInfo> GetTopics() => _topics.Values;

    public bool CreateTopic(string name)
    {
        if (_topics.ContainsKey(name)) return false;

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO Topics (Name) VALUES ($name)";
        cmd.Parameters.AddWithValue("$name", name);
        
        try {
            cmd.ExecuteNonQuery();
            _topics[name] = new TopicInfo { Name = name };
            ManagementAddressAdded?.Invoke($"{name}/$management");
            return true;
        } catch {
            return false;
        }
    }

    public TopicInfo? GetTopic(string name) => _topics.TryGetValue(name, out var t) ? t : null;

    public bool CreateSubscription(string topicName, SubscriptionInfo sub)
    {
        if (!_topics.TryGetValue(topicName, out var topic)) return false;
        
        lock (topic.Subscriptions)
        {
            if (topic.Subscriptions.Exists(s => s.Name == sub.Name)) return false;
        }

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO Subscriptions (TopicName, Name, RequiresSession, LockDurationSeconds, MaxDeliveryCount, DefaultTtlSeconds) VALUES ($tname, $name, $session, $lock, $maxdc, $ttl)";
        cmd.Parameters.AddWithValue("$tname", topicName);
        cmd.Parameters.AddWithValue("$name", sub.Name);
        cmd.Parameters.AddWithValue("$session", sub.RequiresSession ? 1 : 0);
        cmd.Parameters.AddWithValue("$lock", (int)sub.LockDuration.TotalSeconds);
        cmd.Parameters.AddWithValue("$maxdc", sub.MaxDeliveryCount);
        cmd.Parameters.AddWithValue("$ttl", sub.DefaultMessageTimeToLive == TimeSpan.MaxValue ? -1 : (long)sub.DefaultMessageTimeToLive.TotalSeconds);
        
        try {
            cmd.ExecuteNonQuery();
            
            // Auto-insert default rule in SQLite
            var ruleCmd = connection.CreateCommand();
            ruleCmd.CommandText = "INSERT INTO Rules (TopicName, SubscriptionName, RuleName, FilterType) VALUES ($tname, $sname, '$Default', 'True')";
            ruleCmd.Parameters.AddWithValue("$tname", topicName);
            ruleCmd.Parameters.AddWithValue("$sname", sub.Name);
            ruleCmd.ExecuteNonQuery();

            InitializeEntityStorage(sub);
            sub.Rules.Add(new RuleInfo { Name = "$Default", FilterType = "True" });
            
            lock (topic.Subscriptions)
            {
                topic.Subscriptions.Add(sub);
            }
            
            ManagementAddressAdded?.Invoke($"{topicName}/subscriptions/{sub.Name}/$management");
            return true;
        } catch {
            return false;
        }
    }

    public bool DeleteQueue(string name)
    {
        if (!_queues.TryRemove(name, out var queue)) return false;

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
            schedCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $name OR EntityName = $dlqName";
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

        string safeName = queue.Name.Replace("/", "_");
        string entityDir = Path.Combine(_dataDir, "entities", safeName);
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
        if (!_topics.TryRemove(name, out var topic)) return false;

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
            schedCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $name";
            schedCmd.Parameters.AddWithValue("$name", name);
            schedCmd.ExecuteNonQuery();

            foreach (var sub in subsToDispose)
            {
                string subPath = $"{name}/subscriptions/{sub.Name}";
                var schedSubCmd = connection.CreateCommand();
                schedSubCmd.Transaction = transaction;
                schedSubCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $subPath OR EntityName = $dlqPath";
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

        string safeName = name.Replace("/", "_");
        string entityDir = Path.Combine(_dataDir, "entities", safeName);
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
        if (!_topics.TryGetValue(topicName, out var topic)) return false;

        SubscriptionInfo? subscription = null;
        lock (topic.Subscriptions)
        {
            subscription = topic.Subscriptions.Find(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase));
            if (subscription == null) return false;
            topic.Subscriptions.Remove(subscription);
        }

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
            schedCmd.CommandText = "DELETE FROM ScheduledMessages WHERE EntityName = $subPath OR EntityName = $dlqPath";
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

        string safeTopic = topicName.Replace("/", "_");
        string safeSub = name.Replace("/", "_");
        string subDir = Path.Combine(_dataDir, "entities", safeTopic, "subscriptions", safeSub);
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

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save rule {RuleName} for subscription {TopicName}/{SubName}", rule.Name, topicName, subName);
            return false;
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
            cmd.CommandText = "DELETE FROM Rules WHERE TopicName = $topicName AND SubscriptionName = $subName AND RuleName = $ruleName";
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

                    // Skip messages already in-flight; delivery-time check will handle them.
                    if (dispatcher.IsTracked(address)) continue;

                    _logger.LogInformation(
                        "TTL expired for message seq={SeqNum} on entity {EntityName}, dead-lettering",
                        header.SequenceNumber, entity.Name);

                    await DeadLetterExpiredAsync(entity, address, data).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TTL scan for entity {EntityName}", entity.Name);
        }
    }

    private async Task DeadLetterExpiredAsync(IMessageEntity entity, long address, byte[] data)
    {
        var dispatcher = entity.Dispatcher;
        var dlqStore = entity.DlqMessageStore;
        if (dispatcher == null || dlqStore == null) return;

        try
        {
            var (header, message) = MessageSerializer.Decode(data);
            var dlqHeader = header with { IsDeadLettered = true };

            message.ApplicationProperties ??= new ApplicationProperties();
            message.ApplicationProperties["DeadLetterReason"] = "TTLExpiredException";
            message.ApplicationProperties["DeadLetterErrorDescription"] = "The message has expired.";

            byte[] encoded = MessageSerializer.Encode(message, dlqHeader);
            await dlqStore.AppendAsync(encoded).ConfigureAwait(false);

            // Mark the address so the iterator skips it when it reaches this position.
            dispatcher.MarkTtlExpired(address);

            entity.DecrementMessageCount();
            entity.IncrementDeadLetterMessageCount();
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
        try { _ttlTask.GetAwaiter().GetResult(); } catch { }
        try { _schedulerTask.GetAwaiter().GetResult(); } catch { }
        try { _transactionTask.GetAwaiter().GetResult(); } catch { }
        _ttlCts.Dispose();
        _schedulerCts.Dispose();
        _transactionCts.Dispose();

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
        command.CommandText = "SELECT COUNT(*) FROM ScheduledMessages WHERE EntityName = @entityName";
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
                    bool success = await EnqueueScheduledMessageAsync(msg.EntityName, msg.SequenceNumber, msg.Payload).ConfigureAwait(false);
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

    private async Task<bool> EnqueueScheduledMessageAsync(string entityName, long seqNum, byte[] payload)
    {
        var queue = GetQueue(entityName);
        if (queue != null)
        {
            await EnqueueToQueueAsync(queue, seqNum, payload).ConfigureAwait(false);
            return true;
        }

        var topic = GetTopic(entityName);
        if (topic != null)
        {
            await FanOutTopicScheduledMessageAsync(topic, seqNum, payload).ConfigureAwait(false);
            return true;
        }

        _logger.LogWarning("Scheduled message target entity {EntityName} not found. Leaving in scheduled state.", entityName);
        return false;
    }

    private async Task EnqueueToQueueAsync(QueueInfo queue, long seqNum, byte[] payload)
    {
        var store = queue.MessageStore;
        if (store == null) return;

        var innerBuf = new ByteBuffer(payload, 0, payload.Length, payload.Length);
        var message = Message.Decode(innerBuf);

        long enqueueTicks = DateTime.UtcNow.Ticks;
        var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);

        message.MessageAnnotations ??= new MessageAnnotations();
        message.MessageAnnotations[new Symbol("x-opt-sequence-number")] = seqNum;
        message.MessageAnnotations[new Symbol("x-opt-enqueued-time-utc")] = enqueuedTimeUtc;
        message.MessageAnnotations[new Symbol("x-opt-enqueued-time")] = enqueuedTimeUtc;

        long ttlTicks = 0;
        uint? msgTtlMs = message.Header?.Ttl;
        if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
            ttlTicks = TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
        else if (queue.DefaultMessageTimeToLive != TimeSpan.MaxValue)
            ttlTicks = queue.DefaultMessageTimeToLive.Ticks;

        var header = new MessageHeader
        {
            SequenceNumber = seqNum,
            EnqueueTimeUtcTicks = enqueueTicks,
            DeliveryCount = 0,
            IsDeadLettered = false,
            TimeToLiveTicks = ttlTicks
        };

        byte[] encoded = MessageSerializer.Encode(message, header);
        await store.AppendAsync(encoded).ConfigureAwait(false);

        // Thread-safe update NextSequenceNumber if seqNum is higher
        queue.UpdateNextSequenceNumber(seqNum);

        queue.IncrementMessageCount();
    }

    private async Task FanOutTopicScheduledMessageAsync(TopicInfo topic, long scheduledSeqNum, byte[] payload)
    {
        var innerBuf = new ByteBuffer(payload, 0, payload.Length, payload.Length);
        var message = Message.Decode(innerBuf);

        List<SubscriptionInfo> subs;
        lock (topic.Subscriptions)
        {
            subs = new List<SubscriptionInfo>(topic.Subscriptions);
        }

        foreach (var sub in subs)
        {
            RuleInfo? matchedRule = null;
            lock (sub.Rules)
            {
                foreach (var rule in sub.Rules)
                {
                    if (RuleEvaluator.Evaluate(rule, message))
                    {
                        matchedRule = rule;
                        break;
                    }
                }
            }

            if (matchedRule == null) continue;

            var store = sub.MessageStore;
            if (store == null) continue;

            long subSeqNum = sub.AssignNextSequenceNumber();
            long enqueueTicks = DateTime.UtcNow.Ticks;
            var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);

            var messageToStore = new Message
            {
                Header = message.Header,
                Properties = message.Properties,
                ApplicationProperties = message.ApplicationProperties,
                MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations(),
                BodySection = message.BodySection
            };
            messageToStore.MessageAnnotations[new Symbol("x-opt-sequence-number")] = subSeqNum;
            messageToStore.MessageAnnotations[new Symbol("x-opt-enqueued-time-utc")] = enqueuedTimeUtc;
            messageToStore.MessageAnnotations[new Symbol("x-opt-enqueued-time")] = enqueuedTimeUtc;

            long ttlTicks = 0;
            uint? msgTtlMs = message.Header?.Ttl;
            if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
                ttlTicks = TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
            else if (sub.DefaultMessageTimeToLive != TimeSpan.MaxValue)
                ttlTicks = sub.DefaultMessageTimeToLive.Ticks;

            var header = new MessageHeader
            {
                SequenceNumber = subSeqNum,
                EnqueueTimeUtcTicks = enqueueTicks,
                DeliveryCount = 0,
                IsDeadLettered = false,
                TimeToLiveTicks = ttlTicks
            };

            byte[] encoded = MessageSerializer.Encode(messageToStore, header);
            await store.AppendAsync(encoded).ConfigureAwait(false);
            sub.IncrementMessageCount();
        }
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
        var key = (entityPath, sessionId);
        var now = DateTime.UtcNow;

        if (_sessionLocks.TryGetValue(key, out var lockState) && lockState.LockedUntil > now)
        {
            if (lockState.ActiveLink is Amqp.Listener.ListenerLink existingLink && !existingLink.IsClosed)
            {
                if (!string.Equals(lockState.LockedByLink, linkName, StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }
            }

            lockState.LockedByLink = linkName;
            lockState.LockedUntil = now + lockDuration;
            if (activeLink != null) lockState.ActiveLink = activeLink;
            return lockState;
        }

        var newLock = new SessionLockState
        {
            SessionId = sessionId,
            LockedByLink = linkName,
            LockedUntil = now + lockDuration,
            ActiveLink = activeLink
        };
        _sessionLocks[key] = newLock;
        return newLock;
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

            var lockKey = (entityPath, sessionId);
            if (_sessionLocks.TryGetValue(lockKey, out var lockState) && lockState.LockedUntil > now)
            {
                if (lockState.ActiveLink is Amqp.Listener.ListenerLink existingLink && !existingLink.IsClosed)
                {
                    continue;
                }
            }

            var newLock = new SessionLockState
            {
                SessionId = sessionId,
                LockedByLink = linkName,
                LockedUntil = now + lockDuration,
                ActiveLink = activeLink
            };
            _sessionLocks[lockKey] = newLock;
            return newLock;
        }

        return null;
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

    public bool RenewSessionLock(string entityPath, string sessionId, string linkName, TimeSpan lockDuration)
    {
        var key = (entityPath, sessionId);
        if (_sessionLocks.TryGetValue(key, out var lockState) && lockState.LockedUntil > DateTime.UtcNow)
        {
            if (string.Equals(lockState.LockedByLink, linkName, StringComparison.OrdinalIgnoreCase))
            {
                lockState.LockedUntil = DateTime.UtcNow + lockDuration;
                return true;
            }
        }
        return false;
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

    public void BufferSettlement(byte[] txnId, Guid lockToken, object outcome, string? dlReason = null, string? dlDescription = null)
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

        state.Settlements.Add(new BufferedSettlement(lockToken, outcome, dlReason, dlDescription));
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
            var queue = GetQueue(send.EntityPath);
            if (queue != null)
            {
                CommitSendToQueue(queue, send.Message, send.IsDlq);
            }
            else
            {
                var topic = GetTopic(send.EntityPath);
                if (topic != null)
                {
                    CommitSendToTopic(topic, send.Message);
                }
                else
                {
                    _logger.LogWarning("Entity {EntityPath} not found for buffered send during commit", send.EntityPath);
                }
            }
        }

        // 2. Process settlements
        foreach (var settle in state.Settlements)
        {
            ApplySettlement(settle.LockToken, settle.Outcome, settle.DeadLetterReason, settle.DeadLetterDescription);
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

    private void CommitSendToQueue(QueueInfo queue, Message message, bool isDlq)
    {
        var store = isDlq ? queue.DlqMessageStore : queue.MessageStore;
        if (store == null) return;

        long seqNum = queue.AssignNextSequenceNumber();
        long enqueueTicks = DateTime.UtcNow.Ticks;
        long ttlTicks = 0;

        uint? msgTtlMs = message.Header?.Ttl;
        if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
            ttlTicks = TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
        else if (queue.DefaultMessageTimeToLive != TimeSpan.MaxValue)
            ttlTicks = queue.DefaultMessageTimeToLive.Ticks;

        var messageToStore = new Message
        {
            Header = message.Header,
            Properties = message.Properties,
            ApplicationProperties = message.ApplicationProperties,
            MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations(),
            BodySection = message.BodySection
        };
        messageToStore.MessageAnnotations[new Symbol("x-opt-sequence-number")] = seqNum;
        var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);
        messageToStore.MessageAnnotations[new Symbol("x-opt-enqueued-time-utc")] = enqueuedTimeUtc;
        messageToStore.MessageAnnotations[new Symbol("x-opt-enqueued-time")] = enqueuedTimeUtc;

        var header = new MessageHeader
        {
            SequenceNumber = seqNum,
            EnqueueTimeUtcTicks = enqueueTicks,
            DeliveryCount = 0,
            IsDeadLettered = isDlq,
            TimeToLiveTicks = ttlTicks
        };

        byte[] encoded = MessageSerializer.Encode(messageToStore, header);
        long address = store.Append(encoded);

        if (isDlq) queue.IncrementDeadLetterMessageCount();
        else queue.IncrementMessageCount();

        string? sessionId = message.Properties?.GroupId;
        if (!isDlq && queue.RequiresSession && !string.IsNullOrEmpty(sessionId))
        {
            var q = queue.SessionMessageAddresses.GetOrAdd(sessionId, _ => new ConcurrentQueue<long>());
            q.Enqueue(address);
        }
    }

    private void CommitSendToTopic(TopicInfo topic, Message message)
    {
        List<SubscriptionInfo> subs;
        lock (topic.Subscriptions)
        {
            subs = new List<SubscriptionInfo>(topic.Subscriptions);
        }

        foreach (var sub in subs)
        {
            RuleInfo? matchedRule = null;
            lock (sub.Rules)
            {
                foreach (var rule in sub.Rules)
                {
                    if (RuleEvaluator.Evaluate(rule, message))
                    {
                        matchedRule = rule;
                        break;
                    }
                }
            }

            if (matchedRule == null) continue;

            var subStore = sub.MessageStore;
            if (subStore == null) continue;

            try
            {
                long seqNum = sub.AssignNextSequenceNumber();
                long enqueueTicks = DateTime.UtcNow.Ticks;
                long ttlTicks = 0;

                uint? msgTtlMs = message.Header?.Ttl;
                if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
                    ttlTicks = TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
                else if (sub.DefaultMessageTimeToLive != TimeSpan.MaxValue)
                    ttlTicks = sub.DefaultMessageTimeToLive.Ticks;

                var messageToStore = new Message
                {
                    Header = message.Header,
                    Properties = message.Properties,
                    ApplicationProperties = message.ApplicationProperties ?? new ApplicationProperties(),
                    MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations(),
                    BodySection = message.BodySection
                };
                messageToStore.MessageAnnotations[new Symbol("x-opt-sequence-number")] = seqNum;
                var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);
                messageToStore.MessageAnnotations[new Symbol("x-opt-enqueued-time-utc")] = enqueuedTimeUtc;
                messageToStore.MessageAnnotations[new Symbol("x-opt-enqueued-time")] = enqueuedTimeUtc;

                if (sub.RequiresSession && string.IsNullOrEmpty(message.Properties?.GroupId))
                {
                    var subDlqStore = sub.DlqMessageStore;
                    if (subDlqStore != null)
                    {
                        messageToStore.ApplicationProperties["DeadLetterReason"] = "SessionIdIsNull";
                        messageToStore.ApplicationProperties["DeadLetterErrorDescription"] = "The SessionId was not set on a message sent to a session-enabled entity.";

                        var header = new MessageHeader
                        {
                            SequenceNumber = seqNum,
                            EnqueueTimeUtcTicks = enqueueTicks,
                            DeliveryCount = 0,
                            IsDeadLettered = true,
                            TimeToLiveTicks = ttlTicks
                        };

                        byte[] encoded = MessageSerializer.Encode(messageToStore, header);
                        subDlqStore.Append(encoded);
                        sub.IncrementDeadLetterMessageCount();
                    }
                    continue;
                }

                var headerNormal = new MessageHeader
                {
                    SequenceNumber = seqNum,
                    EnqueueTimeUtcTicks = enqueueTicks,
                    DeliveryCount = 0,
                    IsDeadLettered = false,
                    TimeToLiveTicks = ttlTicks
                };

                byte[] encodedNormal = MessageSerializer.Encode(messageToStore, headerNormal);
                long address = subStore.Append(encodedNormal);
                sub.IncrementMessageCount();

                string? sessionId = message.Properties?.GroupId;
                if (sub.RequiresSession && !string.IsNullOrEmpty(sessionId))
                {
                    var q = sub.SessionMessageAddresses.GetOrAdd(sessionId, _ => new ConcurrentQueue<long>());
                    q.Enqueue(address);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fan out transaction message from topic {TopicName} to subscription {SubName}", topic.Name, sub.Name);
            }
        }
    }

    private void ApplySettlement(Guid lockToken, object outcome, string? dlReason, string? dlDescription)
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
                dispatcher.Abandon(lockState.LogAddress, lockToken);
                break;
            case "suspended":
                DeadLetterMessageAsync(entity, lockState.LogAddress, dlReason, dlDescription).GetAwaiter().GetResult();
                break;
            case "defered":
                dispatcher.Defer(lockState.LogAddress, lockToken, lockState.SequenceNumber);
                break;
        }
    }

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
