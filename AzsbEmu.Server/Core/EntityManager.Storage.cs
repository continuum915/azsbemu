using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Core;

// On-disk layout: every storage path is derived here, so the encoding cannot drift between create, load and delete.
//
//   <data>/entities/<encoded queue or topic name>/
//   <data>/entities/<encoded topic name>/subscriptions/<encoded subscription name>/
//
// Each queue/subscription directory holds two FASTER logs, each with its own commit metadata:
//   messages.log + log-commits/        active messages
//   dlq.log      + dlq/log-commits/    dead-lettered messages
public partial class EntityManager
{
    // Written once the data directory is in the current layout; its presence makes the legacy migration a no-op.
    private const string LayoutMarkerFile = "storage-layout-v2";

    private string EntitiesRoot => Path.Combine(_dataDir, "entities");

    /// <summary>Queues and topics share one namespace on disk, as they do in Service Bus.</summary>
    private string QueueDirectory(string queueName) => TopLevelEntityDirectory(queueName);

    private string TopicDirectory(string topicName) => TopLevelEntityDirectory(topicName);

    private string TopLevelEntityDirectory(string name) => Path.Combine(EntitiesRoot, EncodePathSegment(name));

    private string SubscriptionDirectory(string topicName, string subscriptionName) =>
        Path.Combine(TopicDirectory(topicName), "subscriptions", EncodePathSegment(subscriptionName));

    /// <summary>
    /// Commit-metadata base directory for an entity's dead-letter log. It must differ from the active log's
    /// (FASTER's default is the log file's own directory): two logs sharing one commit folder overwrite and prune
    /// each other's commits, and a clean restart then recovers a log to the wrong tail and loses messages.
    /// </summary>
    private static string DeadLetterCommitDirectory(string entityDir) => Path.Combine(entityDir, "dlq");

    /// <summary>
    /// Encodes an entity name as a single, collision-free directory name that every file system treats literally.
    /// ASCII letters, digits and '-' pass through; every other character (including '.', so "." and ".." and
    /// trailing dots cannot occur, and the '_' escape character itself) becomes '_' followed by the two-digit
    /// uppercase hex of each of its UTF-8 bytes: "a/b" → "a_2Fb", "a_b" → "a_5Fb", "a.b" → "a_2Eb".
    /// </summary>
    internal static string EncodePathSegment(string name)
    {
        var sb = new StringBuilder(name.Length);
        Span<byte> utf8 = stackalloc byte[4];
        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];
            if (char.IsAsciiLetterOrDigit(c) || c == '-')
            {
                sb.Append(c);
                continue;
            }

            int charCount = char.IsHighSurrogate(c) && i + 1 < name.Length ? 2 : 1;
            int byteCount = Encoding.UTF8.GetBytes(name.AsSpan(i, charCount), utf8);
            for (int b = 0; b < byteCount; b++) sb.Append('_').Append(utf8[b].ToString("X2"));
            i += charCount - 1;
        }
        return sb.ToString();
    }

    // ---------------- One-time migration from the legacy layout ----------------

    private sealed record PendingMove(string Parent, string EntityName, string LegacyName, string EncodedName)
    {
        public string From => Path.Combine(Parent, LegacyName);
        public string Staging => Path.Combine(Parent, EncodedName + ".migrating"); // '.' never occurs in encoded names
        public string To => Path.Combine(Parent, EncodedName);
    }

    /// <summary>
    /// Moves directories written by the legacy layout (where '/' became '_' and every other character was kept) to
    /// their encoded names, once per data directory. Runs before any storage is opened.
    /// </summary>
    /// <remarks>
    /// Within one parent directory, moves go through a staging name in two phases, because one entity's new
    /// directory can be another's legacy directory ("x/y" → "x_2Fy", which was "x_2Fy"'s legacy home).
    /// Queues and topics move first; subscriptions are then moved inside their topic's final directory.
    /// The legacy layout mapped e.g. "a/b" and "a_b" to the same directory, so their data was already mixed; the
    /// directory goes to the entity whose name literally matches it ("a_b") and the other starts empty.
    /// </remarks>
    private void MigrateLegacyStorageLayout(SqliteConnection connection)
    {
        string marker = Path.Combine(_dataDir, LayoutMarkerFile);
        if (File.Exists(marker)) return;

        try
        {
            var topLevel = ReadColumn(connection, "SELECT Name FROM Queues").Concat(ReadColumn(connection, "SELECT Name FROM Topics")).ToList();
            ExecuteMoves(PlanSiblingMoves(EntitiesRoot, topLevel));

            foreach (var subs in ReadSubscriptionNames(connection).ToLookup(s => s.TopicName, s => s.Name, StringComparer.OrdinalIgnoreCase))
            {
                ExecuteMoves(PlanSiblingMoves(Path.Combine(TopicDirectory(subs.Key), "subscriptions"), subs.ToList()));
            }

            File.WriteAllText(marker, "Entity directories use the encoded layout. Delete this file only to re-run the legacy migration.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Migrating the data directory {DataDir} from the legacy storage layout failed. Fix the cause and restart; completed moves are not repeated.", _dataDir);
            throw;
        }
    }

    private void ExecuteMoves(List<PendingMove> moves)
    {
        foreach (var move in moves) Directory.Move(move.From, move.Staging);
        foreach (var move in moves)
        {
            Directory.Move(move.Staging, move.To);
            WarnIfLegacyDeadLetterDataPresent(move.EntityName, move.To);
            _logger.LogInformation("Migrated storage for {Name} from legacy directory {From} to {To}", move.EntityName, move.LegacyName, move.EncodedName);
        }
    }

    private List<PendingMove> PlanSiblingMoves(string parent, List<string> names)
    {
        var moves = new List<PendingMove>();
        var literalNames = new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
        {
            string legacy = name.Replace("/", "_");
            string encoded = EncodePathSegment(name);
            if (string.Equals(legacy, encoded, StringComparison.OrdinalIgnoreCase)) continue;

            // A previous run crashed between the two phases: finish that move before planning anything else.
            var interrupted = new PendingMove(parent, name, legacy, encoded);
            if (Directory.Exists(interrupted.Staging) && !Directory.Exists(interrupted.To))
            {
                Directory.Move(interrupted.Staging, interrupted.To);
                _logger.LogInformation("Completed interrupted storage migration for {Name} into {To}", name, encoded);
                continue;
            }
            if (!Directory.Exists(Path.Combine(parent, legacy))) continue;

            if (!string.Equals(legacy, name, StringComparison.OrdinalIgnoreCase) && literalNames.Contains(legacy))
            {
                _logger.LogWarning(
                    "Legacy directory {Legacy} was shared by {Name} and {Owner}; it now belongs to {Owner}, and {Name} starts with empty storage.",
                    legacy, name, legacy, legacy, name);
                continue;
            }

            moves.Add(new PendingMove(parent, name, legacy, encoded));
        }
        return moves;
    }

    // Before the dead-letter log got its own commit folder, its commits were mixed with the active log's and cannot
    // be told apart, so pre-upgrade dead-lettered messages are not recoverable.
    private void WarnIfLegacyDeadLetterDataPresent(string entityName, string entityDir)
    {
        bool hasDeadLetterData = Directory.EnumerateFiles(entityDir, "dlq.log.*").Any(f => new FileInfo(f).Length > 0);
        if (hasDeadLetterData && !Directory.Exists(Path.Combine(DeadLetterCommitDirectory(entityDir), "log-commits")))
        {
            _logger.LogWarning("Dead-lettered messages stored for {Name} before this upgrade cannot be recovered and will not be visible.", entityName);
        }
    }

    private static List<string> ReadColumn(SqliteConnection connection, string sql)
    {
        var values = new List<string>();
        var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) values.Add(reader.GetString(0));
        return values;
    }

    private static List<(string TopicName, string Name)> ReadSubscriptionNames(SqliteConnection connection)
    {
        var rows = new List<(string TopicName, string Name)>();
        var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT TopicName, Name FROM Subscriptions";
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) rows.Add((reader.GetString(0), reader.GetString(1)));
        return rows;
    }
}
