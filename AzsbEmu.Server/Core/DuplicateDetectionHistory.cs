namespace AzsbEmu.Server.Core;

/// <summary>
/// The MessageIds a queue or topic with <c>RequiresDuplicateDetection</c> accepted within its
/// <c>DuplicateDetectionHistoryTimeWindow</c>. Held in memory only: a restart forgets it (ADR 0002).
/// </summary>
public sealed class DuplicateDetectionHistory
{
    private readonly object _lock = new();
    private readonly Dictionary<string, DateTime> _expiries = new(StringComparer.Ordinal);
    private readonly Queue<(string MessageId, DateTime Expiry)> _byExpiry = new();

    /// <summary>
    /// Records <paramref name="messageId"/> as accepted for <paramref name="window"/> and returns true, or returns false
    /// (recording nothing) if it was already accepted within its window.
    /// </summary>
    public bool TryAccept(string messageId, TimeSpan window, DateTime now)
    {
        lock (_lock)
        {
            Prune(now);
            if (_expiries.TryGetValue(messageId, out var expiry) && expiry > now) return false;

            var newExpiry = window >= DateTime.MaxValue - now ? DateTime.MaxValue : now + window;
            _expiries[messageId] = newExpiry;
            _byExpiry.Enqueue((messageId, newExpiry));
            return true;
        }
    }

    public int Count
    {
        get { lock (_lock) return _expiries.Count; }
    }

    private void Prune(DateTime now)
    {
        while (_byExpiry.TryPeek(out var oldest) && oldest.Expiry <= now)
        {
            _byExpiry.Dequeue();
            // Only remove the entry if it wasn't accepted again later (which queued a newer expiry for it).
            if (_expiries.TryGetValue(oldest.MessageId, out var current) && current == oldest.Expiry)
                _expiries.Remove(oldest.MessageId);
        }
    }
}
