using System.Collections.Concurrent;
using Amqp;

namespace AzsbEmu.Server.Core;

/// <summary>
/// When a queue, topic or subscription was last used, for <c>AutoDeleteOnIdle</c> and the <c>AccessedAt</c> the
/// management API reports. Sends, receives and management operations record activity; an open receive link keeps the
/// entity active for as long as it stays attached.
/// </summary>
public sealed class EntityActivity
{
    private long _lastActivityTicks = DateTime.UtcNow.Ticks;
    private long _persistedTicks;
    private readonly ConcurrentDictionary<Link, byte> _receivers = new();

    public DateTime LastActivityUtc => new(Volatile.Read(ref _lastActivityTicks), DateTimeKind.Utc);

    public void Record() => Volatile.Write(ref _lastActivityTicks, DateTime.UtcNow.Ticks);

    public void ReceiverAttached(Link link)
    {
        _receivers[link] = 0;
        Record();
    }

    public void ReceiverDetached(Link link)
    {
        Record();
        _receivers.TryRemove(link, out _);
    }

    /// <summary>
    /// Whether a receive link is attached. Links that closed without detaching (for example when their connection
    /// dropped) are dropped here, counting as activity at the moment they are noticed.
    /// </summary>
    public bool HasOpenReceivers()
    {
        foreach (var link in _receivers.Keys)
        {
            if (!link.IsClosed) return true;
            ReceiverDetached(link);
        }
        return false;
    }

    /// <summary>Sets the last activity read back from storage at startup.</summary>
    internal void Restore(DateTime lastActivityUtc)
    {
        Volatile.Write(ref _lastActivityTicks, lastActivityUtc.Ticks);
        Volatile.Write(ref _persistedTicks, lastActivityUtc.Ticks);
    }

    /// <summary>The last activity, if it has changed since it was last stored.</summary>
    internal bool TryGetUnpersisted(out long ticks)
    {
        ticks = Volatile.Read(ref _lastActivityTicks);
        return ticks != Volatile.Read(ref _persistedTicks);
    }

    internal void MarkPersisted(long ticks) => Volatile.Write(ref _persistedTicks, ticks);
}
