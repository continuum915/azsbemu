namespace AzsbEmu.Server.Core;

/// <summary>
/// The undelivered messages of one session, by log address. Dequeues the lowest address first. Log addresses rise
/// with sequence numbers, so a message returned after an abandon or a lock expiry is delivered again ahead of the
/// session's later messages, as Service Bus does.
/// </summary>
public sealed class SessionMessageQueue
{
    private readonly SortedSet<long> _addresses = new();

    public bool IsEmpty
    {
        get { lock (_addresses) return _addresses.Count == 0; }
    }

    public void Enqueue(long address)
    {
        lock (_addresses) _addresses.Add(address);
    }

    /// <summary>The lowest undelivered address, without removing it.</summary>
    public bool TryPeekLowest(out long address)
    {
        lock (_addresses)
        {
            address = _addresses.Count == 0 ? 0 : _addresses.Min;
            return _addresses.Count > 0;
        }
    }

    public bool TryDequeue(out long address)
    {
        lock (_addresses)
        {
            if (_addresses.Count == 0)
            {
                address = 0;
                return false;
            }
            address = _addresses.Min;
            _addresses.Remove(address);
            return true;
        }
    }
}
