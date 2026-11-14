using FASTER.core;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace AzsbEmu.Server.Core;

public class Dispatcher : IDisposable
{
    private readonly MessageStore _store;
    private readonly ILogger _logger;
    private readonly FasterLogScanIterator _iterator;
    private readonly SemaphoreSlim _iteratorLock = new(1, 1);
    private readonly ConcurrentDictionary<long, (DateTimeOffset LockedUntil, Guid LockToken)> _lockedAddresses = new();
    private TimeSpan _lockDuration = TimeSpan.FromSeconds(60);
    private readonly ConcurrentQueue<long> _redeliveryQueue = new();
    // The session queue each delivered session message came from: abandoned and lock-expired session messages go back
    // there, not to _redeliveryQueue, which only non-session receivers read.
    private readonly ConcurrentDictionary<long, SessionMessageQueue> _sessionOfAddress = new();
    private readonly ConcurrentDictionary<long, int> _deliveryCountOverlay = new();
    // Properties to modify from abandons and defers, applied whenever the message is read back. In memory, like the
    // delivery count overlay: the stored record is immutable.
    private readonly ConcurrentDictionary<long, IReadOnlyDictionary<string, object?>> _modifiedProperties = new();
    private readonly ConcurrentDictionary<long, long> _deferredMessages = new();

    // Maps each in-flight address to the NEXT record's start address.
    // Populated on first delivery (from the iterator's GetNext nextAddress param) and
    // retained through abandon/redelivery cycles until the message is permanently settled.
    // Required for safe log truncation: we only advance BeginAddress to a proper record
    // boundary, preventing FASTER from misreading a truncated partial record header.
    private readonly ConcurrentDictionary<long, long> _pendingAddresses = new();
    private readonly object _truncateLock = new();

    // Addresses marked as TTL-expired by the background scanner before the iterator reaches them.
    // When the iterator encounters such an address it skips delivery and cleans up.
    private readonly ConcurrentDictionary<long, byte> _ttlExpiredSet = new();

    private CancellationTokenSource? _monitorCts;

    // The entity's per-session queues of undelivered messages (session entities only). The delivery loop never reaches
    // these messages, so truncation has to stop before the lowest of them, or a restart would lose them.
    private readonly ConcurrentDictionary<string, SessionMessageQueue>? _sessionQueues;

    public Dispatcher(MessageStore store, ILogger logger, ConcurrentDictionary<string, SessionMessageQueue>? sessionQueues = null)
    {
        _store = store;
        _logger = logger;
        _sessionQueues = sessionQueues;
        _iterator = _store.Log.Scan(_store.Log.BeginAddress, long.MaxValue);
    }

    /// <summary>Lock duration for messages locked from now on (entity updates change it at runtime).</summary>
    public TimeSpan LockDuration
    {
        get => _lockDuration;
        set => _lockDuration = value;
    }

    public void StartLockMonitor(TimeSpan lockDuration, Func<long, Guid, Task> onExpired)
    {
        _lockDuration = lockDuration;
        _monitorCts?.Cancel();
        _monitorCts = new CancellationTokenSource();
        _ = MonitorLoopAsync(onExpired, _monitorCts.Token);
    }

    private async Task MonitorLoopAsync(Func<long, Guid, Task> onExpired, CancellationToken ct)
    {
        _logger.LogInformation("Starting lock monitor with duration {Duration}", _lockDuration);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                var now = DateTimeOffset.UtcNow;
                foreach (var kvp in _lockedAddresses)
                {
                    if (now > kvp.Value.LockedUntil)
                    {
                        var address = kvp.Key;
                        var token = kvp.Value.LockToken;
                        if (_lockedAddresses.TryRemove(address, out var removed) && removed.LockToken == token)
                        {
                            _logger.LogDebug("Lock expired for address {Address} with token {Token}", address, token);
                            try { await onExpired(address, token).ConfigureAwait(false); }
                            catch (Exception ex) { _logger.LogError(ex, "Error handling lock expiry for address {Address}", address); }
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        _logger.LogInformation("Lock monitor stopped");
    }

    /// <summary>
    /// Delivers the next available message. Checks the redelivery queue first, then
    /// advances the sequential log scan. Marks the returned address as locked.
    /// </summary>
    public async Task<(byte[] data, long address)?> TryDeliverNextAsync(
        Guid lockToken, 
        string? sessionId = null, 
        ConcurrentDictionary<string, SessionMessageQueue>? sessionQueues = null, 
        CancellationToken ct = default)
    {
        if (!string.IsNullOrEmpty(sessionId) && sessionQueues != null)
        {
            if (sessionQueues.TryGetValue(sessionId, out var q))
            {
                while (q.TryDequeue(out long sessionAddr))
                {
                    if (_ttlExpiredSet.TryRemove(sessionAddr, out _))
                    {
                        ForceComplete(sessionAddr);
                        continue;
                    }

                    if (_lockedAddresses.ContainsKey(sessionAddr))
                    {
                        continue; // Already locked
                    }

                    // Pending before the read: once dequeued, only the pending set holds truncation back. The record's
                    // own address is a safe truncation bound until the read supplies the next record's address.
                    _pendingAddresses.TryAdd(sessionAddr, sessionAddr);
                    try
                    {
                        var (data, nextAddress) = await _store.ReadAsync(sessionAddr).ConfigureAwait(false);
                        _lockedAddresses[sessionAddr] = (DateTimeOffset.UtcNow + _lockDuration, lockToken);
                        _pendingAddresses[sessionAddr] = nextAddress;
                        _sessionOfAddress[sessionAddr] = q;
                        return (data, sessionAddr);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Skipping unreadable session address {Address}", sessionAddr);
                        _pendingAddresses.TryRemove(sessionAddr, out _);
                    }
                }
            }
            return null;
        }

        // Redelivery queue first (abandoned / lock-expired messages).
        while (_redeliveryQueue.TryDequeue(out long redeliveryAddr))
        {
            if (!_pendingAddresses.ContainsKey(redeliveryAddr))
            {
                _logger.LogWarning("Redelivery address {Address} not in pending set; skipping", redeliveryAddr);
                continue;
            }
            try
            {
                var (data, _) = await _store.Log.ReadAsync(redeliveryAddr, token: ct).ConfigureAwait(false);
                _lockedAddresses[redeliveryAddr] = (DateTimeOffset.UtcNow + _lockDuration, lockToken);
                return (data, redeliveryAddr);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Skipping unreadable redelivery address {Address}", redeliveryAddr);
                _pendingAddresses.TryRemove(redeliveryAddr, out _);
            }
        }

        // Sequential scan of the log.
        while (true)
        {
            long availabilityStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
            if (!await _iterator.WaitAsync(ct).ConfigureAwait(false))
                return null;
            BrokerProfiler.AvailabilityWait(availabilityStarted);

            long iteratorLockStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
            await _iteratorLock.WaitAsync(ct).ConfigureAwait(false);
            BrokerProfiler.IteratorLockWait(iteratorLockStarted);
            bool skipAndContinue = false;
            (byte[] data, long address)? result = null;
            long iteratorReadStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
            try
            {
                if (_iterator.GetNext(out byte[] data, out _, out long address, out long nextAddress))
                {
                    if (_ttlExpiredSet.TryRemove(address, out _))
                    {
                        // Background TTL scanner already dead-lettered this message; skip delivery.
                        _pendingAddresses[address] = nextAddress;
                        ForceComplete(address);
                        skipAndContinue = true;
                    }
                    else
                    {
                        _lockedAddresses[address] = (DateTimeOffset.UtcNow + _lockDuration, lockToken);
                        _pendingAddresses[address] = nextAddress; // record exact next-record boundary
                        result = (data, address);
                    }
                }
            }
            finally
            {
                BrokerProfiler.IteratorRead(iteratorReadStarted);
                _iteratorLock.Release();
            }

            if (skipAndContinue) continue;
            if (result.HasValue) return result;
        }
    }

    /// <summary>
    /// Settles a message successfully. Removes it from in-flight tracking and advances
    /// BeginAddress to the safe truncation point (minimum pending record address).
    /// </summary>
    public bool Complete(long address, Guid lockToken)
    {
        _ttlExpiredSet.TryRemove(address, out _); // clean up any stale TTL mark
        if (_lockedAddresses.TryGetValue(address, out var state) && state.LockToken == lockToken)
        {
            Forget(address);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Returns a message to the available pool. Removes the lock but retains the address in
    /// _pendingAddresses so the log is not truncated past it while it awaits redelivery.
    /// </summary>
    public bool Abandon(long address, Guid lockToken, IReadOnlyDictionary<string, object?>? propertiesToModify = null)
    {
        if (_lockedAddresses.TryGetValue(address, out var state) && state.LockToken == lockToken)
        {
            if (_lockedAddresses.TryRemove(address, out _))
            {
                RecordModifiedProperties(address, propertiesToModify);
                Redeliver(address);
                return true;
            }
        }
        return false;
    }

    public bool Defer(long address, Guid lockToken, long sequenceNumber, IReadOnlyDictionary<string, object?>? propertiesToModify = null)
    {
        if (_lockedAddresses.TryGetValue(address, out var state) && state.LockToken == lockToken)
        {
            if (_lockedAddresses.TryRemove(address, out _))
            {
                RecordModifiedProperties(address, propertiesToModify);
                _deferredMessages[sequenceNumber] = address;
                // address stays in _pendingAddresses; deferred messages remain in the log
                return true;
            }
        }
        return false;
    }

    /// <summary>Forcibly completes an address regardless of lock token (dead-lettering, expiry).</summary>
    public void ForceComplete(long address)
    {
        _ttlExpiredSet.TryRemove(address, out _);
        Forget(address);
    }

    /// <summary>
    /// Drops everything tracked for a message that is finished with (completed, dead-lettered, discarded) and lets
    /// the log be truncated past it. Per-message state added to the dispatcher must be removed here.
    /// </summary>
    private void Forget(long address)
    {
        _lockedAddresses.TryRemove(address, out _);
        _deliveryCountOverlay.TryRemove(address, out _);
        _modifiedProperties.TryRemove(address, out _);
        _sessionOfAddress.TryRemove(address, out _);
        if (_pendingAddresses.TryRemove(address, out long nextAddr))
            TruncateToMinPending(nextAddr);
    }

    private void RecordModifiedProperties(long address, IReadOnlyDictionary<string, object?>? propertiesToModify)
    {
        if (propertiesToModify is not { Count: > 0 }) return;
        _modifiedProperties.AddOrUpdate(address, _ => new Dictionary<string, object?>(propertiesToModify), (_, existing) =>
        {
            var merged = new Dictionary<string, object?>(existing);
            foreach (var (key, value) in propertiesToModify) merged[key] = value;
            return merged;
        });
    }

    /// <summary>Applies the properties earlier abandons and defers modified to a message read back from the log.</summary>
    public void ApplyModifiedProperties(long address, Amqp.Message message)
    {
        if (_modifiedProperties.TryGetValue(address, out var properties))
            PropertiesToModify.ApplyTo(properties, message);
    }

    /// <summary>
    /// True if the message at this address has not been handed out yet: the delivery loop has not reached it and it
    /// has not been retired as expired. Records behind the loop that are not tracked were completed or skipped; they
    /// stay in the log until truncation passes them but are no longer messages.
    /// </summary>
    public bool IsAwaitingFirstDelivery(long address) =>
        address >= _iterator.NextAddress && !_ttlExpiredSet.ContainsKey(address) && !IsTracked(address);

    /// <summary>Returns true if this address has been delivered (pending settlement or in redelivery queue).</summary>
    public bool IsTracked(long address) =>
        _pendingAddresses.ContainsKey(address) || _lockedAddresses.ContainsKey(address);

    /// <summary>
    /// Marks an address as TTL-expired so the iterator skips it on next delivery attempt.
    /// Call this AFTER writing the message to the DLQ so the skip is safe.
    /// </summary>
    public void MarkTtlExpired(long address) => _ttlExpiredSet[address] = 0;

    /// <summary>
    /// Makes an abandoned or lock-expired message available again: back to its session's queue for a session message,
    /// otherwise to the redelivery queue. The address stays in _pendingAddresses until it is completed.
    /// </summary>
    public void Redeliver(long address)
    {
        if (_sessionOfAddress.TryGetValue(address, out var sessionQueue))
            sessionQueue.Enqueue(address);
        else
            _redeliveryQueue.Enqueue(address);
    }

    public int IncrementDeliveryCount(long address)
        => _deliveryCountOverlay.AddOrUpdate(address, 1, (_, old) => old + 1);

    public int GetDeliveryCount(long address)
        => _deliveryCountOverlay.TryGetValue(address, out int v) ? v : 0;

    public bool IsLocked(long address) => _lockedAddresses.ContainsKey(address);

    public bool TryRenewLock(long address, Guid lockToken, DateTimeOffset lockedUntil)
    {
        if (_lockedAddresses.TryGetValue(address, out var state) && state.LockToken == lockToken)
        {
            _lockedAddresses[address] = (lockedUntil, lockToken);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Removes a message from the deferred set, reads it from the log, and locks it for delivery.
    /// Returns null if the sequence number is not deferred or the data cannot be read.
    /// </summary>
    public async Task<(byte[] data, long address)?> TryDeliverDeferredAsync(long sequenceNumber, Guid lockToken, CancellationToken ct = default)
    {
        if (!_deferredMessages.TryRemove(sequenceNumber, out long address))
            return null;

        try
        {
            var (data, _) = await _store.ReadAsync(address).ConfigureAwait(false);
            _lockedAddresses[address] = (DateTimeOffset.UtcNow + _lockDuration, lockToken);
            return (data, address);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read deferred message at address {Address} (seq={SeqNum})", address, sequenceNumber);
            _deferredMessages[sequenceNumber] = address;
            return null;
        }
    }

    private void TruncateToMinPending(long completedNextAddress)
    {
        // Serialise the min-scan + TruncateUntil so concurrent completions cannot
        // compute stale minima and truncate past each other.
        lock (_truncateLock)
        {
            // Use the next-record boundary (completedNextAddress) when the pending set is
            // empty, so FASTER's BeginAddress lands exactly on a record boundary.
            long truncateTo = _pendingAddresses.IsEmpty
                ? completedNextAddress
                : _pendingAddresses.Keys.Min();
            if (_sessionQueues != null)
            {
                foreach (var sessionQueue in _sessionQueues.Values)
                {
                    if (sessionQueue.TryPeekLowest(out long undelivered) && undelivered < truncateTo)
                        truncateTo = undelivered;
                }
            }
            _store.TruncateUntil(truncateTo);
        }
    }

    public void Dispose()
    {
        _monitorCts?.Cancel();
        _monitorCts?.Dispose();
        _iterator.Dispose();
        _iteratorLock.Dispose();
    }
}
