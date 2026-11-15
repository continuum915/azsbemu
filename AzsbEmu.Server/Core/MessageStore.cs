using FASTER.core;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Core;

public class MessageStore : IDisposable
{
    private readonly FasterLog _log;
    private readonly IDevice _device;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _commitCts = new();
    private readonly Task _commitTask;

    /// <summary>
    /// Caps FASTER commits in flight across every store in the process. A commit that writes metadata blocks a
    /// thread-pool thread until its file write completes, and on Linux (FASTER's managed device) that write is itself a
    /// thread-pool work item. Every new store makes one such commit, so creating a few hundred entities at once
    /// blocked the whole pool behind writes it had no thread left to run, stalling the process for minutes.
    /// </summary>
    private static readonly SemaphoreSlim CommitGate = new(Math.Max(1, Environment.ProcessorCount / 2));

    public MessageStore(string logPath, ILogger logger, string? commitDir = null)
    {
        _logger = logger;
        _device = Devices.CreateLogDevice(logPath);
        _log = new FasterLog(new FasterLogSettings { LogDevice = _device, LogCommitDir = commitDir });
        _commitTask = RunCommitLoopAsync(_commitCts.Token);
    }

    public FasterLog Log => _log;

    /// <summary>Enqueues a message. Durability is guaranteed on the next 50ms commit tick or clean shutdown.</summary>
    public long Append(byte[] message)
    {
        long started = BrokerProfiler.Enabled ? Stopwatch.GetTimestamp() : 0;
        long address = _log.Enqueue(message);
        BrokerProfiler.Append(started);
        return address;
    }

    /// <summary>Enqueues a message and flushes immediately. Used for settlement-path writes (DLQ) where durability is required before the ACK.</summary>
    public async Task<long> AppendAsync(byte[] message)
    {
        long address = _log.Enqueue(message);
        await CommitAsync().ConfigureAwait(false);
        return address;
    }

    /// <summary>Commits everything appended so far, waiting for it to be durable.</summary>
    public void Commit()
    {
        CommitGate.Wait();
        try { _log.Commit(spinWait: true); }
        finally { CommitGate.Release(); }
    }

    private async Task CommitAsync()
    {
        await CommitGate.WaitAsync().ConfigureAwait(false);
        try { await _log.CommitAsync().ConfigureAwait(false); }
        finally { CommitGate.Release(); }
    }

    public async Task<(byte[] data, long nextAddress)> ReadAsync(long address)
    {
        using var iter = _log.Scan(address, long.MaxValue);
        if (await iter.WaitAsync().ConfigureAwait(false))
        {
            if (iter.GetNext(out byte[] data, out _, out _, out long nextAddress))
                return (data, nextAddress);
        }
        throw new KeyNotFoundException($"No message found at address {address}");
    }

    /// <param name="filter">When given, only stored messages it accepts are returned (and counted).</param>
    public async Task<IReadOnlyList<(byte[] data, long address)>> PeekAsync(long fromSequenceNumber, int maxMessages,
        Func<byte[], bool>? filter = null, CancellationToken cancellationToken = default)
    {
        if (maxMessages <= 0)
        {
            return Array.Empty<(byte[] data, long address)>();
        }

        var results = new List<(byte[] data, long address)>(maxMessages);
        long beginAddress = _log.BeginAddress;
        long snapshotTail = _log.TailAddress;
        if (snapshotTail <= beginAddress)
        {
            return results;
        }

        using var scanner = _log.Scan(beginAddress, snapshotTail);
        while (results.Count < maxMessages && await scanner.WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            while (results.Count < maxMessages && scanner.GetNext(out byte[] data, out _, out long address, out _))
            {
                var header = MessageSerializer.DecodeHeader(data);
                if (header.SequenceNumber >= fromSequenceNumber && (filter is null || filter(data)))
                {
                    results.Add((data, address));
                }
            }
        }

        return results;
    }

    public void TruncateUntil(long address) => _log.TruncateUntil(address);

    private async Task RunCommitLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
                await CommitAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        await CommitAsync().ConfigureAwait(false); // final flush on shutdown
    }

    public void Dispose()
    {
        _commitCts.Cancel();
        _commitTask.GetAwaiter().GetResult(); // wait for final flush to complete
        _commitCts.Dispose();
        _log.Dispose();
        _device.Dispose();
    }
}
