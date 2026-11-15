using Azure.Messaging.ServiceBus;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Core.Connections;

public sealed partial class NamespaceConnection
{
    /// <summary>How many messages one purge receive asks for.</summary>
    private const int PurgeBatchSize = 500;

    /// <summary>
    /// Empties a queue's or subscription's active messages or dead letters, in receive-and-delete batches, reporting
    /// the running total to <paramref name="progress"/> after each batch. A session-enabled entity is purged session
    /// by session until no session with messages is left. Cancelling stops at the next batch and returns what was
    /// removed so far with <see cref="PurgeResult.Cancelled"/> set; removed messages are gone either way.
    /// </summary>
    public async Task<PurgeResult> PurgeAsync(EntityRef entity, MessageSource source, IProgress<PurgeProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        long removed = 0;
        bool cancelled = false;
        try
        {
            bool sessionful = source == MessageSource.Active && await RequiresSessionAsync(entity, cancellationToken).ConfigureAwait(false);
            if (sessionful)
            {
                await PurgeSessionsAsync(entity, Count, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await using var opened = await OpenReceiverAsync(entity, source, ServiceBusReceiveMode.ReceiveAndDelete, null, cancellationToken)
                    .ConfigureAwait(false);
                await DrainAsync(opened!.Receiver, Count, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested
                                   && (ex is OperationCanceledException || ex is ServiceBusException { Reason: ServiceBusFailureReason.ServiceTimeout }))
        {
            cancelled = true;
        }

        // The counts are read even after a cancel, so the result says what is left.
        var counts = await GetCountsAsync(entity, CancellationToken.None).ConfigureAwait(false);
        return new PurgeResult(removed, counts.Scheduled,
            source == MessageSource.Active ? counts.Active : counts.DeadLetter, cancelled);

        void Count(int batch)
        {
            removed += batch;
            progress?.Report(new PurgeProgress(removed));
        }
    }

    /// <summary>
    /// Takes the next available session and drains it, until none is left. A session that gives up nothing (only
    /// deferred messages) is held until the purge ends, so it is not offered again ahead of the others.
    /// </summary>
    private async Task PurgeSessionsAsync(EntityRef entity, Action<int> count, CancellationToken cancellationToken)
    {
        var held = new List<IAsyncDisposable>();
        try
        {
            while (true)
            {
                var opened = await OpenReceiverAsync(entity, MessageSource.Active, ServiceBusReceiveMode.ReceiveAndDelete,
                    SessionChoice.NextAvailable, cancellationToken).ConfigureAwait(false);
                if (opened is null) return; // no session with messages left (or every one is locked)

                int drained = await DrainAsync(opened.Receiver, count, cancellationToken).ConfigureAwait(false);
                if (drained == 0) held.Add(opened);
                else await opened.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            foreach (var receiver in held) await receiver.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Receives and deletes until a receive comes back empty. Returns how many were removed.</summary>
    private async Task<int> DrainAsync(ServiceBusReceiver receiver, Action<int> count, CancellationToken cancellationToken)
    {
        int total = 0;
        while (true)
        {
            var batch = await receiver.ReceiveMessagesAsync(PurgeBatchSize, _options.ReceiveWait, cancellationToken).ConfigureAwait(false);
            if (batch.Count == 0) return total;
            total += batch.Count;
            count(batch.Count);
        }
    }
}
