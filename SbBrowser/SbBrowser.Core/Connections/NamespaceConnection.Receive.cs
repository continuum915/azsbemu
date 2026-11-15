using Azure.Messaging.ServiceBus;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Core.Connections;

public sealed partial class NamespaceConnection
{
    /// <summary>The most messages one receive-and-delete call removes.</summary>
    public const int MaxReceiveCount = 5_000;

    /// <summary>
    /// Removes up to <paramref name="count"/> messages from a queue or subscription (or its dead-letter queue) and
    /// returns them. Stops early when no message arrives within <see cref="NamespaceConnectionOptions.ReceiveWait"/>,
    /// so fewer than <paramref name="count"/> means the entity ran dry. Sessions work as in <see cref="PeekAsync"/>:
    /// a session-enabled entity's active messages need a <paramref name="session"/>, and "next available" with no
    /// session available returns nothing. The removed messages are gone: there is no undo.
    /// </summary>
    /// <exception cref="MessageOperationException">The session choice does not fit the entity, or the session is locked.</exception>
    public async Task<IReadOnlyList<MessageView>> ReceiveAndDeleteAsync(EntityRef entity, MessageSource source, int count,
        SessionChoice? session = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, MaxReceiveCount);

        await using var opened = await OpenReceiverAsync(entity, source, ServiceBusReceiveMode.ReceiveAndDelete, session, cancellationToken)
            .ConfigureAwait(false);
        if (opened is null) return [];

        var removed = new List<MessageView>();
        while (removed.Count < count)
        {
            var batch = await opened.Receiver.ReceiveMessagesAsync(count - removed.Count, _options.ReceiveWait, cancellationToken)
                .ConfigureAwait(false);
            if (batch.Count == 0) break; // ran dry
            removed.AddRange(batch.Select(MessageMapper.ToView));
        }
        return removed;
    }
}
