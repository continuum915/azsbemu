using Azure.Messaging.ServiceBus;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Core.Connections;

public sealed partial class NamespaceConnection
{
    /// <summary>The most messages one peek page returns.</summary>
    public const int MaxPeekPageSize = 250;

    /// <summary>
    /// Peeks up to <paramref name="count"/> messages from a queue or subscription, starting at
    /// <paramref name="fromSequenceNumber"/> (0 for the beginning). Peeking leaves messages where they are.
    /// A session-enabled entity's active messages need a <paramref name="session"/>; its dead-letter queue does not
    /// (dead-letter queues have no sessions). <see cref="MessagePage.SessionId"/> says which session was read; with
    /// <see cref="SessionChoice.NextAvailable"/> and no session available, the page is empty.
    /// </summary>
    /// <exception cref="MessageOperationException">The session choice does not fit the entity, or the session is locked.</exception>
    public async Task<MessagePage> PeekAsync(EntityRef entity, MessageSource source, long fromSequenceNumber, int count,
        SessionChoice? session = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, MaxPeekPageSize);
        ArgumentOutOfRangeException.ThrowIfNegative(fromSequenceNumber);

        await using var opened = await OpenReceiverAsync(entity, source, ServiceBusReceiveMode.PeekLock, session, cancellationToken).ConfigureAwait(false);
        if (opened is null) return new MessagePage([], null);

        // One message past the page shows whether another page exists. A peek may return fewer messages than asked
        // for even when more exist, so keep asking until the page is full or a peek returns nothing.
        var peeked = new List<ServiceBusReceivedMessage>();
        long next = fromSequenceNumber;
        while (peeked.Count <= count)
        {
            var batch = await opened.Receiver.PeekMessagesAsync(count + 1 - peeked.Count, next, cancellationToken).ConfigureAwait(false);
            if (batch.Count == 0) break;
            peeked.AddRange(batch);
            next = batch[^1].SequenceNumber + 1;
        }

        bool more = peeked.Count > count;
        var page = peeked.Take(count).Select(MessageMapper.ToView).ToList();
        return new MessagePage(page, more ? peeked[count].SequenceNumber : null, opened.SessionId);
    }

    /// <summary>A receiver and, for a session receiver, the session it holds. Disposing it releases the session.</summary>
    private sealed class OpenedReceiver(ServiceBusReceiver receiver, string? sessionId) : IAsyncDisposable
    {
        public ServiceBusReceiver Receiver { get; } = receiver;
        public string? SessionId { get; } = sessionId;
        public ValueTask DisposeAsync() => Receiver.DisposeAsync();
    }

    /// <summary>
    /// Opens the right kind of receiver for <paramref name="entity"/>: a session receiver for a session-enabled
    /// entity's active messages, a plain one otherwise. Returns null when the next available session was asked for
    /// and none became available within <see cref="NamespaceConnectionOptions.NextSessionWait"/>.
    /// </summary>
    private async Task<OpenedReceiver?> OpenReceiverAsync(EntityRef entity, MessageSource source, ServiceBusReceiveMode mode,
        SessionChoice? session, CancellationToken cancellationToken)
    {
        if (entity is not (QueueRef or SubscriptionRef))
            throw new ArgumentException("Messages can only be read from a queue or a subscription.", nameof(entity));

        bool sessionful = source == MessageSource.Active && await RequiresSessionAsync(entity, cancellationToken).ConfigureAwait(false);
        if (!sessionful)
        {
            if (session is not null && source == MessageSource.Active)
                throw new MessageOperationException($"'{entity.DisplayName}' is not session-enabled, so its messages cannot be read by session.");
            return new OpenedReceiver(CreateReceiver(entity, source, mode), null);
        }

        if (session is null)
            throw new MessageOperationException(
                $"'{entity.DisplayName}' is session-enabled: choose a session ID, or the next available session.");

        var options = new ServiceBusSessionReceiverOptions { ReceiveMode = mode, PrefetchCount = 0 };
        try
        {
            if (session.SessionId is { } id)
            {
                var receiver = entity switch
                {
                    QueueRef q => await Client.AcceptSessionAsync(q.Name, id, options, cancellationToken).ConfigureAwait(false),
                    SubscriptionRef s => await Client.AcceptSessionAsync(s.Topic, s.Name, id, options, cancellationToken).ConfigureAwait(false),
                    _ => throw new InvalidOperationException(),
                };
                return new OpenedReceiver(receiver, receiver.SessionId);
            }

            using var wait = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            wait.CancelAfter(_options.NextSessionWait);
            try
            {
                var receiver = entity switch
                {
                    QueueRef q => await Client.AcceptNextSessionAsync(q.Name, options, wait.Token).ConfigureAwait(false),
                    SubscriptionRef s => await Client.AcceptNextSessionAsync(s.Topic, s.Name, options, wait.Token).ConfigureAwait(false),
                    _ => throw new InvalidOperationException(),
                };
                return new OpenedReceiver(receiver, receiver.SessionId);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested
                                       && (ex is OperationCanceledException
                                           || ex is ServiceBusException { Reason: ServiceBusFailureReason.ServiceTimeout }))
            {
                return null; // no session available
            }
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.SessionCannotBeLocked)
        {
            throw new MessageOperationException(
                $"The session is locked by another receiver of '{entity.DisplayName}'. Try again when it is released.", ex);
        }
    }

    private async Task<bool> RequiresSessionAsync(EntityRef entity, CancellationToken cancellationToken) => entity switch
    {
        QueueRef q => (await Admin.GetQueueAsync(q.Name, cancellationToken).ConfigureAwait(false)).Value.RequiresSession,
        SubscriptionRef s => (await Admin.GetSubscriptionAsync(s.Topic, s.Name, cancellationToken).ConfigureAwait(false)).Value.RequiresSession,
        _ => false,
    };

    private ServiceBusReceiver CreateReceiver(EntityRef entity, MessageSource source, ServiceBusReceiveMode mode)
    {
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = mode,
            SubQueue = source == MessageSource.DeadLetter ? SubQueue.DeadLetter : SubQueue.None,
            PrefetchCount = 0,
        };
        return entity switch
        {
            QueueRef q => Client.CreateReceiver(q.Name, options),
            SubscriptionRef s => Client.CreateReceiver(s.Topic, s.Name, options),
            _ => throw new ArgumentException("Messages can only be read from a queue or a subscription.", nameof(entity)),
        };
    }
}
