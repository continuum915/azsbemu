using Azure.Messaging.ServiceBus;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Core.Connections;

public sealed partial class NamespaceConnection
{
    /// <summary>The most copies one send call makes.</summary>
    public const int MaxCopies = 100_000;

    /// <summary>How many scheduled messages are scheduled per request.</summary>
    private const int ScheduleChunkSize = 100;

    /// <summary>
    /// Sends <paramref name="copies"/> copies of <paramref name="draft"/> to a queue or topic, in batches that respect
    /// the namespace's message size limit. A draft with a scheduled enqueue time is scheduled instead.
    /// </summary>
    /// <exception cref="MessageOperationException">The target is a subscription, or one message is too large to send.</exception>
    public async Task SendAsync(EntityRef queueOrTopic, MessageDraft draft, int copies = 1, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(draft);
        ArgumentOutOfRangeException.ThrowIfLessThan(copies, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(copies, MaxCopies);

        string name = queueOrTopic switch
        {
            QueueRef q => q.Name,
            TopicRef t => t.Name,
            SubscriptionRef s => throw new MessageOperationException(
                $"Messages cannot be sent to a subscription. Send to the topic '{s.Topic}'; its rules decide which subscriptions get them."),
            _ => throw new ArgumentException($"Unknown entity kind: {queueOrTopic.GetType().Name}", nameof(queueOrTopic)),
        };

        await using var sender = Client.CreateSender(name);
        if (draft.ScheduledEnqueueTime is { } at)
        {
            for (int sent = 0; sent < copies; sent += ScheduleChunkSize)
            {
                var chunk = Enumerable.Range(0, Math.Min(ScheduleChunkSize, copies - sent)).Select(_ => ToMessage(draft)).ToList();
                await sender.ScheduleMessagesAsync(chunk, at, cancellationToken).ConfigureAwait(false);
            }
            return;
        }

        int remaining = copies;
        while (remaining > 0)
        {
            using var batch = await sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
            while (remaining > 0 && batch.TryAddMessage(ToMessage(draft))) remaining--;
            if (batch.Count == 0)
                throw new MessageOperationException(
                    $"The message is too large to send to '{queueOrTopic.DisplayName}' (the limit is {batch.MaxSizeInBytes:N0} bytes).");
            await sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    private static ServiceBusMessage ToMessage(MessageDraft draft)
    {
        var message = new ServiceBusMessage(BinaryData.FromString(draft.Body ?? ""))
        {
            ContentType = draft.ContentType,
            Subject = draft.Subject,
            CorrelationId = draft.CorrelationId,
            SessionId = draft.SessionId,
        };
        if (draft.MessageId is not null) message.MessageId = draft.MessageId;
        if (draft.ApplicationProperties is not null)
            foreach (var (key, value) in draft.ApplicationProperties)
                message.ApplicationProperties[key] = value;
        return message;
    }
}
