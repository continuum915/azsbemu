using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Core;

/// <summary>Options for <see cref="EntityManager.Route(string, Message, RouteOptions)"/>.</summary>
/// <param name="ToDeadLetter">Write to the entity's dead-letter log instead of its active log.</param>
/// <param name="SequenceNumber">Use this sequence number instead of assigning the next one (scheduled messages keep the number handed out at schedule time).</param>
/// <param name="Flush">Commit the log before returning, instead of waiting for the periodic commit tick.</param>
/// <param name="HopCount">Number of times this message has already been forwarded (reserved for auto-forwarding).</param>
public readonly record struct RouteOptions(
    bool ToDeadLetter = false,
    long? SequenceNumber = null,
    bool Flush = false,
    int HopCount = 0);

// Message routing: every write of a new message into an entity goes through Route,
// and every move of an existing message into a dead-letter log goes through MoveToDeadLetterLogAsync.
public partial class EntityManager
{
    private static readonly Symbol SequenceNumberAnnotation = new("x-opt-sequence-number");
    private static readonly Symbol EnqueuedTimeUtcAnnotation = new("x-opt-enqueued-time-utc");
    private static readonly Symbol EnqueuedTimeAnnotation = new("x-opt-enqueued-time");
    private static readonly Symbol HopCountAnnotation = new("x-opt-azsbemu-hop-count");
    /// <summary>On a forwarded dead-lettered message, the entity that dead-lettered it (read by the SDK as <c>DeadLetterSource</c>).</summary>
    private static readonly Symbol DeadLetterSourceAnnotation = new("x-opt-deadletter-source");

    /// <summary>Routes a message to the queue, subscription (<c>topic/subscriptions/sub</c>) or topic at <paramref name="entityPath"/>.</summary>
    /// <returns>False if no entity exists at the path.</returns>
    public bool Route(string entityPath, Message message, RouteOptions options = default)
    {
        var queue = GetQueue(entityPath);
        if (queue != null)
        {
            Route(queue, message, options);
            return true;
        }

        var sub = FindSubscription(entityPath);
        if (sub != null)
        {
            Route(sub, message, options);
            return true;
        }

        var topic = GetTopic(entityPath);
        if (topic != null)
        {
            Route(topic, message, options);
            return true;
        }

        return false;
    }

    /// <summary>Forwards a message may take before it is dead-lettered; also stops forwarding loops.</summary>
    public const int MaxForwardHops = 4;

    /// <summary>
    /// Validates <paramref name="forwardTo"/> as the forward target of <paramref name="sourceName"/> and returns the
    /// reason it is invalid, or null if it is valid (including when there is no target). Targets must be an existing
    /// queue or topic other than the source; a subscription (whose topic is <paramref name="owningTopic"/>) may not
    /// forward into its own topic, which would fan the message back to itself.
    /// </summary>
    /// <summary>Validates both of an entity's forward targets; returns the first problem, or null.</summary>
    public string? ValidateForwardTargets(string sourceName, string? forwardTo, string? forwardDeadLetteredMessagesTo, string? owningTopic = null) =>
        ValidateForwardTarget(sourceName, forwardTo, owningTopic) ?? ValidateForwardTarget(sourceName, forwardDeadLetteredMessagesTo, owningTopic);

    public string? ValidateForwardTarget(string sourceName, string? forwardTo, string? owningTopic = null)
    {
        string? target = ForwardTarget.ToEntityName(forwardTo);
        if (target is null) return null;
        if (string.Equals(target, sourceName, StringComparison.OrdinalIgnoreCase))
            return $"'{sourceName}' cannot forward to itself.";
        if (owningTopic != null && string.Equals(target, owningTopic, StringComparison.OrdinalIgnoreCase))
            return $"'{sourceName}' cannot forward to its own topic '{owningTopic}'.";
        if (GetQueue(target) is null && GetTopic(target) is null)
            return $"Forward target '{target}' must be an existing queue or topic.";
        return null;
    }

    /// <summary>Fans a message out to every subscription of <paramref name="topic"/> whose rules match.</summary>
    public void Route(TopicInfo topic, Message message, RouteOptions options = default)
    {
        topic.Activity.Record();
        if (topic.RequiresDuplicateDetection && IsDuplicate(topic.DuplicateHistory, topic.DuplicateDetectionHistoryTimeWindow, topic.Name, message))
            return;
        List<SubscriptionInfo> subs;
        lock (topic.Subscriptions)
        {
            subs = new List<SubscriptionInfo>(topic.Subscriptions);
        }

        foreach (var sub in subs)
        {
            RuleInfo? matchedRule = null;
            lock (sub.Rules)
            {
                foreach (var rule in sub.Rules)
                {
                    if (RuleEvaluator.Evaluate(rule, message))
                    {
                        matchedRule = rule;
                        break;
                    }
                }
            }

            if (matchedRule == null)
            {
                _logger.LogDebug("Message does not match rules for subscription {SubName} on topic {TopicName}", sub.Name, topic.Name);
                continue;
            }

            try
            {
                // A rule action rewrites this subscription's copy only; other subscriptions see the original.
                var subMessage = message;
                if (!string.IsNullOrWhiteSpace(matchedRule.ActionExpression))
                {
                    subMessage = DeepCopy(message);
                    try
                    {
                        SqlFilterEvaluator.ApplyAction(matchedRule.ActionExpression, subMessage);
                    }
                    catch (Exception ex)
                    {
                        DeadLetterUndeliverable(sub, message, options with { ToDeadLetter = false, SequenceNumber = null },
                            "RuleActionException", $"Rule '{matchedRule.Name}' action failed: {ex.Message}");
                        continue;
                    }
                }

                if (sub.RequiresSession && string.IsNullOrEmpty(subMessage.Properties?.GroupId))
                {
                    DeadLetterUndeliverable(sub, subMessage, options with { SequenceNumber = null }, "SessionIdIsNull",
                        "The SessionId was not set on a message sent to a session-enabled entity.");
                    continue;
                }

                Route(sub, subMessage, options with { ToDeadLetter = false, SequenceNumber = null });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fan out message from topic {TopicName} to subscription {SubName}", topic.Name, sub.Name);
            }
        }
    }

    /// <summary>Stamps sequence number, enqueue time and TTL on a message and appends it to the entity's active (or dead-letter) log.</summary>
    public void Route(IMessageEntity entity, Message message, RouteOptions options = default)
    {
        entity.Activity.Record();
        if (!options.ToDeadLetter && entity is QueueInfo { RequiresDuplicateDetection: true } queue
            && IsDuplicate(queue.DuplicateHistory, queue.DuplicateDetectionHistoryTimeWindow, queue.Name, message))
            return;
        // Auto-forwarding: a message arriving at a forwarding entity is never stored in its active log.
        if (!options.ToDeadLetter && entity.ForwardToEntityName is { } target)
        {
            if (options.HopCount >= MaxForwardHops)
            {
                DeadLetterUndeliverable(entity, message, options, "MaxTransferHopCountExceeded",
                    $"The message was forwarded {options.HopCount} times; at most {MaxForwardHops} forwards are allowed.");
                return;
            }

            if (Route(target, message, options with { HopCount = options.HopCount + 1, SequenceNumber = null }))
                return;

            DeadLetterUndeliverable(entity, message, options, "ForwardToTargetNotFound",
                $"The forward target '{target}' does not exist.");
            return;
        }

        if (options.ToDeadLetter && TryForwardDeadLetter(entity, message, options.HopCount, options.Flush))
            return;

        var store = options.ToDeadLetter ? entity.DlqMessageStore : entity.MessageStore;
        if (store == null)
        {
            _logger.LogWarning("Message store for entity {EntityName} is not initialized.", entity.Name);
            return;
        }

        long seqNum = options.SequenceNumber ?? entity.AssignNextSequenceNumber();
        long enqueueTicks = DateTime.UtcNow.Ticks;
        var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);

        var messageToStore = CopyForStore(message);
        messageToStore.MessageAnnotations[SequenceNumberAnnotation] = seqNum;
        // Forwarded messages remember their hops, so a later dead-letter forward continues the count (loops end).
        if (options.HopCount > 0) messageToStore.MessageAnnotations[HopCountAnnotation] = options.HopCount;
        else messageToStore.MessageAnnotations.Map.Remove(HopCountAnnotation);
        messageToStore.MessageAnnotations[EnqueuedTimeUtcAnnotation] = enqueuedTimeUtc;
        messageToStore.MessageAnnotations[EnqueuedTimeAnnotation] = enqueuedTimeUtc;

        var header = new MessageHeader
        {
            SequenceNumber = seqNum,
            EnqueueTimeUtcTicks = enqueueTicks,
            DeliveryCount = 0,
            IsDeadLettered = options.ToDeadLetter,
            TimeToLiveTicks = ComputeTtlTicks(message, entity)
        };

        long address = store.Append(MessageSerializer.Encode(messageToStore, header));
        if (options.Flush) store.Log.Commit(spinWait: true);

        if (options.SequenceNumber is long assigned) entity.UpdateNextSequenceNumber(assigned);

        if (options.ToDeadLetter) entity.IncrementDeadLetterMessageCount();
        else entity.IncrementMessageCount();

        string? sessionId = message.Properties?.GroupId;
        if (!options.ToDeadLetter && entity.RequiresSession && !string.IsNullOrEmpty(sessionId))
        {
            entity.SessionMessageAddresses.GetOrAdd(sessionId, _ => new SessionMessageQueue()).Enqueue(address);
        }

        _logger.LogDebug("Enqueued message seq={SeqNum} to entity {EntityName} (isDlq={IsDlq}, session={SessionId})", seqNum, entity.Name, options.ToDeadLetter, sessionId);
    }

    /// <summary>
    /// Duplicate detection: true if a message with this MessageId already arrived at the entity within its window, in
    /// which case the message is dropped (the sender still sees success, as with Service Bus). Messages without a
    /// MessageId are never duplicates. Every arrival is checked: sends, batches, transactional commits, scheduled
    /// messages when they come due, and each forwarding hop, at the entity it arrives at.
    /// </summary>
    private bool IsDuplicate(DuplicateDetectionHistory history, TimeSpan window, string entityName, Message message)
    {
        if (message.Properties?.MessageId is not { } id || id.ToString() is not { Length: > 0 } messageId) return false;
        if (history.TryAccept(messageId, window, DateTime.UtcNow)) return false;

        _logger.LogDebug("Dropping duplicate message {MessageId} sent to {EntityName}", messageId, entityName);
        return true;
    }

    /// <summary>
    /// If <paramref name="entity"/> forwards its dead-lettered messages, enqueues <paramref name="message"/> at that
    /// target (with the <c>x-opt-deadletter-source</c> annotation naming the entity) and returns true. Returns false, so the caller keeps the message in
    /// the entity's own DLQ, when there is no target, the hop limit is reached, or the target no longer exists.
    /// </summary>
    private bool TryForwardDeadLetter(IMessageEntity entity, Message message, int hopCount, bool flush)
    {
        if (entity.ForwardDeadLetteredMessagesToEntityName is not { } target) return false;
        if (hopCount >= MaxForwardHops)
        {
            _logger.LogWarning("Not forwarding dead-lettered message from {EntityName} to {Target}: it has been forwarded {Hops} times; keeping it in the DLQ", entity.Name, target, hopCount);
            return false;
        }

        var forwarded = CopyForStore(message);
        forwarded.ApplicationProperties = CloneApplicationProperties(message.ApplicationProperties);
        forwarded.MessageAnnotations = CloneMessageAnnotations(message.MessageAnnotations); // not shared with the stored original
        forwarded.MessageAnnotations[DeadLetterSourceAnnotation] = entity.Name;
        if (Route(target, forwarded, new RouteOptions(Flush: flush, HopCount: hopCount + 1))) return true;

        _logger.LogWarning("Dead-letter forward target {Target} of {EntityName} does not exist; keeping the message in the DLQ", target, entity.Name);
        return false;
    }

    /// <summary>Dead-letters a message on <paramref name="entity"/> because it could not be forwarded.</summary>
    private void DeadLetterUndeliverable(IMessageEntity entity, Message message, RouteOptions options, string reason, string description)
    {
        _logger.LogWarning("Dead-lettering message on {EntityName}: {Reason} ({Description})", entity.Name, reason, description);
        var dlqMessage = CopyForStore(message);
        dlqMessage.ApplicationProperties = CloneApplicationProperties(message.ApplicationProperties);
        dlqMessage.ApplicationProperties[DeadLetterDetails.ReasonProperty] = reason;
        dlqMessage.ApplicationProperties[DeadLetterDetails.DescriptionProperty] = description;
        Route(entity, dlqMessage, options with { ToDeadLetter = true, SequenceNumber = null });
    }

    /// <summary>
    /// Appends an already-stored message (with its existing header) to the entity's dead-letter log and flushes, or,
    /// if the entity forwards dead-lettered messages, enqueues it at that target instead.
    /// The caller is responsible for retiring the original from the active log.
    /// </summary>
    private async Task MoveToDeadLetterLogAsync(IMessageEntity entity, MessageHeader header, Message message)
    {
        // Dead-letter forwarding: re-enqueue at the target, flushed before the caller settles.
        int hops = message.MessageAnnotations?.Map.TryGetValue(HopCountAnnotation, out var stored) == true ? Convert.ToInt32(stored) : 0;
        if (TryForwardDeadLetter(entity, message, hops, flush: true))
        {
            entity.DecrementMessageCount();
            return;
        }

        var dlqStore = entity.DlqMessageStore
            ?? throw new InvalidOperationException($"Dead-letter store for entity {entity.Name} is not initialized.");

        await dlqStore.AppendAsync(MessageSerializer.Encode(message, header with { IsDeadLettered = true })).ConfigureAwait(false);
        entity.DecrementMessageCount();
        entity.IncrementDeadLetterMessageCount();
    }

    private SubscriptionInfo? FindSubscription(string entityPath)
    {
        int subIndex = entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex <= 0) return null;

        var topic = GetTopic(entityPath[..subIndex]);
        string subName = entityPath[(subIndex + "/subscriptions/".Length)..];
        return topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
    }

    private static Message DeepCopy(Message message)
    {
        var encoded = message.Encode();
        return Message.Decode(new ByteBuffer(encoded.Buffer, encoded.Offset, encoded.Length, encoded.Length));
    }

    private static Message CopyForStore(Message message) => new()
    {
        Header = message.Header,
        Properties = message.Properties,
        ApplicationProperties = message.ApplicationProperties,
        MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations(),
        BodySection = message.BodySection
    };

    private static ApplicationProperties CloneApplicationProperties(ApplicationProperties? source)
    {
        var clone = new ApplicationProperties();
        if (source?.Map != null)
        {
            foreach (var kvp in source.Map) clone.Map[kvp.Key] = kvp.Value;
        }
        return clone;
    }

    private static MessageAnnotations CloneMessageAnnotations(MessageAnnotations? source)
    {
        var clone = new MessageAnnotations();
        if (source?.Map != null)
        {
            foreach (var kvp in source.Map) clone.Map[kvp.Key] = kvp.Value;
        }
        return clone;
    }

    private static long ComputeTtlTicks(Message message, IMessageEntity entity)
    {
        uint? msgTtlMs = message.Header?.Ttl;
        if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
            return TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
        if (entity.DefaultMessageTimeToLive != TimeSpan.MaxValue)
            return entity.DefaultMessageTimeToLive.Ticks;
        return 0;
    }
}
