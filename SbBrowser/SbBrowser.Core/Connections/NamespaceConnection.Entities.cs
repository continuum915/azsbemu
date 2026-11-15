using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using SbBrowser.Core.Entities;

namespace SbBrowser.Core.Connections;

public sealed partial class NamespaceConnection
{
    /// <summary>How many topics' subscriptions are listed at once.</summary>
    private const int SubscriptionListingConcurrency = 8;

    /// <summary>How many subscriptions' rules are listed at once, across all topics.</summary>
    private const int RuleListingConcurrency = 16;

    private static readonly StringComparer NameOrder = StringComparer.OrdinalIgnoreCase;

    /// <summary>Lists every queue, topic and subscription with counts, settings and subscription rules.</summary>
    public async Task<NamespaceTree> GetTreeAsync(CancellationToken cancellationToken = default)
    {
        var queuesTask = GetQueuesAsync(cancellationToken);
        var topicsTask = GetTopicsAsync(cancellationToken);
        return new NamespaceTree(await queuesTask.ConfigureAwait(false), await topicsTask.ConfigureAwait(false));
    }

    /// <summary>The current counts of one entity. A topic reports only its scheduled count.</summary>
    public async Task<EntityCounts> GetCountsAsync(EntityRef entity, CancellationToken cancellationToken = default)
    {
        switch (entity)
        {
            case QueueRef q:
                return Counts(await Admin.GetQueueRuntimePropertiesAsync(q.Name, cancellationToken).ConfigureAwait(false));
            case SubscriptionRef s:
                return Counts(await Admin.GetSubscriptionRuntimePropertiesAsync(s.Topic, s.Name, cancellationToken).ConfigureAwait(false));
            case TopicRef t:
                TopicRuntimeProperties topic = await Admin.GetTopicRuntimePropertiesAsync(t.Name, cancellationToken).ConfigureAwait(false);
                return EntityCounts.None with { Scheduled = topic.ScheduledMessageCount };
            default:
                throw new ArgumentException($"Unknown entity kind: {entity.GetType().Name}", nameof(entity));
        }
    }

    private async Task<IReadOnlyList<QueueNode>> GetQueuesAsync(CancellationToken cancellationToken)
    {
        var settingsTask = ToListAsync(Admin.GetQueuesAsync(cancellationToken));
        var runtime = await ToListAsync(Admin.GetQueuesRuntimePropertiesAsync(cancellationToken)).ConfigureAwait(false);
        var settings = (await settingsTask.ConfigureAwait(false)).ToDictionary(q => q.Name, NameOrder);

        return runtime
            .Where(r => settings.ContainsKey(r.Name)) // created between the two listings: picked up on the next refresh
            .Select(r => new QueueNode(new QueueRef(r.Name), Counts(r), Settings(settings[r.Name])))
            .OrderBy(q => q.Ref.Name, NameOrder)
            .ToList();
    }

    private async Task<IReadOnlyList<TopicNode>> GetTopicsAsync(CancellationToken cancellationToken)
    {
        var settingsTask = ToListAsync(Admin.GetTopicsAsync(cancellationToken));
        var runtime = await ToListAsync(Admin.GetTopicsRuntimePropertiesAsync(cancellationToken)).ConfigureAwait(false);
        var settings = (await settingsTask.ConfigureAwait(false)).ToDictionary(t => t.Name, NameOrder);

        using var gate = new SemaphoreSlim(SubscriptionListingConcurrency);
        using var rulesGate = new SemaphoreSlim(RuleListingConcurrency);
        var topics = await Task.WhenAll(runtime.Where(t => settings.ContainsKey(t.Name)).Select(async topic =>
        {
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await GetTopicAsync(topic, settings[topic.Name], rulesGate, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        })).ConfigureAwait(false);

        return topics.OfType<TopicNode>().OrderBy(t => t.Ref.Name, NameOrder).ToList();
    }

    /// <summary>One topic with its subscriptions and their rules, or null if it was deleted while being listed.</summary>
    private async Task<TopicNode?> GetTopicAsync(TopicRuntimeProperties topic, TopicProperties properties, SemaphoreSlim rulesGate,
        CancellationToken cancellationToken)
    {
        try
        {
            var settingsTask = ToListAsync(Admin.GetSubscriptionsAsync(topic.Name, cancellationToken));
            var runtime = await ToListAsync(Admin.GetSubscriptionsRuntimePropertiesAsync(topic.Name, cancellationToken)).ConfigureAwait(false);
            var settings = (await settingsTask.ConfigureAwait(false)).ToDictionary(s => s.SubscriptionName, NameOrder);

            // Rules are listed per subscription: run those listings in parallel, bounded across the whole tree.
            var subscriptions = await Task.WhenAll(runtime.Where(r => settings.ContainsKey(r.SubscriptionName)).Select(async r =>
            {
                await rulesGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var rules = await GetRulesAsync(topic.Name, r.SubscriptionName, cancellationToken).ConfigureAwait(false);
                    return rules is null // deleted while being listed
                        ? null
                        : new SubscriptionNode(new SubscriptionRef(topic.Name, r.SubscriptionName), Counts(r),
                            Settings(settings[r.SubscriptionName]), rules);
                }
                finally
                {
                    rulesGate.Release();
                }
            })).ConfigureAwait(false);

            return new TopicNode(new TopicRef(topic.Name), topic.ScheduledMessageCount, properties.Status.ToString(),
                subscriptions.OfType<SubscriptionNode>().OrderBy(s => s.Ref.Name, NameOrder).ToList());
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            return null;
        }
    }

    /// <summary>A subscription's rules, in name order; null when the subscription no longer exists.</summary>
    private async Task<IReadOnlyList<RuleView>?> GetRulesAsync(string topic, string subscription, CancellationToken cancellationToken)
    {
        try
        {
            var rules = await ToListAsync(Admin.GetRulesAsync(topic, subscription, cancellationToken)).ConfigureAwait(false);
            return rules.Select(RuleMapper.ToView).OrderBy(r => r.Name, NameOrder).ToList();
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            return null;
        }
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> items)
    {
        var list = new List<T>();
        await foreach (var item in items.ConfigureAwait(false)) list.Add(item);
        return list;
    }

    private static EntityCounts Counts(QueueRuntimeProperties q) =>
        new(q.ActiveMessageCount, q.DeadLetterMessageCount, q.ScheduledMessageCount, q.TransferMessageCount, q.TransferDeadLetterMessageCount);

    private static EntityCounts Counts(SubscriptionRuntimeProperties s) =>
        new(s.ActiveMessageCount, s.DeadLetterMessageCount, 0, s.TransferMessageCount, s.TransferDeadLetterMessageCount);

    private static ReceiverSettings Settings(QueueProperties q) => new(
        q.RequiresSession, q.MaxDeliveryCount, q.LockDuration, q.DefaultMessageTimeToLive,
        q.DeadLetteringOnMessageExpiration, RuleMapper.ForwardName(q.ForwardTo), RuleMapper.ForwardName(q.ForwardDeadLetteredMessagesTo), q.Status.ToString());

    private static ReceiverSettings Settings(SubscriptionProperties s) => new(
        s.RequiresSession, s.MaxDeliveryCount, s.LockDuration, s.DefaultMessageTimeToLive,
        s.DeadLetteringOnMessageExpiration, RuleMapper.ForwardName(s.ForwardTo), RuleMapper.ForwardName(s.ForwardDeadLetteredMessagesTo), s.Status.ToString());
}
