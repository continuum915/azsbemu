using SbBrowser.Core.Entities;

namespace SbBrowser.Tests.Entities;

public class TreeCountsTests
{
    private static readonly ReceiverSettings Settings =
        new(false, 10, TimeSpan.FromMinutes(1), TimeSpan.MaxValue, false, null, null, "Active");

    private static readonly NamespaceTree Tree = new(
        [
            new QueueNode(new QueueRef("orders"), EntityCounts.None, Settings),
            new QueueNode(new QueueRef("payments"), EntityCounts.None, Settings),
        ],
        [
            new TopicNode(new TopicRef("events"), 0, "Active",
            [
                new SubscriptionNode(new SubscriptionRef("events", "all"), EntityCounts.None, Settings, []),
                new SubscriptionNode(new SubscriptionRef("events", "audit"), EntityCounts.None, Settings, []),
            ]),
        ]);

    private static readonly EntityCounts Some = new(5, 2, 1, 0, 0);

    [Fact]
    public void WithCounts_ReplacesOnlyTheNamedQueue()
    {
        var updated = Tree.WithCounts(new QueueRef("ORDERS"), Some);

        Assert.Equal(Some, updated.FindQueue("orders")!.Counts);
        Assert.Equal(EntityCounts.None, updated.FindQueue("payments")!.Counts);
        Assert.Equal(EntityCounts.None, Tree.FindQueue("orders")!.Counts); // the original is unchanged
    }

    [Fact]
    public void WithCounts_ReplacesOnlyTheNamedSubscription()
    {
        var updated = Tree.WithCounts(new SubscriptionRef("events", "audit"), Some);

        Assert.Equal(Some, updated.FindSubscription("events", "audit")!.Counts);
        Assert.Equal(EntityCounts.None, updated.FindSubscription("events", "all")!.Counts);
    }

    [Fact]
    public void WithCounts_OnATopic_SetsItsScheduledCount()
    {
        Assert.Equal(1, Tree.WithCounts(new TopicRef("events"), Some).FindTopic("events")!.ScheduledCount);
    }

    [Fact]
    public void WithCounts_ForAnUnknownEntity_ChangesNothing()
    {
        var updated = Tree.WithCounts(new QueueRef("missing"), Some);
        Assert.All(updated.Queues, q => Assert.Equal(EntityCounts.None, q.Counts));
    }
}
