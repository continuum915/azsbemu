using AzsbEmu.Server.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzsbEmu.Tests;

/// <summary>
/// The stores' background commits. A FASTER commit that writes metadata blocks a thread-pool thread until its file
/// write completes, and on Linux that write is itself a thread-pool work item. Every new store makes one such commit,
/// so creating a few hundred entities at once starved the pool for minutes until commits were capped.
/// </summary>
public sealed class MessageStoreCommitTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"azsb-store-test-{Guid.NewGuid():N}");

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private MessageStore NewStore(string name) =>
        new(Path.Combine(_dir, name, "messages.log"), NullLogger.Instance, Path.Combine(_dir, name, "commits"));

    private static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"timed out waiting until {what}");
            await Task.Delay(20);
        }
    }

    [Fact]
    public void ManyStores_AllCommitPromptly()
    {
        const int count = 480; // the stores of 240 entities
        var stores = new List<MessageStore>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        try
        {
            for (int i = 0; i < count; i++)
            {
                var store = NewStore($"s{i}");
                stores.Add(store);
                store.Append([1, 2, 3]);
            }

            // Polled from the test's own thread, not the pool, so a starved pool can't hide the deadline.
            while (stores.Any(s => s.Log.CommittedUntilAddress < s.Log.TailAddress))
            {
                Assert.True(DateTime.UtcNow < deadline,
                    $"{stores.Count(s => s.Log.CommittedUntilAddress < s.Log.TailAddress)} of {count} stores still uncommitted after 15s");
                Thread.Sleep(50);
            }
        }
        finally
        {
            foreach (var store in stores) store.Dispose();
        }
    }

    [Fact]
    public async Task AnAppend_IsCommittedByTheBackgroundLoop()
    {
        using var store = NewStore("one");
        store.Append([1, 2, 3]);

        await WaitUntilAsync(() => store.Log.CommittedUntilAddress >= store.Log.TailAddress, "the append is committed");
    }

    [Fact]
    public async Task ATruncation_IsCommittedByTheBackgroundLoop()
    {
        using var store = NewStore("one");
        store.Append([1, 2, 3]);
        await WaitUntilAsync(() => store.Log.CommittedUntilAddress >= store.Log.TailAddress, "the append is committed");

        store.TruncateUntil(store.Log.TailAddress);

        await WaitUntilAsync(() => store.Log.CommittedBeginAddress >= store.Log.BeginAddress, "the truncation is committed");
    }
}
