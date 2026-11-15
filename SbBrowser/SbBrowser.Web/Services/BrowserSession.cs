using SbBrowser.Core.Connections;
using SbBrowser.Core.Entities;

namespace SbBrowser.Web.Services;

/// <summary>An error shown in the banner: what was being done, and what went wrong.</summary>
public sealed record ErrorReport(string Operation, string Message);

/// <summary>
/// One browser tab's state (a Blazor circuit): the chosen profile, the namespace tree, and the error banner.
/// Components read it and re-render on <see cref="Changed"/>.
/// </summary>
public sealed class BrowserSession(ConnectionHub hub, IConnectionProfileStore store, SbBrowserOptions options) : IDisposable
{
    private readonly CancellationTokenSource _disposed = new();
    private Task? _initialized;
    private int _refreshVersion;

    public event Action? Changed;

    public bool EmulatorMode => options.EmulatorMode;

    public bool CanEditProfiles => !store.IsReadOnly;

    public IReadOnlyList<ConnectionProfile> Profiles { get; private set; } = [];

    public ConnectionProfile? Current { get; private set; }

    public NamespaceTree Tree { get; private set; } = NamespaceTree.Empty;

    /// <summary>True once a refresh has succeeded for the current profile.</summary>
    public bool Connected { get; private set; }

    public bool Loading { get; private set; }

    public DateTimeOffset? RefreshedAt { get; private set; }

    public ErrorReport? Error { get; private set; }

    /// <summary>The current profile's connection, or null when there is no profile or it cannot be opened.</summary>
    public NamespaceConnection? Connection
    {
        get
        {
            if (Current is null) return null;
            try { return hub.Get(Current); }
            catch (InvalidConnectionStringException) { return null; }
        }
    }

    /// <summary>Loads the profiles and the first one's tree. Safe to call from every component; runs once.</summary>
    public Task EnsureInitializedAsync() => _initialized ??= InitializeAsync();

    private async Task InitializeAsync()
    {
        await LoadProfilesAsync();
        Current = Profiles.FirstOrDefault();
        if (Current is not null) await RefreshAsync();
        else Notify();
    }

    public async Task SelectProfileAsync(string name)
    {
        var profile = Profiles.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
        if (profile is null || profile == Current) return;
        Current = profile;
        Tree = NamespaceTree.Empty;
        Connected = false;
        RefreshedAt = null;
        Error = null;
        await RefreshAsync();
    }

    public async Task SaveProfileAsync(ConnectionProfile profile)
    {
        try
        {
            await NamespaceConnection.Open(profile).DisposeAsync(); // only validates the connection string
        }
        catch (InvalidConnectionStringException ex)
        {
            ReportError("Save connection", ex);
            return;
        }

        await store.SaveAsync(profile, _disposed.Token);
        await LoadProfilesAsync();
        Current = null; // force SelectProfileAsync to reload even when the name is unchanged
        await SelectProfileAsync(profile.Name);
    }

    public async Task RemoveProfileAsync(string name)
    {
        await store.RemoveAsync(name, _disposed.Token);
        await hub.CloseAsync(name);
        await LoadProfilesAsync();
        if (Current is not null && string.Equals(Current.Name, name, StringComparison.OrdinalIgnoreCase))
        {
            Current = null;
            Tree = NamespaceTree.Empty;
            Connected = false;
            if (Profiles.Count > 0) await SelectProfileAsync(Profiles[0].Name);
        }
        Notify();
    }

    /// <summary>Reloads the namespace tree. A failure keeps the last tree and shows the error.</summary>
    public async Task RefreshAsync()
    {
        if (Current is null) return;
        int version = Interlocked.Increment(ref _refreshVersion);
        Loading = true;
        Notify();
        try
        {
            var connection = hub.Get(Current);
            var tree = await connection.GetTreeAsync(_disposed.Token);
            if (version != _refreshVersion) return; // a newer refresh (or profile switch) superseded this one
            Tree = tree;
            Connected = true;
            RefreshedAt = DateTimeOffset.Now;
            if (Error?.Operation == "Load entities") Error = null;
        }
        catch (Exception ex) when (!_disposed.IsCancellationRequested)
        {
            if (version != _refreshVersion) return;
            Connected = false;
            ReportError("Load entities", ex);
        }
        finally
        {
            if (version == _refreshVersion)
            {
                Loading = false;
                Notify();
            }
        }
    }

    /// <summary>Refreshes one entity's counts, leaving the rest of the tree as it is.</summary>
    public async Task RefreshCountsAsync(EntityRef entity)
    {
        var connection = Connection;
        if (connection is null) return;
        try
        {
            var counts = await connection.GetCountsAsync(entity, _disposed.Token);
            Tree = Tree.WithCounts(entity, counts);
            Notify();
        }
        catch (Exception ex) when (!_disposed.IsCancellationRequested)
        {
            ReportError("Refresh counts", ex);
        }
    }

    /// <summary>
    /// Refreshes the counts an action on <paramref name="entity"/> can change. A topic fans out to its subscriptions
    /// and a forwarding entity moves messages on, so those reload the whole tree; anything else reloads just its counts.
    /// </summary>
    public Task RefreshAfterChangeAsync(EntityRef entity)
    {
        bool reachesOthers = entity switch
        {
            TopicRef => true,
            QueueRef q => Forwards(Tree.FindQueue(q.Name)?.Settings),
            SubscriptionRef sub => Forwards(Tree.FindSubscription(sub.Topic, sub.Name)?.Settings),
            _ => false,
        };
        return reachesOthers ? RefreshAsync() : RefreshCountsAsync(entity);

        static bool Forwards(ReceiverSettings? s) => s is not null && (s.ForwardTo is not null || s.ForwardDeadLetteredMessagesTo is not null);
    }

    public void ReportError(string operation, Exception exception)
    {
        Error = new ErrorReport(operation, ErrorText.Describe(exception));
        Notify();
    }

    public void DismissError()
    {
        Error = null;
        Notify();
    }

    public void Dispose()
    {
        _disposed.Cancel();
        _disposed.Dispose();
    }

    private async Task LoadProfilesAsync() => Profiles = await store.ListAsync(_disposed.Token);

    private void Notify() => Changed?.Invoke();
}
