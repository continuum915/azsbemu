using System.Collections.Concurrent;
using SbBrowser.Core.Connections;

namespace SbBrowser.Web.Services;

/// <summary>
/// The open <see cref="NamespaceConnection"/>s, one per profile, shared by every browser tab. A profile whose settings
/// changed gets a new connection; the old one is disposed.
/// </summary>
public sealed class ConnectionHub : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, NamespaceConnection> _connections = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _gate = new();

    /// <summary>The connection for <paramref name="profile"/>, opened on first use.</summary>
    /// <exception cref="InvalidConnectionStringException">The profile's connection string cannot be used.</exception>
    public NamespaceConnection Get(ConnectionProfile profile)
    {
        NamespaceConnection? stale = null;
        NamespaceConnection connection;
        lock (_gate)
        {
            if (_connections.TryGetValue(profile.Name, out var existing) && existing.Profile == profile)
                return existing;

            stale = existing;
            connection = NamespaceConnection.Open(profile);
            _connections[profile.Name] = connection;
        }

        if (stale is not null) _ = stale.DisposeAsync().AsTask();
        return connection;
    }

    /// <summary>Closes the connection for the named profile, if one is open.</summary>
    public async Task CloseAsync(string profileName)
    {
        if (_connections.TryRemove(profileName, out var connection))
            await connection.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var name in _connections.Keys.ToList())
            await CloseAsync(name);
    }
}
