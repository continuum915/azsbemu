using System.Text.Json;

namespace SbBrowser.Core.Connections;

/// <summary>Where connection profiles are kept.</summary>
public interface IConnectionProfileStore
{
    /// <summary>True when profiles cannot be added or removed (the emulator-hosted browser).</summary>
    bool IsReadOnly { get; }

    Task<IReadOnlyList<ConnectionProfile>> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>Adds the profile, or replaces the one with the same name (names compare case-insensitively).</summary>
    Task SaveAsync(ConnectionProfile profile, CancellationToken cancellationToken = default);

    Task RemoveAsync(string name, CancellationToken cancellationToken = default);
}

/// <summary>A single profile that cannot be changed: the emulator-hosted browser's connection to its own emulator.</summary>
public sealed class FixedProfileStore(ConnectionProfile profile) : IConnectionProfileStore
{
    public bool IsReadOnly => true;

    public Task<IReadOnlyList<ConnectionProfile>> ListAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<ConnectionProfile>>([profile]);

    public Task SaveAsync(ConnectionProfile profile, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Connection profiles are read-only here.");

    public Task RemoveAsync(string name, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Connection profiles are read-only here.");
}

/// <summary>
/// Profiles in a JSON file. Connection strings are stored as entered (protect the file like any other credential
/// store); writes replace the file atomically.
/// </summary>
public sealed class JsonFileProfileStore(string path) : IConnectionProfileStore
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };
    private readonly SemaphoreSlim _lock = new(1, 1);

    public string Path { get; } = path;

    public bool IsReadOnly => false;

    /// <summary>The current user's store: <c>%APPDATA%\SbBrowser\profiles.json</c> (<c>~/.config/SbBrowser</c> elsewhere).</summary>
    public static JsonFileProfileStore ForCurrentUser() =>
        new(System.IO.Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData, Environment.SpecialFolderOption.Create),
            "SbBrowser", "profiles.json"));

    public async Task<IReadOnlyList<ConnectionProfile>> ListAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { return await ReadAsync(cancellationToken).ConfigureAwait(false); }
        finally { _lock.Release(); }
    }

    public async Task SaveAsync(ConnectionProfile profile, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profile.Name);
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var profiles = (await ReadAsync(cancellationToken).ConfigureAwait(false))
                .Where(p => !SameName(p.Name, profile.Name))
                .Append(profile)
                .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
            await WriteAsync(profiles, cancellationToken).ConfigureAwait(false);
        }
        finally { _lock.Release(); }
    }

    public async Task RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var profiles = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var kept = profiles.Where(p => !SameName(p.Name, name)).ToList();
            if (kept.Count != profiles.Count) await WriteAsync(kept, cancellationToken).ConfigureAwait(false);
        }
        finally { _lock.Release(); }
    }

    private static bool SameName(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    private async Task<IReadOnlyList<ConnectionProfile>> ReadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(Path)) return [];
        await using var stream = File.OpenRead(Path);
        return await JsonSerializer.DeserializeAsync<List<ConnectionProfile>>(stream, Json, cancellationToken).ConfigureAwait(false) ?? [];
    }

    private async Task WriteAsync(IReadOnlyList<ConnectionProfile> profiles, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(Path))!);
        string temp = Path + ".tmp";
        await using (var stream = File.Create(temp))
            await JsonSerializer.SerializeAsync(stream, profiles, Json, cancellationToken).ConfigureAwait(false);
        File.Move(temp, Path, overwrite: true);
    }
}
