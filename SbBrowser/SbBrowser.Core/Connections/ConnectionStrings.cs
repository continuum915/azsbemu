namespace SbBrowser.Core.Connections;

/// <summary>Connection-string handling. Nothing here puts a connection string, or part of one, into a message.</summary>
internal static class ConnectionStrings
{
    private const string EmulatorFlag = "UseDevelopmentEmulator";

    public static bool IsEmulator(string? connectionString)
    {
        foreach (var (key, value) in Pairs(connectionString))
            if (key.Equals(EmulatorFlag, StringComparison.OrdinalIgnoreCase))
                return value.Trim().Equals("true", StringComparison.OrdinalIgnoreCase);
        return false;
    }

    /// <summary>
    /// For an emulator connection string, returns it with the endpoint's port set to <paramref name="port"/>;
    /// any other connection string is returned unchanged.
    /// </summary>
    public static string WithEmulatorPort(string connectionString, int port)
    {
        if (!IsEmulator(connectionString)) return connectionString;

        var parts = connectionString.Split(';');
        for (int i = 0; i < parts.Length; i++)
        {
            int eq = parts[i].IndexOf('=');
            if (eq < 0 || !parts[i][..eq].Trim().Equals("Endpoint", StringComparison.OrdinalIgnoreCase)) continue;

            if (Uri.TryCreate(parts[i][(eq + 1)..].Trim(), UriKind.Absolute, out var endpoint))
                parts[i] = "Endpoint=" + new UriBuilder(endpoint) { Port = port, Path = "/" }.Uri.AbsoluteUri;
            break;
        }
        return string.Join(';', parts);
    }

    private static IEnumerable<(string Key, string Value)> Pairs(string? connectionString)
    {
        if (string.IsNullOrEmpty(connectionString)) yield break;
        foreach (var part in connectionString.Split(';'))
        {
            int eq = part.IndexOf('=');
            if (eq > 0) yield return (part[..eq].Trim(), part[(eq + 1)..]);
        }
    }
}
