namespace AzsbEmu.Server.Core;

/// <summary>Turns a configured forward target into the entity name messages are routed to.</summary>
public static class ForwardTarget
{
    /// <summary>
    /// Accepts a bare entity name ("orders", "myapp/orders") or an absolute address whose path is the entity name
    /// (the SDK sends "http://host/orders"; Service Bus also uses "sb://namespace/orders"). Returns null for no target.
    /// </summary>
    public static string? ToEntityName(string? target)
    {
        if (string.IsNullOrWhiteSpace(target)) return null;

        if (Uri.TryCreate(target, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" or "sb")
            return Uri.UnescapeDataString(uri.AbsolutePath).Trim('/');

        return target.Trim('/');
    }
}
