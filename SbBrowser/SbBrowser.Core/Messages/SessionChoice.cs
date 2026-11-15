namespace SbBrowser.Core.Messages;

/// <summary>
/// Which session to read from a session-enabled queue or subscription: a specific <see cref="SessionId"/>, or
/// (when it is null) the next session that has messages and is not locked by another receiver.
/// </summary>
public sealed record SessionChoice(string? SessionId)
{
    public static SessionChoice NextAvailable { get; } = new((string?)null);

    public static SessionChoice Id(string sessionId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sessionId);
        return new SessionChoice(sessionId);
    }

    /// <summary>
    /// The choice to use for the pages after <paramref name="page"/>: "next available" becomes the session that page
    /// read, so paging stays in one session instead of picking a new one each time.
    /// </summary>
    public SessionChoice After(MessagePage page) =>
        SessionId is null && page.SessionId is { } read ? Id(read) : this;
}

/// <summary>A message operation that cannot run as asked; the message says what to do instead.</summary>
public sealed class MessageOperationException(string message, Exception? inner = null) : InvalidOperationException(message, inner);
