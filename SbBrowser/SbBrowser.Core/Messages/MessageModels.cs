namespace SbBrowser.Core.Messages;

/// <summary>Which queue of an entity to read: its messages, or its dead-letter queue.</summary>
public enum MessageSource
{
    Active,
    DeadLetter,
}

/// <summary>An application property, with its value already formatted for display.</summary>
public sealed record MessageProperty(string Name, string Value, string TypeName);

/// <summary>A message as SbBrowser shows it. Built from the SDK's message; no SDK types are exposed.</summary>
public sealed record MessageView(
    long SequenceNumber,
    string? MessageId,
    string? CorrelationId,
    string? SessionId,
    string? Subject,
    string? ContentType,
    string? To,
    string? ReplyTo,
    DateTimeOffset EnqueuedTime,
    DateTimeOffset? ScheduledEnqueueTime,
    DateTimeOffset ExpiresAt,
    TimeSpan TimeToLive,
    int DeliveryCount,
    string State,
    FormattedBody Body,
    IReadOnlyList<MessageProperty> ApplicationProperties,
    string? DeadLetterReason,
    string? DeadLetterErrorDescription,
    string? DeadLetterSource)
{
    /// <summary>The body's size in bytes.</summary>
    public int SizeBytes => Body.SizeBytes;
}

/// <summary>
/// One page of peeked messages. <see cref="NextFromSequenceNumber"/> is where the next page starts, or null when this
/// page reached the end. <see cref="SessionId"/> is the session that was read, for a session-enabled entity.
/// </summary>
public sealed record MessagePage(IReadOnlyList<MessageView> Messages, long? NextFromSequenceNumber, string? SessionId = null);
