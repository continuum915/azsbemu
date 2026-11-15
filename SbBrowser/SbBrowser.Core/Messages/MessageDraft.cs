namespace SbBrowser.Core.Messages;

/// <summary>
/// A message to send. The body is sent as UTF-8 text. Application property values must be types Service Bus can carry
/// (strings, numbers, booleans, dates, GUIDs, ...). With <see cref="ScheduledEnqueueTime"/> set, the message is
/// scheduled rather than sent straight away.
/// </summary>
public sealed record MessageDraft(
    string Body,
    string? ContentType = null,
    string? Subject = null,
    string? MessageId = null,
    string? CorrelationId = null,
    string? SessionId = null,
    IReadOnlyDictionary<string, object?>? ApplicationProperties = null,
    DateTimeOffset? ScheduledEnqueueTime = null);
