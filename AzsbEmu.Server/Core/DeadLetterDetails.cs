using Amqp.Framing;

namespace AzsbEmu.Server.Core;

/// <summary>
/// What a client supplies when it dead-letters a message: an optional reason and description, and application
/// properties to set on the message (MassTransit puts its MT-Fault-* headers here).
/// </summary>
public sealed record DeadLetterDetails(string? Reason, string? Description, IReadOnlyDictionary<string, object?> PropertiesToModify)
{
    public const string ReasonProperty = "DeadLetterReason";
    public const string DescriptionProperty = "DeadLetterErrorDescription";
    /// <summary>Set on dead-lettered messages that were forwarded to another entity: the entity that dead-lettered them.</summary>
    public const string SourceProperty = "DeadLetterSource";

    public static readonly DeadLetterDetails None = new(null, null, new Dictionary<string, object?>());

    /// <summary>What Service Bus records on a message dead-lettered because its time to live passed.</summary>
    public static readonly DeadLetterDetails TtlExpired = WithReason("TTLExpiredException", "The message has expired.");

    public static DeadLetterDetails WithReason(string? reason, string? description) =>
        new(reason, description, new Dictionary<string, object?>());

    /// <summary>
    /// Reads a dead-letter disposition: the SDK sends the reason, the description and the properties to modify as
    /// entries of the error's info. The error condition is always <c>com.microsoft:dead-letter</c>, so it is not a reason.
    /// </summary>
    public static DeadLetterDetails FromRejected(Rejected? rejected)
    {
        string? reason = null;
        string? description = null;
        var properties = new Dictionary<string, object?>();
        if (rejected?.Error?.Info is { } info)
        {
            foreach (var key in info.Keys)
            {
                string name = key.ToString()!;
                object? value = info[key];
                if (name == ReasonProperty) reason = value?.ToString();
                else if (name == DescriptionProperty) description = value?.ToString();
                else properties[name] = value;
            }
        }
        return new DeadLetterDetails(reason, description, properties);
    }
}
