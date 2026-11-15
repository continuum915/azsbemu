using System.Globalization;
using System.Text;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;

namespace SbBrowser.Core.Messages;

/// <summary>Maps SDK messages to <see cref="MessageView"/>.</summary>
internal static class MessageMapper
{
    public static MessageView ToView(ServiceBusReceivedMessage m) => new(
        m.SequenceNumber,
        m.MessageId,
        m.CorrelationId,
        m.SessionId,
        m.Subject,
        m.ContentType,
        m.To,
        m.ReplyTo,
        m.EnqueuedTime,
        m.ScheduledEnqueueTime == default ? null : m.ScheduledEnqueueTime,
        m.ExpiresAt,
        m.TimeToLive,
        m.DeliveryCount,
        m.State.ToString(),
        BodyFormatter.Format(BodyBytes(m), m.ContentType),
        m.ApplicationProperties.OrderBy(p => p.Key, StringComparer.OrdinalIgnoreCase).Select(p => Property(p.Key, p.Value)).ToList(),
        m.DeadLetterReason,
        m.DeadLetterErrorDescription,
        m.DeadLetterSource);

    /// <summary>
    /// The body's bytes. Most senders use an AMQP data body; a value body (some non-.NET clients send a plain string)
    /// is shown as its text, and a sequence body as its items' text.
    /// </summary>
    private static ReadOnlyMemory<byte> BodyBytes(ServiceBusReceivedMessage m)
    {
        AmqpMessageBody body = m.GetRawAmqpMessage().Body;
        if (body.BodyType == AmqpMessageBodyType.Data) return m.Body.ToMemory();
        if (body.TryGetValue(out object? value)) return Encoding.UTF8.GetBytes(Display(value));
        if (body.TryGetSequence(out IEnumerable<IList<object>>? sequence) && sequence is not null)
            return Encoding.UTF8.GetBytes(string.Join("\n", sequence.Select(items => string.Join(", ", items.Select(Display)))));
        return ReadOnlyMemory<byte>.Empty;
    }

    private static MessageProperty Property(string name, object? value) =>
        new(name, Display(value), value?.GetType().Name ?? "null");

    private static string Display(object? value) => value switch
    {
        null => "",
        string s => s,
        DateTime d => d.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset d => d.ToString("O", CultureInfo.InvariantCulture),
        byte[] b => Convert.ToHexString(b),
        ReadOnlyMemory<byte> b => Convert.ToHexString(b.Span),
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };
}
