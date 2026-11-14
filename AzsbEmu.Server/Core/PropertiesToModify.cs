using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace AzsbEmu.Server.Core;

/// <summary>
/// Application properties a client asks to set on a message when it settles it (abandon, defer, dead-letter).
/// </summary>
public static class PropertiesToModify
{
    public static readonly IReadOnlyDictionary<string, object?> None = new Dictionary<string, object?>();

    /// <summary>Reads them from an abandon or defer over a link: the SDK sends them as the Modified outcome's message annotations.</summary>
    public static IReadOnlyDictionary<string, object?> FromModified(Modified? modified) => FromMap(modified?.MessageAnnotations);

    public static IReadOnlyDictionary<string, object?> FromMap(Map? map)
    {
        if (map == null || map.Count == 0) return None;
        var properties = new Dictionary<string, object?>();
        foreach (var key in map.Keys) properties[key.ToString()!] = map[key];
        return properties;
    }

    public static void ApplyTo(IReadOnlyDictionary<string, object?> properties, Message message)
    {
        if (properties.Count == 0) return;
        message.ApplicationProperties ??= new ApplicationProperties();
        foreach (var (key, value) in properties) message.ApplicationProperties[key] = value;
    }
}
