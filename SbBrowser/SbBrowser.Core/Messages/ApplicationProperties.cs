using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace SbBrowser.Core.Messages;

/// <summary>The value types a send form offers for an application property.</summary>
public enum PropertyType { String, Integer, Number, Boolean }

/// <summary>One application property as a user typed it: a name, a chosen type, and the value as text.</summary>
public sealed record PropertyEntry(string Name, PropertyType Type, string Value);

/// <summary>The properties that parsed, and one message per row that did not.</summary>
public sealed record PropertyParseResult(IReadOnlyDictionary<string, object?> Properties, IReadOnlyList<string> Errors);

/// <summary>Turns typed property rows into a <see cref="MessageDraft.ApplicationProperties"/> dictionary.</summary>
public static class ApplicationProperties
{
    public static PropertyParseResult Parse(IEnumerable<PropertyEntry> entries)
    {
        var properties = new Dictionary<string, object?>();
        var errors = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in entries)
        {
            var entry = row with { Name = row.Name.Trim() };
            if (entry.Name.Length == 0)
            {
                if (entry.Value.Length > 0) errors.Add($"A property has a value but no name: {entry.Value}");
                continue; // a blank row the form left empty
            }
            if (!seen.Add(entry.Name))
            {
                errors.Add($"'{entry.Name}' is set more than once.");
                continue;
            }
            if (TryConvert(entry, out var value, out string? error)) properties[entry.Name] = value;
            else errors.Add(error);
        }
        return new PropertyParseResult(properties, errors);
    }

    private static bool TryConvert(PropertyEntry entry, out object? value, [NotNullWhen(false)] out string? error)
    {
        string text = entry.Value.Trim();
        error = null;
        switch (entry.Type)
        {
            case PropertyType.Integer when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l):
                value = l;
                return true;
            case PropertyType.Integer:
                error = $"'{entry.Name}' is not a whole number: {entry.Value}";
                break;
            case PropertyType.Number when double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d):
                value = d;
                return true;
            case PropertyType.Number:
                error = $"'{entry.Name}' is not a number: {entry.Value}";
                break;
            case PropertyType.Boolean when bool.TryParse(text, out bool b):
                value = b;
                return true;
            case PropertyType.Boolean:
                error = $"'{entry.Name}' must be true or false: {entry.Value}";
                break;
            default:
                value = entry.Value; // strings keep their spaces
                return true;
        }
        value = null;
        return false;
    }
}
