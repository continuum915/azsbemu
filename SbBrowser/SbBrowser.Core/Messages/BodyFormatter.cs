using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace SbBrowser.Core.Messages;

public enum BodyKind
{
    Empty,
    Json,
    Text,
    Binary,
}

/// <summary>
/// A message body ready to display. <see cref="Text"/> is indented JSON, plain text, or a hex dump for binary.
/// <see cref="Truncated"/> means only the first part of the body is shown; <see cref="SizeBytes"/> is the full size.
/// </summary>
public sealed record FormattedBody(BodyKind Kind, string Text, int SizeBytes, bool Truncated);

/// <summary>Turns message bodies into display text the same way for every UI.</summary>
public static class BodyFormatter
{
    /// <summary>Bodies up to this size are shown in full (JSON is indented only within this size).</summary>
    public const int DefaultTextLimitBytes = 256 * 1024;

    /// <summary>Binary bodies are hex-dumped up to this size.</summary>
    public const int DefaultBinaryLimitBytes = 16 * 1024;

    private const int HexBytesPerLine = 16;

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions Indented = new() { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public static FormattedBody Format(ReadOnlyMemory<byte> body, string? contentType = null,
        int textLimitBytes = DefaultTextLimitBytes, int binaryLimitBytes = DefaultBinaryLimitBytes)
    {
        var bytes = body.Span;
        if (bytes.IsEmpty) return new FormattedBody(BodyKind.Empty, "", 0, false);

        bool truncated = bytes.Length > textLimitBytes;
        if (!TryDecodeText(truncated ? bytes[..textLimitBytes] : bytes, allowCutCharacter: truncated, out string text))
            return HexDump(bytes, binaryLimitBytes);

        if (LooksLikeJson(text, contentType))
        {
            if (!truncated && TryIndentJson(text, out string indented))
                return new FormattedBody(BodyKind.Json, indented, bytes.Length, false);
            if (truncated)
                return new FormattedBody(BodyKind.Json, text, bytes.Length, true);
        }
        return new FormattedBody(BodyKind.Text, text, bytes.Length, truncated);
    }

    private static bool TryDecodeText(ReadOnlySpan<byte> bytes, bool allowCutCharacter, out string text)
    {
        // A truncated body may end part-way through a multi-byte character; drop at most one such partial character.
        for (int trim = 0; trim <= (allowCutCharacter ? 3 : 0) && trim < bytes.Length; trim++)
        {
            try
            {
                text = StrictUtf8.GetString(bytes[..(bytes.Length - trim)]);
                return !HasBinaryControlCharacters(text);
            }
            catch (DecoderFallbackException)
            {
            }
        }
        text = "";
        return false;
    }

    private static bool HasBinaryControlCharacters(string text)
    {
        foreach (char c in text)
            if (char.IsControl(c) && c is not ('\t' or '\r' or '\n'))
                return true;
        return false;
    }

    private static bool LooksLikeJson(string text, string? contentType)
    {
        if (contentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true) return true;
        var start = text.AsSpan().TrimStart();
        return !start.IsEmpty && start[0] is '{' or '[';
    }

    private static bool TryIndentJson(string text, out string indented)
    {
        try
        {
            using var document = JsonDocument.Parse(text);
            indented = JsonSerializer.Serialize(document.RootElement, Indented);
            return true;
        }
        catch (JsonException)
        {
            indented = "";
            return false;
        }
    }

    private static FormattedBody HexDump(ReadOnlySpan<byte> bytes, int limit)
    {
        var shown = bytes.Length > limit ? bytes[..limit] : bytes;
        var sb = new StringBuilder();
        for (int offset = 0; offset < shown.Length; offset += HexBytesPerLine)
        {
            var line = shown.Slice(offset, Math.Min(HexBytesPerLine, shown.Length - offset));
            sb.Append(offset.ToString("X8")).Append("  ");
            for (int i = 0; i < HexBytesPerLine; i++)
                sb.Append(i < line.Length ? line[i].ToString("X2") + " " : "   ");
            sb.Append(' ');
            foreach (byte b in line) sb.Append(b is >= 0x20 and < 0x7F ? (char)b : '.');
            sb.Append('\n');
        }
        return new FormattedBody(BodyKind.Binary, sb.ToString(), bytes.Length, bytes.Length > limit);
    }
}
