using System.Text;
using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public class BodyFormatterTests
{
    private static FormattedBody Format(string text, string? contentType = null, int textLimit = BodyFormatter.DefaultTextLimitBytes) =>
        BodyFormatter.Format(Encoding.UTF8.GetBytes(text), contentType, textLimit);

    [Fact]
    public void AnEmptyBody_IsEmpty()
    {
        var body = BodyFormatter.Format(ReadOnlyMemory<byte>.Empty);
        Assert.Equal(BodyKind.Empty, body.Kind);
        Assert.Equal(0, body.SizeBytes);
    }

    [Fact]
    public void Json_IsIndented_AndKeepsNonAsciiText()
    {
        var body = Format("{\"id\":1,\"name\":\"Zoë\",\"tags\":[\"a\"]}");

        Assert.Equal(BodyKind.Json, body.Kind);
        Assert.Equal("{\n  \"id\": 1,\n  \"name\": \"Zoë\",\n  \"tags\": [\n    \"a\"\n  ]\n}", body.Text.ReplaceLineEndings("\n"));
        Assert.False(body.Truncated);
    }

    [Fact]
    public void JsonDeclaredByContentType_ThatDoesNotParse_IsShownAsText()
    {
        var body = Format("not json at all", "application/json");
        Assert.Equal(BodyKind.Text, body.Kind);
        Assert.Equal("not json at all", body.Text);
    }

    [Fact]
    public void PlainText_IsShownAsIs()
    {
        var body = Format("line one\r\nline two\tend");
        Assert.Equal(BodyKind.Text, body.Kind);
        Assert.Equal("line one\r\nline two\tend", body.Text);
    }

    [Fact]
    public void BinaryBodies_AreHexDumped()
    {
        byte[] bytes = [0x00, 0x01, 0xFF, 0x41, 0x42, 0x10];
        var body = BodyFormatter.Format(bytes);

        Assert.Equal(BodyKind.Binary, body.Kind);
        Assert.StartsWith("00000000  00 01 FF 41 42 10 ", body.Text);
        Assert.Contains("...AB.", body.Text);
        Assert.Equal(6, body.SizeBytes);
    }

    [Fact]
    public void ALargeTextBody_IsTruncated_WithItsFullSizeReported()
    {
        string text = new string('x', 100) + "é" + new string('y', 100); // the cut lands inside the two-byte 'é'
        var body = Format(text, textLimit: 101);

        Assert.True(body.Truncated);
        Assert.Equal(Encoding.UTF8.GetByteCount(text), body.SizeBytes);
        Assert.Equal(new string('x', 100), body.Text);
    }

    [Fact]
    public void ALargeJsonBody_IsTruncatedRatherThanIndented()
    {
        string json = "[" + string.Join(",", Enumerable.Range(0, 1000)) + "]";
        var body = Format(json, textLimit: 50);

        Assert.Equal(BodyKind.Json, body.Kind);
        Assert.True(body.Truncated);
        Assert.Equal(json[..50], body.Text);
    }

    [Fact]
    public void ALargeBinaryBody_IsTruncatedToTheBinaryLimit()
    {
        var body = BodyFormatter.Format(new byte[100_000], binaryLimitBytes: 32);

        Assert.Equal(BodyKind.Binary, body.Kind);
        Assert.True(body.Truncated);
        Assert.Equal(2, body.Text.TrimEnd('\n').Split('\n').Length);
        Assert.Equal(100_000, body.SizeBytes);
    }
}
