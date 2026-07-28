using Amqp;
using Amqp.Framing;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests;

public class MessageSerializerTests
{
    [Fact]
    public void HeaderSize_Is32Bytes()
    {
        Assert.Equal(32, MessageHeader.Size);
        Assert.Equal(24, MessageHeader.LegacySize);
    }

    [Fact]
    public void RoundTrip_PreservesAllHeaderFields()
    {
        var header = new MessageHeader
        {
            SequenceNumber = 42,
            EnqueueTimeUtcTicks = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc).Ticks,
            DeliveryCount = 3,
            IsDeadLettered = true
        };

        var (decoded, _) = MessageSerializer.Decode(MessageSerializer.Encode(new Message("x"), header));

        Assert.Equal(42, decoded.SequenceNumber);
        Assert.Equal(header.EnqueueTimeUtcTicks, decoded.EnqueueTimeUtcTicks);
        Assert.Equal(3, decoded.DeliveryCount);
        Assert.True(decoded.IsDeadLettered);
    }

    [Fact]
    public void RoundTrip_IsDeadLettered_False()
    {
        var header = new MessageHeader { IsDeadLettered = false };

        var (decoded, _) = MessageSerializer.Decode(MessageSerializer.Encode(new Message("x"), header));

        Assert.False(decoded.IsDeadLettered);
    }

    [Fact]
    public void RoundTrip_PreservesStringBody()
    {
        var header = new MessageHeader { SequenceNumber = 1 };

        var (_, message) = MessageSerializer.Decode(MessageSerializer.Encode(new Message("hello world"), header));

        Assert.Equal("hello world", message.Body);
    }

    [Fact]
    public void RoundTrip_PreservesBinaryBody()
    {
        var body = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var header = new MessageHeader { SequenceNumber = 1 };

        var (_, message) = MessageSerializer.Decode(MessageSerializer.Encode(new Message(body), header));

        Assert.Equal(body, (byte[])message.Body);
    }

    [Fact]
    public void RoundTrip_PreservesMessageProperties()
    {
        var header = new MessageHeader { SequenceNumber = 7 };
        var original = new Message("payload")
        {
            Properties = new Properties
            {
                MessageId = "msg-123",
                CorrelationId = "corr-456"
            }
        };

        var (_, decoded) = MessageSerializer.Decode(MessageSerializer.Encode(original, header));

        Assert.Equal("msg-123", decoded.Properties.MessageId);
        Assert.Equal("corr-456", decoded.Properties.CorrelationId);
    }

    [Fact]
    public void EncodedSize_IsHeaderPlusMessageBytes()
    {
        var header = new MessageHeader();
        var message = new Message("test");
        byte[] bytes = MessageSerializer.Encode(message, header);

        Assert.True(bytes.Length > MessageHeader.Size);
    }

    [Fact]
    public void Decode_ThrowsOnTooShortData()
    {
        var tooShort = new byte[MessageHeader.LegacySize - 1];
        Assert.Throws<ArgumentException>(() => MessageSerializer.Decode(tooShort));
    }

    [Fact]
    public void RoundTrip_ZeroHeader()
    {
        var header = new MessageHeader
        {
            SequenceNumber = 0,
            EnqueueTimeUtcTicks = 0,
            DeliveryCount = 0,
            IsDeadLettered = false
        };

        var (decoded, _) = MessageSerializer.Decode(MessageSerializer.Encode(new Message("x"), header));

        Assert.Equal(0, decoded.SequenceNumber);
        Assert.Equal(0, decoded.EnqueueTimeUtcTicks);
        Assert.Equal(0, decoded.DeliveryCount);
        Assert.False(decoded.IsDeadLettered);
    }

    [Fact]
    public void RoundTrip_MaxValues()
    {
        var header = new MessageHeader
        {
            SequenceNumber = long.MaxValue,
            EnqueueTimeUtcTicks = long.MaxValue,
            DeliveryCount = int.MaxValue,
            IsDeadLettered = true
        };

        var (decoded, _) = MessageSerializer.Decode(MessageSerializer.Encode(new Message("x"), header));

        Assert.Equal(long.MaxValue, decoded.SequenceNumber);
        Assert.Equal(long.MaxValue, decoded.EnqueueTimeUtcTicks);
        Assert.Equal(int.MaxValue, decoded.DeliveryCount);
        Assert.True(decoded.IsDeadLettered);
    }

    [Fact]
    public void DecodeHeader_ExtractsCorrectFields()
    {
        var header = new MessageHeader
        {
            SequenceNumber = 99,
            EnqueueTimeUtcTicks = new DateTime(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            DeliveryCount = 5,
            IsDeadLettered = true
        };
        byte[] encoded = MessageSerializer.Encode(new Message("body"), header);

        var decoded = MessageSerializer.DecodeHeader(encoded);

        Assert.Equal(99, decoded.SequenceNumber);
        Assert.Equal(header.EnqueueTimeUtcTicks, decoded.EnqueueTimeUtcTicks);
        Assert.Equal(5, decoded.DeliveryCount);
        Assert.True(decoded.IsDeadLettered);
    }

    [Fact]
    public void DecodeHeader_MatchesFullDecode()
    {
        var header = new MessageHeader { SequenceNumber = 7, DeliveryCount = 2, IsDeadLettered = false };
        byte[] encoded = MessageSerializer.Encode(new Message("x"), header);

        var headerOnly = MessageSerializer.DecodeHeader(encoded);
        var (fullHeader, _) = MessageSerializer.Decode(encoded);

        Assert.Equal(fullHeader.SequenceNumber, headerOnly.SequenceNumber);
        Assert.Equal(fullHeader.EnqueueTimeUtcTicks, headerOnly.EnqueueTimeUtcTicks);
        Assert.Equal(fullHeader.DeliveryCount, headerOnly.DeliveryCount);
        Assert.Equal(fullHeader.IsDeadLettered, headerOnly.IsDeadLettered);
    }

    [Fact]
    public void DecodeHeader_ThrowsOnTooShortData()
    {
        var tooShort = new byte[MessageHeader.LegacySize - 1];
        Assert.Throws<ArgumentException>(() => MessageSerializer.DecodeHeader(tooShort));
    }

    [Fact]
    public void RoundTrip_PreservesTimeToLiveTicks()
    {
        long ttl = TimeSpan.FromSeconds(30).Ticks;
        var header = new MessageHeader { SequenceNumber = 1, TimeToLiveTicks = ttl };

        var (decoded, _) = MessageSerializer.Decode(MessageSerializer.Encode(new Message("x"), header));

        Assert.Equal(ttl, decoded.TimeToLiveTicks);
    }

    [Fact]
    public void Decode_LegacyHeader_ReturnsTtlZero()
    {
        // Manually build a 24-byte (legacy) header entry with version byte = 0
        var legacyHeader = new MessageHeader
        {
            SequenceNumber = 5,
            EnqueueTimeUtcTicks = 12345,
            DeliveryCount = 1,
            IsDeadLettered = false,
            TimeToLiveTicks = 99999 // should be ignored when version = 0
        };
        byte[] full = MessageSerializer.Encode(new Message("legacy"), legacyHeader);

        // Simulate a legacy entry: truncate to 24 header bytes + message bytes, clear version byte
        byte[] msgBytes = full[MessageHeader.Size..];
        byte[] legacyEntry = new byte[MessageHeader.LegacySize + msgBytes.Length];
        full.AsSpan(0, MessageHeader.LegacySize).CopyTo(legacyEntry);
        legacyEntry[21] = 0; // clear version byte to simulate legacy format
        msgBytes.CopyTo(legacyEntry.AsSpan(MessageHeader.LegacySize));

        var (decoded, _) = MessageSerializer.Decode(legacyEntry);

        Assert.Equal(5, decoded.SequenceNumber);
        Assert.Equal(0, decoded.TimeToLiveTicks); // TTL not stored in legacy format
    }
}
