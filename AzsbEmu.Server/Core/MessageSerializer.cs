using Amqp;
using System.Buffers.Binary;

namespace AzsbEmu.Server.Core;

/// <summary>
/// Fixed-size binary header prepended to every FASTER log entry.
///
/// Legacy format (24 bytes, version byte = 0):
///   [0..7]  SequenceNumber      (long)
///   [8..15] EnqueueTimeUtcTicks (long)
///  [16..19] DeliveryCount       (int)
///     [20]  IsDeadLettered      (byte, 0 or 1)
///     [21]  FormatVersion       (byte, 0 = legacy 24-byte)
///  [22..23] reserved
///
/// Current format (32 bytes, version byte = 1):
///   Same as above, plus:
///  [24..31] TimeToLiveTicks     (long, 0 = no per-message TTL)
/// </summary>
public readonly struct MessageHeader
{
    public const int Size = 32;
    public const int LegacySize = 24;

    public long SequenceNumber { get; init; }
    public long EnqueueTimeUtcTicks { get; init; }
    public int DeliveryCount { get; init; }
    public bool IsDeadLettered { get; init; }
    /// <summary>Per-message TTL in ticks. 0 means no per-message TTL (use entity default at check time).</summary>
    public long TimeToLiveTicks { get; init; }
}

public static class MessageSerializer
{
    public static byte[] Encode(Message message, MessageHeader header)
    {
        ByteBuffer msgBuffer = message.Encode();

        byte[] result = new byte[MessageHeader.Size + msgBuffer.Length];

        BinaryPrimitives.WriteInt64BigEndian(result.AsSpan(0), header.SequenceNumber);
        BinaryPrimitives.WriteInt64BigEndian(result.AsSpan(8), header.EnqueueTimeUtcTicks);
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(16), header.DeliveryCount);
        result[20] = header.IsDeadLettered ? (byte)1 : (byte)0;
        result[21] = 1; // FormatVersion
        // bytes 22-23 reserved
        BinaryPrimitives.WriteInt64BigEndian(result.AsSpan(24), header.TimeToLiveTicks);

        Buffer.BlockCopy(msgBuffer.Buffer, msgBuffer.Offset, result, MessageHeader.Size, msgBuffer.Length);

        return result;
    }

    /// <summary>Returns the header size based on the format version byte embedded in the data.</summary>
    public static int GetHeaderSize(ReadOnlySpan<byte> data)
    {
        if (data.Length < MessageHeader.LegacySize)
            throw new ArgumentException($"Data too short: expected at least {MessageHeader.LegacySize} bytes, got {data.Length}");
        return (data[21] >= 1 && data.Length >= MessageHeader.Size) ? MessageHeader.Size : MessageHeader.LegacySize;
    }

    public static MessageHeader DecodeHeader(ReadOnlySpan<byte> data)
    {
        if (data.Length < MessageHeader.LegacySize)
            throw new ArgumentException($"Data too short: expected at least {MessageHeader.LegacySize} bytes, got {data.Length}");

        bool isV1 = data[21] >= 1 && data.Length >= MessageHeader.Size;
        return new MessageHeader
        {
            SequenceNumber = BinaryPrimitives.ReadInt64BigEndian(data[0..]),
            EnqueueTimeUtcTicks = BinaryPrimitives.ReadInt64BigEndian(data[8..]),
            DeliveryCount = BinaryPrimitives.ReadInt32BigEndian(data[16..]),
            IsDeadLettered = data[20] != 0,
            TimeToLiveTicks = isV1 ? BinaryPrimitives.ReadInt64BigEndian(data[24..]) : 0
        };
    }

    public static (MessageHeader header, Message message) Decode(ReadOnlySpan<byte> data)
    {
        if (data.Length < MessageHeader.LegacySize)
            throw new ArgumentException($"Data too short: expected at least {MessageHeader.LegacySize} bytes, got {data.Length}");

        bool isV1 = data[21] >= 1 && data.Length >= MessageHeader.Size;
        int headerSize = isV1 ? MessageHeader.Size : MessageHeader.LegacySize;

        var header = new MessageHeader
        {
            SequenceNumber = BinaryPrimitives.ReadInt64BigEndian(data[0..]),
            EnqueueTimeUtcTicks = BinaryPrimitives.ReadInt64BigEndian(data[8..]),
            DeliveryCount = BinaryPrimitives.ReadInt32BigEndian(data[16..]),
            IsDeadLettered = data[20] != 0,
            TimeToLiveTicks = isV1 ? BinaryPrimitives.ReadInt64BigEndian(data[24..]) : 0
        };

        byte[] msgBytes = data[headerSize..].ToArray();
        var msgBuffer = new ByteBuffer(msgBytes, 0, msgBytes.Length, msgBytes.Length);
        var message = Message.Decode(msgBuffer);

        return (header, message);
    }
}
