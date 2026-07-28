using System.Diagnostics;

namespace AzsbEmu.Server.Core;

public static class BrokerProfiler
{
    private static readonly string[] Names = ["FASTER append", "Dispatch total", "Log availability wait", "Iterator-lock wait", "Iterator read + bookkeeping", "Message decode", "Send-lock wait", "AMQP delivery write", "Accepted settlement", "Truncation lock + scan"];
    private static readonly long[] Ticks = new long[Names.Length];
    private static readonly long[] Counts = new long[Names.Length];

    public static bool Enabled { get; set; }

    public static void Reset()
    {
        Array.Clear(Ticks);
        Array.Clear(Counts);
    }

    public static BrokerProfileSnapshot Snapshot() => new(
        Names.Select((name, index) => new BrokerProfileStage(name, Counts[index], Counts[index] == 0 ? 0 : Ticks[index] * 1000d / Stopwatch.Frequency / Counts[index])).ToArray());

    public static void Append(long started) => Record(0, started);
    public static void Dispatch(long started) => Record(1, started);
    public static void AvailabilityWait(long started) => Record(2, started);
    public static void IteratorLockWait(long started) => Record(3, started);
    public static void IteratorRead(long started) => Record(4, started);
    public static void Decode(long started) => Record(5, started);
    public static void SendLock(long started) => Record(6, started);
    public static void Send(long started) => Record(7, started);
    public static void Settlement(long started) => Record(8, started);
    public static void Truncate(long started) => Record(9, started);

    private static void Record(int index, long started)
    {
        if (!Enabled) return;
        Interlocked.Add(ref Ticks[index], Stopwatch.GetTimestamp() - started);
        Interlocked.Increment(ref Counts[index]);
    }
}

public sealed record BrokerProfileStage(string Name, long Count, double AverageMilliseconds);
public sealed record BrokerProfileSnapshot(params BrokerProfileStage[] Stages);