using System;
using System.Collections.Generic;

// Message contracts shared by the MassTransit 7.3 tests and the MassTransit 8 peer. MassTransit identifies message
// types by namespace and name (urn:message:AzsbEmu.Interop.Contracts:...), so both sides must use these exact types.
// Plain classes with settable properties, so both Newtonsoft.Json (MassTransit 7) and System.Text.Json (MassTransit 8)
// can write and read them without help.
namespace AzsbEmu.Interop.Contracts
{
    public enum Shade { Red, Green, Blue }

    public class Nested
    {
        public string Name { get; set; }
        public List<int> Values { get; set; }
    }

    /// <summary>A payload covering the value types most likely to serialize differently between the two serializers.</summary>
    public class Payload
    {
        public Guid Id { get; set; }
        public string Text { get; set; }
        public int Count { get; set; }
        public long Big { get; set; }
        public decimal Price { get; set; }
        public double Ratio { get; set; }
        public bool Flag { get; set; }
        public DateTime AtUtc { get; set; }
        public DateTimeOffset AtOffset { get; set; }
        public TimeSpan Duration { get; set; }
        public int? Missing { get; set; }
        public int? Present { get; set; }
        public Shade Shade { get; set; }
        public byte[] Data { get; set; }
        public List<string> Tags { get; set; }
        public Dictionary<string, int> Counts { get; set; }
        public Nested Child { get; set; }

        public static Payload Sample(string text) => new Payload
        {
            Id = Guid.NewGuid(),
            Text = text + " – ünïcödé ✓",
            Count = 42,
            Big = 9_007_199_254_740_993, // beyond double precision: must stay exact
            Price = 1234.5678m,
            Ratio = 0.1 + 0.2,
            Flag = true,
            AtUtc = new DateTime(2026, 11, 13, 10, 30, 15, 123, DateTimeKind.Utc),
            AtOffset = new DateTimeOffset(2026, 11, 13, 10, 30, 15, 456, TimeSpan.FromHours(-5)),
            Duration = TimeSpan.FromMinutes(90) + TimeSpan.FromMilliseconds(250),
            Missing = null,
            Present = 7,
            Shade = Shade.Blue,
            Data = new byte[] { 0, 1, 2, 254, 255 },
            Tags = new List<string> { "a", "b" },
            Counts = new Dictionary<string, int> { ["x"] = 1, ["y"] = 2 },
            Child = new Nested { Name = "child", Values = new List<int> { 3, 2, 1 } },
        };
    }

    /// <summary>Published by either side; <see cref="Origin"/> says which.</summary>
    public class InteropEvent
    {
        public string Kind { get; set; }
        public string Origin { get; set; }
        public Payload Payload { get; set; }
    }

    /// <summary>Sent to a queue by either side.</summary>
    public class InteropCommand
    {
        public string Kind { get; set; }
        public string Origin { get; set; }
        public Payload Payload { get; set; }
    }

    public class EchoRequest
    {
        public string Kind { get; set; }
        public Payload Payload { get; set; }
    }

    public class EchoResponse
    {
        public string Kind { get; set; }
        public string RespondedBy { get; set; }
        public Payload Payload { get; set; }
    }

    /// <summary>A message whose consumer always throws.</summary>
    public class Poison
    {
        public string Kind { get; set; }
    }

    /// <summary>Consumed on an endpoint that both versions declare with the same name.</summary>
    public class SharedEvent
    {
        public string Kind { get; set; }
        public int Index { get; set; }
    }

    /// <summary>Tells the MassTransit 8 peer to do something to the MassTransit 7 side.</summary>
    public class PeerInstruction
    {
        public string Kind { get; set; }
        /// <summary>publish, send, request or poison.</summary>
        public string Action { get; set; }
        public Payload Payload { get; set; }
    }

    /// <summary>Published by the MassTransit 8 peer to report what it received.</summary>
    public class PeerObserved
    {
        public string Kind { get; set; }
        /// <summary>publish, send, response, fault or shared.</summary>
        public string Via { get; set; }
        public Payload Payload { get; set; }
        public string Detail { get; set; }
    }
}
