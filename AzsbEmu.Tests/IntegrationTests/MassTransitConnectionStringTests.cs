using System.Collections.Concurrent;
using MassTransit;
using Xunit.Abstractions;

namespace AzsbEmu.Tests.IntegrationTests;

[CollectionDefinition(Name, DisableParallelization = true)]
public class StandardPortsCollection
{
    /// <summary>Tests that bind the emulator's standard ports (5672, 80) and so must not run alongside each other.</summary>
    public const string Name = "Standard ports";
}

/// <summary>
/// MassTransit 8 configured the way an application would be: only the documented connection string, with the
/// emulator on its standard ports. Skipped (with a note) when those ports are already taken, e.g. by a running emulator.
/// </summary>
[Collection(StandardPortsCollection.Name)]
public class MassTransitConnectionStringTests(ITestOutputHelper output)
{
    [Fact]
    public async Task MassTransit_ConfiguredWithOnlyTheConnectionString_DeploysTopologyAndConsumes()
    {
        // Something already answering on these ports (e.g. a running emulator) could take the test's traffic, and on
        // Windows a localhost bind can succeed alongside another process's wildcard bind, so probe before binding.
        using var broker = !AnyoneListening(5672) && !AnyoneListening(80) ? BrokerFixture.TryCreateOnStandardPorts() : null;
        if (broker is null)
        {
            output.WriteLine("SKIPPED: ports 5672 and 80 must be free to run the emulator on its standard ports.");
            return;
        }

        var received = new ConcurrentQueue<OrderPlaced>();
        var bus = Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(broker.ConnectionString);
            cfg.ReceiveEndpoint("plain-config", e => e.Consumer(() => new RecordingConsumer<OrderPlaced>(received)));
        });

        await bus.StartAsync(MassTransitWait.NewTimeoutToken());
        try
        {
            await bus.Publish(new OrderPlaced("plain"));
            await MassTransitWait.UntilAsync(() => !received.IsEmpty, "MassTransit to consume the published message");
            Assert.Equal("plain", Assert.Single(received).OrderId);
        }
        finally
        {
            await bus.StopAsync(MassTransitWait.NewTimeoutToken());
        }
    }

    private static bool AnyoneListening(int port)
    {
        try
        {
            using var probe = new System.Net.Sockets.TcpClient();
            return probe.ConnectAsync("localhost", port).Wait(TimeSpan.FromMilliseconds(500)) && probe.Connected;
        }
        catch
        {
            return false;
        }
    }
}
