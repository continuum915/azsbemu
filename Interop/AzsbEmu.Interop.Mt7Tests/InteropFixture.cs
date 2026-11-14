using System.Collections.Concurrent;
using System.Diagnostics;
using AzsbEmu.Interop.Contracts;
using AzsbEmu.Tests.IntegrationTests;
using MassTransit;
using MassTransit.Azure.ServiceBus.Core;

namespace AzsbEmu.Interop.Mt7Tests;

/// <summary>What either bus received, for the tests to wait on.</summary>
public sealed record Observation(string Side, string Via, string Kind, Payload? Payload, string? Detail);

/// <summary>
/// One emulator, a MassTransit 8 bus in a child process (<c>AzsbEmu.Interop.Mt8Peer</c>) and a MassTransit 7.3 bus in
/// this process, shared by all the interop tests. Every test uses its own <c>Kind</c> to pick out its messages.
/// </summary>
public sealed class InteropFixture : IAsyncLifetime
{
    public const string Mt7Endpoint = "mt7-endpoint";
    public const string Mt8Endpoint = "mt8-peer";
    public const string SharedEndpoint = "shared-endpoint";

    public BrokerFixture Broker { get; } = new();
    public IBusControl Bus { get; private set; } = null!;
    public ConcurrentQueue<Observation> Observations { get; } = new();

    private Process? _peer;
    private readonly ConcurrentQueue<string> _peerOutput = new();

    public async Task InitializeAsync()
    {
        await StartPeerAsync();
        Bus = CreateBus(cfg =>
        {
            cfg.ReceiveEndpoint(Mt7Endpoint, e => e.Consumer(() => new Mt7Consumer(Observations)));
            cfg.ReceiveEndpoint("mt7-observer", e => e.Consumer(() => new PeerObservedConsumer(Observations)));
        });
        await Bus.StartAsync(Timeout(60));
    }

    /// <summary>A MassTransit 7.3 bus on the emulator (the tests start and stop it).</summary>
    public IBusControl CreateBus(Action<IServiceBusBusFactoryConfigurator> configure) =>
        MassTransit.Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new EmulatorHostSettings(Broker));
            configure(cfg);
        });

    public async Task DisposeAsync()
    {
        try { if (Bus != null) await Bus.StopAsync(Timeout(30)); } catch { /* best effort */ }
        await StopPeerAsync();
        Broker.Dispose();
    }

    public static CancellationToken Timeout(int seconds) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    /// <summary>Waits for an observation matching <paramref name="match"/>, failing after <paramref name="seconds"/>.</summary>
    public async Task<Observation> WaitForAsync(Func<Observation, bool> match, string what, int seconds = 20)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (true)
        {
            var found = Observations.FirstOrDefault(match);
            if (found != null) return found;
            if (_peer is { HasExited: true }) Assert.Fail($"The MassTransit 8 peer exited while waiting for {what}:\n{PeerOutput}");
            if (DateTime.UtcNow > deadline) Assert.Fail($"Timed out waiting for {what}. Peer output:\n{PeerOutput}");
            await Task.Delay(50);
        }
    }

    public string PeerOutput => string.Join("\n", _peerOutput.TakeLast(40));

    private async Task StartPeerAsync()
    {
        // ...\Interop\AzsbEmu.Interop.Mt7Tests\bin\<config>\<tfm>\ -> ...\Interop\AzsbEmu.Interop.Mt8Peer\bin\<config>\<tfm>\
        string testBin = AppContext.BaseDirectory;
        string peerDll = Path.Combine(testBin.Replace("AzsbEmu.Interop.Mt7Tests", "AzsbEmu.Interop.Mt8Peer"), "AzsbEmu.Interop.Mt8Peer.dll");
        if (!File.Exists(peerDll)) throw new FileNotFoundException("Build AzsbEmu.Interop.Mt8Peer first.", peerDll);

        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _peer = new Process
        {
            StartInfo = new ProcessStartInfo("dotnet", [peerDll, Broker.Port.ToString(), Broker.HttpPort.ToString()])
            {
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            },
            EnableRaisingEvents = true,
        };
        _peer.OutputDataReceived += (_, e) =>
        {
            if (e.Data == null) return;
            _peerOutput.Enqueue(e.Data);
            if (e.Data == "READY") ready.TrySetResult();
        };
        _peer.ErrorDataReceived += (_, e) => { if (e.Data != null) _peerOutput.Enqueue("stderr: " + e.Data); };
        _peer.Exited += (_, _) => ready.TrySetException(new InvalidOperationException("The MassTransit 8 peer exited during startup."));
        _peer.Start();
        _peer.BeginOutputReadLine();
        _peer.BeginErrorReadLine();

        try
        {
            await ready.Task.WaitAsync(TimeSpan.FromSeconds(60));
        }
        catch (Exception ex)
        {
            await StopPeerAsync();
            throw new InvalidOperationException($"The MassTransit 8 peer did not start: {ex.Message}\n{PeerOutput}", ex);
        }
    }

    private async Task StopPeerAsync()
    {
        if (_peer == null) return;
        try
        {
            if (!_peer.HasExited)
            {
                await _peer.StandardInput.WriteLineAsync("stop");
                if (!_peer.WaitForExit(TimeSpan.FromSeconds(30))) _peer.Kill(entireProcessTree: true);
            }
        }
        catch { try { _peer.Kill(entireProcessTree: true); } catch { } }
        _peer.Dispose();
        _peer = null;
    }
}

[CollectionDefinition(Name)]
public sealed class InteropCollection : ICollectionFixture<InteropFixture>
{
    public const string Name = "MassTransit 7.3 and 8 interop";
}

/// <summary>The MassTransit 7.3 side's consumers, recording what MassTransit 8 sent it.</summary>
public sealed class Mt7Consumer(ConcurrentQueue<Observation> observations) :
    IConsumer<InteropEvent>,
    IConsumer<InteropCommand>,
    IConsumer<EchoRequest>,
    IConsumer<Poison>,
    IConsumer<Fault<Poison>>
{
    public Task Consume(ConsumeContext<InteropEvent> context)
    {
        if (context.Message.Origin == "mt8") observations.Enqueue(new("mt7", "publish", context.Message.Kind, context.Message.Payload, null));
        return Task.CompletedTask;
    }

    public Task Consume(ConsumeContext<InteropCommand> context)
    {
        observations.Enqueue(new("mt7", "send", context.Message.Kind, context.Message.Payload, null));
        return Task.CompletedTask;
    }

    public Task Consume(ConsumeContext<EchoRequest> context) =>
        context.RespondAsync(new EchoResponse { Kind = context.Message.Kind, RespondedBy = "mt7", Payload = context.Message.Payload });

    public Task Consume(ConsumeContext<Poison> context) =>
        throw new InvalidOperationException($"mt7 refuses {context.Message.Kind}");

    public Task Consume(ConsumeContext<Fault<Poison>> context)
    {
        observations.Enqueue(new("mt7", "fault", context.Message.Message.Kind, null,
            string.Join(" | ", context.Message.Exceptions.Select(e => $"{e.ExceptionType}: {e.Message}"))));
        return Task.CompletedTask;
    }
}

/// <summary>Records what the MassTransit 8 peer reports it received.</summary>
public sealed class PeerObservedConsumer(ConcurrentQueue<Observation> observations) : IConsumer<PeerObserved>
{
    public Task Consume(ConsumeContext<PeerObserved> context)
    {
        var m = context.Message;
        observations.Enqueue(new("mt8", m.Via, m.Kind, m.Payload, m.Detail));
        return Task.CompletedTask;
    }
}

/// <summary>MassTransit 7.3 host settings using the emulator's clients (the admin client rewritten to its HTTP port).</summary>
public sealed class EmulatorHostSettings(BrokerFixture broker) : ServiceBusHostSettings
{
    public Uri ServiceUri { get; } = new("sb://localhost/");
    public string ConnectionString => broker.ConnectionString;
    public Azure.Messaging.ServiceBus.ServiceBusClient ServiceBusClient { get; } = broker.CreateServiceBusClient();
    public Azure.Messaging.ServiceBus.Administration.ServiceBusAdministrationClient ServiceBusAdministrationClient { get; } = broker.CreateAdministrationClient();
    public Azure.Core.TokenCredential? TokenCredential => null;
    public Azure.AzureNamedKeyCredential? NamedKeyCredential => null;
    public Azure.AzureSasCredential? SasCredential => null;
    public Azure.Messaging.ServiceBus.ServiceBusTransportType TransportType => Azure.Messaging.ServiceBus.ServiceBusTransportType.AmqpTcp;
    public int RetryLimit => 5;
    public TimeSpan RetryMinBackoff => TimeSpan.FromMilliseconds(100);
    public TimeSpan RetryMaxBackoff => TimeSpan.FromSeconds(5);
}
