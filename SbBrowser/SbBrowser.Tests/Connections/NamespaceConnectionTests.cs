using System.Net;
using System.Net.Sockets;
using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using SbBrowser.Core.Connections;

namespace SbBrowser.Tests.Connections;

public sealed class NamespaceConnectionTests : IDisposable
{
    private const string Secret = "do-not-print-this-key";
    private readonly BrokerFixture _broker = new();

    public void Dispose() => _broker.Dispose();

    /// <summary>A port-less emulator connection string, with the fixture's (non-default) ports in the profile.</summary>
    private ConnectionProfile EmulatorProfile() => new(
        "test emulator",
        $"Endpoint=sb://localhost;SharedAccessKeyName=Root;SharedAccessKey={Secret};UseDevelopmentEmulator=true",
        EmulatorManagementPort: _broker.HttpPort,
        EmulatorMessagingPort: _broker.Port);

    [Fact]
    public async Task AProfileWithNonDefaultPorts_ReachesBothTheManagementApiAndAmqp()
    {
        _broker.EnsureQueue("profile-queue");
        await using var connection = NamespaceConnection.Open(EmulatorProfile());

        var summary = await connection.GetSummaryAsync();
        Assert.Equal("localhost", summary.Name);
        Assert.Equal("Standard", summary.Sku);

        await using (var sender = connection.Client.CreateSender("profile-queue"))
            await sender.SendMessageAsync(new ServiceBusMessage("over amqp"));
        await using var receiver = connection.Client.CreateReceiver("profile-queue");
        Assert.Equal("over amqp", (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)))?.Body.ToString());
    }

    [Fact]
    public void AProfilesTextForm_LeavesOutTheConnectionString()
    {
        string text = EmulatorProfile().ToString();
        Assert.Contains("test emulator", text);
        Assert.DoesNotContain(Secret, text);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not a connection string " + Secret)]
    [InlineData("SharedAccessKeyName=Root;SharedAccessKey=" + Secret)]
    [InlineData("Endpoint=sb://localhost/;SharedAccessKey=" + Secret)]
    public void AnUnusableConnectionString_IsRejected_WithoutRepeatingIt(string connectionString)
    {
        var ex = Assert.Throws<InvalidConnectionStringException>(() => NamespaceConnection.Open(new ConnectionProfile("bad", connectionString)));
        Assert.DoesNotContain(Secret, ex.ToString());
    }

    [Fact]
    public async Task AnUnreachableNamespace_FailsPromptly_WithoutRepeatingTheKey()
    {
        var profile = EmulatorProfile() with { EmulatorManagementPort = UnusedPort() };
        await using var connection = NamespaceConnection.Open(profile, new NamespaceConnectionOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(5) });

        var started = DateTime.UtcNow;
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => connection.GetSummaryAsync());

        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(15), "the failure took too long to report");
        Assert.DoesNotContain(Secret, ex.ToString());
    }

    private static int UnusedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
