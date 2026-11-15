using SbBrowser.Core.Connections;

namespace SbBrowser.Tests.Connections;

public class ConnectionStringsTests
{
    private const string Keys = "SharedAccessKeyName=Root;SharedAccessKey=dummy";

    [Theory]
    [InlineData("Endpoint=sb://localhost;" + Keys + ";UseDevelopmentEmulator=true")]
    [InlineData("Endpoint=sb://localhost/;" + Keys + ";UseDevelopmentEmulator=true")]
    [InlineData("Endpoint=sb://localhost:5300/;" + Keys + ";UseDevelopmentEmulator=true")]
    [InlineData("Endpoint=sb://localhost:5300;" + Keys + ";UseDevelopmentEmulator=true;")]
    [InlineData("endpoint = sb://localhost/ ;" + Keys + "; UseDevelopmentEmulator = TRUE")]
    public void AnEmulatorEndpoint_GetsTheRequestedPort(string connectionString)
    {
        string rewritten = ConnectionStrings.WithEmulatorPort(connectionString, 8080);

        Assert.Contains("Endpoint=sb://localhost:8080/;", rewritten);
        Assert.Contains("SharedAccessKey=dummy", rewritten);
    }

    [Fact]
    public void AnIpv4Host_KeepsItsAddress()
    {
        string rewritten = ConnectionStrings.WithEmulatorPort("Endpoint=sb://127.0.0.1/;" + Keys + ";UseDevelopmentEmulator=true", 80);
        Assert.StartsWith("Endpoint=sb://127.0.0.1:80/;", rewritten);
    }

    [Theory]
    [InlineData("Endpoint=sb://contoso.servicebus.windows.net/;" + Keys)]
    [InlineData("Endpoint=sb://localhost/;" + Keys + ";UseDevelopmentEmulator=false")]
    public void OtherConnectionStrings_AreUnchanged(string connectionString)
    {
        Assert.Equal(connectionString, ConnectionStrings.WithEmulatorPort(connectionString, 8080));
        Assert.False(ConnectionStrings.IsEmulator(connectionString));
    }
}
