using System.Text.RegularExpressions;
using AzsbEmu.Server.Browser;
using AzsbEmu.Tests.IntegrationTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using SbBrowser.Core.Connections;
using SbBrowser.Web.Services;

namespace SbBrowser.Tests.Hosting;

/// <summary>The emulator serving SbBrowser on its own port, connected to itself.</summary>
public sealed class EmulatorBrowserHostTests : IAsyncLifetime
{
    private readonly BrokerFixture _broker = new();
    private BrowserHost _host = null!;

    public async Task InitializeAsync()
    {
        var emulator = new ConnectionProfile("Emulator",
            "Endpoint=sb://localhost/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true",
            _broker.HttpPort, _broker.Port);
        _host = new BrowserHost(NullLoggerFactory.Instance, host: "localhost", port: 0, emulator);
        await _host.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
        _broker.Dispose();
    }

    [Fact]
    public async Task ItServesTheSbBrowserPage_OnItsOwnPort()
    {
        using var http = new HttpClient { BaseAddress = new Uri($"http://localhost:{_host.Port}/") };

        var page = await http.GetStringAsync("");

        Assert.Contains("<title>SbBrowser</title>", page);
        // Each asset the page references must come back with content: a missing file can still answer 200, empty.
        foreach (string pattern in new[]
                 {
                     "src=\"(_framework/blazor\\.web[^\"]*\\.js)\"",
                     "href=\"(_content/SbBrowser\\.Web/sbbrowser[^\"]*\\.css)\"",
                     "src=\"(_content/SbBrowser\\.Web/sbbrowser[^\"]*\\.js)\"",
                 })
        {
            var asset = Regex.Match(page, pattern);
            Assert.True(asset.Success, $"the page does not reference {pattern}");
            var content = await http.GetByteArrayAsync(asset.Groups[1].Value);
            Assert.True(content.Length > 0, $"{asset.Groups[1].Value} was served empty");
        }
    }

    [Fact]
    public async Task ABrowserTab_SeesTheEmulatorsEntities_AndCannotAddConnections()
    {
        _broker.EnsureQueue("orders");
        _broker.EnsureTopic("events");

        // What one browser tab (a Blazor circuit) gets: its own scoped session.
        using var scope = _host.Services.CreateScope();
        var session = scope.ServiceProvider.GetRequiredService<BrowserSession>();
        await session.EnsureInitializedAsync();

        Assert.True(session.Connected, session.Error?.Message);
        Assert.NotNull(session.Tree.FindQueue("orders"));
        Assert.NotNull(session.Tree.FindTopic("events"));
        Assert.True(session.EmulatorMode);
        Assert.False(session.CanEditProfiles);
        Assert.Single(session.Profiles);
    }
}
