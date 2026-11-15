using AzsbEmu.Server.Protocol.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SbBrowser.Core.Connections;
using SbBrowser.Web;

namespace AzsbEmu.Server.Browser;

/// <summary>
/// SbBrowser, served by the emulator on its own port. It is locked to one read-only profile, the emulator itself, so
/// no other connection string can be entered into (and stored by) an emulator that may listen on every interface.
/// SbBrowser reaches the emulator through the SDK over AMQP and the management API, like any client.
/// </summary>
/// <remarks>
/// A separate Kestrel app from <see cref="ManagementHttpHost"/>: the management API owns every path on its port,
/// because entity names are arbitrary paths.
/// </remarks>
public sealed class BrowserHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly ILogger _logger;

    /// <summary>The port actually bound (resolves a requested port of 0).</summary>
    public int Port { get; private set; }

    /// <param name="host">The address to listen on, as for the emulator's other listeners.</param>
    /// <param name="port">The port to serve SbBrowser on; 0 picks a free one.</param>
    /// <param name="emulator">
    /// The emulator's own connection: its connection string, AMQP port and management API port. The only profile
    /// SbBrowser offers.
    /// </param>
    public BrowserHost(ILoggerFactory loggerFactory, string host, int port, ConnectionProfile emulator)
    {
        _logger = loggerFactory.CreateLogger<BrowserHost>();

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            // The emulator's own assembly and folder, whatever the working directory: the static asset manifest is
            // found by application name next to the binaries.
            ApplicationName = typeof(BrowserHost).Assembly.GetName().Name,
            ContentRootPath = AppContext.BaseDirectory,
            Args = [],
        });
        builder.Logging.ClearProviders();
        // A build (dotnet run, tests) keeps SbBrowser's assets in their source folders and NuGet packages, listed by
        // a manifest next to the binaries; ASP.NET Core reads it only in Development unless asked. A publish copies
        // the assets into wwwroot and has no manifest, and then this does nothing.
        builder.WebHost.UseStaticWebAssets();
        builder.WebHost.ConfigureKestrel(kestrel => KestrelListen.On(kestrel, host, port));
        builder.Services.AddSbBrowser(new SbBrowserOptions { EmulatorMode = true, ProfileStore = new FixedProfileStore(emulator) });

        _app = builder.Build();
        _app.MapSbBrowser();
    }

    /// <summary>The host's services, for tests that look at what a browser tab would see.</summary>
    internal IServiceProvider Services => _app.Services;

    public async Task StartAsync()
    {
        await _app.StartAsync().ConfigureAwait(false);
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        Port = new Uri(address).Port;
        _logger.LogInformation("SbBrowser listening on {Address}", address);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
    }
}
