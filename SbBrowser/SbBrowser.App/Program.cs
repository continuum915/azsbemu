using System.Diagnostics;
using SbBrowser.App;
using SbBrowser.Web;

var launch = LaunchOptions.Parse(args);
if (launch.Error is not null)
{
    Console.Error.WriteLine(launch.Error);
    Console.Error.WriteLine("Usage: SbBrowser [--port <port>] [--no-browser] [--urls <urls>]");
    return 2;
}

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = launch.HostArgs,
    // Next to the binaries, whatever the working directory: a published exe finds its pages in wwwroot there.
    ContentRootPath = AppContext.BaseDirectory,
});

// A build (dotnet run) keeps the assets in their source folders and NuGet packages, listed by a manifest next to the
// binaries, which ASP.NET Core reads only in Development unless asked. A publish has wwwroot instead, and then this
// does nothing.
builder.WebHost.UseStaticWebAssets();

// Loopback only unless the user passes --urls (or ASPNETCORE_URLS): the app holds connection strings.
if (string.IsNullOrEmpty(builder.Configuration["urls"]))
    builder.WebHost.UseUrls(launch.LoopbackUrl);

builder.Services.AddSbBrowser(new SbBrowserOptions { EmulatorMode = false });

var app = builder.Build();
app.MapSbBrowser();

try
{
    await app.StartAsync();
}
catch (IOException ex)
{
    // Most often the port is taken, for example by the emulator's own SbBrowser on 8081.
    Console.Error.WriteLine($"SbBrowser could not start: {ex.Message}");
    Console.Error.WriteLine("Choose another port with --port <port>.");
    return 1;
}

string url = BrowsableUrl(app.Urls.First());
Console.WriteLine($"SbBrowser is running at {url} (Ctrl+C to stop).");
if (launch.OpenBrowser) OpenInBrowser(url, app.Logger);

await app.WaitForShutdownAsync();
return 0;

// A wildcard address (--urls http://0.0.0.0:8081) can be listened on but not browsed to.
static string BrowsableUrl(string address)
{
    var uri = new UriBuilder(address);
    if (uri.Host is "0.0.0.0" or "[::]" or "+" or "*") uri.Host = "localhost";
    return uri.Uri.GetLeftPart(UriPartial.Authority);
}

static void OpenInBrowser(string url, ILogger logger)
{
    try
    {
        Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
    }
    catch (Exception ex)
    {
        logger.LogWarning("Could not open a browser ({Reason}). Open {Url} yourself.", ex.Message, url);
    }
}

namespace SbBrowser.App
{
    /// <summary>Identifies this app's assembly to WebApplicationFactory in the tests.</summary>
    public sealed class AppEntryPoint;
}
