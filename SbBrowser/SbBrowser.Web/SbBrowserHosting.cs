using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using SbBrowser.Core.Connections;
using SbBrowser.Web.Components;
using SbBrowser.Web.Services;

namespace SbBrowser.Web;

/// <summary>How a host runs SbBrowser.</summary>
public sealed class SbBrowserOptions
{
    /// <summary>
    /// True when the emulator hosts SbBrowser: it is locked to the emulator's own connection and offers no profile
    /// management. False for the standalone app.
    /// </summary>
    public bool EmulatorMode { get; init; }

    /// <summary>
    /// Where connection profiles come from. The emulator passes a <see cref="FixedProfileStore"/> with its own
    /// connection; when null, the current user's profile file is used.
    /// </summary>
    public IConnectionProfileStore? ProfileStore { get; init; }
}

/// <summary>Adds SbBrowser to an ASP.NET Core host.</summary>
public static class SbBrowserHosting
{
    public static IServiceCollection AddSbBrowser(this IServiceCollection services, SbBrowserOptions options)
    {
        services.AddSingleton(options);
        services.AddSingleton(options.ProfileStore ?? JsonFileProfileStore.ForCurrentUser());
        services.AddSingleton<ConnectionHub>();
        services.AddScoped<BrowserSession>();
        services.AddRazorComponents().AddInteractiveServerComponents();
        return services;
    }

    /// <summary>Maps SbBrowser's pages and assets. SbBrowser owns the whole app, so call this on a dedicated host.</summary>
    public static WebApplication MapSbBrowser(this WebApplication app)
    {
        app.UseAntiforgery();
        app.MapStaticAssets();
        app.MapRazorComponents<App>().AddInteractiveServerRenderMode();
        return app;
    }
}
