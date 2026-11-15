using System.Globalization;

namespace SbBrowser.App;

/// <summary>
/// The standalone app's own command-line options: <c>--port</c> and <c>--no-browser</c>. Every other argument
/// (<c>--urls</c>, configuration overrides) is left for ASP.NET Core in <see cref="HostArgs"/>.
/// </summary>
public sealed record LaunchOptions(int Port, bool OpenBrowser, string[] HostArgs, string? Error)
{
    public const int DefaultPort = 8081;

    /// <summary>Where the app listens unless <c>--urls</c> says otherwise: loopback only, since it holds connection strings.</summary>
    public string LoopbackUrl => $"http://127.0.0.1:{Port}";

    public static LaunchOptions Parse(string[] args)
    {
        int port = DefaultPort;
        bool openBrowser = true;
        var hostArgs = new List<string>();

        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            if (arg == "--no-browser")
            {
                openBrowser = false;
            }
            else if (arg == "--port" || arg.StartsWith("--port=", StringComparison.Ordinal))
            {
                string? value = arg == "--port" ? (i + 1 < args.Length ? args[++i] : null) : arg["--port=".Length..];
                if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port is < 1 or > 65535)
                    return new LaunchOptions(DefaultPort, openBrowser, [], $"--port needs a port number from 1 to 65535, not '{value}'.");
            }
            else
            {
                hostArgs.Add(arg);
            }
        }

        return new LaunchOptions(port, openBrowser, hostArgs.ToArray(), null);
    }
}
