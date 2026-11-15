using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace AzsbEmu.Server.Protocol.Http;

/// <summary>How the emulator's HTTP hosts (the management API and SbBrowser) listen on the configured host.</summary>
internal static class KestrelListen
{
    /// <summary>
    /// Listens on <paramref name="host"/>:<paramref name="port"/>, the way the AMQP listener does: loopback names on
    /// both loopback addresses, a wildcard on every interface, and a specific address on that address only.
    /// A port of 0 on loopback picks a free port.
    /// </summary>
    public static void On(KestrelServerOptions kestrel, string host, int port)
    {
        if (host is "localhost" or "127.0.0.1" or "::1")
        {
            // Both loopback addresses: clients resolving "localhost" try ::1 first, and on Windows a refused
            // IPv6 attempt costs ~2s per new connection before falling back to 127.0.0.1.
            kestrel.ListenLocalhost(port == 0 ? FindFreeLoopbackPort() : port);
        }
        else if (host is not ("0.0.0.0" or "::" or "*") && IPAddress.TryParse(host, out var address))
        {
            kestrel.Listen(address, port);
        }
        else
        {
            kestrel.ListenAnyIP(port);
        }
    }

    private static int FindFreeLoopbackPort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }
}
