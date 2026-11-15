using System.Text;

namespace SbBrowser.Core.Connections;

/// <summary>
/// A saved way to reach a Service Bus namespace. For an emulator connection string (<c>UseDevelopmentEmulator=true</c>)
/// the ports say where its management (HTTP) API and AMQP endpoint listen; other connection strings ignore them.
/// </summary>
public sealed record ConnectionProfile(
    string Name,
    string ConnectionString,
    int EmulatorManagementPort = ConnectionProfile.DefaultEmulatorManagementPort,
    int EmulatorMessagingPort = ConnectionProfile.DefaultEmulatorMessagingPort)
{
    public const int DefaultEmulatorManagementPort = 80;
    public const int DefaultEmulatorMessagingPort = 5672;

    /// <summary>True when the connection string targets an emulator.</summary>
    public bool IsEmulator => ConnectionStrings.IsEmulator(ConnectionString);

    // Records print every property; the connection string holds a secret, so it is left out.
    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append($"Name = {Name}, IsEmulator = {IsEmulator}");
        return true;
    }
}
