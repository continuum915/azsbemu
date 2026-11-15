using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace SbBrowser.Core.Connections;

/// <summary>How hard a connection retries before reporting a failure.</summary>
public sealed record NamespaceConnectionOptions
{
    /// <summary>Retries after a failed attempt. Kept low so an unreachable namespace is reported in seconds.</summary>
    public int MaxRetries { get; init; } = 1;

    /// <summary>How long one attempt may take.</summary>
    public TimeSpan TryTimeout { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>How long "next available session" waits for a session before reporting that there is none.</summary>
    public TimeSpan NextSessionWait { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// How long receive-and-delete waits for more messages before deciding the entity has run dry. Short, so an empty
    /// entity answers promptly instead of waiting out the SDK's one-minute default.
    /// </summary>
    public TimeSpan ReceiveWait { get; init; } = TimeSpan.FromSeconds(2);
}

/// <summary>What a namespace says about itself.</summary>
public sealed record NamespaceSummary(string Name, string Sku, DateTimeOffset CreatedTime);

/// <summary>The connection-string problem, described without repeating the connection string.</summary>
public sealed class InvalidConnectionStringException(string reason) : Exception($"The connection string is not valid: {reason}");

/// <summary>
/// An open connection to one namespace: owns the SDK clients for a <see cref="ConnectionProfile"/>. Dispose it when
/// the user disconnects.
/// </summary>
public sealed partial class NamespaceConnection : IAsyncDisposable
{
    private readonly NamespaceConnectionOptions _options;

    private NamespaceConnection(ConnectionProfile profile, NamespaceConnectionOptions options, ServiceBusClient client,
        ServiceBusAdministrationClient admin)
    {
        _options = options;
        Profile = profile;
        Client = client;
        Admin = admin;
    }

    public ConnectionProfile Profile { get; }

    internal ServiceBusClient Client { get; }

    internal ServiceBusAdministrationClient Admin { get; }

    /// <summary>
    /// Creates the clients for <paramref name="profile"/>. Nothing is sent yet; call <see cref="GetSummaryAsync"/> to
    /// check the namespace answers.
    /// </summary>
    /// <exception cref="InvalidConnectionStringException">The connection string cannot be used.</exception>
    public static NamespaceConnection Open(ConnectionProfile profile, NamespaceConnectionOptions? options = null)
    {
        options ??= new NamespaceConnectionOptions();
        Validate(profile.ConnectionString);

        string management = ConnectionStrings.WithEmulatorPort(profile.ConnectionString, profile.EmulatorManagementPort);
        string messaging = ConnectionStrings.WithEmulatorPort(profile.ConnectionString, profile.EmulatorMessagingPort);

        var adminOptions = new ServiceBusAdministrationClientOptions();
        adminOptions.Retry.MaxRetries = options.MaxRetries;
        adminOptions.Retry.NetworkTimeout = options.TryTimeout;

        var clientOptions = new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = options.MaxRetries, TryTimeout = options.TryTimeout },
        };

        try
        {
            return new NamespaceConnection(profile, options,
                new ServiceBusClient(messaging, clientOptions),
                new ServiceBusAdministrationClient(management, adminOptions));
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException)
        {
            // SDK messages describe the problem without echoing the value; keep only that description.
            throw new InvalidConnectionStringException(ex.Message.Split(" (Parameter", 2)[0]);
        }
    }

    /// <summary>Asks the namespace for its name and SKU; the first call that actually reaches it.</summary>
    public async Task<NamespaceSummary> GetSummaryAsync(CancellationToken cancellationToken = default)
    {
        NamespaceProperties ns = await Admin.GetNamespacePropertiesAsync(cancellationToken).ConfigureAwait(false);
        return new NamespaceSummary(ns.Name, ns.MessagingSku.ToString(), ns.CreatedTime);
    }

    public ValueTask DisposeAsync() => Client.DisposeAsync();

    private static void Validate(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidConnectionStringException("it is empty.");

        ServiceBusConnectionStringProperties properties;
        try
        {
            properties = ServiceBusConnectionStringProperties.Parse(connectionString);
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException)
        {
            throw new InvalidConnectionStringException("it is not in the form Endpoint=...;SharedAccessKeyName=...;SharedAccessKey=...");
        }

        if (properties.Endpoint is null)
            throw new InvalidConnectionStringException("it has no Endpoint.");
        if (string.IsNullOrEmpty(properties.SharedAccessSignature)
            && (string.IsNullOrEmpty(properties.SharedAccessKeyName) || string.IsNullOrEmpty(properties.SharedAccessKey)))
            throw new InvalidConnectionStringException("it needs SharedAccessKeyName and SharedAccessKey, or SharedAccessSignature.");
    }
}
