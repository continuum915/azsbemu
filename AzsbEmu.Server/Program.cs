using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using AzsbEmu.Server.Core;
using AzsbEmu.Server.Config;
using AzsbEmu.Server.Browser;
using AzsbEmu.Server.Protocol.Http;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables("AZSB_EMU_")
    .AddCommandLine(args, new Dictionary<string, string>
    {
        ["--host"]     = "Host",
        ["--port"]     = "Port",
        ["--http-port"] = "HttpPort",
        ["--browser-port"] = "BrowserPort",
        ["--data-dir"] = "DataDirectory"
    })
    .Build();

var config = configuration.Get<EmulatorConfig>() ?? new EmulatorConfig();

if (string.IsNullOrEmpty(config.DataDirectory))
    config.DataDirectory = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "AzsbEmu");

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Debug);
});

var logger = loggerFactory.CreateLogger("Program");

Directory.CreateDirectory(config.DataDirectory);
logger.LogInformation("Data directory: {DataDirectory}", config.DataDirectory);

var entityManager = new EntityManager(config.DataDirectory, loggerFactory);

foreach (var queueCfg in config.Queues)
{
    if (entityManager.GetQueue(queueCfg.Name) == null)
    {
        entityManager.CreateQueue(new QueueInfo
        {
            Name = queueCfg.Name,
            RequiresSession = queueCfg.RequiresSession,
            LockDuration = TimeSpan.FromSeconds(queueCfg.LockDurationSeconds),
            MaxSizeInMegabytes = queueCfg.MaxSizeInMegabytes,
            MaxDeliveryCount = queueCfg.MaxDeliveryCount,
            DeadLetteringOnMessageExpiration = queueCfg.DeadLetteringOnMessageExpiration
        });
        logger.LogInformation("Declared queue '{Name}'", queueCfg.Name);
    }
}

foreach (var topicCfg in config.Topics)
{
    entityManager.CreateTopic(topicCfg.Name);

    foreach (var subCfg in topicCfg.Subscriptions)
    {
        entityManager.CreateSubscription(topicCfg.Name, new SubscriptionInfo
        {
            Name = subCfg.Name,
            TopicName = topicCfg.Name,
            RequiresSession = subCfg.RequiresSession,
            DeadLetteringOnMessageExpiration = subCfg.DeadLetteringOnMessageExpiration
        });
    }
}

string host = config.Host;
int port = config.Port;
string amqpAddress = $"amqp://{host}:{port}";
// Clients can't connect to a wildcard address; show the loopback name instead. An IPv6 address needs brackets in a URL.
string clientHost = host is "0.0.0.0" or "::" or "*" ? "localhost"
    : System.Net.IPAddress.TryParse(host, out var ip) && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? $"[{host}]"
    : host;
string connectionString = $"Endpoint=sb://{clientHost}/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true";

var broker = new Broker(amqpAddress, entityManager, loggerFactory);
broker.Start();

// The management API is optional for AMQP-only clients: if its port can't be bound, keep running without it.
ManagementHttpHost? managementApi = new ManagementHttpHost(entityManager, loggerFactory, host, config.HttpPort);
string managementAddress;
try
{
    await managementApi.StartAsync();
    managementAddress = $"http://{host}:{managementApi.Port}";
}
catch (Exception ex)
{
    logger.LogError(ex, "Could not start the HTTP management API on port {HttpPort}; ServiceBusAdministrationClient (and MassTransit topology deployment) will not work. Set HttpPort / --http-port to a free port behind a mapping to port 80.", config.HttpPort);
    try { await managementApi.DisposeAsync(); } catch { /* never started */ }
    managementApi = null;
    managementAddress = "unavailable";
}

// SbBrowser is optional too: BrowserPort 0 turns it off, and a port that can't be bound only loses the web UI. It
// reads entities through the management API, so without that it stays off rather than browse whatever holds the port.
BrowserHost? browser = null;
string browserAddress = "off";
if (config.BrowserPort > 0 && managementApi is null)
{
    logger.LogWarning("SbBrowser is off: it needs the HTTP management API, which did not start.");
    browserAddress = "off (needs the management API)";
}
else if (config.BrowserPort > 0)
{
    browser = new BrowserHost(loggerFactory, host, config.BrowserPort,
        new SbBrowser.Core.Connections.ConnectionProfile("Emulator", connectionString, managementApi!.Port, port));
    try
    {
        await browser.StartAsync();
        browserAddress = $"http://{clientHost}:{browser.Port}";
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Could not start SbBrowser on port {BrowserPort}; the emulator runs without it. Set BrowserPort / --browser-port to a free port, or 0 to turn it off.", config.BrowserPort);
        try { await browser.DisposeAsync(); } catch { /* never started */ }
        browser = null;
        browserAddress = "unavailable";
    }
}

Console.WriteLine("Azure Service Bus Emulator is running.");
Console.WriteLine($"Listening:         {amqpAddress}");
Console.WriteLine($"Management API:    {managementAddress}");
Console.WriteLine($"SbBrowser:         {browserAddress}");
Console.WriteLine($"Connection String: {connectionString}");
Console.WriteLine("Press Ctrl+C to stop.");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (s, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    await Task.Delay(-1, cts.Token);
}
catch (TaskCanceledException) { }

// A failed web UI shutdown (open browser tabs, say) must not stop the broker from closing its logs.
if (browser != null)
{
    try { await browser.DisposeAsync(); }
    catch (Exception ex) { logger.LogWarning(ex, "SbBrowser did not shut down cleanly."); }
}
if (managementApi != null) await managementApi.DisposeAsync();
broker.Stop();
entityManager.Dispose();
