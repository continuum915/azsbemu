using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using AzsbEmu.Server.Core;
using AzsbEmu.Server.Config;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables("AZSB_EMU_")
    .AddCommandLine(args, new Dictionary<string, string>
    {
        ["--host"]     = "Host",
        ["--port"]     = "Port",
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
            MaxDeliveryCount = queueCfg.MaxDeliveryCount
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
            RequiresSession = subCfg.RequiresSession
        });
    }
}

string host = config.Host;
int port = config.Port;
string amqpAddress = $"amqp://{host}:{port}";
string connectionString = $"Endpoint=sb://{host}/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true";

var broker = new Broker(amqpAddress, entityManager, loggerFactory);
broker.Start();

Console.WriteLine("Azure Service Bus Emulator is running.");
Console.WriteLine($"Listening:         {amqpAddress}");
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

broker.Stop();
entityManager.Dispose();
