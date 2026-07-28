using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Sasl;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using AzsbEmu.Server.Protocol;
using Amqp.Transactions;

namespace AzsbEmu.Server.Core;

public class Broker
{
    private readonly ContainerHost _host;
    private readonly ILogger<Broker> _logger;
    private readonly string _address;

    public Broker(string address, EntityManager entityManager, ILoggerFactory loggerFactory)
    {
        _address = address;
        _logger = loggerFactory.CreateLogger<Broker>();

        var uri = new Address(address);
        _host = new ContainerHost(uri);
        _host.AddressResolver = ResolveAddress;

        // The Azure SDK negotiates SASL using the proprietary MSSBCBS mechanism before the
        // AMQP connection is opened. Accept it unconditionally; real auth goes through CBS.
        foreach (var listener in _host.Listeners)
            listener.SASL.EnableMechanism("MSSBCBS", new MssbcbsSaslProfile());

        // CBS uses request-response; must be registered before Open()
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor(loggerFactory.CreateLogger<CbsRequestProcessor>()));

        // Single catch-all link processor handles queues, topics, and management nodes
        _host.RegisterLinkProcessor(new EntityLinkProcessor(entityManager, loggerFactory.CreateLogger<EntityLinkProcessor>()));
    }

    public void Start()
    {
        _host.Open();
        _logger.LogInformation("Broker started at {Address}", _address);
    }

    public void Stop()
    {
        _host.Close();
        _logger.LogInformation("Broker stopped.");
    }

    private static string? ResolveAddress(ContainerHost _, Attach attach)
    {
        string? address = null;

        if (attach.Role)
        {
            if (attach.Source is Source source)
            {
                address = source.Address;
            }
        }
        else if (attach.Target is Target target)
        {
            address = target.Address;
        }
        else if (attach.Target is Coordinator)
        {
            address = "$coordinator";
        }

        return NormalizeAddress(address);
    }

    private static string? NormalizeAddress(string? address)
    {
        return string.IsNullOrWhiteSpace(address)
            ? address
            : address.TrimStart('/').ToLowerInvariant();
    }
}
