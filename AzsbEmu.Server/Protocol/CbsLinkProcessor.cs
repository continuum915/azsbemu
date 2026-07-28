using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Protocol;

// CBS uses the AMQP request-response pattern, not a streaming link.
public class CbsRequestProcessor : IRequestProcessor
{
    private readonly ILogger<CbsRequestProcessor> _logger;

    public int Credit => 300;

    public CbsRequestProcessor(ILogger<CbsRequestProcessor> logger)
    {
        _logger = logger;
    }

    public void Process(RequestContext context)
    {
        var operation = context.Message.ApplicationProperties?["operation"] as string;
        _logger.LogDebug("CBS put-token received (operation={Operation})", operation);

        var response = new Message { ApplicationProperties = new ApplicationProperties() };
        response.ApplicationProperties["status-code"] = 200;
        response.ApplicationProperties["status-description"] = "OK";

        if (context.Message.Properties?.MessageId != null)
        {
            response.Properties = new Properties { CorrelationId = context.Message.Properties.MessageId };
        }

        context.Complete(response);
    }
}
