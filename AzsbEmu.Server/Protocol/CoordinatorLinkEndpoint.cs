using Amqp;
using Amqp.Listener;
using Amqp.Transactions;
using Microsoft.Extensions.Logging;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Server.Protocol;

internal sealed class CoordinatorLinkEndpoint : LinkEndpoint
{
    private readonly EntityManager _entityManager;
    private readonly ILogger _logger;

    public CoordinatorLinkEndpoint(EntityManager entityManager, ILogger logger)
    {
        _entityManager = entityManager;
        _logger = logger;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        var message = messageContext.Message;

        if (message.Body is Declare)
        {
            try
            {
                byte[] txnId = _entityManager.DeclareTransaction();
                _logger.LogDebug("Declared transaction: {TxnIdHex}", Convert.ToHexString(txnId));
                var outcome = new Declared { TxnId = txnId };
                var disposeMethod = typeof(Context).GetMethod("Dispose", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                disposeMethod?.Invoke(messageContext, new object[] { outcome });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error declaring transaction.");
                messageContext.Complete(new Amqp.Framing.Error(ErrorCode.InternalError) { Description = ex.Message });
            }
        }
        else if (message.Body is Discharge discharge)
        {
            try
            {
                byte[] txnId = discharge.TxnId;
                bool fail = discharge.Fail;

                if (fail)
                {
                    _entityManager.RollbackTransaction(txnId);
                }
                else
                {
                    _entityManager.CommitTransaction(txnId);
                }

                messageContext.Complete();
            }
            catch (AmqpException ex)
            {
                _logger.LogError(ex, "AMQP error discharging transaction.");
                messageContext.Complete(ex.Error);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error discharging transaction.");
                messageContext.Complete(new Amqp.Framing.Error(ErrorCode.InternalError) { Description = ex.Message });
            }
        }
        else
        {
            _logger.LogWarning("Unknown coordinator request body type: {Type}", message.Body?.GetType().Name);
            messageContext.Complete(new Amqp.Framing.Error(ErrorCode.NotAllowed) { Description = "Invalid coordinator operation." });
        }
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // No-op for coordinator
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        // No-op for coordinator
    }
}
