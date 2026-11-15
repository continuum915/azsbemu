using Amqp;
using Amqp.Listener;
using Amqp.Framing;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using AzsbEmu.Server.Core;
using System.Collections.Concurrent;
using System.Reflection;
using Amqp.Transactions;

namespace AzsbEmu.Server.Protocol;

public class EntityLinkProcessor : ILinkProcessor
{
    private readonly EntityManager _entityManager;
    private readonly ILogger<EntityLinkProcessor> _logger;
    private readonly ReplyToLinkRegistry _replyToLinks = new();

    public EntityLinkProcessor(EntityManager entityManager, ILogger<EntityLinkProcessor> logger)
    {
        _entityManager = entityManager;
        _logger = logger;
    }

    public void Process(AttachContext context)
    {
        string? address = null;
        if (context.Attach.Role) // Client is Receiver
        {
            if (context.Attach.Source is Source source) address = source.Address;
        }
        else // Client is Sender
        {
            if (context.Attach.Target is Target target) address = target.Address;
            else if (context.Attach.Target is Coordinator) address = "$coordinator";
        }

        _logger.LogDebug("Processing attach request for address: {Address}, isReceiver: {IsReceiver}", address, context.Attach.Role);

        if (string.IsNullOrEmpty(address))
        {
            context.Complete(new Error(ErrorCode.InvalidField) { Description = "Missing link address" });
            return;
        }

        address = address.TrimStart('/');
        context.Attach.MaxMessageSize = 1024 * 1024;

        if (address == "$coordinator")
        {
            context.Complete(new CoordinatorLinkEndpoint(_entityManager, _logger), 300);
            return;
        }

        if (context.Attach.Role
            && (address == "$management" || address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
            && context.Attach.Target is Target managementResponseTarget
            && !string.IsNullOrWhiteSpace(managementResponseTarget.Address))
        {
            _logger.LogDebug("Registering standard management response link for address {Address}", managementResponseTarget.Address);
            context.Complete(new ReplyToLinkEndpoint(managementResponseTarget.Address, _replyToLinks, _logger), 300);
            return;
        }

        if (address == "$management" || address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
        {
            string entityPath = address == "$management" ? "" : address[..^"/$management".Length];
            context.Complete(new ManagementLinkEndpoint(entityPath, _entityManager, _replyToLinks, _logger), 300);
            return;
        }

        const string dlqSuffix = "/$deadletterqueue";
        bool isDlq = false;
        string baseAddress = address;
        if (address.EndsWith(dlqSuffix, StringComparison.OrdinalIgnoreCase))
        {
            isDlq = true;
            baseAddress = address[..^dlqSuffix.Length];
        }

        bool isReceiver = context.Attach.Role;
        var queue = _entityManager.GetQueue(baseAddress);
        if (queue != null)
        {
            AttachEntityLink(context, queue, baseAddress, isReceiver, isDlq);
            return;
        }

        int subIndex = baseAddress.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex > 0)
        {
            string topicName = baseAddress[..subIndex];
            string subName = baseAddress[(subIndex + "/subscriptions/".Length)..];
            var topic = _entityManager.GetTopic(topicName);
            if (topic != null)
            {
                var sub = topic.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                if (sub != null)
                {
                    AttachEntityLink(context, sub, baseAddress, isReceiver, isDlq);
                    return;
                }
            }
        }

        var topicForSender = _entityManager.GetTopic(baseAddress);
        if (topicForSender != null)
        {
            if (!context.Attach.Role) // Sender
            {
                context.Complete(new TopicLinkEndpoint(topicForSender, _entityManager, _logger), 300);
                return;
            }
            else // Receiver
            {
                _logger.LogWarning("Cannot receive directly from topic '{Address}'", address);
                context.Complete(new Error(ErrorCode.NotAllowed) { Description = "Cannot receive directly from a topic; must receive from a subscription." });
                return;
            }
        }

        if (context.Attach.Role && IsReplyToLinkCandidate(context.Attach.LinkName, address))
        {
            _logger.LogDebug("Treating unknown receiver address '{Address}' as a reply-to link", address);
            context.Complete(new ReplyToLinkEndpoint(address, _replyToLinks, _logger), 300);
            return;
        }

        _logger.LogWarning("Entity '{Address}' not found", address);
        context.Complete(new Error(ErrorCode.NotFound) { Description = $"Entity '{address}' not found" });
    }

    private void AttachEntityLink(AttachContext context, IMessageEntity entity, string entityPath, bool isReceiver, bool isDlq)
    {
        if (!isReceiver || isDlq || !entity.RequiresSession)
        {
            CompleteEntityLink(context, entity, entityPath, isReceiver, isDlq, sessionLock: null);
            return;
        }

        string? sessionId = ExtractSessionId(context.Attach);
        if (string.IsNullOrEmpty(sessionId))
        {
            _ = AttachToNextAvailableSessionAsync(context, entity, entityPath);
            return;
        }

        var sessionLock = _entityManager.TryLockSession(entityPath, sessionId, context.Attach.LinkName, entity.LockDuration, context.Link);
        if (sessionLock == null)
        {
            _logger.LogWarning("Rejecting receiver attach to '{Address}': Session '{SessionId}' is already locked", entityPath, sessionId);
            // The condition Service Bus uses; the SDK maps it to ServiceBusFailureReason.SessionCannotBeLocked.
            context.Complete(new Error(SessionCannotBeLockedError) { Description = $"Session '{sessionId}' is already locked by another receiver." });
            return;
        }

        CompleteEntityLink(context, entity, entityPath, isReceiver: true, isDlq: false, sessionLock);
    }

    /// <summary>
    /// A session receiver that names no session gets the next session with messages that no other receiver holds.
    /// Like Service Bus, the attach waits for one until the client's timeout and then fails with a timeout error.
    /// </summary>
    private async Task AttachToNextAvailableSessionAsync(AttachContext context, IMessageEntity entity, string entityPath)
    {
        var link = context.Link;
        var deadline = DateTime.UtcNow + AttachTimeout(context.Attach);
        HeldAttach.Hold(link);
        try
        {
            while (!link.IsClosed)
            {
                var lockState = _entityManager.TryLockNextAvailableSession(entityPath, context.Attach.LinkName, entity.LockDuration, link);
                if (lockState != null)
                {
                    if (HeldAttach.TryAnswer(link, () => CompleteEntityLink(context, entity, entityPath, isReceiver: true, isDlq: false, lockState)))
                    {
                        _logger.LogDebug("Receiver '{LinkName}' on '{Address}' accepted session '{SessionId}'", context.Attach.LinkName, entityPath, lockState.SessionId);
                    }
                    else
                    {
                        _entityManager.ReleaseSessionLock(entityPath, lockState.SessionId, context.Attach.LinkName);
                    }
                    return;
                }

                if (DateTime.UtcNow >= deadline)
                {
                    HeldAttach.TryAnswer(link, () => context.Complete(new Error(TimeoutSymbol)
                    {
                        Description = $"No session on '{entityPath}' became available before the timeout."
                    }));
                    return;
                }
                await Task.Delay(NextSessionPollInterval);
            }
            _logger.LogDebug("Receiver '{LinkName}' on '{Address}' detached while waiting for a session", context.Attach.LinkName, entityPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Accepting the next available session on '{Address}' failed", entityPath);
            HeldAttach.TryAnswer(link, () => context.Complete(new Error(ErrorCode.InternalError) { Description = ex.Message }));
        }
    }

    private static readonly Symbol TimeoutSymbol = new("com.microsoft:timeout");
    private static readonly Symbol SessionCannotBeLockedError = new("com.microsoft:session-cannot-be-locked");
    private static readonly Symbol SessionIdProperty = new("com.microsoft:session-id");
    private static readonly Symbol SessionFilter = new("com.microsoft:session-filter");
    private static readonly Symbol LockedUntilUtcProperty = new("com.microsoft:locked-until-utc");
    private static readonly TimeSpan NextSessionPollInterval = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan DefaultAttachTimeout = TimeSpan.FromSeconds(60);

    /// <summary>The client's operation timeout, sent as the <c>com.microsoft:timeout</c> link property in milliseconds.</summary>
    private static TimeSpan AttachTimeout(Attach attach)
    {
        return attach.Properties?[TimeoutSymbol] switch
        {
            uint ms => TimeSpan.FromMilliseconds(ms),
            int ms when ms > 0 => TimeSpan.FromMilliseconds(ms),
            long ms when ms > 0 => TimeSpan.FromMilliseconds(ms),
            _ => DefaultAttachTimeout,
        };
    }

    private void CompleteEntityLink(AttachContext context, IMessageEntity entity, string entityPath, bool isReceiver, bool isDlq, SessionLockState? sessionLock)
    {
        if (sessionLock != null)
        {
            // The client learns which session it holds, and until when, from the attach the broker sends back.
            context.Attach.Properties ??= new Fields();
            context.Attach.Properties[SessionIdProperty] = sessionLock.SessionId;
            context.Attach.Properties[LockedUntilUtcProperty] = sessionLock.LockedUntil.Ticks;
            if (context.Attach.Source is Source source)
            {
                source.FilterSet ??= new Map();
                source.FilterSet[SessionFilter] = sessionLock.SessionId;
            }
        }

        bool isReceiveAndDelete = isReceiver && context.Attach.RcvSettleMode == ReceiverSettleMode.First;
        // An attached receiver keeps the entity from being auto-deleted as idle.
        if (isReceiver) entity.Activity.ReceiverAttached(context.Link);
        context.Complete(new EntityLinkEndpoint(entity, entityPath, _entityManager, isReceiveAndDelete, isDlq, sessionLock?.SessionId, _logger), 300);
    }

    private static bool IsReplyToLinkCandidate(string? linkName, string address)
    {
        return (linkName?.Contains("mgmt", StringComparison.OrdinalIgnoreCase) ?? false)
            || address.Contains("temp", StringComparison.OrdinalIgnoreCase)
            || address.Contains("reply", StringComparison.OrdinalIgnoreCase);
    }

    private string? ExtractSessionId(Attach attach)
    {
        string? sessionId = null;
        if (attach.Source is Source source && source.FilterSet != null)
        {
            foreach (var key in source.FilterSet.Keys)
            {
                if (key.ToString() == "com.microsoft:session-filter")
                {
                    var val = source.FilterSet[key];
                    if (val is DescribedValue describedValue)
                    {
                        sessionId = describedValue.Value?.ToString();
                    }
                    else
                    {
                        sessionId = val?.ToString();
                    }
                    break;
                }
            }
        }

        if (string.IsNullOrEmpty(sessionId) && attach.Properties != null)
        {
            foreach (var key in attach.Properties.Keys)
            {
                if (key.ToString() == "com.microsoft:session-id")
                {
                    sessionId = attach.Properties[key]?.ToString();
                    break;
                }
            }
        }
        return sessionId;
    }
}

public class TopicLinkEndpoint : LinkEndpoint
{
    private readonly TopicInfo _topic;
    private readonly EntityManager _entityManager;
    private readonly ILogger _logger;

    public TopicLinkEndpoint(TopicInfo topic, EntityManager entityManager, ILogger logger)
    {
        _topic = topic;
        _entityManager = entityManager;
        _logger = logger;
    }

    private static readonly Symbol ScheduledEnqueueTimeAnnotation = new("x-opt-scheduled-enqueue-time");

    private bool TryGetScheduledEnqueueTime(Message message, out DateTime scheduledTimeUtc)
    {
        scheduledTimeUtc = default;
        if (message.MessageAnnotations?.Map == null) return false;

        if (message.MessageAnnotations.Map.TryGetValue(ScheduledEnqueueTimeAnnotation, out var val))
        {
            if (val is DateTime dt)
            {
                scheduledTimeUtc = dt.ToUniversalTime();
                return scheduledTimeUtc > DateTime.UtcNow;
            }
            else if (val is long ticks)
            {
                scheduledTimeUtc = new DateTime(ticks, DateTimeKind.Utc);
                return scheduledTimeUtc > DateTime.UtcNow;
            }
        }
        return false;
    }

    private static byte[] SerializeMessage(Message message)
    {
        ByteBuffer buffer = message.Encode();
        byte[] payload = new byte[buffer.Length];
        Buffer.BlockCopy(buffer.Buffer, buffer.Offset, payload, 0, buffer.Length);
        return payload;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        var outerMessage = messageContext.Message;

        var deliveryState = outerMessage.GetDelivery().State;
        if (deliveryState is TransactionalState txnState)
        {
            byte[] txnId = txnState.TxnId;
            try
            {
                if (outerMessage.BodySection is DataList txnDataList)
                {
                    for (int i = 0; i < txnDataList.Count; i++)
                    {
                        var innerBytes = txnDataList[i].Binary;
                        if (innerBytes == null || innerBytes.Length == 0) continue;
                        var innerBuf = new ByteBuffer(innerBytes, 0, innerBytes.Length, innerBytes.Length);
                        var innerMsg = Message.Decode(innerBuf);
                        _entityManager.BufferSend(txnId, _topic.Name, innerMsg, false);
                    }
                }
                else
                {
                    _entityManager.BufferSend(txnId, _topic.Name, outerMessage, false);
                }
                messageContext.Complete();
            }
            catch (AmqpException ex)
            {
                _logger.LogError(ex, "Transaction buffer topic send failed.");
                messageContext.Complete(ex.Error);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in transaction buffer topic send.");
                messageContext.Complete(new Error(ErrorCode.InternalError) { Description = ex.Message });
            }
            return;
        }
        _logger.LogDebug("Received message on topic {TopicName}", _topic.Name);

        if (outerMessage.BodySection is DataList dataList)
        {
            for (int i = 0; i < dataList.Count; i++)
            {
                var innerBytes = dataList[i].Binary;
                if (innerBytes == null || innerBytes.Length == 0) continue;
                try
                {
                    var innerBuf = new ByteBuffer(innerBytes, 0, innerBytes.Length, innerBytes.Length);
                    var innerMsg = Message.Decode(innerBuf);
                    if (TryGetScheduledEnqueueTime(innerMsg, out var scheduledTime))
                    {
                        var payload = new byte[innerBytes.Length];
                        Buffer.BlockCopy(innerBytes, 0, payload, 0, innerBytes.Length);
                        _entityManager.ScheduleMessages(_topic.Name, new List<(byte[] payload, DateTime scheduledEnqueueTimeUtc)> { (payload, scheduledTime) });
                    }
                    else
                    {
                        _entityManager.Route(_topic, innerMsg);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to decode batch message {Index} for topic {TopicName}", i, _topic.Name);
                }
            }
        }
        else
        {
            if (TryGetScheduledEnqueueTime(outerMessage, out var scheduledTime))
            {
                byte[] payload = SerializeMessage(outerMessage);
                _entityManager.ScheduleMessages(_topic.Name, new List<(byte[] payload, DateTime scheduledEnqueueTimeUtc)> { (payload, scheduledTime) });
            }
            else
            {
                _entityManager.Route(_topic, outerMessage);
            }
        }

        messageContext.Complete();
    }

    public override void OnFlow(FlowContext flowContext)
    {
        var link = flowContext.Link;
        if (link.IsDraining)
        {
            link.CompleteDrain();
        }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        // No-op for sender endpoint
    }
}

public class EntityLinkEndpoint : LinkEndpoint
{
    private static readonly Symbol SequenceNumberAnnotation = new("x-opt-sequence-number");
    private static readonly Symbol EnqueuedTimeUtcAnnotation = new("x-opt-enqueued-time-utc");
    private static readonly Symbol EnqueuedTimeAnnotation = new("x-opt-enqueued-time");
    private static readonly Symbol LockTokenAnnotation = new("x-opt-lock-token");
    private static readonly Symbol LockedUntilUtcAnnotation = new("x-opt-locked-until-utc");
    private static readonly Symbol LockedUntilAnnotation = new("x-opt-locked-until");
    private static readonly PropertyInfo MessageDeliveryProperty =
        typeof(Message).GetProperty("Delivery", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Message.Delivery property not found.");
    private static readonly Type DeliveryType =
        typeof(Message).Assembly.GetType("Amqp.Delivery")
        ?? throw new InvalidOperationException("Amqp.Delivery type not found.");
    private static readonly PropertyInfo DeliveryTagProperty =
        DeliveryType.GetProperty("Tag", BindingFlags.Instance | BindingFlags.Public)
        ?? throw new InvalidOperationException("Amqp.Delivery.Tag property not found.");
    private static readonly PropertyInfo DeliveryMessageProperty =
        DeliveryType.GetProperty("Message", BindingFlags.Instance | BindingFlags.Public)
        ?? throw new InvalidOperationException("Amqp.Delivery.Message property not found.");
    private static readonly FieldInfo DeliveryHandleField =
        DeliveryType.GetField("Handle", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Delivery.Handle field not found.");
    private static readonly FieldInfo DeliveryLinkField =
        DeliveryType.GetField("Link", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Delivery.Link field not found.");
    private static readonly FieldInfo DeliveryBufferField =
        DeliveryType.GetField("Buffer", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Delivery.Buffer field not found.");
    private static readonly MethodInfo SessionSendDeliveryMethod =
        typeof(Session).GetMethod("SendDelivery", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Session.SendDelivery method not found.");

    private readonly IMessageEntity _entity;
    private readonly string _entityPath;
    private readonly EntityManager _entityManager;
    private readonly bool _receiveAndDelete;
    private readonly bool _isDlq;
    private readonly string? _sessionId;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private MessageStore? Store => _isDlq ? _entity.DlqMessageStore : _entity.MessageStore;
    private Dispatcher? Dispatcher => _isDlq ? _entity.DlqDispatcher : _entity.Dispatcher;

    public EntityLinkEndpoint(IMessageEntity entity, string entityPath, EntityManager entityManager, bool receiveAndDelete, bool isDlq, string? sessionId, ILogger logger)
    {
        _entity = entity;
        _entityPath = entityPath;
        _entityManager = entityManager;
        _receiveAndDelete = receiveAndDelete;
        _isDlq = isDlq;
        _sessionId = sessionId;
        _logger = logger;
    }

    private static readonly Symbol ScheduledEnqueueTimeAnnotation = new("x-opt-scheduled-enqueue-time");

    private bool TryGetScheduledEnqueueTime(Message message, out DateTime scheduledTimeUtc)
    {
        scheduledTimeUtc = default;
        if (message.MessageAnnotations?.Map == null) return false;

        if (message.MessageAnnotations.Map.TryGetValue(ScheduledEnqueueTimeAnnotation, out var val))
        {
            if (val is DateTime dt)
            {
                scheduledTimeUtc = dt.ToUniversalTime();
                return scheduledTimeUtc > DateTime.UtcNow;
            }
            else if (val is long ticks)
            {
                scheduledTimeUtc = new DateTime(ticks, DateTimeKind.Utc);
                return scheduledTimeUtc > DateTime.UtcNow;
            }
        }
        return false;
    }

    private static byte[] SerializeMessage(Message message)
    {
        ByteBuffer buffer = message.Encode();
        byte[] payload = new byte[buffer.Length];
        Buffer.BlockCopy(buffer.Buffer, buffer.Offset, payload, 0, buffer.Length);
        return payload;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        var store = Store;
        if (store == null) return;

        var outerMessage = messageContext.Message;

        var deliveryState = outerMessage.GetDelivery().State;
        if (deliveryState is TransactionalState txnState)
        {
            byte[] txnId = txnState.TxnId;
            try
            {
                if (outerMessage.BodySection is DataList txnDataList)
                {
                    for (int i = 0; i < txnDataList.Count; i++)
                    {
                        var innerBytes = txnDataList[i].Binary;
                        if (innerBytes == null || innerBytes.Length == 0) continue;
                        var innerBuf = new ByteBuffer(innerBytes, 0, innerBytes.Length, innerBytes.Length);
                        var innerMsg = Message.Decode(innerBuf);
                        _entityManager.BufferSend(txnId, _entity.Name, innerMsg, _isDlq);
                    }
                }
                else
                {
                    _entityManager.BufferSend(txnId, _entity.Name, outerMessage, _isDlq);
                }
                messageContext.Complete();
            }
            catch (AmqpException ex)
            {
                _logger.LogError(ex, "Transaction buffer send failed.");
                messageContext.Complete(ex.Error);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in transaction buffer send.");
                messageContext.Complete(new Error(ErrorCode.InternalError) { Description = ex.Message });
            }
            return;
        }

        // Send validation: check RequiresSession
        if (!_isDlq && _entity.RequiresSession)
        {
            if (outerMessage.BodySection is DataList dataList)
            {
                for (int i = 0; i < dataList.Count; i++)
                {
                    var innerBytes = dataList[i].Binary;
                    if (innerBytes == null || innerBytes.Length == 0) continue;
                    try
                    {
                        var innerBuf = new ByteBuffer(innerBytes, 0, innerBytes.Length, innerBytes.Length);
                        var innerMsg = Message.Decode(innerBuf);
                        if (string.IsNullOrEmpty(innerMsg.Properties?.GroupId))
                        {
                            _logger.LogWarning("Message rejected on batch send to session-enabled entity {EntityName}: Missing SessionId", _entity.Name);
                            messageContext.Complete(new Error(ErrorCode.PreconditionFailed)
                            {
                                Description = "The SessionId was not set on a message sent to a session-enabled entity."
                            });
                            return;
                        }
                    }
                    catch
                    {
                        // Decoding errors will be handled in the main processing loop below
                    }
                }
            }
            else
            {
                if (string.IsNullOrEmpty(outerMessage.Properties?.GroupId))
                {
                    _logger.LogWarning("Message rejected on send to session-enabled entity {EntityName}: Missing SessionId", _entity.Name);
                    messageContext.Complete(new Error(ErrorCode.PreconditionFailed)
                    {
                        Description = "The SessionId was not set on a message sent to a session-enabled entity."
                    });
                    return;
                }
            }
        }

        if (outerMessage.BodySection is DataList dataListVal)
        {
            for (int i = 0; i < dataListVal.Count; i++)
            {
                var innerBytes = dataListVal[i].Binary;
                if (innerBytes == null || innerBytes.Length == 0) continue;
                try
                {
                    var innerBuf = new ByteBuffer(innerBytes, 0, innerBytes.Length, innerBytes.Length);
                    var innerMsg = Message.Decode(innerBuf);
                    if (TryGetScheduledEnqueueTime(innerMsg, out var scheduledTime))
                    {
                        var payload = new byte[innerBytes.Length];
                        Buffer.BlockCopy(innerBytes, 0, payload, 0, innerBytes.Length);
                        _entityManager.ScheduleMessages(_entity.Name, new List<(byte[] payload, DateTime scheduledEnqueueTimeUtc)> { (payload, scheduledTime) });
                    }
                    else
                    {
                        _entityManager.Route(_entity, innerMsg, new RouteOptions(ToDeadLetter: _isDlq));
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to decode batch message {Index} for entity {EntityName}", i, _entity.Name);
                }
            }
        }
        else
        {
            if (TryGetScheduledEnqueueTime(outerMessage, out var scheduledTime))
            {
                byte[] payload = SerializeMessage(outerMessage);
                _entityManager.ScheduleMessages(_entity.Name, new List<(byte[] payload, DateTime scheduledEnqueueTimeUtc)> { (payload, scheduledTime) });
            }
            else
            {
                _entityManager.Route(_entity, outerMessage, new RouteOptions(ToDeadLetter: _isDlq));
            }
        }

        messageContext.Complete();
    }

    public override void OnFlow(FlowContext flowContext)
    {
        var link = flowContext.Link;
        int credits = flowContext.Messages;
        _logger.LogDebug("OnFlow: credits={Credits} for entity {EntityName} (isDlq={IsDlq})", credits, _entity.Name, _isDlq);

        if (!_isDlq && !string.IsNullOrEmpty(_sessionId))
        {
            _entityManager.TryLockSession(_entityPath, _sessionId, link.Name, _entity.LockDuration, link);
        }

        if (link.IsDraining)
        {
            _logger.LogDebug("Completing drain for entity {EntityName}", _entity.Name);
            link.CompleteDrain();
            return;
        }

        if (credits <= 0) return;

        _ = DeliverMessagesAsync(link, credits);
    }

    private async Task DeliverMessagesAsync(ListenerLink link, int count)
    {
        var dispatcher = Dispatcher;
        if (dispatcher == null) return;

        try
        {
            // Credit is used only by messages actually sent: one dead-lettered or discarded on the way (max delivery
            // count, expiry) must not use it up, or the receiver waits for a message that is already available.
            for (int sent = 0; sent < count;)
            {
                if (link.IsClosed || _cts.IsCancellationRequested) break;

                var lockToken = Guid.NewGuid();
                (byte[] data, long logAddress)? result;
                long dispatchStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                try { result = await dispatcher.TryDeliverNextAsync(lockToken, _sessionId, _entity.SessionMessageAddresses, _cts.Token); BrokerProfiler.Dispatch(dispatchStarted); }
                catch (OperationCanceledException) { break; }

                if (result == null)
                {
                    if (link.IsDraining)
                        link.CompleteDrain();

                    break;
                }

                var (data, logAddressValue) = result.Value;
                long decodeStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                var (header, decoded) = MessageSerializer.Decode(data);
                BrokerProfiler.Decode(decodeStarted);

                var lockedUntil = DateTimeOffset.UtcNow.Add(_entity.LockDuration);

                // Fully reconstruct message for delivery
                var message = new Message
                {
                    Header = decoded.Header,
                    Properties = decoded.Properties,
                    ApplicationProperties = decoded.ApplicationProperties,
                    BodySection = decoded.BodySection,
                    MessageAnnotations = new MessageAnnotations()
                };

                // Copy original annotations
                if (decoded.MessageAnnotations != null)
                {
                    foreach (var key in (System.Collections.IEnumerable)decoded.MessageAnnotations.Map.Keys)
                        message.MessageAnnotations[key] = decoded.MessageAnnotations[key];
                }

                message.MessageAnnotations[SequenceNumberAnnotation] = header.SequenceNumber;
                message.Header ??= new Header();
                dispatcher.ApplyModifiedProperties(logAddressValue, message);
                
                int overlayDeliveryCount = dispatcher.GetDeliveryCount(logAddressValue);
                uint currentDeliveryCount = (uint)(header.DeliveryCount + overlayDeliveryCount);

                if (!_isDlq && currentDeliveryCount >= (uint)_entity.MaxDeliveryCount)
                {
                    _logger.LogInformation("Message seq={SeqNum} reached MaxDeliveryCount ({MaxDC}) on entity {EntityName}, dead-lettering",
                        header.SequenceNumber, _entity.MaxDeliveryCount, _entity.Name);

                    await _entityManager.DeadLetterMessageAsync(_entity, logAddressValue, "MaxDeliveryCountExceeded", $"Delivery count {currentDeliveryCount} exceeds limit {_entity.MaxDeliveryCount}");
                    continue;
                }

                // Delivery-time TTL check
                if (!_isDlq)
                {
                    long effectiveTtlTicks = header.TimeToLiveTicks;
                    if (effectiveTtlTicks == 0 && _entity.DefaultMessageTimeToLive != TimeSpan.MaxValue)
                        effectiveTtlTicks = _entity.DefaultMessageTimeToLive.Ticks;

                    if (effectiveTtlTicks > 0 && DateTime.UtcNow.Ticks > header.EnqueueTimeUtcTicks + effectiveTtlTicks)
                    {
                        await _entityManager.ExpireMessageAsync(_entity, logAddressValue, header.SequenceNumber);
                        continue;
                    }
                }

                message.Header.DeliveryCount = currentDeliveryCount;
                
                var enqueuedTimeUtc = new DateTime(header.EnqueueTimeUtcTicks, DateTimeKind.Utc);
                message.MessageAnnotations[EnqueuedTimeUtcAnnotation] = enqueuedTimeUtc;
                message.MessageAnnotations[EnqueuedTimeAnnotation] = enqueuedTimeUtc;

                if (!_receiveAndDelete)
                {
                    message.MessageAnnotations[LockTokenAnnotation] = lockToken;
                    message.MessageAnnotations[LockedUntilUtcAnnotation] = lockedUntil.UtcDateTime;
                    message.MessageAnnotations[LockedUntilAnnotation] = lockedUntil.UtcDateTime;

                    _entityManager.RegisterLock(lockToken, header.SequenceNumber, logAddressValue, lockedUntil, _entityPath, _isDlq, link.Name);
                }

                long sendLockStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                await _sendLock.WaitAsync(_cts.Token);
                BrokerProfiler.SendLock(sendLockStarted);
                long sendStarted = BrokerProfiler.Enabled ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                try
                {
                    if (_receiveAndDelete)
                    {
                        // In ReceiveAndDelete mode the message is settled when it is sent, so it leaves the entity
                        // (completed, and no longer counted) before the client can see it: a client that reads the
                        // count right after receiving must not find the message still counted.
                        dispatcher.Complete(logAddressValue, lockToken);
                        if (_isDlq) _entity.DecrementDeadLetterMessageCount();
                        else _entity.DecrementMessageCount();
                        link.SendMessage(message);
                    }
                    else
                    {
                        SendMessageWithDeliveryTag(link, message, lockToken.ToByteArray());
                    }
                    sent++;
                    _logger.LogDebug("Sent message seq={SeqNum} (ReceiveAndDelete={Rad}, isDlq={IsDlq})", header.SequenceNumber, _receiveAndDelete, _isDlq);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to deliver message seq={SeqNum}, abandoning", header.SequenceNumber);
                    if (!_receiveAndDelete)
                    {
                        _entityManager.TryRemoveLock(lockToken, out _);
                        dispatcher.Abandon(logAddressValue, lockToken);
                    }
                    break;
                }
                finally { BrokerProfiler.Send(sendStarted); _sendLock.Release(); }
            }
        }
        catch (Exception ex) { _logger.LogError(ex, "Unhandled error in delivery loop for entity {EntityName}", _entity.Name); }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        if (_receiveAndDelete)
        {
            dispositionContext.Complete();
            return;
        }

        var deliveryTag = dispositionContext.Message.DeliveryTag;
        if (deliveryTag == null || deliveryTag.Length != 16)
        {
            dispositionContext.Complete();
            return;
        }

        var lockToken = new Guid(deliveryTag);

        var state = dispositionContext.Message.GetDelivery().State;
        if (state is TransactionalState txnState)
        {
            byte[] txnId = txnState.TxnId;
            if (!_entityManager.TryGetLock(lockToken, out var txnLockState))
            {
                _logger.LogWarning("Lock token {LockToken} not found for transactional settlement", lockToken);
                dispositionContext.Complete();
                return;
            }
            try
            {
                _entityManager.BufferSettlement(txnId, lockToken, txnState.Outcome);
                dispositionContext.Complete();
            }
            catch (AmqpException ex)
            {
                _logger.LogError(ex, "Transaction buffer settlement failed.");
                dispositionContext.Complete();
            }
            return;
        }

        if (!_entityManager.TryRemoveLock(lockToken, out var lockState)) { dispositionContext.Complete(); return; }

        var dispatcher = Dispatcher;
        if (dispatcher == null) { dispositionContext.Complete(); return; }
        if (state is Accepted)
        {
            if (dispatcher.Complete(lockState.LogAddress, lockToken))
            {
                if (_isDlq) _entity.DecrementDeadLetterMessageCount();
                else _entity.DecrementMessageCount();
                _logger.LogDebug("Settled message seq={SeqNum} on entity {EntityName} (isDlq={IsDlq})", lockState.SequenceNumber, _entity.Name, _isDlq);
            }
            else
            {
                _logger.LogWarning("Failed to complete message seq={SeqNum} on entity {EntityName}. Lock may have expired.", lockState.SequenceNumber, _entity.Name);
            }
        }
        else if (state is Released or Modified)
        {
            bool undeliverableHere = false;
            if (state is Modified modified) undeliverableHere = modified.UndeliverableHere;
            var propertiesToModify = PropertiesToModify.FromModified(state as Modified);

            if (undeliverableHere)
            {
                if (dispatcher.Defer(lockState.LogAddress, lockToken, lockState.SequenceNumber, propertiesToModify))
                {
                    _logger.LogDebug("Deferred message seq={SeqNum} on entity {EntityName}", lockState.SequenceNumber, _entity.Name);
                }
                else
                {
                    _logger.LogWarning("Failed to defer message seq={SeqNum} on entity {EntityName}. Lock may have expired.", lockState.SequenceNumber, _entity.Name);
                }
            }
            else
            {
                dispatcher.IncrementDeliveryCount(lockState.LogAddress);
                if (dispatcher.Abandon(lockState.LogAddress, lockToken, propertiesToModify))
                {
                    _logger.LogDebug("Abandoned message seq={SeqNum} on entity {EntityName}", lockState.SequenceNumber, _entity.Name);
                }
                else
                {
                    _logger.LogWarning("Failed to abandon message seq={SeqNum} on entity {EntityName}. Lock may have expired.", lockState.SequenceNumber, _entity.Name);
                }
            }
        }
        else if (state is Rejected rejected)
        {
            var details = DeadLetterDetails.FromRejected(rejected);

            if (!_isDlq)
            {
                // We must use synchronous wait here because OnDisposition is synchronous
                _entityManager.DeadLetterMessageAsync(_entity, lockState.LogAddress, details).GetAwaiter().GetResult();
            }
            else
            {
                _logger.LogWarning("Cannot dead-letter a message already in the DLQ. Settling seq={SeqNum}", lockState.SequenceNumber);
                if (dispatcher.Complete(lockState.LogAddress, lockToken))
                {
                    _entity.DecrementDeadLetterMessageCount();
                }
            }
        }
        dispositionContext.Complete();
    }

    private static void SendMessageWithDeliveryTag(ListenerLink link, Message message, byte[] deliveryTag)
    {
        var delivery = Activator.CreateInstance(DeliveryType)
            ?? throw new InvalidOperationException("Unable to create Amqp.Delivery.");

        DeliveryTagProperty.SetValue(delivery, deliveryTag);
        DeliveryMessageProperty.SetValue(delivery, message);
        DeliveryHandleField.SetValue(delivery, link.Handle);
        DeliveryLinkField.SetValue(delivery, link);
        DeliveryBufferField.SetValue(delivery, message.Encode());
        MessageDeliveryProperty.SetValue(message, delivery);

        SessionSendDeliveryMethod.Invoke(link.Session, [delivery]);
    }

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        _cts.Cancel();
        _entity.Activity.ReceiverDetached(link);
        if (!_isDlq && !string.IsNullOrEmpty(_sessionId))
        {
            _entityManager.ReleaseSessionLock(_entityPath, _sessionId, link.Name);
        }
        base.OnLinkClosed(link, error);
    }
}
