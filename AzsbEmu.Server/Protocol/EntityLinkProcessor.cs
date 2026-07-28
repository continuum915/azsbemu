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
            string? sessionId = null;
            if (isReceiver && !isDlq && queue.RequiresSession)
            {
                sessionId = ExtractSessionId(context.Attach);
                if (string.IsNullOrEmpty(sessionId))
                {
                    _logger.LogWarning("Rejecting receiver attach to session-enabled queue '{Address}': Session ID is missing", baseAddress);
                    context.Complete(new Error(ErrorCode.PreconditionFailed) { Description = "It is not possible to create a receiver on a session-enabled entity without a session." });
                    return;
                }

                var lockDuration = queue.LockDuration;
                var lockState = _entityManager.TryLockSession(baseAddress, sessionId, context.Attach.LinkName, lockDuration);
                if (lockState == null)
                {
                    _logger.LogWarning("Rejecting receiver attach to session-enabled queue '{Address}': Session '{SessionId}' is already locked", baseAddress, sessionId);
                    context.Complete(new Error(ErrorCode.NotAllowed) { Description = $"Session '{sessionId}' is already locked by another receiver." });
                    return;
                }

                context.Attach.Properties ??= new Fields();
                context.Attach.Properties[new Symbol("com.microsoft:session-id")] = sessionId;

                if (context.Attach.Source is Source sourceVal)
                {
                    sourceVal.FilterSet ??= new Map();
                    sourceVal.FilterSet[new Symbol("com.microsoft:session-filter")] = sessionId;
                }
            }

            bool isReceiveAndDelete = isReceiver && context.Attach.RcvSettleMode == ReceiverSettleMode.First;
            context.Complete(new EntityLinkEndpoint(queue, baseAddress, _entityManager, isReceiveAndDelete, isDlq, sessionId, _logger), 300);
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
                    string? sessionId = null;
                    if (isReceiver && !isDlq && sub.RequiresSession)
                    {
                        sessionId = ExtractSessionId(context.Attach);
                        if (string.IsNullOrEmpty(sessionId))
                        {
                            _logger.LogWarning("Rejecting receiver attach to session-enabled subscription '{Address}': Session ID is missing", baseAddress);
                            context.Complete(new Error(ErrorCode.PreconditionFailed) { Description = "It is not possible to create a receiver on a session-enabled entity without a session." });
                            return;
                        }

                        var lockDuration = sub.LockDuration;
                        var lockState = _entityManager.TryLockSession(baseAddress, sessionId, context.Attach.LinkName, lockDuration);
                        if (lockState == null)
                        {
                            _logger.LogWarning("Rejecting receiver attach to session-enabled subscription '{Address}': Session '{SessionId}' is already locked", baseAddress, sessionId);
                            context.Complete(new Error(ErrorCode.NotAllowed) { Description = $"Session '{sessionId}' is already locked by another receiver." });
                            return;
                        }

                        context.Attach.Properties ??= new Fields();
                        context.Attach.Properties[new Symbol("com.microsoft:session-id")] = sessionId;

                        if (context.Attach.Source is Source sourceVal)
                        {
                            sourceVal.FilterSet ??= new Map();
                            sourceVal.FilterSet[new Symbol("com.microsoft:session-filter")] = sessionId;
                        }
                    }

                    bool isReceiveAndDelete = isReceiver && context.Attach.RcvSettleMode == ReceiverSettleMode.First;
                    context.Complete(new EntityLinkEndpoint(sub, baseAddress, _entityManager, isReceiveAndDelete, isDlq, sessionId, _logger), 300);
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
    private static readonly Symbol SequenceNumberAnnotation = new("x-opt-sequence-number");
    private static readonly Symbol EnqueuedTimeUtcAnnotation = new("x-opt-enqueued-time-utc");
    private static readonly Symbol EnqueuedTimeAnnotation = new("x-opt-enqueued-time");

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
                        FanOutToSubscriptions(innerMsg);
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
                FanOutToSubscriptions(outerMessage);
            }
        }

        messageContext.Complete();
    }

    private void FanOutToSubscriptions(Message message)
    {
        List<SubscriptionInfo> subs;
        lock (_topic.Subscriptions)
        {
            subs = new List<SubscriptionInfo>(_topic.Subscriptions);
        }

        foreach (var sub in subs)
        {
            RuleInfo? matchedRule = null;
            lock (sub.Rules)
            {
                foreach (var rule in sub.Rules)
                {
                    if (RuleEvaluator.Evaluate(rule, message))
                    {
                        matchedRule = rule;
                        break;
                    }
                }
            }

            if (matchedRule == null)
            {
                _logger.LogDebug("Message does not match rules for subscription {SubName} on topic {TopicName}", sub.Name, _topic.Name);
                continue;
            }

            var subStore = sub.MessageStore;
            if (subStore == null)
            {
                _logger.LogWarning("Subscription {SubName} message store is not initialized.", sub.Name);
                continue;
            }

            try
            {
                if (!string.IsNullOrWhiteSpace(matchedRule.ActionExpression))
                {
                    _logger.LogWarning(
                        "Subscription rule {RuleName} on {TopicName}/{SubName} has an action expression, which is not implemented yet.",
                        matchedRule.Name,
                        _topic.Name,
                        sub.Name);
                }

                long seqNum = sub.AssignNextSequenceNumber();
                long enqueueTicks = DateTime.UtcNow.Ticks;

                long ttlTicks = 0;
                uint? msgTtlMs = message.Header?.Ttl;
                if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
                    ttlTicks = TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
                else if (sub.DefaultMessageTimeToLive != TimeSpan.MaxValue)
                    ttlTicks = sub.DefaultMessageTimeToLive.Ticks;

                var messageToStore = new Message
                {
                    Header = message.Header,
                    Properties = message.Properties,
                    ApplicationProperties = message.ApplicationProperties ?? new ApplicationProperties(),
                    MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations(),
                    BodySection = message.BodySection
                };
                messageToStore.MessageAnnotations[SequenceNumberAnnotation] = seqNum;
                var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);
                messageToStore.MessageAnnotations[EnqueuedTimeUtcAnnotation] = enqueuedTimeUtc;
                messageToStore.MessageAnnotations[EnqueuedTimeAnnotation] = enqueuedTimeUtc;

                if (sub.RequiresSession && string.IsNullOrEmpty(message.Properties?.GroupId))
                {
                    _logger.LogWarning("Message seq={SeqNum} is missing SessionId on session-enabled subscription {SubName} under topic {TopicName}, dead-lettering", seqNum, sub.Name, _topic.Name);
                    var subDlqStore = sub.DlqMessageStore;
                    if (subDlqStore != null)
                    {
                        messageToStore.ApplicationProperties["DeadLetterReason"] = "SessionIdIsNull";
                        messageToStore.ApplicationProperties["DeadLetterErrorDescription"] = "The SessionId was not set on a message sent to a session-enabled entity.";

                        var header = new MessageHeader
                        {
                            SequenceNumber = seqNum,
                            EnqueueTimeUtcTicks = enqueueTicks,
                            DeliveryCount = 0,
                            IsDeadLettered = true,
                            TimeToLiveTicks = ttlTicks
                        };

                        byte[] encoded = MessageSerializer.Encode(messageToStore, header);
                        subDlqStore.Append(encoded);
                        sub.IncrementDeadLetterMessageCount();
                    }
                    continue;
                }

                var headerNormal = new MessageHeader
                {
                    SequenceNumber = seqNum,
                    EnqueueTimeUtcTicks = enqueueTicks,
                    DeliveryCount = 0,
                    IsDeadLettered = false,
                    TimeToLiveTicks = ttlTicks
                };

                byte[] encodedNormal = MessageSerializer.Encode(messageToStore, headerNormal);
                long address = subStore.Append(encodedNormal);
                sub.IncrementMessageCount();

                string? sessionId = message.Properties?.GroupId;
                if (sub.RequiresSession && !string.IsNullOrEmpty(sessionId))
                {
                    var q = sub.SessionMessageAddresses.GetOrAdd(sessionId, _ => new ConcurrentQueue<long>());
                    q.Enqueue(address);
                }

                _logger.LogDebug("Enqueued message seq={SeqNum} to subscription {SubName} under topic {TopicName} (session={SessionId})", seqNum, sub.Name, _topic.Name, sessionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fan out message from topic {TopicName} to subscription {SubName}", _topic.Name, sub.Name);
            }
        }
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
                        AppendToStore(innerMsg, store);
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
                AppendToStore(outerMessage, store);
            }
        }

        messageContext.Complete();
    }

    private void AppendToStore(Message message, MessageStore store)
    {
        long seqNum = _entity.AssignNextSequenceNumber();
        long enqueueTicks = DateTime.UtcNow.Ticks;

        long ttlTicks = ComputeTtlTicks(message, _entity);

        var messageToStore = new Message
        {
            Header = message.Header,
            Properties = message.Properties,
            ApplicationProperties = message.ApplicationProperties,
            MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations(),
            BodySection = message.BodySection
        };
        messageToStore.MessageAnnotations[SequenceNumberAnnotation] = seqNum;
        var enqueuedTimeUtc = new DateTime(enqueueTicks, DateTimeKind.Utc);
        messageToStore.MessageAnnotations[EnqueuedTimeUtcAnnotation] = enqueuedTimeUtc;
        messageToStore.MessageAnnotations[EnqueuedTimeAnnotation] = enqueuedTimeUtc;

        var header = new MessageHeader
        {
            SequenceNumber = seqNum,
            EnqueueTimeUtcTicks = enqueueTicks,
            DeliveryCount = 0,
            IsDeadLettered = _isDlq,
            TimeToLiveTicks = ttlTicks
        };

        byte[] encoded = MessageSerializer.Encode(messageToStore, header);
        long address = store.Append(encoded);
        if (_isDlq) _entity.IncrementDeadLetterMessageCount();
        else _entity.IncrementMessageCount();

        string? sessionId = message.Properties?.GroupId;
        if (!_isDlq && _entity.RequiresSession && !string.IsNullOrEmpty(sessionId))
        {
            var q = _entity.SessionMessageAddresses.GetOrAdd(sessionId, _ => new ConcurrentQueue<long>());
            q.Enqueue(address);
        }

        _logger.LogDebug("Enqueued message seq={SeqNum} to entity {EntityName} (isDlq={IsDlq}, session={SessionId})", seqNum, _entity.Name, _isDlq, sessionId);
    }

    private static long ComputeTtlTicks(Message message, IMessageEntity entity)
    {
        uint? msgTtlMs = message.Header?.Ttl;
        if (msgTtlMs.HasValue && msgTtlMs.Value > 0)
            return TimeSpan.FromMilliseconds(msgTtlMs.Value).Ticks;
        if (entity.DefaultMessageTimeToLive != TimeSpan.MaxValue)
            return entity.DefaultMessageTimeToLive.Ticks;
        return 0;
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
            for (int i = 0; i < count; i++)
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
                        _logger.LogInformation("Message seq={SeqNum} has expired (TTL) on entity {EntityName}, dead-lettering",
                            header.SequenceNumber, _entity.Name);
                        await _entityManager.DeadLetterMessageAsync(_entity, logAddressValue, "TTLExpiredException", "The message has expired.");
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
                        // In ReceiveAndDelete mode, the message is settled on the wire immediately.
                        link.SendMessage(message);
                        dispatcher.Complete(logAddressValue, lockToken);
                        if (_isDlq) _entity.DecrementDeadLetterMessageCount();
                        else _entity.DecrementMessageCount();
                    }
                    else
                    {
                        SendMessageWithDeliveryTag(link, message, lockToken.ToByteArray());
                    }
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

            if (undeliverableHere)
            {
                if (dispatcher.Defer(lockState.LogAddress, lockToken, lockState.SequenceNumber))
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
                if (dispatcher.Abandon(lockState.LogAddress, lockToken))
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
            string? reason = rejected.Error?.Condition;
            string? description = rejected.Error?.Description;

            if (rejected.Error?.Info != null)
            {
                if (rejected.Error.Info.TryGetValue(new Symbol("DeadLetterReason"), out var r)) reason = r?.ToString();
                if (rejected.Error.Info.TryGetValue(new Symbol("DeadLetterErrorDescription"), out var d)) description = d?.ToString();
            }

            if (!_isDlq)
            {
                // We must use synchronous wait here because OnDisposition is synchronous
                _entityManager.DeadLetterMessageAsync(_entity, lockState.LogAddress, reason, description).GetAwaiter().GetResult();
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
        if (!_isDlq && !string.IsNullOrEmpty(_sessionId))
        {
            _entityManager.ReleaseSessionLock(_entityPath, _sessionId, link.Name);
        }
        base.OnLinkClosed(link, error);
    }
}
