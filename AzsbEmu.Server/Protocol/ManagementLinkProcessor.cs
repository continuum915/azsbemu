using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Amqp.Transactions;
using AzsbEmu.Server.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace AzsbEmu.Server.Protocol;

internal sealed class ReplyToLinkRegistry
{
    private readonly ConcurrentDictionary<string, ListenerLink> _links = new(StringComparer.OrdinalIgnoreCase);

    public void Register(string address, ListenerLink link)
    {
        _links[Normalize(address)] = link;
    }

    public bool TryGet(string address, out ListenerLink? link)
    {
        if (_links.TryGetValue(Normalize(address), out var registeredLink) && !registeredLink.IsClosed)
        {
            link = registeredLink;
            return true;
        }

        link = null;
        return false;
    }

    public void Unregister(string address, ListenerLink? link)
    {
        var normalized = Normalize(address);
        if (_links.TryGetValue(normalized, out var current) && (link == null || ReferenceEquals(current, link)))
        {
            _links.TryRemove(normalized, out _);
        }
    }

    private static string Normalize(string address) => address.TrimStart('/').ToLowerInvariant();
}

internal sealed class ReplyToLinkEndpoint : LinkEndpoint
{
    private readonly string _address;
    private readonly ReplyToLinkRegistry _registry;
    private readonly ILogger _logger;

    public ReplyToLinkEndpoint(string address, ReplyToLinkRegistry registry, ILogger logger)
    {
        _address = address;
        _registry = registry;
        _logger = logger;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        _registry.Register(_address, flowContext.Link);
        _logger.LogDebug("Registered reply-to link for address {Address}", _address);

        if (flowContext.Link.IsDraining)
        {
            flowContext.Link.CompleteDrain();
        }
    }

    public override void OnMessage(MessageContext messageContext)
    {
        messageContext.Complete(new Error(ErrorCode.NotAllowed) { Description = "Reply-to links are receive-only." });
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        dispositionContext.Complete();
    }

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        _registry.Unregister(_address, link);
    }
}

internal sealed class ManagementLinkEndpoint : LinkEndpoint
{
    private static readonly Symbol SequenceNumberAnnotation = new("x-opt-sequence-number");
    private static readonly Symbol EnqueuedTimeUtcAnnotation = new("x-opt-enqueued-time-utc");
    private static readonly Symbol EnqueuedTimeAnnotation = new("x-opt-enqueued-time");
    private static readonly Symbol LockTokenAnnotation = new("x-opt-lock-token");
    private static readonly Symbol LockedUntilUtcAnnotation = new("x-opt-locked-until-utc");
    private static readonly Symbol LockedUntilAnnotation = new("x-opt-locked-until");
    private const string DlqSuffix = "/$deadletterqueue";



    private readonly string _entityPath;
    private readonly EntityManager _entityManager;
    private readonly ReplyToLinkRegistry _replyToLinks;
    private readonly ILogger _logger;

    public ManagementLinkEndpoint(string entityPath, EntityManager entityManager, ReplyToLinkRegistry replyToLinks, ILogger logger)
    {
        _entityPath = entityPath;
        _entityManager = entityManager;
        _replyToLinks = replyToLinks;
        _logger = logger;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        var message = messageContext.Message;
        var operation = message.ApplicationProperties?["operation"] as string;

        _logger.LogDebug("Management: operation {Operation} for {EntityPath}", operation, _entityPath);
        // Operations on the entity count as activity for AutoDeleteOnIdle; reading its runtime info does not.
        if (operation != "com.microsoft:get-runtime-info") _entityManager.RecordActivity(_entityPath);

        Message response;
        switch (operation)
        {
            case "com.microsoft:get-runtime-info":
                response = HandleGetRuntimeInfo(message);
                break;
            case "com.microsoft:peek-message":
                response = HandlePeekMessage(message);
                break;
            case "com.microsoft:add-rule":
                response = HandleAddRule(message);
                break;
            case "com.microsoft:remove-rule":
                response = HandleRemoveRule(message);
                break;
            case "com.microsoft:enumerate-rules":
            case "com.microsoft:get-rules":
                response = HandleEnumerateRules(message);
                break;
            case "com.microsoft:receive-by-sequence-number":
                response = HandleReceiveBySequenceNumber(message);
                break;
            case "com.microsoft:update-disposition":
                response = HandleUpdateDisposition(message);
                break;
            case "com.microsoft:schedule-message":
                response = HandleScheduleMessage(message);
                break;
            case "com.microsoft:cancel-scheduled-message":
                response = HandleCancelScheduledMessage(message);
                break;
            case "com.microsoft:create-queue":
                response = HandleCreateQueue(message);
                break;
            case "com.microsoft:create-topic":
                response = HandleCreateTopic(message);
                break;
            case "com.microsoft:create-subscription":
                response = HandleCreateSubscription(message);
                break;
            case "com.microsoft:delete-queue":
                response = HandleDeleteQueue(message);
                break;
            case "com.microsoft:delete-topic":
                response = HandleDeleteTopic(message);
                break;
            case "com.microsoft:delete-subscription":
                response = HandleDeleteSubscription(message);
                break;
            case "com.microsoft:accept-session":
                response = HandleAcceptSession(message);
                break;
            case "com.microsoft:get-session-state":
                response = HandleGetSessionState(message);
                break;
            case "com.microsoft:set-session-state":
                response = HandleSetSessionState(message);
                break;
            case "com.microsoft:renew-lock":
                response = HandleRenewLock(message);
                break;
            case "com.microsoft:renew-session-lock":
                response = HandleRenewSessionLock(message);
                break;
            case "com.microsoft:get-message-sessions":
                response = HandleGetMessageSessions(message);
                break;
            default:
                response = new Message { ApplicationProperties = new ApplicationProperties() };
                response.ApplicationProperties["status-code"] = 404;
                response.ApplicationProperties["status-description"] = $"Operation {operation} not implemented.";
                break;
        }

        if (message.Properties?.MessageId != null)
        {
            response.Properties ??= new Properties();
            response.Properties.CorrelationId = message.Properties.MessageId;
        }

        // The SDK reads "statusCode"; older clients and some handlers use "status-code". Always send both.
        var appProps = response.ApplicationProperties ??= new ApplicationProperties();
        if (appProps.Map.ContainsKey("status-code") && !appProps.Map.ContainsKey("statusCode"))
            appProps["statusCode"] = appProps["status-code"];
        if (appProps.Map.ContainsKey("status-description") && !appProps.Map.ContainsKey("statusDescription"))
            appProps["statusDescription"] = appProps["status-description"];

        var replyTo = message.Properties?.ReplyTo;
        if (string.IsNullOrWhiteSpace(replyTo) || !_replyToLinks.TryGet(replyTo, out var responseLink))
        {
            messageContext.Complete(new Error(ErrorCode.NotFound)
            {
                Description = "No response link was found. Ensure the reply-to link is attached before sending the request."
            });
            return;
        }

        responseLink!.SendMessage(response);
        messageContext.Complete();
    }

    private Message HandleGetRuntimeInfo(Message request)
    {
        var queue = _entityManager.GetQueue(_entityPath);
        if (queue != null)
        {
            return CreateManagementResponse(200, CreateEntityRuntimeInfoBody(queue.MessageCount, queue.DeadLetterMessageCount, _entityManager.GetScheduledMessageCount(_entityPath)));
        }

        int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex > 0)
        {
            string topicName = _entityPath[..subIndex];
            string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];
            var topic = _entityManager.GetTopic(topicName);
            var subscription = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
            if (subscription != null)
            {
                return CreateManagementResponse(200, CreateEntityRuntimeInfoBody(subscription.MessageCount, subscription.DeadLetterMessageCount, _entityManager.GetScheduledMessageCount(_entityPath)));
            }
        }

        var topicInfo = _entityManager.GetTopic(_entityPath);
        if (topicInfo != null)
        {
            long activeMessageCount = 0;
            long deadLetterMessageCount = 0;
            foreach (var subscription in topicInfo.Subscriptions)
            {
                activeMessageCount += subscription.MessageCount;
                deadLetterMessageCount += subscription.DeadLetterMessageCount;
            }

            var body = CreateEntityRuntimeInfoBody(activeMessageCount, deadLetterMessageCount, _entityManager.GetScheduledMessageCount(_entityPath));
            body["SubscriptionCount"] = topicInfo.Subscriptions.Count;
            return CreateManagementResponse(200, body);
        }

        return CreateManagementResponse(404, null, $"Entity {_entityPath} not found.");
    }

    private Message HandlePeekMessage(Message request)
    {
        if (!TryResolvePeekTarget(out var store, out var dispatcher, out var notFoundResponse))
        {
            return notFoundResponse;
        }

        if (!TryGetPeekRequestParameters(request, out long fromSequenceNumber, out int messageCount, out string? error))
        {
            return CreateManagementResponse(400, CreatePeekResponseBody(new List()), error);
        }

        // A session receiver's peek names its session and sees only that session's messages.
        string? sessionId = GetStringParameter(request, "session-id");
        Func<byte[], bool>? inSession = string.IsNullOrEmpty(sessionId)
            ? null
            : data => string.Equals(MessageSerializer.Decode(data).message.Properties?.GroupId, sessionId, StringComparison.Ordinal);

        var entries = store!.PeekAsync(fromSequenceNumber, messageCount, inSession).GetAwaiter().GetResult();
        var messages = new List();
        foreach (var (data, address) in entries)
        {
            var encodedMessage = EncodePeekedMessage(data, address, dispatcher);
            var messageMap = new Map
            {
                ["message"] = encodedMessage
            };
            messages.Add(messageMap);
        }

        return CreateManagementResponse(entries.Count > 0 ? 200 : 204, CreatePeekResponseBody(messages));
    }

    private Message HandleAddRule(Message request)
    {
        int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex <= 0)
        {
            var errorResponse = new Message { ApplicationProperties = new ApplicationProperties() };
            errorResponse.ApplicationProperties["status-code"] = 400;
            errorResponse.ApplicationProperties["status-description"] = "Invalid entity path for rule management.";
            return errorResponse;
        }

        string topicName = _entityPath[..subIndex];
        string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];

        try
        {
            var rule = ParseRuleDescription(request.Body);
            rule.NormalizeFilter(); // the SDK sends TrueRuleFilter/FalseRuleFilter as sql-filter "1=1" / "1=0"
            if (string.Equals(rule.FilterType, "Sql", StringComparison.OrdinalIgnoreCase))
            {
                SqlFilterEvaluator.Validate(rule.FilterExpression);
            }
            SqlFilterEvaluator.ValidateAction(rule.ActionExpression);

            var outcome = _entityManager.CreateRule(topicName, subName, rule);
            var response = new Message { ApplicationProperties = new ApplicationProperties() };
            response.ApplicationProperties["status-code"] = outcome switch
            {
                { Found: false } => 404,
                { Error: not null } => 409,
                _ => 200,
            };
            if (outcome.Error is { } error) response.ApplicationProperties["status-description"] = error;
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adding rule to {Topic}/{Sub}", topicName, subName);
            var response = new Message { ApplicationProperties = new ApplicationProperties() };
            response.ApplicationProperties["status-code"] = 400;
            response.ApplicationProperties["status-description"] = ex.Message;
            return response;
        }
    }

    private Message HandleRemoveRule(Message request)
    {
        int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex <= 0)
        {
            var errorResponse = new Message { ApplicationProperties = new ApplicationProperties() };
            errorResponse.ApplicationProperties["status-code"] = 400;
            errorResponse.ApplicationProperties["status-description"] = "Invalid entity path for rule management.";
            return errorResponse;
        }

        string topicName = _entityPath[..subIndex];
        string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];

        string? ruleName = null;
        if (request.Body is string sBody)
        {
            ruleName = sBody;
        }
        else if (request.Body is Map mapBody)
        {
            ruleName = MapValue(mapBody, "rule-name") as string ?? MapValue(mapBody, "ruleName") as string;
        }
        else if (request.ApplicationProperties?.Map.ContainsKey("ruleName") == true)
        {
            ruleName = request.ApplicationProperties["ruleName"] as string;
        }

        if (string.IsNullOrEmpty(ruleName))
        {
            var response = new Message { ApplicationProperties = new ApplicationProperties() };
            response.ApplicationProperties["status-code"] = 400;
            response.ApplicationProperties["status-description"] = "Missing ruleName parameter.";
            return response;
        }

        try
        {
            bool success = _entityManager.DeleteRule(topicName, subName, ruleName);
            if (success)
            {
                var response = new Message { ApplicationProperties = new ApplicationProperties() };
                response.ApplicationProperties["status-code"] = 200;
                return response;
            }

            var notFound = new Message { ApplicationProperties = new ApplicationProperties() };
            notFound.ApplicationProperties["status-code"] = 404;
            notFound.ApplicationProperties["status-description"] = $"Rule {ruleName} not found.";
            return notFound;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error removing rule {Rule} from {Topic}/{Sub}", ruleName, topicName, subName);
            var response = new Message { ApplicationProperties = new ApplicationProperties() };
            response.ApplicationProperties["status-code"] = 400;
            response.ApplicationProperties["status-description"] = ex.Message;
            return response;
        }
    }

    private Message HandleEnumerateRules(Message request)
    {
        int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex <= 0)
        {
            var errorResponse = new Message { ApplicationProperties = new ApplicationProperties() };
            errorResponse.ApplicationProperties["status-code"] = 400;
            errorResponse.ApplicationProperties["status-description"] = "Invalid entity path for rule management.";
            return errorResponse;
        }

        string topicName = _entityPath[..subIndex];
        string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];

        var topic = _entityManager.GetTopic(topicName);
        if (topic == null)
        {
            var errorResponse = new Message { ApplicationProperties = new ApplicationProperties() };
            errorResponse.ApplicationProperties["status-code"] = 404;
            errorResponse.ApplicationProperties["status-description"] = $"Topic {topicName} not found.";
            return errorResponse;
        }

        var sub = topic.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
        if (sub == null)
        {
            var errorResponse = new Message { ApplicationProperties = new ApplicationProperties() };
            errorResponse.ApplicationProperties["status-code"] = 404;
            errorResponse.ApplicationProperties["status-description"] = $"Subscription {subName} on Topic {topicName} not found.";
            return errorResponse;
        }

        int skip = 0;
        int top = 100;
        if (request.Body is Map mapBody)
        {
            if (mapBody.ContainsKey("skip") && mapBody["skip"] is int s) skip = s;
            if (mapBody.ContainsKey("top") && mapBody["top"] is int t) top = t;
        }
        else if (request.ApplicationProperties != null)
        {
            if (request.ApplicationProperties.Map.ContainsKey("skip") && request.ApplicationProperties["skip"] is int s) skip = s;
            if (request.ApplicationProperties.Map.ContainsKey("top") && request.ApplicationProperties["top"] is int t) top = t;
        }

        // Each entry is a map holding the described rule-description, as the Service Bus protocol documents.
        var rulesList = new List();
        lock (sub.Rules)
        {
            foreach (var rule in sub.Rules.Skip(skip).Take(top))
            {
                rulesList.Add(new Map { ["rule-description"] = EncodeRuleDescription(rule) });
            }
        }

        var responseMap = new Map();
        responseMap["rules"] = rulesList;

        var response = new Message(responseMap) { ApplicationProperties = new ApplicationProperties() };
        response.ApplicationProperties["status-code"] = rulesList.Count > 0 ? 200 : 204;
        return response;
    }

    private RuleInfo ParseRuleDescription(object bodyVal)
    {
        if (bodyVal is DescribedValue describedRule)
        {
            var desc = describedRule.Descriptor;
            if (IsDescriptor(desc, "com.microsoft:rule-description", RuleDescriptionCode))
            {
                if (describedRule.Value is List ruleList && ruleList.Count >= 3)
                {
                    var filterObj = ruleList[0];
                    var actionObj = ruleList[1];
                    var ruleName = ruleList[2] as string ?? "$Default";

                    var rule = new RuleInfo
                    {
                        Name = ruleName,
                        ActionExpression = ReadRuleAction(actionObj)
                    };
                    PopulateFilter(rule, filterObj);
                    return rule;
                }
            }
        }
        else if (bodyVal is Map ruleMap && MapValue(ruleMap, "rule-description") is Map description)
        {
            // The Service Bus SDK's shape: { rule-name, rule-description: { sql-filter | correlation-filter | true-filter
            // | false-filter: {...}, sql-rule-action: { expression } | null, rule-name } }.
            var rule = new RuleInfo
            {
                Name = MapValue(ruleMap, "rule-name") as string ?? MapValue(description, "rule-name") as string ?? "$Default",
            };
            ReadRuleDescriptionMap(rule, description);
            return rule;
        }
        else if (bodyVal is Map legacyMap)
        {
            var rule = new RuleInfo { Name = MapValue(legacyMap, "RuleName") as string ?? "$Default" };
            if (MapValue(legacyMap, "Filter") is { } filter)
            {
                PopulateFilter(rule, filter);
            }

            rule.ActionExpression = ReadRuleAction(MapValue(legacyMap, "Action"));
            return rule;
        }

        throw new ArgumentException("Unsupported rule-description format in message body");
    }

    /// <summary>A map entry by name, whether the key was sent as a string or an AMQP symbol; null if absent.</summary>
    private static object? MapValue(Map map, string key)
    {
        if (map.TryGetValue(key, out var value)) return value;
        if (map.TryGetValue(new Symbol(key), out value)) return value;
        return null;
    }

    private static bool MapHasKey(Map map, string key) => map.ContainsKey(key) || map.ContainsKey(new Symbol(key));

    private static void ReadRuleDescriptionMap(RuleInfo rule, Map description)
    {
        if (MapValue(description, "sql-filter") is Map sql)
        {
            rule.FilterType = "Sql";
            rule.FilterExpression = MapValue(sql, "expression") as string ?? "";
        }
        else if (MapValue(description, "correlation-filter") is Map corr)
        {
            var dto = new CorrelationFilterDto
            {
                CorrelationId = MapValue(corr, "correlation-id") as string,
                MessageId = MapValue(corr, "message-id") as string,
                To = MapValue(corr, "to") as string,
                ReplyTo = MapValue(corr, "reply-to") as string,
                Label = MapValue(corr, "label") as string,
                SessionId = MapValue(corr, "session-id") as string,
                ReplyToSessionId = MapValue(corr, "reply-to-session-id") as string,
                ContentType = MapValue(corr, "content-type") as string,
            };
            if (MapValue(corr, "properties") is Map properties && properties.Count > 0)
            {
                dto.Properties = new Dictionary<string, object>();
                foreach (var key in properties.Keys)
                {
                    if (properties[key] is { } value) dto.SetProperty(key.ToString()!, value);
                }
            }
            rule.FilterType = "Correlation";
            rule.FilterExpression = System.Text.Json.JsonSerializer.Serialize(dto);
        }
        else if (MapHasKey(description, "false-filter"))
        {
            rule.FilterType = "False";
        }
        else if (MapHasKey(description, "true-filter"))
        {
            rule.FilterType = "True";
        }
        else
        {
            throw new ArgumentException("The rule description has no supported filter (sql-filter, correlation-filter, true-filter, false-filter).");
        }

        rule.ActionExpression = MapValue(description, "sql-rule-action") is Map action
            ? MapValue(action, "expression") as string ?? string.Empty
            : string.Empty;
    }

    /// <summary>
    /// The SQL text of a rule action: a plain string, a described <c>sql-rule-action</c> (its first field), or empty
    /// for no action / a described <c>empty-rule-action</c>.
    /// </summary>
    private static string ReadRuleAction(object? action) => action switch
    {
        null => string.Empty,
        string sql => sql,
        DescribedValue { Value: List fields } d when IsDescriptor(d.Descriptor, "com.microsoft:sql-rule-action", SqlRuleActionCode)
            => fields.Count > 0 ? fields[0] as string ?? string.Empty : string.Empty,
        DescribedValue d when IsDescriptor(d.Descriptor, "com.microsoft:empty-rule-action", EmptyRuleActionCode) => string.Empty,
        _ => throw new ArgumentException($"Unsupported rule action '{action}'."),
    };

    private static bool IsDescriptor(object descriptor, string symbol, ulong code)
    {
        if (descriptor is Symbol s) return s.ToString() == symbol;
        if (descriptor is ulong u) return u == code;
        if (descriptor is int i) return (ulong)i == code;
        if (descriptor is long l) return (ulong)l == code;
        return false;
    }

    private void PopulateFilter(RuleInfo rule, object? filterObj)
    {
        if (filterObj == null)
        {
            rule.FilterType = "True";
            return;
        }

        if (filterObj is DescribedValue describedFilter)
        {
            var desc = describedFilter.Descriptor;
            if (IsDescriptor(desc, "com.microsoft:true-filter", TrueFilterCode))
            {
                rule.FilterType = "True";
            }
            else if (IsDescriptor(desc, "com.microsoft:false-filter", FalseFilterCode))
            {
                rule.FilterType = "False";
            }
            else if (IsDescriptor(desc, "com.microsoft:sql-filter", SqlFilterCode))
            {
                rule.FilterType = "Sql";
                if (describedFilter.Value is List sqlList && sqlList.Count > 0)
                {
                    rule.FilterExpression = sqlList[0] as string ?? "";
                }
            }
            else if (IsDescriptor(desc, "com.microsoft:correlation-filter", CorrelationFilterCode))
            {
                rule.FilterType = "Correlation";
                if (describedFilter.Value is List corrList)
                {
                    var dto = new CorrelationFilterDto();
                    if (corrList.Count > 0) dto.CorrelationId = corrList[0] as string;
                    if (corrList.Count > 1) dto.MessageId = corrList[1] as string;
                    if (corrList.Count > 2) dto.To = corrList[2] as string;
                    if (corrList.Count > 3) dto.ReplyTo = corrList[3] as string;
                    if (corrList.Count > 4) dto.Label = corrList[4] as string;
                    if (corrList.Count > 5) dto.SessionId = corrList[5] as string;
                    if (corrList.Count > 6) dto.ReplyToSessionId = corrList[6] as string;
                    if (corrList.Count > 7) dto.ContentType = corrList[7] as string;
                    if (corrList.Count > 8 && corrList[8] is Map mapProps)
                    {
                        dto.Properties = new Dictionary<string, object>();
                        foreach (var key in mapProps.Keys)
                        {
                            if (key is string sKey && mapProps[key] != null)
                            {
                                dto.SetProperty(sKey, mapProps[key]!);
                            }
                        }
                    }

                    rule.FilterExpression = System.Text.Json.JsonSerializer.Serialize(dto);
                }
            }
        }
        else if (filterObj is Map filterMap)
        {
            var filterType = filterMap["FilterType"] as string ?? "True";
            rule.FilterType = filterType;
            if (string.Equals(filterType, "Sql", StringComparison.OrdinalIgnoreCase))
            {
                rule.FilterExpression = filterMap["SqlExpression"] as string ?? "";
            }
            else if (string.Equals(filterType, "Correlation", StringComparison.OrdinalIgnoreCase))
            {
                var dto = new CorrelationFilterDto();
                if (filterMap.ContainsKey("CorrelationId")) dto.CorrelationId = filterMap["CorrelationId"] as string;
                if (filterMap.ContainsKey("MessageId")) dto.MessageId = filterMap["MessageId"] as string;
                if (filterMap.ContainsKey("To")) dto.To = filterMap["To"] as string;
                if (filterMap.ContainsKey("ReplyTo")) dto.ReplyTo = filterMap["ReplyTo"] as string;
                if (filterMap.ContainsKey("Label")) dto.Label = filterMap["Label"] as string;
                if (filterMap.ContainsKey("SessionId")) dto.SessionId = filterMap["SessionId"] as string;
                if (filterMap.ContainsKey("ReplyToSessionId")) dto.ReplyToSessionId = filterMap["ReplyToSessionId"] as string;
                if (filterMap.ContainsKey("ContentType")) dto.ContentType = filterMap["ContentType"] as string;
                if (filterMap.ContainsKey("Properties") && filterMap["Properties"] is Map mapProps)
                {
                    dto.Properties = new Dictionary<string, object>();
                    foreach (var key in mapProps.Keys)
                    {
                        if (key is string sKey && mapProps[key] != null)
                        {
                            dto.SetProperty(sKey, mapProps[key]!);
                        }
                    }
                }

                rule.FilterExpression = System.Text.Json.JsonSerializer.Serialize(dto);
            }
        }
    }

    // Numeric descriptor codes from the Service Bus AMQP protocol: the SDK decodes described values into its rule
    // types by code, so symbolic descriptors would reach it as untyped values. Note the filter codes are one hex
    // digit shorter than the rule-description and action codes (sql-filter 0x1370000006 vs sql-rule-action 0x13700000006).
    private const ulong RuleDescriptionCode = 0x0000013700000004UL;
    private const ulong EmptyRuleActionCode = 0x0000013700000005UL;
    private const ulong SqlRuleActionCode = 0x0000013700000006UL;
    private const ulong SqlFilterCode = 0x000001370000006UL;
    private const ulong TrueFilterCode = 0x000001370000007UL;
    private const ulong FalseFilterCode = 0x000001370000008UL;
    private const ulong CorrelationFilterCode = 0x000001370000009UL;

    private DescribedValue EncodeRuleDescription(RuleInfo rule)
    {
        DescribedValue filter = rule.FilterType.ToLowerInvariant() switch
        {
            "sql" => new DescribedValue(SqlFilterCode, new List { rule.FilterExpression ?? "", 20 }),
            "false" => new DescribedValue(FalseFilterCode, new List { "1=0", 20 }),
            "correlation" => new DescribedValue(CorrelationFilterCode, EncodeCorrelationFilter(rule)),
            _ => new DescribedValue(TrueFilterCode, new List { "1=1", 20 }),
        };

        var action = string.IsNullOrWhiteSpace(rule.ActionExpression)
            ? new DescribedValue(EmptyRuleActionCode, new List())
            : new DescribedValue(SqlRuleActionCode, new List { rule.ActionExpression, 20 });

        return new DescribedValue(RuleDescriptionCode, new List { filter, action, rule.Name, DateTime.UtcNow });
    }

    private List EncodeCorrelationFilter(RuleInfo rule)
    {
        var dto = CorrelationFilterDto.Parse(rule.FilterExpression);

        var properties = new Map();
        foreach (var kvp in dto.TypedProperties())
        {
            properties[kvp.Key] = NormalizeFilterValue(kvp.Value);
        }

        return new List { dto.CorrelationId, dto.MessageId, dto.To, dto.ReplyTo, dto.Label, dto.SessionId, dto.ReplyToSessionId, dto.ContentType, properties };
    }

    private static object? NormalizeFilterValue(object? value)
    {
        if (value is not System.Text.Json.JsonElement element)
        {
            return value;
        }

        return element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => element.GetString(),
            System.Text.Json.JsonValueKind.Number when element.TryGetInt64(out var longValue) => longValue,
            System.Text.Json.JsonValueKind.Number when element.TryGetDouble(out var doubleValue) => doubleValue,
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Null => null,
            _ => element.ToString()
        };
    }

    private Message HandleReceiveBySequenceNumber(Message request)
    {
        if (!TryResolveDispatchTarget(out var entity, out var dispatcher, out var notFoundResponse))
            return notFoundResponse;

        var seqNums = new List<long>();
        if (request.Body is Map mapBody && mapBody.ContainsKey("sequence-numbers"))
        {
            var seqNumsVal = mapBody["sequence-numbers"];
            if (seqNumsVal is long[] longArr)
            {
                seqNums.AddRange(longArr);
            }
            else if (seqNumsVal is List seqList)
            {
                foreach (var item in seqList)
                {
                    if (TryConvertToInt64(item, out long sn))
                        seqNums.Add(sn);
                }
            }
            else if (TryConvertToInt64(seqNumsVal, out long single))
            {
                seqNums.Add(single);
            }
        }

        if (seqNums.Count == 0)
            return CreateManagementResponse(400, null, "Missing or empty sequence-numbers parameter.");

        var messages = new List();
        foreach (var seqNum in seqNums)
        {
            var lockToken = Guid.NewGuid();
            var result = dispatcher!.TryDeliverDeferredAsync(seqNum, lockToken).GetAwaiter().GetResult();
            if (result == null)
            {
                _logger.LogWarning("Deferred message seq={SeqNum} not found for entity {EntityPath}", seqNum, _entityPath);
                continue;
            }

            var (data, logAddress) = result.Value;
            var (header, decoded) = MessageSerializer.Decode(data);
            var lockedUntil = DateTimeOffset.UtcNow.Add(entity!.LockDuration);

            decoded.MessageAnnotations ??= new MessageAnnotations();
            decoded.Header ??= new Header();
            // Include the in-memory increments from abandons and lock expiries (ADR 0001), as link delivery and peek do.
            decoded.Header.DeliveryCount = (uint)(header.DeliveryCount + dispatcher.GetDeliveryCount(logAddress));
            dispatcher.ApplyModifiedProperties(logAddress, decoded);
            decoded.MessageAnnotations[SequenceNumberAnnotation] = header.SequenceNumber;
            decoded.MessageAnnotations[EnqueuedTimeUtcAnnotation] = new DateTime(header.EnqueueTimeUtcTicks, DateTimeKind.Utc);
            decoded.MessageAnnotations[EnqueuedTimeAnnotation] = new DateTime(header.EnqueueTimeUtcTicks, DateTimeKind.Utc);
            decoded.MessageAnnotations[LockTokenAnnotation] = lockToken;
            decoded.MessageAnnotations[LockedUntilUtcAnnotation] = lockedUntil.UtcDateTime;
            decoded.MessageAnnotations[LockedUntilAnnotation] = lockedUntil.UtcDateTime;

            _entityManager.RegisterLock(lockToken, seqNum, logAddress, lockedUntil, _entityPath, _entityPath.EndsWith(DlqSuffix, StringComparison.OrdinalIgnoreCase), "");

            var encoded = decoded.Encode();
            var encodedBytes = encoded.Buffer.AsSpan(encoded.Offset, encoded.Length).ToArray();
            messages.Add(new Map { ["message"] = encodedBytes, ["lock-token"] = lockToken });
        }

        return CreateManagementResponse(messages.Count > 0 ? 200 : 204, CreatePeekResponseBody(messages));
    }

    private Message HandleScheduleMessage(Message request)
    {
        if (!(request.Body is Map requestBody) || !requestBody.ContainsKey("messages"))
        {
            return CreateManagementResponse(400, null, "Missing 'messages' parameter in body.");
        }

        var messagesVal = requestBody["messages"];
        if (!(messagesVal is List messagesList))
        {
            return CreateManagementResponse(400, null, "'messages' parameter must be a list of maps.");
        }

        var messagesToSchedule = new System.Collections.Generic.List<(byte[] payload, DateTime scheduledTimeUtc)>();
        foreach (var item in messagesList)
        {
            if (!(item is Map msgMap) || !msgMap.ContainsKey("message"))
            {
                return CreateManagementResponse(400, null, "Each scheduled message entry must be a map containing a 'message' field.");
            }

            var messageBytesVal = msgMap["message"];
            byte[]? messageBytes = null;
            if (messageBytesVal is byte[] barr)
            {
                messageBytes = barr;
            }
            else if (messageBytesVal is ArraySegment<byte> segment)
            {
                messageBytes = new byte[segment.Count];
                Buffer.BlockCopy(segment.Array!, segment.Offset, messageBytes, 0, segment.Count);
            }

            if (messageBytes == null || messageBytes.Length == 0)
            {
                return CreateManagementResponse(400, null, "'message' field must be a non-empty byte array.");
            }

            try
            {
                var innerBuf = new ByteBuffer(messageBytes, 0, messageBytes.Length, messageBytes.Length);
                var decoded = Message.Decode(innerBuf);

                DateTime? scheduledTime = null;
                if (decoded.MessageAnnotations != null)
                {
                    var key = new Symbol("x-opt-scheduled-enqueue-time");
                    if (decoded.MessageAnnotations.Map.TryGetValue(key, out var val))
                    {
                        if (val is DateTime dt)
                        {
                            scheduledTime = dt;
                        }
                        else if (val is long ticks)
                        {
                            scheduledTime = new DateTime(ticks, DateTimeKind.Utc);
                        }
                    }
                }

                if (scheduledTime == null)
                {
                    return CreateManagementResponse(400, null, "Message does not contain the required 'x-opt-scheduled-enqueue-time' annotation.");
                }

                messagesToSchedule.Add((messageBytes, scheduledTime.Value.ToUniversalTime()));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to decode scheduled message payload");
                return CreateManagementResponse(400, null, $"Failed to decode AMQP message payload: {ex.Message}");
            }
        }

        try
        {
            var seqNumbers = _entityManager.ScheduleMessages(_entityPath, messagesToSchedule);
            
            var arr = seqNumbers.ToArray();
            var responseMap = new Map
            {
                ["sequence-numbers"] = arr,
                [new Symbol("sequence-numbers")] = arr
            };
            return CreateManagementResponse(200, responseMap);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to schedule messages in database");
            return CreateManagementResponse(500, null, $"Failed to persist scheduled messages: {ex.Message}");
        }
    }

    private Message HandleCancelScheduledMessage(Message request)
    {
        if (!(request.Body is Map requestBody) || !requestBody.ContainsKey("sequence-numbers"))
        {
            return CreateManagementResponse(400, null, "Missing 'sequence-numbers' parameter in body.");
        }

        var seqNumsVal = requestBody["sequence-numbers"];
        var seqNums = new System.Collections.Generic.List<long>();
        if (seqNumsVal is long[] longArr)
        {
            seqNums.AddRange(longArr);
        }
        else if (seqNumsVal is List seqList)
        {
            foreach (var item in seqList)
            {
                if (TryConvertToInt64(item, out long sn))
                    seqNums.Add(sn);
            }
        }
        else if (TryConvertToInt64(seqNumsVal, out long single))
        {
            seqNums.Add(single);
        }

        if (seqNums.Count == 0)
        {
            return CreateManagementResponse(400, null, "Missing or empty sequence-numbers parameter.");
        }

        try
        {
            _entityManager.CancelScheduledMessages(seqNums);
            return CreateManagementResponse(200, new Map());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to cancel scheduled messages");
            return CreateManagementResponse(500, null, $"Failed to cancel scheduled messages: {ex.Message}");
        }
    }

    private Message HandleUpdateDisposition(Message request)
    {
        if (request.Body is not Map mapBody)
            return CreateManagementResponse(400, null, "Invalid request body for update-disposition.");

        string? dispositionStatus = mapBody.ContainsKey("disposition-status") ? mapBody["disposition-status"] as string : null;
        if (string.IsNullOrEmpty(dispositionStatus))
            return CreateManagementResponse(400, null, "Missing disposition-status parameter.");

        var lockTokens = new List<Guid>();
        if (mapBody.ContainsKey("lock-tokens"))
        {
            var tokensVal = mapBody["lock-tokens"];
            if (tokensVal is Guid[] guidArr)
            {
                lockTokens.AddRange(guidArr);
            }
            else if (tokensVal is List tokenList)
            {
                foreach (var item in tokenList)
                {
                    if (item is Guid g) lockTokens.Add(g);
                    else if (item is byte[] bytes && bytes.Length == 16) lockTokens.Add(new Guid(bytes));
                }
            }
            else if (tokensVal is Guid single)
            {
                lockTokens.Add(single);
            }
        }

        if (lockTokens.Count == 0)
            return CreateManagementResponse(400, null, "Missing or empty lock-tokens parameter.");

        string? dlReason = mapBody.ContainsKey("deadletter-reason") ? mapBody["deadletter-reason"] as string : null;
        string? dlDescription = mapBody.ContainsKey("deadletter-description") ? mapBody["deadletter-description"] as string : null;
        var propertiesToModify = PropertiesToModify.FromMap(MapValue(mapBody, "properties-to-modify") as Map);
        var deadLetter = new DeadLetterDetails(dlReason, dlDescription, propertiesToModify);

        var deliveryState = request.GetDelivery().State;
        if (deliveryState is TransactionalState txnState)
        {
            byte[] txnId = txnState.TxnId;
            try
            {
                foreach (var lockToken in lockTokens)
                {
                    if (!_entityManager.TryGetLock(lockToken, out var lockState))
                    {
                        _logger.LogWarning("Lock token {LockToken} not found for transactional management update-disposition", lockToken);
                        continue;
                    }

                    _entityManager.BufferSettlement(txnId, lockToken, dispositionStatus, deadLetter);
                }
                return CreateManagementResponse(200, null);
            }
            catch (AmqpException ex)
            {
                _logger.LogError(ex, "Transaction buffer management settlement failed.");
                return CreateManagementResponse(400, null, ex.Error.Description);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in transaction buffer management settlement.");
                return CreateManagementResponse(500, null, ex.Message);
            }
        }

        foreach (var lockToken in lockTokens)
        {
            if (!_entityManager.TryRemoveLock(lockToken, out var lockState))
            {
                _logger.LogWarning("Lock token {LockToken} not found in active locks for update-disposition", lockToken);
                continue;
            }

            IMessageEntity? entity = null;
            Dispatcher? dispatcher = null;

            var queue = _entityManager.GetQueue(lockState.EntityPath);
            if (queue != null)
            {
                entity = queue;
                dispatcher = lockState.IsDlq ? queue.DlqDispatcher : queue.Dispatcher;
            }
            else
            {
                int subIndex = lockState.EntityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
                if (subIndex > 0)
                {
                    string topicName = lockState.EntityPath[..subIndex];
                    string subName = lockState.EntityPath[(subIndex + "/subscriptions/".Length)..];
                    var topic = _entityManager.GetTopic(topicName);
                    var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                    if (sub != null)
                    {
                        entity = sub;
                        dispatcher = lockState.IsDlq ? sub.DlqDispatcher : sub.Dispatcher;
                    }
                }
            }

            if (entity == null || dispatcher == null)
            {
                _logger.LogWarning("Entity or dispatcher not found for lock token {LockToken} (path: {EntityPath}, isDlq: {IsDlq})", 
                    lockToken, lockState.EntityPath, lockState.IsDlq);
                continue;
            }

            switch (dispositionStatus.ToLowerInvariant())
            {
                case "completed":
                    if (dispatcher.Complete(lockState.LogAddress, lockToken))
                    {
                        if (lockState.IsDlq) entity.DecrementDeadLetterMessageCount();
                        else entity.DecrementMessageCount();
                    }
                    break;
                case "abandoned":
                    dispatcher.IncrementDeliveryCount(lockState.LogAddress);
                    dispatcher.Abandon(lockState.LogAddress, lockToken, propertiesToModify);
                    break;
                case "suspended":
                    _entityManager.DeadLetterMessageAsync(entity, lockState.LogAddress, deadLetter).GetAwaiter().GetResult();
                    break;
                case "defered":
                    dispatcher.Defer(lockState.LogAddress, lockToken, lockState.SequenceNumber, propertiesToModify);
                    break;
            }
        }

        return CreateManagementResponse(200, null);
    }

    private bool TryResolveDispatchTarget(out IMessageEntity? entity, out Dispatcher? dispatcher, out Message notFoundResponse)
    {
        entity = null;
        dispatcher = null;

        var path = _entityPath.TrimStart('/');
        if (string.IsNullOrWhiteSpace(path))
        {
            notFoundResponse = CreateManagementResponse(400, null, "Operation requires an entity-scoped management address.");
            return false;
        }

        if (TryResolveEntityAndDispatcher(path, out entity, out dispatcher))
        {
            notFoundResponse = new Message();
            return true;
        }

        notFoundResponse = CreateManagementResponse(404, null, $"Entity {_entityPath} not found.");
        return false;
    }

    private bool TryResolveEntityAndDispatcher(string path, out IMessageEntity? entity, out Dispatcher? dispatcher)
    {
        var queue = _entityManager.GetQueue(path);
        if (queue?.Dispatcher != null)
        {
            entity = queue;
            dispatcher = queue.Dispatcher;
            return true;
        }

        int subIndex = path.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex > 0)
        {
            string topicName = path[..subIndex];
            string subName = path[(subIndex + "/subscriptions/".Length)..];
            var topic = _entityManager.GetTopic(topicName);
            var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
            if (sub?.Dispatcher != null)
            {
                entity = sub;
                dispatcher = sub.Dispatcher;
                return true;
            }
        }

        entity = null;
        dispatcher = null;
        return false;
    }

    private bool TryResolvePeekTarget(out MessageStore? store, out Dispatcher? dispatcher, out Message notFoundResponse)
    {
        var path = _entityPath.TrimStart('/');
        if (string.IsNullOrWhiteSpace(path))
        {
            store = null;
            dispatcher = null;
            notFoundResponse = CreateManagementResponse(400, null, "Peek requires an entity-scoped management address.");
            return false;
        }

        bool isDlq = false;
        string basePath = path;
        if (path.EndsWith(DlqSuffix, StringComparison.OrdinalIgnoreCase))
        {
            isDlq = true;
            basePath = path[..^DlqSuffix.Length];
        }

        var queue = _entityManager.GetQueue(basePath);
        if (queue != null)
        {
            store = isDlq ? queue.DlqMessageStore : queue.MessageStore;
            dispatcher = isDlq ? queue.DlqDispatcher : queue.Dispatcher;
            notFoundResponse = store != null
                ? new Message()
                : CreateManagementResponse(500, null, $"Entity {basePath} storage is not initialized.");
            return store != null;
        }

        int subIndex = basePath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIndex > 0)
        {
            string topicName = basePath[..subIndex];
            string subName = basePath[(subIndex + "/subscriptions/".Length)..];
            var topic = _entityManager.GetTopic(topicName);
            var subscription = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
            if (subscription != null)
            {
                store = isDlq ? subscription.DlqMessageStore : subscription.MessageStore;
                dispatcher = isDlq ? subscription.DlqDispatcher : subscription.Dispatcher;
                notFoundResponse = store != null
                    ? new Message()
                    : CreateManagementResponse(500, null, $"Entity {basePath} storage is not initialized.");
                return store != null;
            }
        }

        store = null;
        dispatcher = null;
        notFoundResponse = CreateManagementResponse(404, null, $"Entity {_entityPath} not found.");
        return false;
    }

    private static bool TryGetPeekRequestParameters(Message request, out long fromSequenceNumber, out int messageCount, out string? error)
    {
        fromSequenceNumber = 1;
        messageCount = 1;
        error = null;

        if (!TryGetLongValue(request, "from-sequence-number", out fromSequenceNumber))
        {
            error = "Missing or invalid from-sequence-number parameter.";
            return false;
        }

        if (!TryGetIntValue(request, "message-count", out messageCount))
        {
            error = "Missing or invalid message-count parameter.";
            return false;
        }

        if (messageCount <= 0)
        {
            error = "message-count must be greater than zero.";
            return false;
        }

        return true;
    }

    private static bool TryGetLongValue(Message request, string key, out long value)
    {
        if (request.Body is Map mapBody && mapBody.ContainsKey(key) && TryConvertToInt64(mapBody[key], out value))
        {
            return true;
        }

        if (request.ApplicationProperties?.Map.ContainsKey(key) == true
            && TryConvertToInt64(request.ApplicationProperties[key], out value))
        {
            return true;
        }

        value = default;
        return false;
    }

    private static bool TryGetIntValue(Message request, string key, out int value)
    {
        if (request.Body is Map mapBody && mapBody.ContainsKey(key) && TryConvertToInt32(mapBody[key], out value))
        {
            return true;
        }

        if (request.ApplicationProperties?.Map.ContainsKey(key) == true
            && TryConvertToInt32(request.ApplicationProperties[key], out value))
        {
            return true;
        }

        value = default;
        return false;
    }

    private static bool TryConvertToInt64(object? value, out long result)
    {
        switch (value)
        {
            case long longValue:
                result = longValue;
                return true;
            case int intValue:
                result = intValue;
                return true;
            case uint uintValue:
                result = uintValue;
                return true;
            case ulong ulongValue when ulongValue <= long.MaxValue:
                result = (long)ulongValue;
                return true;
            case short shortValue:
                result = shortValue;
                return true;
            case ushort ushortValue:
                result = ushortValue;
                return true;
            case byte byteValue:
                result = byteValue;
                return true;
            case sbyte sbyteValue:
                result = sbyteValue;
                return true;
            default:
                result = default;
                return false;
        }
    }

    private static bool TryConvertToInt32(object? value, out int result)
    {
        switch (value)
        {
            case int intValue:
                result = intValue;
                return true;
            case uint uintValue when uintValue <= int.MaxValue:
                result = (int)uintValue;
                return true;
            case short shortValue:
                result = shortValue;
                return true;
            case ushort ushortValue:
                result = ushortValue;
                return true;
            case byte byteValue:
                result = byteValue;
                return true;
            case sbyte sbyteValue:
                result = sbyteValue;
                return true;
            case long longValue when longValue is >= int.MinValue and <= int.MaxValue:
                result = (int)longValue;
                return true;
            default:
                result = default;
                return false;
        }
    }

    private static Map CreatePeekResponseBody(List messages)
    {
        var body = new Map();
        body["messages"] = messages;
        return body;
    }

    private static Map CreateEntityRuntimeInfoBody(long activeMessageCount, long deadLetterMessageCount, long scheduledMessageCount = 0L)
    {
        var totalMessageCount = activeMessageCount + deadLetterMessageCount + scheduledMessageCount;
        var body = new Map
        {
            ["MessageCount"] = activeMessageCount,
            ["ActiveMessageCount"] = activeMessageCount,
            ["DeadLetterMessageCount"] = deadLetterMessageCount,
            ["TotalMessageCount"] = totalMessageCount,
            ["ScheduledMessageCount"] = scheduledMessageCount,
            ["TransferMessageCount"] = 0L,
            ["TransferDeadLetterMessageCount"] = 0L
        };

        return body;
    }

    private static Message CreateManagementResponse(int statusCode, object? body, string? statusDescription = null)
    {
        var response = body == null ? new Message() : new Message(body);
        response.ApplicationProperties = new ApplicationProperties();
        response.ApplicationProperties["status-code"] = statusCode;
        response.ApplicationProperties["statusCode"] = statusCode;
        if (!string.IsNullOrWhiteSpace(statusDescription))
        {
            response.ApplicationProperties["status-description"] = statusDescription;
            response.ApplicationProperties["statusDescription"] = statusDescription;
        }

        return response;
    }

    private static byte[] EncodePeekedMessage(byte[] storedData, long address, Dispatcher? dispatcher)
    {
        var (header, message) = MessageSerializer.Decode(storedData);
        message.MessageAnnotations ??= new MessageAnnotations();
        message.MessageAnnotations[SequenceNumberAnnotation] = header.SequenceNumber;

        var enqueuedTimeUtc = new DateTime(header.EnqueueTimeUtcTicks, DateTimeKind.Utc);
        message.MessageAnnotations[EnqueuedTimeUtcAnnotation] = enqueuedTimeUtc;
        message.MessageAnnotations[EnqueuedTimeAnnotation] = enqueuedTimeUtc;

        message.Header ??= new Header();
        message.Header.DeliveryCount = (uint)(header.DeliveryCount + (dispatcher?.GetDeliveryCount(address) ?? 0));
        dispatcher?.ApplyModifiedProperties(address, message);

        var buffer = message.Encode();
        return buffer.Buffer.AsSpan(buffer.Offset, buffer.Length).ToArray();
    }

    private Message HandleCreateQueue(Message request)
    {
        if (!string.IsNullOrEmpty(_entityPath))
        {
            return CreateManagementResponse(400, null, "Create queue must be executed on the global management node.");
        }

        string? name = GetStringParameter(request, "entity-name")
            ?? GetStringParameter(request, "entityName")
            ?? GetStringParameter(request, "name");

        if (string.IsNullOrEmpty(name))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: name or entity-name.");
        }

        bool requiresSession = GetPropertyValue<bool>(request, "requires-session", "RequiresSession");
        
        TimeSpan lockDuration = GetPropertyValue<TimeSpan>(request, "lock-duration", "LockDuration");
        if (lockDuration == default) lockDuration = TimeSpan.FromSeconds(60);

        long maxSizeInMegabytes = GetPropertyValue<long>(request, "max-size-in-megabytes", "MaxSizeInMegabytes");
        if (maxSizeInMegabytes == default) maxSizeInMegabytes = 1024;

        int maxDeliveryCount = GetPropertyValue<int>(request, "max-delivery-count", "MaxDeliveryCount");
        if (maxDeliveryCount == default) maxDeliveryCount = 10;

        TimeSpan defaultTtl = GetPropertyValue<TimeSpan>(request, "default-message-time-to-live", "DefaultMessageTimeToLive");
        if (defaultTtl == default) defaultTtl = TimeSpan.MaxValue;

        bool deadLetterOnExpiration = GetPropertyValue<bool>(request, "dead-lettering-on-message-expiration", "DeadLetteringOnMessageExpiration");

        var queue = new QueueInfo
        {
            Name = name,
            RequiresSession = requiresSession,
            LockDuration = lockDuration,
            MaxSizeInMegabytes = maxSizeInMegabytes,
            MaxDeliveryCount = maxDeliveryCount,
            DefaultMessageTimeToLive = defaultTtl,
            DeadLetteringOnMessageExpiration = deadLetterOnExpiration,
        };

        if (_entityManager.CreateQueue(queue))
        {
            var responseMap = new Map
            {
                ["name"] = name,
                ["requires-session"] = requiresSession,
                ["lock-duration"] = (long)lockDuration.TotalSeconds,
                ["max-size-in-megabytes"] = maxSizeInMegabytes,
                ["max-delivery-count"] = maxDeliveryCount,
                ["default-message-time-to-live"] = defaultTtl == TimeSpan.MaxValue ? -1L : (long)defaultTtl.TotalSeconds,
                ["dead-lettering-on-message-expiration"] = deadLetterOnExpiration,
            };
            return CreateManagementResponse(201, responseMap);
        }

        return CreateManagementResponse(409, null, $"Queue '{name}' already exists.");
    }

    private Message HandleCreateTopic(Message request)
    {
        if (!string.IsNullOrEmpty(_entityPath))
        {
            return CreateManagementResponse(400, null, "Create topic must be executed on the global management node.");
        }

        string? name = GetStringParameter(request, "entity-name")
            ?? GetStringParameter(request, "entityName")
            ?? GetStringParameter(request, "name");

        if (string.IsNullOrEmpty(name))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: name or entity-name.");
        }

        if (_entityManager.CreateTopic(name))
        {
            var responseMap = new Map
            {
                ["name"] = name
            };
            return CreateManagementResponse(201, responseMap);
        }

        return CreateManagementResponse(409, null, $"Topic '{name}' already exists.");
    }

    private Message HandleCreateSubscription(Message request)
    {
        if (!string.IsNullOrEmpty(_entityPath))
        {
            return CreateManagementResponse(400, null, "Create subscription must be executed on the global management node.");
        }

        string? topicName = GetStringParameter(request, "topic-name")
            ?? GetStringParameter(request, "topicName")
            ?? GetStringParameter(request, "parent-path")
            ?? GetStringParameter(request, "parentPath");

        string? subName = GetStringParameter(request, "subscription-name")
            ?? GetStringParameter(request, "subscriptionName")
            ?? GetStringParameter(request, "name")
            ?? GetStringParameter(request, "entity-name")
            ?? GetStringParameter(request, "entityName");

        if (string.IsNullOrEmpty(topicName) || string.IsNullOrEmpty(subName))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: topic-name or subscription-name.");
        }

        bool requiresSession = GetPropertyValue<bool>(request, "requires-session", "RequiresSession");
        
        TimeSpan lockDuration = GetPropertyValue<TimeSpan>(request, "lock-duration", "LockDuration");
        if (lockDuration == default) lockDuration = TimeSpan.FromSeconds(60);

        int maxDeliveryCount = GetPropertyValue<int>(request, "max-delivery-count", "MaxDeliveryCount");
        if (maxDeliveryCount == default) maxDeliveryCount = 10;

        TimeSpan defaultTtl = GetPropertyValue<TimeSpan>(request, "default-message-time-to-live", "DefaultMessageTimeToLive");
        if (defaultTtl == default) defaultTtl = TimeSpan.MaxValue;

        bool deadLetterOnExpiration = GetPropertyValue<bool>(request, "dead-lettering-on-message-expiration", "DeadLetteringOnMessageExpiration");

        var sub = new SubscriptionInfo
        {
            Name = subName,
            TopicName = topicName,
            RequiresSession = requiresSession,
            LockDuration = lockDuration,
            MaxDeliveryCount = maxDeliveryCount,
            DefaultMessageTimeToLive = defaultTtl,
            DeadLetteringOnMessageExpiration = deadLetterOnExpiration,
        };

        if (_entityManager.CreateSubscription(topicName, sub))
        {
            var responseMap = new Map
            {
                ["topic-name"] = topicName,
                ["subscription-name"] = subName,
                ["requires-session"] = requiresSession,
                ["lock-duration"] = (long)lockDuration.TotalSeconds,
                ["max-delivery-count"] = maxDeliveryCount,
                ["default-message-time-to-live"] = defaultTtl == TimeSpan.MaxValue ? -1L : (long)defaultTtl.TotalSeconds,
                ["dead-lettering-on-message-expiration"] = deadLetterOnExpiration,
            };
            return CreateManagementResponse(201, responseMap);
        }

        return CreateManagementResponse(409, null, $"Subscription '{subName}' on Topic '{topicName}' already exists or Topic does not exist.");
    }

    private Message HandleDeleteQueue(Message request)
    {
        if (!string.IsNullOrEmpty(_entityPath))
        {
            return CreateManagementResponse(400, null, "Delete queue must be executed on the global management node.");
        }

        string? name = GetStringParameter(request, "entity-name")
            ?? GetStringParameter(request, "entityName")
            ?? GetStringParameter(request, "name");

        if (string.IsNullOrEmpty(name))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: name or entity-name.");
        }

        if (_entityManager.DeleteQueue(name))
        {
            return CreateManagementResponse(200, new Map());
        }

        return CreateManagementResponse(404, null, $"Queue '{name}' not found.");
    }

    private Message HandleDeleteTopic(Message request)
    {
        if (!string.IsNullOrEmpty(_entityPath))
        {
            return CreateManagementResponse(400, null, "Delete topic must be executed on the global management node.");
        }

        string? name = GetStringParameter(request, "entity-name")
            ?? GetStringParameter(request, "entityName")
            ?? GetStringParameter(request, "name");

        if (string.IsNullOrEmpty(name))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: name or entity-name.");
        }

        if (_entityManager.DeleteTopic(name))
        {
            return CreateManagementResponse(200, new Map());
        }

        return CreateManagementResponse(404, null, $"Topic '{name}' not found.");
    }

    private Message HandleDeleteSubscription(Message request)
    {
        if (!string.IsNullOrEmpty(_entityPath))
        {
            return CreateManagementResponse(400, null, "Delete subscription must be executed on the global management node.");
        }

        string? topicName = GetStringParameter(request, "topic-name")
            ?? GetStringParameter(request, "topicName")
            ?? GetStringParameter(request, "parent-path")
            ?? GetStringParameter(request, "parentPath");

        string? subName = GetStringParameter(request, "subscription-name")
            ?? GetStringParameter(request, "subscriptionName")
            ?? GetStringParameter(request, "name")
            ?? GetStringParameter(request, "entity-name")
            ?? GetStringParameter(request, "entityName");

        if (string.IsNullOrEmpty(topicName) || string.IsNullOrEmpty(subName))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: topic-name or subscription-name.");
        }

        if (_entityManager.DeleteSubscription(topicName, subName))
        {
            return CreateManagementResponse(200, new Map());
        }

        return CreateManagementResponse(404, null, $"Subscription '{subName}' on Topic '{topicName}' not found.");
    }

    private static string? GetStringParameter(Message request, string key)
    {
        if (request.ApplicationProperties?.Map.ContainsKey(key) == true)
        {
            return request.ApplicationProperties[key] as string;
        }
        if (request.Body is Map mapBody && mapBody.ContainsKey(key))
        {
            return mapBody[key] as string;
        }
        return null;
    }

    private static T? GetPropertyValue<T>(Message request, string key1, string key2)
    {
        object? val = null;
        if (request.Body is Map mapBody)
        {
            if (mapBody.ContainsKey(key1)) val = mapBody[key1];
            else if (mapBody.ContainsKey(key2)) val = mapBody[key2];
        }
        if (val == null && request.ApplicationProperties != null)
        {
            if (request.ApplicationProperties.Map.ContainsKey(key1)) val = request.ApplicationProperties[key1];
            else if (request.ApplicationProperties.Map.ContainsKey(key2)) val = request.ApplicationProperties[key2];
        }

        if (val == null) return default;

        try
        {
            if (typeof(T) == typeof(TimeSpan))
            {
                if (val is TimeSpan ts) return (T)(object)ts;
                if (TryConvertToInt64(val, out long ticks))
                {
                    if (ticks > 10_000_000_000L) return (T)(object)TimeSpan.FromTicks(ticks);
                    return (T)(object)TimeSpan.FromSeconds(ticks);
                }
            }
            if (typeof(T) == typeof(long))
            {
                if (TryConvertToInt64(val, out long l)) return (T)(object)l;
            }
            if (typeof(T) == typeof(int))
            {
                if (TryConvertToInt32(val, out int i)) return (T)(object)i;
            }
            if (typeof(T) == typeof(bool))
            {
                if (val is bool b) return (T)(object)b;
                if (TryConvertToInt32(val, out int i)) return (T)(object)(i != 0);
            }
        }
        catch
        {
        }

        return default;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        if (flowContext.Link.IsDraining)
        {
            flowContext.Link.CompleteDrain();
        }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        dispositionContext.Complete();
    }

    private Message HandleAcceptSession(Message request)
    {
        string? sessionId = GetStringParameter(request, "session-id")
            ?? GetStringParameter(request, "sessionId");

        _logger.LogInformation("AcceptSession requested on {EntityPath} for session '{SessionId}'", _entityPath, sessionId);

        TimeSpan lockDuration = TimeSpan.FromSeconds(60);
        var queue = _entityManager.GetQueue(_entityPath);
        if (queue != null) lockDuration = queue.LockDuration;
        else
        {
            int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
            if (subIndex > 0)
            {
                string topicName = _entityPath[..subIndex];
                string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];
                var topic = _entityManager.GetTopic(topicName);
                var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                if (sub != null) lockDuration = sub.LockDuration;
            }
        }

        SessionLockState? lockState = null;
        if (!string.IsNullOrEmpty(sessionId))
        {
            lockState = _entityManager.TryLockSession(_entityPath, sessionId, "accept-session", lockDuration);
            if (lockState == null)
            {
                return CreateManagementResponse(400, null, $"Session '{sessionId}' cannot be locked because it is already locked by another receiver.");
            }
        }
        else
        {
            lockState = _entityManager.TryLockNextAvailableSession(_entityPath, "accept-session", lockDuration);
            if (lockState == null)
            {
                // No sessions available (or none have messages and are unlocked). Return 204 No Content.
                return CreateManagementResponse(204, null, "No sessions available.");
            }
        }

        // Return locked session details
        var body = new Map();
        body["session-id"] = lockState.SessionId;
        body["sessionId"] = lockState.SessionId;
        
        byte[]? state = _entityManager.GetSessionState(_entityPath, lockState.SessionId);
        if (state != null)
        {
            body["session-state"] = state;
            body["sessionState"] = state;
        }
        body["expiration"] = lockState.LockedUntil;

        return CreateManagementResponse(200, body);
    }

    private Message HandleGetSessionState(Message request)
    {
        string? sessionId = GetStringParameter(request, "session-id")
            ?? GetStringParameter(request, "sessionId");

        if (string.IsNullOrEmpty(sessionId))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: session-id.");
        }

        var body = new Map();
        byte[]? state = _entityManager.GetSessionState(_entityPath, sessionId);
        if (state != null)
        {
            body["session-state"] = state;
            body["sessionState"] = state;
        }
        return CreateManagementResponse(200, body);
    }

    private Message HandleSetSessionState(Message request)
    {
        string? sessionId = GetStringParameter(request, "session-id")
            ?? GetStringParameter(request, "sessionId");

        if (string.IsNullOrEmpty(sessionId))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: session-id.");
        }

        byte[]? state = null;
        if (request.Body is Map mapBody)
        {
            if (mapBody.ContainsKey("session-state")) state = mapBody["session-state"] as byte[];
            else if (mapBody.ContainsKey("sessionState")) state = mapBody["sessionState"] as byte[];
        }
        if (state == null && request.ApplicationProperties != null)
        {
            if (request.ApplicationProperties.Map.ContainsKey("session-state")) state = request.ApplicationProperties["session-state"] as byte[];
            else if (request.ApplicationProperties.Map.ContainsKey("sessionState")) state = request.ApplicationProperties["sessionState"] as byte[];
        }

        _entityManager.SetSessionState(_entityPath, sessionId, state);
        return CreateManagementResponse(200, new Map());
    }

    private Message HandleRenewLock(Message request)
    {
        if (request.Body is not Map mapBody)
            return CreateManagementResponse(400, null, "Invalid request body for renew-lock.");

        var lockTokens = new System.Collections.Generic.List<Guid>();
        if (mapBody.ContainsKey("lock-tokens"))
        {
            var tokensVal = mapBody["lock-tokens"];
            if (tokensVal is Guid[] guidArr)
            {
                lockTokens.AddRange(guidArr);
            }
            else if (tokensVal is List tokenList)
            {
                foreach (var item in tokenList)
                {
                    if (item is Guid g) lockTokens.Add(g);
                    else if (item is byte[] bytes && bytes.Length == 16) lockTokens.Add(new Guid(bytes));
                }
            }
            else if (tokensVal is Guid single)
            {
                lockTokens.Add(single);
            }
        }

        if (lockTokens.Count == 0)
            return CreateManagementResponse(400, null, "Missing or empty lock-tokens parameter.");

        // Resolve lock duration for the entity
        TimeSpan lockDuration = TimeSpan.FromSeconds(60);
        var queue = _entityManager.GetQueue(_entityPath);
        if (queue != null)
        {
            lockDuration = queue.LockDuration;
        }
        else
        {
            int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
            if (subIndex > 0)
            {
                string topicName = _entityPath[..subIndex];
                string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];
                var topic = _entityManager.GetTopic(topicName);
                var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                if (sub != null)
                {
                    lockDuration = sub.LockDuration;
                }
            }
        }

        var expirationsList = new System.Collections.Generic.List<DateTime>();
        foreach (var lockToken in lockTokens)
        {
            if (_entityManager.RenewLock(lockToken, lockDuration, _entityPath, out var newLockedUntil))
            {
                expirationsList.Add(newLockedUntil.UtcDateTime);
            }
            else
            {
                _logger.LogWarning("Failed to renew lock for token {LockToken} on entity {EntityPath}", lockToken, _entityPath);
                return CreateManagementResponse(410, null, "The lock on the message has been lost.");
            }
        }

        var arr = expirationsList.ToArray();
        var responseMap = new Map();
        responseMap["expirations"] = arr;
        responseMap[new Symbol("expirations")] = arr;
        return CreateManagementResponse(200, responseMap);
    }

    private Message HandleRenewSessionLock(Message request)
    {
        string? sessionId = GetStringParameter(request, "session-id")
            ?? GetStringParameter(request, "sessionId");

        if (string.IsNullOrEmpty(sessionId))
        {
            return CreateManagementResponse(400, null, "Missing required parameter: session-id.");
        }

        TimeSpan lockDuration = TimeSpan.FromSeconds(60);
        var queue = _entityManager.GetQueue(_entityPath);
        if (queue != null) lockDuration = queue.LockDuration;
        else
        {
            int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
            if (subIndex > 0)
            {
                string topicName = _entityPath[..subIndex];
                string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];
                var topic = _entityManager.GetTopic(topicName);
                var sub = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
                if (sub != null) lockDuration = sub.LockDuration;
            }
        }

        var lockedUntil = _entityManager.RenewSessionLock(_entityPath, sessionId, lockDuration);
        if (lockedUntil == null)
        {
            return CreateManagementResponse(400, null, $"Failed to renew lock for session '{sessionId}'. The lock may have expired or is not held by this caller.");
        }

        var body = new Map();
        body["expiration"] = lockedUntil.Value;
        return CreateManagementResponse(200, body);
    }

    private Message HandleGetMessageSessions(Message request)
    {
        var activeSessions = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var s in _entityManager.GetActiveSessions(_entityPath))
        {
            activeSessions.Add(s);
        }

        IMessageEntity? entity = _entityManager.GetQueue(_entityPath);
        if (entity == null)
        {
            int subIndex = _entityPath.IndexOf("/subscriptions/", StringComparison.OrdinalIgnoreCase);
            if (subIndex > 0)
            {
                string topicName = _entityPath[..subIndex];
                string subName = _entityPath[(subIndex + "/subscriptions/".Length)..];
                var topic = _entityManager.GetTopic(topicName);
                entity = topic?.Subscriptions.Find(s => string.Equals(s.Name, subName, StringComparison.OrdinalIgnoreCase));
            }
        }

        if (entity != null)
        {
            foreach (var kvp in entity.SessionMessageAddresses)
            {
                if (!kvp.Value.IsEmpty)
                {
                    activeSessions.Add(kvp.Key);
                }
            }
        }

        var sessionList = new System.Collections.Generic.List<string>(activeSessions);
        var body = new Map();
        body["sessions-ids"] = sessionList;
        body["sessionsIds"] = sessionList;
        return CreateManagementResponse(200, body);
    }
}
