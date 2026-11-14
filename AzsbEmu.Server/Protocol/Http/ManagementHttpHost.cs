using System.Net;
using AzsbEmu.Server.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AzsbEmu.Server.Protocol.Http;

/// <summary>
/// The Service Bus HTTP/Atom management API that <c>ServiceBusAdministrationClient</c> talks to, hosted in-process
/// next to the AMQP broker and sharing its <see cref="EntityManager"/>.
/// </summary>
/// <remarks>
/// With <c>UseDevelopmentEmulator=true</c> the SDK's admin client always connects to <c>http://host:80</c>, whatever
/// port the connection string's endpoint names, so 80 is the default; see the spike result in the MassTransit spec.
/// </remarks>
public sealed class ManagementHttpHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly EntityManager _entityManager;
    private readonly ILogger _logger;

    /// <summary>The port actually bound (resolves a requested port of 0).</summary>
    public int Port { get; private set; }

    public ManagementHttpHost(EntityManager entityManager, ILoggerFactory loggerFactory, string host, int port)
    {
        _entityManager = entityManager;
        _logger = loggerFactory.CreateLogger<ManagementHttpHost>();

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            if (host is "localhost" or "127.0.0.1" or "::1")
            {
                // Both loopback addresses: clients resolving "localhost" try ::1 first, and on Windows a refused
                // IPv6 attempt costs ~2s per new connection before falling back to 127.0.0.1.
                kestrel.ListenLocalhost(port == 0 ? FindFreeLoopbackPort() : port);
            }
            else
            {
                kestrel.ListenAnyIP(port);
            }
        });

        _app = builder.Build();
        _app.Run(HandleAsync);
    }

    public async Task StartAsync()
    {
        await _app.StartAsync().ConfigureAwait(false);
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        Port = new Uri(address).Port;
        _logger.LogInformation("Management HTTP API listening on {Address}", address);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
    }

    private async Task HandleAsync(HttpContext context)
    {
        var request = context.Request;
        string entityPath = (request.Path.Value ?? "").Trim('/');

        int status;
        string body;
        try
        {
            var resource = ManagementPath.Parse(entityPath);
            (status, body) = (resource.Kind, request.Method) switch
            {
                (ManagementPathKind.Entity, "GET") => GetEntity(resource.EntityName, request),
                (ManagementPathKind.Entity, "PUT") => await PutEntityAsync(resource.EntityName, request).ConfigureAwait(false),
                (ManagementPathKind.Entity, "DELETE") => DeleteEntity(resource.EntityName),
                (ManagementPathKind.Subscription, "GET") => GetSubscription(resource, request),
                (ManagementPathKind.Subscription, "PUT") => await PutSubscriptionAsync(resource, request).ConfigureAwait(false),
                (ManagementPathKind.Subscription, "DELETE") => DeleteSubscription(resource),
                (ManagementPathKind.Rule, "GET") => GetRule(resource, request),
                (ManagementPathKind.Rule, "PUT") => await PutRuleAsync(resource, request).ConfigureAwait(false),
                (ManagementPathKind.Rule, "DELETE") => DeleteRule(resource),
                (ManagementPathKind.RuleList, "GET") => ListRules(resource, request),
                (ManagementPathKind.QueueList, "GET") => ListQueues(request),
                (ManagementPathKind.TopicList, "GET") => ListTopics(request),
                (ManagementPathKind.SubscriptionList, "GET") => ListSubscriptions(resource, request),
                (ManagementPathKind.NotSupported, _) => (501, AtomSerializer.WriteError(501, $"'{entityPath}' is not supported yet.")),
                _ => (405, AtomSerializer.WriteError(405, $"Method {request.Method} is not supported on '{entityPath}'.")),
            };
        }
        catch (Exception ex) when (ex is FormatException or System.Xml.XmlException)
        {
            (status, body) = (400, AtomSerializer.WriteError(400, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Management HTTP {Method} {Path} failed", request.Method, request.Path);
            (status, body) = (500, AtomSerializer.WriteError(500, ex.Message));
        }

        _logger.LogDebug("Management HTTP {Method} {Path}{Query} -> {Status}", request.Method, request.Path, request.QueryString, status);
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/atom+xml;type=entry;charset=utf-8";
        await context.Response.WriteAsync(body).ConfigureAwait(false);
    }

    private static int FindFreeLoopbackPort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private static string BaseAddress(HttpRequest request) => $"{request.Scheme}://{request.Host}";

    private (int, string) GetEntity(string name, HttpRequest request)
    {
        // One GET serves both GetQueueAsync and GetTopicAsync; the SDK reports "not found" when the returned
        // description is of the other kind.
        if (_entityManager.GetQueue(name) is { } queue) return (200, QueueEntry(queue, request));
        if (_entityManager.GetTopic(name) is { } topic) return (200, TopicEntry(topic, request));
        return NotFound(name);
    }

    private async Task<(int, string)> PutEntityAsync(string name, HttpRequest request)
    {
        if (request.Headers.ContainsKey("If-Match"))
            return await UpdateEntityAsync(name, request).ConfigureAwait(false);
        if (ManagementPath.InvalidEntityNameReason(name) is { } invalid)
            return (400, AtomSerializer.WriteError(400, invalid));

        using var reader = new StreamReader(request.Body);
        var description = AtomSerializer.ReadDescription(await reader.ReadToEndAsync().ConfigureAwait(false));
        switch (description?.Name.LocalName)
        {
            case "QueueDescription":
                var queue = AtomSerializer.ReadQueue(name, description);
                if (_entityManager.ValidateForwardTargets(name, queue.ForwardTo, queue.ForwardDeadLetteredMessagesTo) is { } queueForwardError)
                    return (400, AtomSerializer.WriteError(400, queueForwardError));
                if (!_entityManager.CreateQueue(queue)) return AlreadyExists(name);
                return (201, QueueEntry(_entityManager.GetQueue(name)!, request));

            case "TopicDescription":
                if (!_entityManager.CreateTopic(AtomSerializer.ReadTopic(name, description))) return AlreadyExists(name);
                return (201, TopicEntry(_entityManager.GetTopic(name)!, request));

            default:
                return UnsupportedDescription(description);
        }
    }

    /// <summary>PUT with <c>If-Match</c>: update an existing queue or topic (404 if there is none of that kind).</summary>
    private async Task<(int, string)> UpdateEntityAsync(string name, HttpRequest request)
    {
        var description = await ReadDescriptionAsync(request).ConfigureAwait(false);
        switch (description?.Name.LocalName)
        {
            case "QueueDescription":
                return ToUpdateResponse(name, _entityManager.UpdateQueue(AtomSerializer.ReadQueue(name, description)),
                    () => QueueEntry(_entityManager.GetQueue(name)!, request));
            case "TopicDescription":
                return ToUpdateResponse(name, _entityManager.UpdateTopic(AtomSerializer.ReadTopic(name, description)),
                    () => TopicEntry(_entityManager.GetTopic(name)!, request));
            default:
                return UnsupportedDescription(description);
        }
    }

    private async Task<(int, string)> UpdateSubscriptionAsync(ManagementPath path, HttpRequest request)
    {
        var description = await ReadDescriptionAsync(request).ConfigureAwait(false);
        if (description?.Name.LocalName != "SubscriptionDescription")
            return UnsupportedDescription(description);

        var desired = AtomSerializer.ReadSubscription(path.EntityName, path.SubscriptionName, description);
        return ToUpdateResponse(path.ToString(), _entityManager.UpdateSubscription(path.EntityName, desired),
            () => SubscriptionEntry(_entityManager.GetSubscription(path.EntityName, path.SubscriptionName)!, request));
    }

    private static (int, string) ToUpdateResponse(string name, EntityManager.UpdateOutcome outcome, Func<string> entry) => outcome switch
    {
        { Found: false } => NotFound(name),
        { Error: { } error } => (400, AtomSerializer.WriteError(400, error)),
        _ => (200, entry()),
    };

    private static async Task<System.Xml.Linq.XElement?> ReadDescriptionAsync(HttpRequest request)
    {
        using var reader = new StreamReader(request.Body);
        return AtomSerializer.ReadDescription(await reader.ReadToEndAsync().ConfigureAwait(false));
    }

    private (int, string) DeleteEntity(string name)
    {
        if (_entityManager.GetQueue(name) != null && _entityManager.DeleteQueue(name)) return (200, "");
        if (_entityManager.GetTopic(name) != null && _entityManager.DeleteTopic(name)) return (200, "");
        return NotFound(name);
    }

    private (int, string) GetSubscription(ManagementPath path, HttpRequest request)
    {
        var sub = _entityManager.GetSubscription(path.EntityName, path.SubscriptionName);
        return sub is null ? NotFound(path.ToString()) : (200, SubscriptionEntry(sub, request));
    }

    private async Task<(int, string)> PutSubscriptionAsync(ManagementPath path, HttpRequest request)
    {
        if (request.Headers.ContainsKey("If-Match"))
            return await UpdateSubscriptionAsync(path, request).ConfigureAwait(false);
        if (path.SubscriptionName.Length == 0 || path.SubscriptionName.StartsWith('$'))
            return (400, AtomSerializer.WriteError(400, $"Invalid subscription name '{path.SubscriptionName}'."));
        if (_entityManager.GetTopic(path.EntityName) is null)
            return NotFound(path.EntityName);

        using var reader = new StreamReader(request.Body);
        var description = AtomSerializer.ReadDescription(await reader.ReadToEndAsync().ConfigureAwait(false));
        if (description?.Name.LocalName != "SubscriptionDescription")
            return UnsupportedDescription(description);

        var sub = AtomSerializer.ReadSubscription(path.EntityName, path.SubscriptionName, description);
        if (_entityManager.ValidateForwardTargets(path.ToString(), sub.ForwardTo, sub.ForwardDeadLetteredMessagesTo, owningTopic: path.EntityName) is { } subscriptionForwardError)
            return (400, AtomSerializer.WriteError(400, subscriptionForwardError));
        var defaultRule = description.Element(AtomSerializer.Sb + "DefaultRuleDescription") is { } ruleElement
            ? AtomSerializer.ReadRule(ruleElement)
            : null;
        if (!_entityManager.CreateSubscription(path.EntityName, sub, defaultRule))
            return _entityManager.GetTopic(path.EntityName) is null ? NotFound(path.EntityName) : AlreadyExists(path.ToString());

        return (201, SubscriptionEntry(sub, request));
    }

    private (int, string) DeleteSubscription(ManagementPath path) =>
        _entityManager.DeleteSubscription(path.EntityName, path.SubscriptionName) ? (200, "") : NotFound(path.ToString());

    private (int, string) ListQueues(HttpRequest request) =>
        Feed("Queues", request, _entityManager.GetQueues().OrderBy(q => q.Name, StringComparer.OrdinalIgnoreCase).Select(q => QueueEntry(q, request)));

    private (int, string) ListTopics(HttpRequest request) =>
        Feed("Topics", request, _entityManager.GetTopics().OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase).Select(t => TopicEntry(t, request)));

    private (int, string) ListSubscriptions(ManagementPath path, HttpRequest request)
    {
        if (_entityManager.GetSubscriptions(path.EntityName) is not { } subs) return NotFound(path.EntityName);
        return Feed("Subscriptions", request, subs.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase).Select(s => SubscriptionEntry(s, request)));
    }

    private (int, string) GetRule(ManagementPath path, HttpRequest request) =>
        _entityManager.GetRule(path.EntityName, path.SubscriptionName, path.RuleName) is { } rule
            ? (200, RuleEntry(rule, path, request))
            : NotFound(path.ToString());

    /// <summary>PUT creates a rule (409 if it exists); with <c>If-Match</c> it replaces an existing one (404 if missing).</summary>
    private async Task<(int, string)> PutRuleAsync(ManagementPath path, HttpRequest request)
    {
        var description = await ReadDescriptionAsync(request).ConfigureAwait(false);
        if (description?.Name.LocalName != "RuleDescription") return UnsupportedDescription(description);

        var rule = AtomSerializer.ReadRule(description);
        rule.Name = path.RuleName; // the path names the rule
        bool update = request.Headers.ContainsKey("If-Match");
        var outcome = update
            ? _entityManager.UpdateRule(path.EntityName, path.SubscriptionName, rule)
            : _entityManager.CreateRule(path.EntityName, path.SubscriptionName, rule);

        return outcome switch
        {
            { Found: false } => NotFound(path.ToString()),
            { Error: { } } => AlreadyExists(path.ToString()),
            _ => (update ? 200 : 201, RuleEntry(_entityManager.GetRule(path.EntityName, path.SubscriptionName, path.RuleName)!, path, request)),
        };
    }

    private (int, string) DeleteRule(ManagementPath path) =>
        _entityManager.DeleteRule(path.EntityName, path.SubscriptionName, path.RuleName) ? (200, "") : NotFound(path.ToString());

    private (int, string) ListRules(ManagementPath path, HttpRequest request)
    {
        if (_entityManager.GetSubscription(path.EntityName, path.SubscriptionName) is not { } sub
            || _entityManager.GetRules(path.EntityName, path.SubscriptionName) is not { } rules)
            return NotFound(path.ToString());
        return Feed("Rules", request, rules.OrderBy(r => r.Name, StringComparer.OrdinalIgnoreCase)
            .Select(r => AtomSerializer.WriteRuleEntry(r, sub.TopicName, sub.Name, BaseAddress(request))));
    }

    /// <summary>A rule's entry, addressed by the stored topic and subscription names.</summary>
    private string RuleEntry(RuleInfo rule, ManagementPath path, HttpRequest request)
    {
        var sub = _entityManager.GetSubscription(path.EntityName, path.SubscriptionName);
        return AtomSerializer.WriteRuleEntry(rule, sub?.TopicName ?? path.EntityName, sub?.Name ?? path.SubscriptionName, BaseAddress(request));
    }

    /// <summary>One page (<c>$skip</c>, <c>$top</c>; default 100) of an ordered listing as an Atom feed.</summary>
    private static (int, string) Feed(string title, HttpRequest request, IEnumerable<string> orderedEntries)
    {
        int skip = int.TryParse(request.Query["$skip"], out var s) && s > 0 ? s : 0;
        int top = int.TryParse(request.Query["$top"], out var t) && t > 0 ? t : 100;
        string self = $"{BaseAddress(request)}{request.Path}{request.QueryString}";
        return (200, AtomSerializer.WriteFeed(title, self, orderedEntries.Skip(skip).Take(top)));
    }

    // Entries always carry runtime properties (counts); the SDK reads them for Get*RuntimePropertiesAsync and
    // ignores them otherwise.
    private string QueueEntry(QueueInfo queue, HttpRequest request) =>
        AtomSerializer.WriteQueueEntry(queue, CountsOf(queue), BaseAddress(request));

    private string TopicEntry(TopicInfo topic, HttpRequest request)
    {
        int subscriptionCount = _entityManager.GetSubscriptions(topic.Name)?.Count ?? 0;
        var counts = new EntityCounts(0, 0, _entityManager.GetScheduledMessageCount(topic.Name));
        return AtomSerializer.WriteTopicEntry(topic, counts, subscriptionCount, BaseAddress(request));
    }

    private string SubscriptionEntry(SubscriptionInfo sub, HttpRequest request) =>
        AtomSerializer.WriteSubscriptionEntry(sub, CountsOf(sub), BaseAddress(request));

    private EntityCounts CountsOf(IMessageEntity entity) =>
        new(entity.MessageCount, entity.DeadLetterMessageCount, _entityManager.GetScheduledMessageCount(entity));

    private static (int, string) UnsupportedDescription(System.Xml.Linq.XElement? description) =>
        (400, AtomSerializer.WriteError(400, $"Unsupported entity description '{description?.Name.LocalName}'."));

    private static (int, string) AlreadyExists(string name) =>
        (409, AtomSerializer.WriteError(409, $"The messaging entity '{name}' already exists."));

    private static (int, string) NotFound(string name) =>
        (404, AtomSerializer.WriteError(404, $"The messaging entity '{name}' could not be found."));
}
