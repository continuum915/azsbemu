using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.WebUtilities;
using SbBrowser.Core.Entities;
using SbBrowser.Core.Messages;

namespace SbBrowser.Web.Components;

/// <summary>
/// SbBrowser's URLs. Entity names go in the query string, not the path: MassTransit topic names contain '/'.
/// </summary>
public static class Links
{
    public const string Overview = "";

    public static string Queue(string name, bool deadLetters = false) =>
        "queue?name=" + Uri.EscapeDataString(name) + (deadLetters ? "&dlq=true" : "");

    public static string Topic(string name) => "topic?name=" + Uri.EscapeDataString(name);

    public static string Subscription(string topic, string name, bool deadLetters = false) =>
        "subscription?topic=" + Uri.EscapeDataString(topic) + "&name=" + Uri.EscapeDataString(name) + (deadLetters ? "&dlq=true" : "");

    public static string To(EntityRef entity, bool deadLetters = false) => entity switch
    {
        QueueRef q => Queue(q.Name, deadLetters),
        TopicRef t => Topic(t.Name),
        SubscriptionRef s => Subscription(s.Topic, s.Name, deadLetters),
        _ => Overview,
    };

    /// <summary>
    /// A key for the entity the current URL shows: "overview", "queue:name", "topic:name" or "sub:topic/name".
    /// The sidebar compares it with each row's <see cref="KeyOf"/> to highlight the selection.
    /// </summary>
    public static string CurrentKey(NavigationManager navigation)
    {
        var uri = new Uri(navigation.Uri);
        string page = navigation.ToBaseRelativePath(uri.GetLeftPart(UriPartial.Path)).Trim('/');
        var query = QueryHelpers.ParseQuery(uri.Query);
        string Get(string key) => query.TryGetValue(key, out var v) ? v.ToString() : "";
        return page switch
        {
            "queue" => "queue:" + Get("name"),
            "topic" => "topic:" + Get("name"),
            "subscription" => "sub:" + Get("topic") + "/" + Get("name"),
            _ => "overview",
        };
    }

    public static string KeyOf(EntityRef entity) => entity switch
    {
        QueueRef q => "queue:" + q.Name,
        TopicRef t => "topic:" + t.Name,
        SubscriptionRef s => "sub:" + s.Topic + "/" + s.Name,
        _ => "overview",
    };
}

/// <summary>Display formatting shared by the pages.</summary>
public static class Fmt
{
    private static readonly CultureInfo Culture = CultureInfo.InvariantCulture;

    public static string Count(long value) => value.ToString("N0", Culture);

    /// <summary>"active messages" or "dead letters".</summary>
    public static string Source(MessageSource source) => source == MessageSource.DeadLetter ? "dead letters" : "active messages";

    public static string Plural(long count, string one, string many) => $"{Count(count)} {(count == 1 ? one : many)}";

    public static string Size(int bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => (bytes / 1024.0).ToString("0.#", Culture) + " KB",
        _ => (bytes / (1024.0 * 1024)).ToString("0.#", Culture) + " MB",
    };

    /// <summary>A TTL or lock duration: "never" for the SDK's unlimited value, otherwise its largest whole units.</summary>
    public static string Duration(TimeSpan value)
    {
        if (value >= TimeSpan.FromDays(3650)) return "never";
        if (value.TotalDays >= 1 && value.Hours == 0 && value.Minutes == 0) return Plural((long)value.TotalDays, "day", "days");
        if (value.TotalHours >= 1 && value.Minutes == 0 && value.Seconds == 0) return Plural((long)value.TotalHours, "hour", "hours");
        if (value.TotalMinutes >= 1 && value.Seconds == 0) return Plural((long)value.TotalMinutes, "min", "min");
        if (value.TotalSeconds >= 1 && value.Milliseconds == 0) return Plural((long)value.TotalSeconds, "s", "s");
        return value.ToString("c", Culture);
    }

    /// <summary>A timestamp in the server's local time; today's times show without the date.</summary>
    public static string Time(DateTimeOffset value, bool full = false)
    {
        if (value.Year >= 9999) return "never"; // an unlimited TTL expires at DateTimeOffset.MaxValue
        var local = value.ToLocalTime();
        if (full) return local.ToString("yyyy-MM-dd HH:mm:ss", Culture);
        return local.Date == DateTime.Today ? local.ToString("HH:mm:ss", Culture) : local.ToString("MM-dd HH:mm", Culture);
    }

    /// <summary>"4s ago", "3 min ago", or a time.</summary>
    public static string Ago(DateTimeOffset? value)
    {
        if (value is null) return "never";
        var age = DateTimeOffset.Now - value.Value;
        if (age < TimeSpan.FromSeconds(5)) return "just now";
        if (age < TimeSpan.FromMinutes(1)) return $"{(int)age.TotalSeconds}s ago";
        if (age < TimeSpan.FromHours(1)) return $"{(int)age.TotalMinutes} min ago";
        return "at " + value.Value.ToLocalTime().ToString("HH:mm", Culture);
    }

    /// <summary>A short summary of what a subscription lets in.</summary>
    public static string Filter(SubscriptionNode subscription)
    {
        if (subscription.ReceivesAll) return "All messages";
        if (subscription.Rules.Count == 0) return "No rules: receives nothing";
        if (subscription.Rules.All(r => r.Kind == RuleFilterKind.False)) return "Receives nothing";
        var filters = subscription.Rules.Where(r => r.Kind != RuleFilterKind.False).Select(r => r.Filter).ToList();
        return filters.Count == 1 ? filters[0] : string.Join("  OR  ", filters.Select(f => "(" + f + ")"));
    }

    /// <summary>True for MassTransit's fault and skipped queues, which hold failed or unconsumed messages.</summary>
    public static string? FaultKind(string queueName) =>
        queueName.EndsWith("_error", StringComparison.OrdinalIgnoreCase) ? "MassTransit fault queue"
        : queueName.EndsWith("_skipped", StringComparison.OrdinalIgnoreCase) ? "MassTransit skipped queue"
        : null;
}
