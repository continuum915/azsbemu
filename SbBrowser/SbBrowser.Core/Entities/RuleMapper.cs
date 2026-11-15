using System.Globalization;
using Azure.Messaging.ServiceBus.Administration;

namespace SbBrowser.Core.Entities;

/// <summary>Maps the SDK's rule types to <see cref="RuleView"/>, and forward targets to entity names.</summary>
internal static class RuleMapper
{
    public static RuleView ToView(RuleProperties rule)
    {
        string? action = (rule.Action as SqlRuleAction)?.SqlExpression;
        if (string.IsNullOrWhiteSpace(action)) action = null;

        return rule.Filter switch
        {
            TrueRuleFilter => new RuleView(rule.Name, RuleFilterKind.True, "1=1", action),
            FalseRuleFilter => new RuleView(rule.Name, RuleFilterKind.False, "1=0", action),
            SqlRuleFilter sql when IsAlwaysTrue(sql.SqlExpression) => new RuleView(rule.Name, RuleFilterKind.True, sql.SqlExpression, action),
            SqlRuleFilter sql => new RuleView(rule.Name, RuleFilterKind.Sql, sql.SqlExpression, action),
            CorrelationRuleFilter correlation => new RuleView(rule.Name, RuleFilterKind.Correlation, Describe(correlation), action),
            _ => new RuleView(rule.Name, RuleFilterKind.Sql, rule.Filter?.ToString() ?? "", action),
        };
    }

    /// <summary>
    /// The entity name in a ForwardTo value. Azure returns a full endpoint URL (<c>sb://ns.servicebus.windows.net/q</c>);
    /// the emulator returns the name.
    /// </summary>
    public static string? ForwardName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        if (Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme is "sb" or "amqp" or "amqps" or "http" or "https")
            return Uri.UnescapeDataString(uri.AbsolutePath.Trim('/'));
        return value;
    }

    private static bool IsAlwaysTrue(string? expression) =>
        string.Concat((expression ?? "").Where(c => !char.IsWhiteSpace(c))) == "1=1";

    private static string Describe(CorrelationRuleFilter filter)
    {
        var parts = new List<string>();
        void Add(string name, string? value)
        {
            if (!string.IsNullOrEmpty(value)) parts.Add($"{name} = {Quote(value)}");
        }

        Add("CorrelationId", filter.CorrelationId);
        Add("MessageId", filter.MessageId);
        Add("To", filter.To);
        Add("ReplyTo", filter.ReplyTo);
        Add("Subject", filter.Subject);
        Add("SessionId", filter.SessionId);
        Add("ReplyToSessionId", filter.ReplyToSessionId);
        Add("ContentType", filter.ContentType);
        foreach (var (key, value) in filter.ApplicationProperties)
            parts.Add($"{key} = {Literal(value)}");

        return parts.Count == 0 ? "(matches every message)" : string.Join(" AND ", parts);
    }

    private static string Quote(string value) => "'" + value.Replace("'", "''") + "'";

    private static string Literal(object? value) => value switch
    {
        null => "NULL",
        string s => Quote(s),
        bool b => b ? "TRUE" : "FALSE",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => Quote(value.ToString() ?? ""),
    };
}
