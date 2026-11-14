using Amqp;
using Amqp.Framing;
using Amqp.Types;
using System;
using System.Collections.Generic;

namespace AzsbEmu.Server.Core;

public static class RuleEvaluator
{
    public static bool Evaluate(RuleInfo rule, Message message)
    {
        if (string.Equals(rule.FilterType, "True", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }
        if (string.Equals(rule.FilterType, "False", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        if (string.Equals(rule.FilterType, "Correlation", StringComparison.OrdinalIgnoreCase))
        {
            return MatchCorrelationFilter(rule, message);
        }
        if (string.Equals(rule.FilterType, "Sql", StringComparison.OrdinalIgnoreCase))
        {
            var expr = rule.FilterExpression?.Trim();
            return SqlFilterEvaluator.Evaluate(expr, message);
        }
        return false;
    }

    private static bool MatchCorrelationFilter(RuleInfo rule, Message message)
    {
        if (string.IsNullOrEmpty(rule.FilterExpression)) return true;

        try
        {
            var filter = CorrelationFilterDto.Parse(rule.FilterExpression);
            if (filter == null) return false;

            if (filter.CorrelationId != null && !MatchStringValue(filter.CorrelationId, message.Properties?.CorrelationId)) return false;
            if (filter.MessageId != null && !MatchStringValue(filter.MessageId, message.Properties?.MessageId)) return false;
            if (filter.To != null && !MatchStringValue(filter.To, message.Properties?.To)) return false;
            if (filter.ReplyTo != null && !MatchStringValue(filter.ReplyTo, message.Properties?.ReplyTo)) return false;
            if (filter.Label != null && !MatchStringValue(filter.Label, message.Properties?.Subject)) return false;
            if (filter.SessionId != null && !MatchStringValue(filter.SessionId, message.Properties?.GroupId)) return false;
            if (filter.ReplyToSessionId != null && !MatchStringValue(filter.ReplyToSessionId, message.Properties?.ReplyToGroupId)) return false;
            if (filter.ContentType != null && !MatchStringValue(filter.ContentType, message.Properties?.ContentType)) return false;

            if (filter.Properties != null && filter.Properties.Count > 0)
            {
                if (message.ApplicationProperties == null) return false;
                foreach (var kvp in filter.Properties)
                {
                    if (!message.ApplicationProperties.Map.ContainsKey(kvp.Key)) return false;
                    var msgVal = message.ApplicationProperties[kvp.Key];
                    if (!MatchJsonValue(kvp.Value, msgVal)) return false;
                }
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool MatchStringValue(string filterVal, object? messageVal)
    {
        if (messageVal == null) return false;
        return string.Equals(filterVal, messageVal.ToString(), StringComparison.Ordinal);
    }

    private static bool MatchJsonValue(object filterVal, object? messageVal)
    {
        if (messageVal == null) return false;

        if (filterVal is System.Text.Json.JsonElement element)
        {
            switch (element.ValueKind)
            {
                case System.Text.Json.JsonValueKind.String:
                    return string.Equals(element.GetString(), messageVal.ToString(), StringComparison.Ordinal);
                case System.Text.Json.JsonValueKind.Number:
                    if (element.TryGetInt64(out long lVal))
                    {
                        if (messageVal is long lMsg) return lVal == lMsg;
                        if (messageVal is int iMsg) return lVal == iMsg;
                        return string.Equals(lVal.ToString(), messageVal.ToString(), StringComparison.Ordinal);
                    }
                    if (element.TryGetDouble(out double dVal))
                    {
                        if (messageVal is double dMsg) return dVal == dMsg;
                        if (messageVal is float fMsg) return (float)dVal == fMsg;
                        return string.Equals(dVal.ToString(), messageVal.ToString(), StringComparison.Ordinal);
                    }
                    return string.Equals(element.ToString(), messageVal.ToString(), StringComparison.Ordinal);
                case System.Text.Json.JsonValueKind.True:
                    return messageVal is bool bMsgTrue && bMsgTrue;
                case System.Text.Json.JsonValueKind.False:
                    return messageVal is bool bMsgFalse && !bMsgFalse;
                case System.Text.Json.JsonValueKind.Null:
                    return false;
                default:
                    return string.Equals(element.ToString(), messageVal.ToString(), StringComparison.Ordinal);
            }
        }

        return string.Equals(filterVal.ToString(), messageVal.ToString(), StringComparison.Ordinal);
    }
}
