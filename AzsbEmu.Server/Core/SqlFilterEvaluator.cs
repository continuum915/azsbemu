using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Amqp;
using Amqp.Framing;

namespace AzsbEmu.Server.Core;

internal static class SqlFilterEvaluator
{
    public static bool Evaluate(string? expression, Message message)
    {
        var root = Parse(expression);
        return root.Evaluate(message);
    }

    public static void Validate(string? expression)
    {
        _ = Parse(expression);
    }

    /// <summary>
    /// Applies a SQL rule action to <paramref name="message"/> in place. Supported statements, separated by ';':
    /// <c>SET &lt;property&gt; = &lt;literal or property&gt;</c> and <c>REMOVE &lt;property&gt;</c>, where a property is an
    /// application property (optionally prefixed <c>user.</c>) or a <c>sys.</c> system property.
    /// </summary>
    public static void ApplyAction(string? action, Message message)
    {
        foreach (var statement in ParseAction(action))
            statement.Apply(message);
    }

    public static void ValidateAction(string? action) => _ = ParseAction(action);

    private static List<ActionStatement> ParseAction(string? action)
    {
        var statements = new List<ActionStatement>();
        foreach (var text in SplitStatements(action ?? ""))
        {
            var parser = new Parser(text);
            statements.Add(parser.ParseActionStatement());
            parser.Expect(TokenKind.End);
        }
        return statements;
    }

    /// <summary>Splits on ';' outside single-quoted strings, dropping empty statements.</summary>
    private static IEnumerable<string> SplitStatements(string action)
    {
        var current = new StringBuilder();
        bool inString = false;
        foreach (char c in action)
        {
            if (c == '\'') inString = !inString;
            if (c == ';' && !inString)
            {
                if (current.ToString().Trim() is { Length: > 0 } statement) yield return statement;
                current.Clear();
                continue;
            }
            current.Append(c);
        }
        if (current.ToString().Trim() is { Length: > 0 } last) yield return last;
    }

    private abstract record ActionStatement(ValueReference Target)
    {
        public abstract void Apply(Message message);
    }

    private sealed record SetStatement(ValueReference Target, ValueNode Value) : ActionStatement(Target)
    {
        public override void Apply(Message message)
        {
            object? value = Value is ValueReference reference ? reference.Resolve(message) : Value.GetValue();
            Target.Assign(message, value);
        }
    }

    private sealed record RemoveStatement(ValueReference Target) : ActionStatement(Target)
    {
        public override void Apply(Message message) => Target.Remove(message);
    }

    private static IExpression Parse(string? expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
        {
            return new LiteralBooleanExpression(true);
        }

        var parser = new Parser(expression);
        var root = parser.ParseExpression();
        parser.Expect(TokenKind.End);
        return root;
    }

    private interface IExpression
    {
        bool Evaluate(Message message);
    }

    private sealed record LiteralBooleanExpression(bool Value) : IExpression
    {
        public bool Evaluate(Message message) => Value;
    }

    private sealed record OrExpression(IExpression Left, IExpression Right) : IExpression
    {
        public bool Evaluate(Message message) => Left.Evaluate(message) || Right.Evaluate(message);
    }

    private sealed record AndExpression(IExpression Left, IExpression Right) : IExpression
    {
        public bool Evaluate(Message message) => Left.Evaluate(message) && Right.Evaluate(message);
    }

    private sealed record NotExpression(IExpression Inner) : IExpression
    {
        public bool Evaluate(Message message) => !Inner.Evaluate(message);
    }

    private sealed record ComparisonExpression(ValueReference Left, TokenKind Operator, ValueNode Right) : IExpression
    {
        public bool Evaluate(Message message)
        {
            var leftValue = Left.Resolve(message);
            var rightValue = Right.GetValue();
            return Compare(leftValue, rightValue, Operator);
        }
    }

    private sealed record LikeExpression(ValueReference Left, ValueNode Pattern) : IExpression
    {
        public bool Evaluate(Message message)
        {
            var leftValue = Left.Resolve(message)?.ToString();
            var patternValue = Pattern.GetValue()?.ToString();
            if (leftValue == null || patternValue == null)
            {
                return false;
            }

            var regex = "^" + Regex.Escape(patternValue)
                .Replace("%", ".*")
                .Replace("_", ".") + "$";

            return Regex.IsMatch(leftValue, regex, RegexOptions.CultureInvariant);
        }
    }

    private sealed record InExpression(ValueReference Left, IReadOnlyList<ValueNode> Values) : IExpression
    {
        public bool Evaluate(Message message)
        {
            var leftValue = Left.Resolve(message);
            return Values.Any(value => Compare(leftValue, value.GetValue(), TokenKind.Equal));
        }
    }

    private sealed record IsNullExpression(ValueReference Left, bool Negated) : IExpression
    {
        public bool Evaluate(Message message)
        {
            var isNull = Left.Resolve(message) == null;
            return Negated ? !isNull : isNull;
        }
    }

    private abstract record ValueNode
    {
        public abstract object? GetValue();
    }

    private sealed record LiteralValue(object? Value) : ValueNode
    {
        public override object? GetValue() => Value;
    }

    private sealed record ValueReference(string Name) : ValueNode
    {
        public override object? GetValue() => Name;

        private bool IsSystem => Name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase);

        /// <summary>The application property name: <c>user.x</c> and <c>x</c> both mean property <c>x</c>.</summary>
        private string UserPropertyName => Name.StartsWith("user.", StringComparison.OrdinalIgnoreCase) ? Name[5..] : Name;

        public object? Resolve(Message message)
        {
            if (IsSystem)
            {
                return ResolveSystemProperty(message, Name[4..]);
            }

            return FindUserPropertyKey(message) is { } key ? message.ApplicationProperties!.Map[key] : null;
        }

        public void Assign(Message message, object? value)
        {
            if (IsSystem)
            {
                AssignSystemProperty(message, Name[4..], value);
                return;
            }

            message.ApplicationProperties ??= new ApplicationProperties();
            // Overwrite an existing property under its own key (names match case-insensitively; keep its casing).
            message.ApplicationProperties.Map[FindUserPropertyKey(message) ?? UserPropertyName] = value;
        }

        public void Remove(Message message)
        {
            if (IsSystem)
            {
                AssignSystemProperty(message, Name[4..], null); // system properties are fields: removing clears them
                return;
            }

            if (FindUserPropertyKey(message) is { } key) message.ApplicationProperties!.Map.Remove(key);
        }

        private object? FindUserPropertyKey(Message message)
        {
            if (message.ApplicationProperties == null) return null;
            foreach (var key in message.ApplicationProperties.Map.Keys)
            {
                if (key is string stringKey && string.Equals(stringKey, UserPropertyName, StringComparison.OrdinalIgnoreCase))
                    return key;
            }
            return null;
        }

        private static void AssignSystemProperty(Message message, string propertyName, object? value)
        {
            if (!SystemProperties.TryGetValue(propertyName, out var property) || property.Set is null)
                throw new ArgumentException($"System property 'sys.{propertyName}' cannot be set by a rule action.");
            message.Properties ??= new Properties();
            property.Set(message.Properties, value?.ToString());
        }

        private static object? ResolveSystemProperty(Message message, string propertyName) =>
            message.Properties is { } properties && SystemProperties.TryGetValue(propertyName, out var property)
                ? property.Get(properties)
                : null;

        /// <summary>Throws if a rule action may not SET or REMOVE this reference (an unknown or read-only system property).</summary>
        public void EnsureAssignable()
        {
            if (IsSystem && (!SystemProperties.TryGetValue(Name[4..], out var property) || property.Set is null))
                throw new ArgumentException($"System property '{Name}' cannot be set by a rule action.");
        }
    }

    /// <summary>The <c>sys.</c> properties rules can read, and which of them actions can set (null setter: read-only).</summary>
    private static readonly Dictionary<string, (Func<Properties, object?> Get, Action<Properties, string?>? Set)> SystemProperties =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["CorrelationId"] = (p => p.CorrelationId, (p, v) => p.CorrelationId = v),
            ["MessageId"] = (p => p.MessageId, (p, v) => p.MessageId = v),
            ["To"] = (p => p.To, (p, v) => p.To = v),
            ["ReplyTo"] = (p => p.ReplyTo, (p, v) => p.ReplyTo = v),
            ["Label"] = (p => p.Subject, (p, v) => p.Subject = v),
            ["Subject"] = (p => p.Subject, (p, v) => p.Subject = v),
            ["SessionId"] = (p => p.GroupId, null),
            ["ReplyToSessionId"] = (p => p.ReplyToGroupId, (p, v) => p.ReplyToGroupId = v),
            ["ContentType"] = (p => p.ContentType, (p, v) => p.ContentType = v),
        };

    private sealed class Parser
    {
        private readonly Tokenizer _tokenizer;
        private Token _current;

        public Parser(string input)
        {
            _tokenizer = new Tokenizer(input);
            _current = _tokenizer.NextToken();
        }

        public IExpression ParseExpression() => ParseOr();

        public ActionStatement ParseActionStatement()
        {
            if (IsKeyword("SET"))
            {
                Advance();
                var target = ParseReference();
                target.EnsureAssignable();
                Expect(TokenKind.Equal);
                return new SetStatement(target, ParseValue());
            }

            if (IsKeyword("REMOVE"))
            {
                Advance();
                var target = ParseReference();
                target.EnsureAssignable();
                return new RemoveStatement(target);
            }

            throw new ArgumentException($"Unsupported SQL rule action '{_current.Text}'; expected SET or REMOVE.");
        }

        public void Expect(TokenKind kind)
        {
            if (_current.Kind != kind)
            {
                throw new ArgumentException($"Expected token {kind} but found '{_current.Text}'.");
            }

            Advance();
        }

        private IExpression ParseOr()
        {
            var left = ParseAnd();
            while (IsKeyword("OR"))
            {
                Advance();
                left = new OrExpression(left, ParseAnd());
            }

            return left;
        }

        private IExpression ParseAnd()
        {
            var left = ParseNot();
            while (IsKeyword("AND"))
            {
                Advance();
                left = new AndExpression(left, ParseNot());
            }

            return left;
        }

        private IExpression ParseNot()
        {
            if (IsKeyword("NOT"))
            {
                Advance();
                return new NotExpression(ParseNot());
            }

            return ParsePrimary();
        }

        private IExpression ParsePrimary()
        {
            if (_current.Kind == TokenKind.LeftParen)
            {
                Advance();
                var inner = ParseExpression();
                Expect(TokenKind.RightParen);
                return inner;
            }

            return ParsePredicate();
        }

        private IExpression ParsePredicate()
        {
            var left = ParseReference();

            if (IsKeyword("IS"))
            {
                Advance();
                var negated = false;
                if (IsKeyword("NOT"))
                {
                    Advance();
                    negated = true;
                }

                if (!IsKeyword("NULL"))
                {
                    throw new ArgumentException("Expected NULL after IS in SQL filter.");
                }

                Advance();
                return new IsNullExpression(left, negated);
            }

            if (IsKeyword("IN"))
            {
                Advance();
                Expect(TokenKind.LeftParen);
                var values = new List<ValueNode> { ParseValue() };
                while (_current.Kind == TokenKind.Comma)
                {
                    Advance();
                    values.Add(ParseValue());
                }

                Expect(TokenKind.RightParen);
                return new InExpression(left, values);
            }

            if (IsKeyword("LIKE"))
            {
                Advance();
                return new LikeExpression(left, ParseValue());
            }

            if (_current.Kind is not (TokenKind.Equal or TokenKind.NotEqual or TokenKind.LessThan or TokenKind.LessThanOrEqual or TokenKind.GreaterThan or TokenKind.GreaterThanOrEqual))
            {
                throw new ArgumentException($"Unsupported SQL filter operator '{_current.Text}'.");
            }

            var op = _current.Kind;
            Advance();
            return new ComparisonExpression(left, op, ParseValue());
        }

        private ValueReference ParseReference()
        {
            if (_current.Kind != TokenKind.Identifier)
            {
                throw new ArgumentException($"Expected identifier in SQL filter but found '{_current.Text}'.");
            }

            var identifier = _current.Text;
            Advance();
            return new ValueReference(identifier);
        }

        private ValueNode ParseValue()
        {
            return _current.Kind switch
            {
                TokenKind.String => ConsumeLiteral(_current.Text),
                TokenKind.Number => ConsumeNumber(_current.Text),
                TokenKind.Identifier when IsKeyword("NULL") => ConsumeAndReturn(new LiteralValue(null)),
                TokenKind.Identifier when IsKeyword("TRUE") => ConsumeAndReturn(new LiteralValue(true)),
                TokenKind.Identifier when IsKeyword("FALSE") => ConsumeAndReturn(new LiteralValue(false)),
                TokenKind.Identifier => new ValueReference(ConsumeIdentifier()),
                _ => throw new ArgumentException($"Unsupported SQL filter value '{_current.Text}'.")
            };
        }

        private LiteralValue ConsumeLiteral(string value)
        {
            Advance();
            return new LiteralValue(value);
        }

        private LiteralValue ConsumeNumber(string value)
        {
            Advance();

            if (value.Contains('.') || value.Contains('e', StringComparison.OrdinalIgnoreCase))
            {
                return new LiteralValue(double.Parse(value, CultureInfo.InvariantCulture));
            }

            return new LiteralValue(long.Parse(value, CultureInfo.InvariantCulture));
        }

        private string ConsumeIdentifier()
        {
            var value = _current.Text;
            Advance();
            return value;
        }

        private T ConsumeAndReturn<T>(T value)
        {
            Advance();
            return value;
        }

        private bool IsKeyword(string keyword)
        {
            return _current.Kind == TokenKind.Identifier &&
                   string.Equals(_current.Text, keyword, StringComparison.OrdinalIgnoreCase);
        }

        private void Advance() => _current = _tokenizer.NextToken();
    }

    private readonly record struct Token(TokenKind Kind, string Text);

    private enum TokenKind
    {
        End,
        Identifier,
        String,
        Number,
        LeftParen,
        RightParen,
        Comma,
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual
    }

    private sealed class Tokenizer
    {
        private readonly string _input;
        private int _index;

        public Tokenizer(string input)
        {
            _input = input;
        }

        public Token NextToken()
        {
            SkipWhitespace();
            if (_index >= _input.Length)
            {
                return new Token(TokenKind.End, string.Empty);
            }

            var current = _input[_index];
            return current switch
            {
                '(' => TakeSingle(TokenKind.LeftParen),
                ')' => TakeSingle(TokenKind.RightParen),
                ',' => TakeSingle(TokenKind.Comma),
                '=' => TakeSingle(TokenKind.Equal),
                '<' => TakeLessThan(),
                '>' => TakeGreaterThan(),
                '\'' => TakeString(),
                _ when char.IsDigit(current) || current == '-' => TakeNumber(),
                _ when IsIdentifierStart(current) => TakeIdentifier(),
                _ => throw new ArgumentException($"Unexpected SQL filter character '{current}'.")
            };
        }

        private Token TakeSingle(TokenKind kind)
        {
            var text = _input[_index].ToString();
            _index++;
            return new Token(kind, text);
        }

        private Token TakeLessThan()
        {
            _index++;
            if (TryTake('='))
            {
                return new Token(TokenKind.LessThanOrEqual, "<=");
            }

            if (TryTake('>'))
            {
                return new Token(TokenKind.NotEqual, "<>");
            }

            return new Token(TokenKind.LessThan, "<");
        }

        private Token TakeGreaterThan()
        {
            _index++;
            if (TryTake('='))
            {
                return new Token(TokenKind.GreaterThanOrEqual, ">=");
            }

            return new Token(TokenKind.GreaterThan, ">");
        }

        private Token TakeString()
        {
            _index++;
            var builder = new StringBuilder();

            while (_index < _input.Length)
            {
                var current = _input[_index++];
                if (current == '\'')
                {
                    if (_index < _input.Length && _input[_index] == '\'')
                    {
                        builder.Append('\'');
                        _index++;
                        continue;
                    }

                    return new Token(TokenKind.String, builder.ToString());
                }

                builder.Append(current);
            }

            throw new ArgumentException("Unterminated string literal in SQL filter.");
        }

        private Token TakeNumber()
        {
            var start = _index;
            if (_input[_index] == '-')
            {
                _index++;
            }

            while (_index < _input.Length && (char.IsDigit(_input[_index]) || _input[_index] == '.'))
            {
                _index++;
            }

            if (_index < _input.Length && (_input[_index] == 'e' || _input[_index] == 'E'))
            {
                _index++;
                if (_index < _input.Length && (_input[_index] == '+' || _input[_index] == '-'))
                {
                    _index++;
                }

                while (_index < _input.Length && char.IsDigit(_input[_index]))
                {
                    _index++;
                }
            }

            return new Token(TokenKind.Number, _input[start.._index]);
        }

        private Token TakeIdentifier()
        {
            var start = _index;
            _index++;
            while (_index < _input.Length && IsIdentifierPart(_input[_index]))
            {
                _index++;
            }

            return new Token(TokenKind.Identifier, _input[start.._index]);
        }

        private bool TryTake(char expected)
        {
            if (_index < _input.Length && _input[_index] == expected)
            {
                _index++;
                return true;
            }

            return false;
        }

        private void SkipWhitespace()
        {
            while (_index < _input.Length && char.IsWhiteSpace(_input[_index]))
            {
                _index++;
            }
        }

        private static bool IsIdentifierStart(char value) => char.IsLetter(value) || value is '_' or '$';

        private static bool IsIdentifierPart(char value) => char.IsLetterOrDigit(value) || value is '_' or '.' or '$';
    }

    private static bool Compare(object? left, object? right, TokenKind op)
    {
        if (left == null || right == null)
        {
            return false;
        }

        if (TryConvertNumber(left, out var leftNumber) && TryConvertNumber(right, out var rightNumber))
        {
            return op switch
            {
                TokenKind.Equal => leftNumber == rightNumber,
                TokenKind.NotEqual => leftNumber != rightNumber,
                TokenKind.LessThan => leftNumber < rightNumber,
                TokenKind.LessThanOrEqual => leftNumber <= rightNumber,
                TokenKind.GreaterThan => leftNumber > rightNumber,
                TokenKind.GreaterThanOrEqual => leftNumber >= rightNumber,
                _ => false
            };
        }

        if (left is bool leftBool && TryConvertBool(right, out var rightBool))
        {
            return op switch
            {
                TokenKind.Equal => leftBool == rightBool,
                TokenKind.NotEqual => leftBool != rightBool,
                _ => false
            };
        }

        var comparison = string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
        return op switch
        {
            TokenKind.Equal => comparison == 0,
            TokenKind.NotEqual => comparison != 0,
            TokenKind.LessThan => comparison < 0,
            TokenKind.LessThanOrEqual => comparison <= 0,
            TokenKind.GreaterThan => comparison > 0,
            TokenKind.GreaterThanOrEqual => comparison >= 0,
            _ => false
        };
    }

    private static bool TryConvertNumber(object value, out decimal number)
    {
        switch (value)
        {
            case byte b:
                number = b;
                return true;
            case short s:
                number = s;
                return true;
            case int i:
                number = i;
                return true;
            case long l:
                number = l;
                return true;
            case float f:
                number = (decimal)f;
                return true;
            case double d:
                number = (decimal)d;
                return true;
            case decimal m:
                number = m;
                return true;
            case string str when decimal.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed):
                number = parsed;
                return true;
            default:
                number = default;
                return false;
        }
    }

    private static bool TryConvertBool(object value, out bool boolean)
    {
        switch (value)
        {
            case bool b:
                boolean = b;
                return true;
            case string str when bool.TryParse(str, out var parsed):
                boolean = parsed;
                return true;
            default:
                boolean = default;
                return false;
        }
    }
}
