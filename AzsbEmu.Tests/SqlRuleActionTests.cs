using Amqp;
using Amqp.Framing;
using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests;

public class SqlRuleActionTests
{
    private static Message NewMessage()
    {
        var message = new Message("body")
        {
            Properties = new Properties { Subject = "original", CorrelationId = "c1" },
            ApplicationProperties = new ApplicationProperties(),
        };
        message.ApplicationProperties["Region"] = "eu";
        message.ApplicationProperties["secret"] = "x";
        return message;
    }

    [Fact]
    public void Statements_SeparatedBySemicolons_AreAppliedInOrder_IgnoringSemicolonsInsideStrings()
    {
        var message = NewMessage();

        SqlFilterEvaluator.ApplyAction("SET note = 'a;b'; SET note = note; REMOVE secret;", message);

        Assert.Equal("a;b", message.ApplicationProperties["note"]);
        Assert.False(message.ApplicationProperties.Map.ContainsKey("secret"));
    }

    [Fact]
    public void UserPrefix_AndPropertyNameCasing_ResolveToTheSameApplicationProperty()
    {
        var message = NewMessage();

        SqlFilterEvaluator.ApplyAction("SET user.REGION = 'us'; SET copy = user.region", message);

        Assert.Equal("us", message.ApplicationProperties["Region"]);
        Assert.Equal("us", message.ApplicationProperties["copy"]);
        Assert.False(message.ApplicationProperties.Map.ContainsKey("REGION"));
    }

    [Fact]
    public void LiteralTypes_AreKept()
    {
        var message = NewMessage();

        SqlFilterEvaluator.ApplyAction("SET n = 42; SET d = 1.5; SET b = TRUE; SET z = NULL", message);

        Assert.Equal(42L, message.ApplicationProperties["n"]);
        Assert.Equal(1.5, message.ApplicationProperties["d"]);
        Assert.Equal(true, message.ApplicationProperties["b"]);
        Assert.Null(message.ApplicationProperties["z"]);
    }

    [Fact]
    public void SystemProperties_CanBeSetAndRemoved()
    {
        var message = NewMessage();

        SqlFilterEvaluator.ApplyAction("SET sys.Label = 'renamed'; REMOVE sys.CorrelationId", message);

        Assert.Equal("renamed", message.Properties.Subject);
        Assert.Null(message.Properties.CorrelationId);
    }

    [Theory]
    [InlineData("UPDATE x")]
    [InlineData("SET a = b + 1")]
    [InlineData("SET a")]
    [InlineData("SET sys.SessionId = 'x'")]      // read-only
    [InlineData("REMOVE sys.DeliveryCount")]     // not a known system property
    public void InvalidActions_AreRejectedByValidation(string action)
    {
        Assert.ThrowsAny<ArgumentException>(() => SqlFilterEvaluator.ValidateAction(action));
    }

    [Fact]
    public void AnEmptyAction_IsValidAndChangesNothing()
    {
        var message = NewMessage();

        SqlFilterEvaluator.ValidateAction("");
        SqlFilterEvaluator.ApplyAction("  ;  ", message);

        Assert.Equal("original", message.Properties.Subject);
        Assert.Equal(2, message.ApplicationProperties.Map.Count);
    }
}
