using SbBrowser.Core.Messages;

namespace SbBrowser.Tests.Messages;

public class ApplicationPropertiesTests
{
    [Fact]
    public void EachRow_BecomesAValueOfItsChosenType()
    {
        var result = ApplicationProperties.Parse([
            new PropertyEntry("tenant", PropertyType.String, "acme"),
            new PropertyEntry("priority", PropertyType.Integer, "3"),
            new PropertyEntry("ratio", PropertyType.Number, "0.25"),
            new PropertyEntry("urgent", PropertyType.Boolean, "true"),
        ]);

        Assert.Empty(result.Errors);
        Assert.Equal("acme", result.Properties["tenant"]);
        Assert.Equal(3L, result.Properties["priority"]);
        Assert.Equal(0.25, result.Properties["ratio"]);
        Assert.Equal(true, result.Properties["urgent"]);
    }

    [Theory]
    [InlineData(PropertyType.Integer, "3.5", "'priority' is not a whole number: 3.5")]
    [InlineData(PropertyType.Number, "lots", "'priority' is not a number: lots")]
    [InlineData(PropertyType.Boolean, "yes", "'priority' must be true or false: yes")]
    public void AValueThatDoesNotFitItsType_IsAnError(PropertyType type, string value, string error)
    {
        var result = ApplicationProperties.Parse([new PropertyEntry("priority", type, value)]);

        Assert.Equal([error], result.Errors);
        Assert.Empty(result.Properties);
    }

    [Fact]
    public void ABlankRow_IsIgnored_ButAValueWithNoName_IsAnError()
    {
        var result = ApplicationProperties.Parse([
            new PropertyEntry("", PropertyType.String, ""),
            new PropertyEntry("  ", PropertyType.String, "orphan"),
        ]);

        Assert.Equal(["A property has a value but no name: orphan"], result.Errors);
        Assert.Empty(result.Properties);
    }

    [Fact]
    public void NamesAreTrimmed_AndARepeatedName_IsAnError()
    {
        var result = ApplicationProperties.Parse([
            new PropertyEntry(" tenant ", PropertyType.String, "acme"),
            new PropertyEntry("tenant", PropertyType.String, "other"),
        ]);

        Assert.Equal(["'tenant' is set more than once."], result.Errors);
        Assert.Equal("acme", result.Properties["tenant"]);
    }
}
