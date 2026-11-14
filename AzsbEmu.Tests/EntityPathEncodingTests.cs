using AzsbEmu.Server.Core;

namespace AzsbEmu.Tests;

public class EntityPathEncodingTests
{
    [Theory]
    [InlineData("orders", "orders")]
    [InlineData("Orders-1", "Orders-1")]
    [InlineData("a/b", "a_2Fb")]
    [InlineData("a_b", "a_5Fb")]
    [InlineData("myapp.contracts/ordersubmitted", "myapp_2Econtracts_2Fordersubmitted")]
    [InlineData(".", "_2E")]
    [InlineData("..", "_2E_2E")]
    [InlineData("a.", "a_2E")]
    [InlineData("a b", "a_20b")]
    [InlineData("é", "_C3_A9")]          // 2-byte UTF-8
    [InlineData("a😀b", "a_F0_9F_98_80b")] // surrogate pair → 4-byte UTF-8
    public void EncodePathSegment_KnownExamples(string name, string expected)
    {
        Assert.Equal(expected, EntityManager.EncodePathSegment(name));
    }

    [Fact]
    public void EncodePathSegment_NamesThatTheLegacyLayoutMerged_StayDistinct()
    {
        var names = new[] { "a/b", "a_b", "a_2Fb", "a.b", "a-b", "a b" };
        var encoded = names.Select(EntityManager.EncodePathSegment).ToList();
        Assert.Equal(names.Length, encoded.Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public void EncodePathSegment_OutputContainsOnlyPortableCharactersAndNoDots()
    {
        string encoded = EntityManager.EncodePathSegment("weird:name*with?<every>|\"char\\./");
        Assert.Matches("^[A-Za-z0-9_-]+$", encoded);
    }
}
