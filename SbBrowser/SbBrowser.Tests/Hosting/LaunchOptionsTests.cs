using SbBrowser.App;

namespace SbBrowser.Tests.Hosting;

/// <summary>The standalone app's own command-line options; everything else goes to ASP.NET Core.</summary>
public class LaunchOptionsTests
{
    [Fact]
    public void ByDefault_ItListensOnLoopbackPort8081_AndOpensTheBrowser()
    {
        var options = LaunchOptions.Parse([]);

        Assert.Null(options.Error);
        Assert.Equal("http://127.0.0.1:8081", options.LoopbackUrl);
        Assert.True(options.OpenBrowser);
        Assert.Empty(options.HostArgs);
    }

    [Theory]
    [InlineData("--port", "9000")]
    [InlineData("--port=9000")]
    public void PortChoosesTheLoopbackPort(params string[] args)
    {
        var options = LaunchOptions.Parse(args);

        Assert.Null(options.Error);
        Assert.Equal("http://127.0.0.1:9000", options.LoopbackUrl);
    }

    [Fact]
    public void NoBrowser_SkipsOpeningIt()
    {
        Assert.False(LaunchOptions.Parse(["--no-browser"]).OpenBrowser);
    }

    [Fact]
    public void OtherArguments_PassThroughToTheHost()
    {
        var options = LaunchOptions.Parse(["--urls", "http://0.0.0.0:5000", "--no-browser", "--Logging:LogLevel:Default=Warning"]);

        Assert.Equal(["--urls", "http://0.0.0.0:5000", "--Logging:LogLevel:Default=Warning"], options.HostArgs);
    }

    [Theory]
    [InlineData("--port")]
    [InlineData("--port", "abc")]
    [InlineData("--port", "70000")]
    [InlineData("--port=-1")]
    public void ABadPort_IsAnError(params string[] args)
    {
        Assert.NotNull(LaunchOptions.Parse(args).Error);
    }
}
