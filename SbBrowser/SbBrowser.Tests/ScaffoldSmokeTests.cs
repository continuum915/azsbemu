using Azure.Messaging.ServiceBus;
using AzsbEmu.Tests.IntegrationTests;
using Microsoft.AspNetCore.Mvc.Testing;

namespace SbBrowser.Tests;

public class ScaffoldSmokeTests
{
    [Fact]
    public async Task TheInProcessEmulator_AcceptsAnSdkClient()
    {
        using var broker = new BrokerFixture();
        broker.EnsureQueue("smoke");
        await using var client = broker.CreateServiceBusClient();

        await using (var sender = client.CreateSender("smoke"))
            await sender.SendMessageAsync(new ServiceBusMessage("hello"));
        await using var receiver = client.CreateReceiver("smoke");
        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

        Assert.Equal("hello", message?.Body.ToString());
    }

    [Fact]
    public async Task TheStandaloneApp_ServesItsPageScriptsAndStyles()
    {
        await using var app = new WebApplicationFactory<SbBrowser.App.AppEntryPoint>();
        using var http = app.CreateClient();

        // Pages render interactively only (no prerendering), so the static page is the shell: title and assets.
        var page = await http.GetStringAsync("/");
        Assert.Contains("<title>SbBrowser</title>", page);

        foreach (string pattern in new[]
                 {
                     "src=\"(_framework/blazor\\.web[^\"]*\\.js)\"",
                     "href=\"(_content/SbBrowser\\.Web/sbbrowser[^\"]*\\.css)\"",
                     "src=\"(_content/SbBrowser\\.Web/sbbrowser[^\"]*\\.js)\"",
                 })
        {
            var asset = System.Text.RegularExpressions.Regex.Match(page, pattern);
            Assert.True(asset.Success, $"the page does not reference {pattern}");
            var response = await http.GetAsync("/" + asset.Groups[1].Value);
            Assert.True(response.IsSuccessStatusCode, $"{asset.Groups[1].Value} returned {(int)response.StatusCode}");
        }
    }
}
