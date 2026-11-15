using SbBrowser.Core.Connections;

namespace SbBrowser.Tests.Connections;

public sealed class ConnectionProfileStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"sbbrowser-profiles-{Guid.NewGuid():N}");

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private JsonFileProfileStore NewStore() => new(Path.Combine(_dir, "nested", "profiles.json"));

    [Fact]
    public async Task AMissingFile_ListsNothing()
    {
        Assert.Empty(await NewStore().ListAsync());
    }

    [Fact]
    public async Task Profiles_RoundTripThroughTheFile_IncludingPorts()
    {
        var emulator = new ConnectionProfile("Local emulator", "Endpoint=sb://localhost/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true", 8080, 5673);
        var azure = new ConnectionProfile("Azure", "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=secret");
        await NewStore().SaveAsync(emulator);
        await NewStore().SaveAsync(azure);

        var listed = await NewStore().ListAsync();

        Assert.Equal([azure, emulator], listed); // sorted by name
    }

    [Fact]
    public async Task SavingAnExistingName_ReplacesIt_IgnoringCase()
    {
        var store = NewStore();
        await store.SaveAsync(new ConnectionProfile("Dev", "Endpoint=sb://one/;SharedAccessKeyName=a;SharedAccessKey=b"));
        await store.SaveAsync(new ConnectionProfile("dev", "Endpoint=sb://two/;SharedAccessKeyName=a;SharedAccessKey=b"));

        var profile = Assert.Single(await store.ListAsync());
        Assert.Contains("sb://two/", profile.ConnectionString);
    }

    [Fact]
    public async Task Remove_DeletesByName_AndIgnoresUnknownNames()
    {
        var store = NewStore();
        await store.SaveAsync(new ConnectionProfile("Keep", "Endpoint=sb://one/;SharedAccessKeyName=a;SharedAccessKey=b"));
        await store.SaveAsync(new ConnectionProfile("Drop", "Endpoint=sb://two/;SharedAccessKeyName=a;SharedAccessKey=b"));

        await store.RemoveAsync("DROP");
        await store.RemoveAsync("never-saved");

        Assert.Equal("Keep", Assert.Single(await store.ListAsync()).Name);
    }

    [Fact]
    public async Task TheFixedStore_ListsItsProfile_AndRefusesChanges()
    {
        var profile = new ConnectionProfile("This emulator", "Endpoint=sb://localhost/;SharedAccessKeyName=Root;SharedAccessKey=dummy;UseDevelopmentEmulator=true");
        var store = new FixedProfileStore(profile);

        Assert.True(store.IsReadOnly);
        Assert.Equal([profile], await store.ListAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(profile));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RemoveAsync(profile.Name));
    }
}
