using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

[TestFixture]
public class JsonExplorerConfigStoreTests
{
    private string _directory = string.Empty;
    private string _filePath = string.Empty;

    [SetUp]
    public void SetUp()
    {
        _directory = Path.Combine(Path.GetTempPath(), "lattice-explorer-tests", Guid.NewGuid().ToString("n"));
        _filePath = Path.Combine(_directory, "config.json");
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private JsonExplorerConfigStore NewStore() => new(new ExplorerConfigStoreOptions { FilePath = _filePath });

    [Test]
    public async Task LoadAsync_WhenMissing_ReturnsNull()
    {
        var store = NewStore();

        Assert.That(store.Exists, Is.False);
        Assert.That(await store.LoadAsync(), Is.Null);
    }

    [Test]
    public async Task SaveAsync_ThenLoadAsync_RoundTrips()
    {
        var store = NewStore();
        var config = new ExplorerConfiguration
        {
            Endpoint = "https://host:443",
            AllowUnencryptedHttp2 = true,
            Headers = new Dictionary<string, string> { ["authorization"] = "Bearer t" },
        };

        await store.SaveAsync(config);
        var loaded = await store.LoadAsync();

        Assert.That(store.Exists, Is.True);
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.Endpoint, Is.EqualTo("https://host:443"));
        Assert.That(loaded.AllowUnencryptedHttp2, Is.True);
        Assert.That(loaded.Headers, Is.Not.Null);
        Assert.That(loaded.Headers!["authorization"], Is.EqualTo("Bearer t"));
    }

    [Test]
    public async Task SaveAsync_CreatesMissingDirectory()
    {
        var store = NewStore();

        await store.SaveAsync(new ExplorerConfiguration { Endpoint = "https://host" });

        Assert.That(File.Exists(_filePath), Is.True);
    }

    [Test]
    public async Task LoadAsync_WhenMalformed_ReturnsNull()
    {
        Directory.CreateDirectory(_directory);
        await File.WriteAllTextAsync(_filePath, "{ this is not valid json ]");
        var store = NewStore();

        Assert.That(store.Exists, Is.True);
        Assert.That(await store.LoadAsync(), Is.Null);
    }

    [Test]
    public async Task SaveAsync_OverwritesExistingDocument()
    {
        var store = NewStore();
        await store.SaveAsync(new ExplorerConfiguration { Endpoint = "https://first" });
        await store.SaveAsync(new ExplorerConfiguration { Endpoint = "https://second" });

        var loaded = await store.LoadAsync();

        Assert.That(loaded!.Endpoint, Is.EqualTo("https://second"));
        Assert.That(File.Exists(_filePath + ".tmp"), Is.False);
    }
}
