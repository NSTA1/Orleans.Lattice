using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="TypedLatticeExtensions"/>.
/// </summary>
[TestFixture]
public class TypedLatticeExtensionsIntegrationTests
{
    private record Product(string Name, decimal Price);

    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Set_and_Get_roundtrips_typed_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-roundtrip");
        var product = new Product("Widget", 9.99m);

        await tree.SetAsync("p1", product);
        var result = await tree.GetAsync<Product>("p1");

        Assert.That(result, Is.EqualTo(product));
    }

    [Test]
    public async Task Get_returns_default_for_missing_key()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-miss");

        var result = await tree.GetAsync<Product>("nonexistent");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Set_overwrites_with_typed_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-overwrite");
        await tree.SetAsync("p1", new Product("Old", 1.00m));
        await tree.SetAsync("p1", new Product("New", 2.00m));

        var result = await tree.GetAsync<Product>("p1");

        Assert.That(result, Is.EqualTo(new Product("New", 2.00m)));
    }

    [Test]
    public async Task GetMany_deserializes_multiple_values()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-getmany");
        await tree.SetAsync("a", new Product("A", 1.00m));
        await tree.SetAsync("b", new Product("B", 2.00m));

        var result = await tree.GetManyAsync<Product>(["a", "b", "missing"]);

        Assert.That(result, Has.Count.EqualTo(2));
        Assert.That(result["a"], Is.EqualTo(new Product("A", 1.00m)));
        Assert.That(result["b"], Is.EqualTo(new Product("B", 2.00m)));
    }

    [Test]
    public async Task SetMany_then_GetMany_roundtrips()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-setmany");
        var entries = new List<KeyValuePair<string, Product>>
        {
            new("x", new Product("X", 10.00m)),
            new("y", new Product("Y", 20.00m)),
        };

        await tree.SetManyAsync(entries);

        var result = await tree.GetManyAsync<Product>(["x", "y"]);
        Assert.That(result["x"], Is.EqualTo(new Product("X", 10.00m)));
        Assert.That(result["y"], Is.EqualTo(new Product("Y", 20.00m)));
    }

    [Test]
    public async Task Typed_set_is_readable_by_raw_get()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-raw-compat");
        var product = new Product("RawTest", 5.00m);

        await tree.SetAsync("p1", product);

        var raw = await tree.GetAsync("p1");
        Assert.That(raw, Is.Not.Null);

        var deserialized = System.Text.Json.JsonSerializer.Deserialize<Product>(raw);
        Assert.That(deserialized, Is.EqualTo(product));
    }

    [Test]
    public async Task Custom_serializer_roundtrips()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-custom-ser");
        var serializer = new JsonLatticeSerializer<Product>(
            new System.Text.Json.JsonSerializerOptions
            {
                PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase,
            });

        await tree.SetAsync("p1", new Product("Custom", 7.00m), serializer);
        var result = await tree.GetAsync("p1", serializer);

        Assert.That(result, Is.EqualTo(new Product("Custom", 7.00m)));
    }

    [Test]
    public async Task Delete_works_after_typed_set()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-delete");
        await tree.SetAsync("p1", new Product("ToDelete", 1.00m));

        var deleted = await tree.DeleteAsync("p1");
        Assert.That(deleted, Is.True);

        var result = await tree.GetAsync<Product>("p1");
        Assert.That(result, Is.Null);
    }

    // --- TTL ---

    [Test]
    public async Task Set_with_ttl_expires_entry_after_window()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-ttl-expire");
        await tree.SetAsync("p1", new Product("Ephemeral", 1.00m), TimeSpan.FromMilliseconds(50));

        // Give the expiry a small cushion past the TTL.
        await Task.Delay(200);

        var result = await tree.GetAsync<Product>("p1");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Set_with_ttl_visible_before_expiry()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-ttl-live");
        var product = new Product("Ephemeral", 2.00m);
        await tree.SetAsync("p1", product, TimeSpan.FromMinutes(10));

        var result = await tree.GetAsync<Product>("p1");
        Assert.That(result, Is.EqualTo(product));
    }

    [Test]
    public void Set_with_zero_ttl_throws()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-ttl-zero");
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            tree.SetAsync("p1", new Product("x", 1m), TimeSpan.Zero));
    }
}
