using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="TypedLatticeExtensions"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
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

    // --- Predicate push-down (GetMany) ---

    [Test]
    public async Task GetMany_with_predicate_returns_only_matching_values()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-getmany-pred");
        await tree.SetAsync("a", new Product("Cheap", 5.00m));
        await tree.SetAsync("b", new Product("Mid", 50.00m));
        await tree.SetAsync("c", new Product("Pricey", 500.00m));

        var result = await tree.GetManyAsync<Product>(
            ["a", "b", "c"],
            p => p.Price > 40m);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { "b", "c" }));
        Assert.That(result["b"], Is.EqualTo(new Product("Mid", 50.00m)));
        Assert.That(result["c"], Is.EqualTo(new Product("Pricey", 500.00m)));
    }

    [Test]
    public async Task GetMany_with_predicate_omits_missing_and_nonmatching_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-getmany-pred-miss");
        await tree.SetAsync("a", new Product("A", 1.00m));
        await tree.SetAsync("b", new Product("B", 99.00m));

        var result = await tree.GetManyAsync<Product>(
            ["a", "b", "missing"],
            p => p.Name == "B");

        Assert.That(result.Keys, Is.EquivalentTo(new[] { "b" }));
    }

    [Test]
    public async Task GetMany_with_predicate_matching_none_returns_empty()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-getmany-pred-none");
        await tree.SetAsync("a", new Product("A", 1.00m));
        await tree.SetAsync("b", new Product("B", 2.00m));

        var result = await tree.GetManyAsync<Product>(
            ["a", "b"],
            p => p.Price > 1000m);

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task GetMany_with_predicate_honours_explicit_serializer()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("typed-getmany-pred-ser");
        var serializer = new JsonLatticeSerializer<Product>();
        await tree.SetAsync("a", new Product("A", 10.00m), serializer);
        await tree.SetAsync("b", new Product("B", 20.00m), serializer);

        var result = await tree.GetManyAsync<Product>(
            ["a", "b"],
            p => p.Price >= 20m,
            serializer);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { "b" }));
    }

    // --- Predicate push-down (streaming scans) ---

    private async Task<ILattice> SeededScanTreeAsync(string id)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(id);
        await tree.SetAsync("k1", new Product("A", 5.00m));
        await tree.SetAsync("k2", new Product("B", 15.00m));
        await tree.SetAsync("k3", new Product("C", 25.00m));
        await tree.SetAsync("k4", new Product("D", 35.00m));
        return tree;
    }

    [Test]
    public async Task ScanKeys_with_predicate_returns_only_matching_keys()
    {
        var tree = await SeededScanTreeAsync("scan-keys-pred");

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync<Product>(p => p.Price >= 15m))
            keys.Add(k);

        Assert.That(keys, Is.EqualTo(new[] { "k2", "k3", "k4" }));
    }

    [Test]
    public async Task ScanKeys_with_predicate_preserves_reverse_order()
    {
        var tree = await SeededScanTreeAsync("scan-keys-pred-rev");

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync<Product>(p => p.Price >= 15m, reverse: true))
            keys.Add(k);

        Assert.That(keys, Is.EqualTo(new[] { "k4", "k3", "k2" }));
    }

    [Test]
    public async Task ScanEntries_with_predicate_returns_only_matching_entries()
    {
        var tree = await SeededScanTreeAsync("scan-entries-pred");

        var entries = new List<KeyValuePair<string, Product>>();
        await foreach (var e in tree.ScanEntriesAsync<Product>(p => p.Price < 20m))
            entries.Add(e);

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2" }));
        Assert.That(entries[0].Value, Is.EqualTo(new Product("A", 5.00m)));
        Assert.That(entries[1].Value, Is.EqualTo(new Product("B", 15.00m)));
    }

    [Test]
    public async Task ScanValues_with_predicate_yields_only_matching_values()
    {
        var tree = await SeededScanTreeAsync("scan-values-pred");

        var values = new List<Product>();
        await foreach (var v in tree.ScanValuesAsync<Product>(p => p.Price > 20m))
            values.Add(v);

        Assert.That(values, Is.EqualTo(new[] { new Product("C", 25.00m), new Product("D", 35.00m) }));
    }

    [Test]
    public async Task ScanValues_without_predicate_yields_all_values_in_order()
    {
        var tree = await SeededScanTreeAsync("scan-values-all");

        var values = new List<Product>();
        await foreach (var v in tree.ScanValuesAsync<Product>())
            values.Add(v);

        Assert.That(values, Is.EqualTo(new[]
        {
            new Product("A", 5.00m),
            new Product("B", 15.00m),
            new Product("C", 25.00m),
            new Product("D", 35.00m),
        }));
    }

    [Test]
    public async Task ScanEntries_with_predicate_honours_explicit_serializer()
    {
        var tree = await SeededScanTreeAsync("scan-entries-pred-ser");
        var serializer = new JsonLatticeSerializer<Product>();

        var keys = new List<string>();
        await foreach (var e in tree.ScanEntriesAsync<Product>(p => p.Name == "C", serializer))
            keys.Add(e.Key);

        Assert.That(keys, Is.EqualTo(new[] { "k3" }));
    }

    [Test]
    public async Task ScanKeys_with_predicate_respects_range_bounds()
    {
        var tree = await SeededScanTreeAsync("scan-keys-pred-range");

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync<Product>(p => p.Price >= 5m, startInclusive: "k2", endExclusive: "k4"))
            keys.Add(k);

        Assert.That(keys, Is.EqualTo(new[] { "k2", "k3" }));
    }
}
