using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionStore"/>: the round-trip through
/// the dogfooded <c>sys-schema-version</c> tree (set / get / clear / list),
/// argument validation, and the reserved-tree guard on writes. Exercised against
/// an in-memory <see cref="ILattice"/> so no cluster is required.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaVersionStoreTests
{
    private static (LatticeSchemaVersionStore Store, SortedDictionary<string, byte[]> Backing) CreateStore()
    {
        var backing = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var lattice = InMemoryLatticeFake.Create(backing);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(SchemaConstants.VersionConfigTree).Returns(lattice);
        return (new LatticeSchemaVersionStore(grainFactory), backing);
    }

    [Test]
    public async Task SetConfigAsync_then_GetConfigAsync_round_trips_the_config()
    {
        var (store, _) = CreateStore();
        var config = new LatticeSchemaVersionConfig(schemaId: 7, targetVersion: 3, strictIngest: true);

        await store.SetConfigAsync("orders", config);
        var read = await store.GetConfigAsync("orders");

        Assert.That(read, Is.EqualTo(config));
    }

    [Test]
    public async Task GetConfigAsync_missing_tree_returns_default()
    {
        var (store, _) = CreateStore();

        // A missing key deserializes to the value-type default (the reserved
        // "unversioned" sentinel: TargetVersion 0), not a null nullable.
        Assert.That(await store.GetConfigAsync("orders"), Is.EqualTo(default(LatticeSchemaVersionConfig)));
    }

    [Test]
    public void SetConfigAsync_empty_tree_throws()
    {
        var (store, _) = CreateStore();

        Assert.That(
            () => store.SetConfigAsync("", new LatticeSchemaVersionConfig(1, 1)),
            Throws.ArgumentException);
    }

    [Test]
    public void SetConfigAsync_reserved_tree_throws()
    {
        var (store, _) = CreateStore();

        Assert.That(
            () => store.SetConfigAsync("sys-schema-version", new LatticeSchemaVersionConfig(1, 1)),
            Throws.ArgumentException);
    }

    [Test]
    public void GetConfigAsync_empty_tree_throws()
    {
        var (store, _) = CreateStore();

        Assert.That(() => store.GetConfigAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ClearConfigAsync_removes_an_existing_config()
    {
        var (store, _) = CreateStore();
        await store.SetConfigAsync("orders", new LatticeSchemaVersionConfig(1, 5));

        var cleared = await store.ClearConfigAsync("orders");

        Assert.That(cleared, Is.True);
        Assert.That(await store.GetConfigAsync("orders"), Is.EqualTo(default(LatticeSchemaVersionConfig)));
    }

    [Test]
    public async Task ClearConfigAsync_missing_config_returns_false()
    {
        var (store, _) = CreateStore();

        Assert.That(await store.ClearConfigAsync("orders"), Is.False);
    }

    [Test]
    public void ClearConfigAsync_empty_tree_throws()
    {
        var (store, _) = CreateStore();

        Assert.That(() => store.ClearConfigAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ListConfigsAsync_yields_every_stored_config()
    {
        var (store, _) = CreateStore();
        await store.SetConfigAsync("orders", new LatticeSchemaVersionConfig(1, 1));
        await store.SetConfigAsync("users", new LatticeSchemaVersionConfig(2, 4));

        var listed = new Dictionary<string, LatticeSchemaVersionConfig>(StringComparer.Ordinal);
        await foreach (var pair in store.ListConfigsAsync())
        {
            listed[pair.Key] = pair.Value;
        }

        Assert.That(listed, Has.Count.EqualTo(2));
        Assert.That(listed["orders"].TargetVersion, Is.EqualTo(1u));
        Assert.That(listed["users"].TargetVersion, Is.EqualTo(4u));
    }

    [Test]
    public async Task ListConfigsAsync_empty_tree_yields_nothing()
    {
        var (store, _) = CreateStore();

        var any = false;
        await foreach (var _ in store.ListConfigsAsync())
        {
            any = true;
        }

        Assert.That(any, Is.False);
    }
}
