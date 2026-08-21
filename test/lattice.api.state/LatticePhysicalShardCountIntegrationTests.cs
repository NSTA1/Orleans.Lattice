namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage for the physical-shard-count query endpoint
/// (<see cref="ILatticeStateQuery.GetPhysicalShardCountAsync"/>) - the
/// fan-out-free degraded-metrics read that serves the shard count from routing
/// alone. Asserts the happy path against a multi-shard tree, the read-only
/// view-tree path, and the not-found paths for an unknown and a reserved system
/// tree. Reuses <see cref="StructureClusterFixture"/> because that fixture
/// already stands up a single silo with the state API wired and helpers to
/// populate multi-shard and view-backing trees.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticePhysicalShardCountIntegrationTests
{
    private StructureClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new StructureClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task GetPhysicalShardCountAsync_returns_the_routing_physical_shard_count()
    {
        var tree = await _fixture.CreatePopulatedTreeAsync("shardcount-basic", keyCount: 40, shardCount: 3);
        var routing = await tree.GetRoutingAsync();
        var expected = routing.Map.GetPhysicalShardIndices().Count;

        var count = await _fixture.Query.GetPhysicalShardCountAsync("shardcount-basic");

        Assert.That(count, Is.EqualTo(expected));
        Assert.That(count, Is.EqualTo(3), "a freshly-registered 3-shard tree has three physical shards");
    }

    [Test]
    public async Task GetPhysicalShardCountAsync_returns_null_for_unknown_tree()
    {
        var count = await _fixture.Query.GetPhysicalShardCountAsync("no-such-tree");

        Assert.That(count, Is.Null);
    }

    [Test]
    public async Task GetPhysicalShardCountAsync_serves_view_tree_as_read_only()
    {
        await _fixture.RegisterViewBackingTreeAsync("view-shardcount-probe");

        var count = await _fixture.Query.GetPhysicalShardCountAsync("view-shardcount-probe");

        Assert.That(count, Is.Not.Null, "a materialised view is a read-only tree whose shard count is inspectable");
        Assert.That(count, Is.GreaterThanOrEqualTo(1), "an empty view tree still has at least one physical shard");
    }

    [Test]
    public async Task GetPhysicalShardCountAsync_returns_null_for_system_tree()
    {
        var count = await _fixture.Query.GetPhysicalShardCountAsync("_lattice_shardcount-probe");

        Assert.That(count, Is.Null, "system trees stay invisible to the physical-shard-count surface");
    }
}
