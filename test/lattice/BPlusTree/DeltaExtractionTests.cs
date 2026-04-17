using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class DeltaExtractionTests
{
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
    public async Task GetDeltaSinceAsync_returns_all_entries_for_empty_version()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await leaf.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var delta = await leaf.GetDeltaSinceAsync(new VersionVector());

        Assert.That(delta.IsEmpty, Is.False);
        Assert.That(delta.Entries.Count, Is.EqualTo(2));
        Assert.That(delta.Entries.ContainsKey("a"), Is.True);
        Assert.That(delta.Entries.ContainsKey("b"), Is.True);
    }

    [Test]
    public async Task GetDeltaSinceAsync_returns_empty_when_up_to_date()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("x", Encoding.UTF8.GetBytes("val"));

        // First delta: get everything.
        var delta1 = await leaf.GetDeltaSinceAsync(new VersionVector());
        Assert.That(delta1.IsEmpty, Is.False);

        // Second delta using the version from the first: should be empty.
        var delta2 = await leaf.GetDeltaSinceAsync(delta1.Version);
        Assert.That(delta2.IsEmpty, Is.True);
    }

    [Test]
    public async Task GetDeltaSinceAsync_returns_only_new_entries()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("first", Encoding.UTF8.GetBytes("1"));

        var delta1 = await leaf.GetDeltaSinceAsync(new VersionVector());

        await leaf.SetAsync("second", Encoding.UTF8.GetBytes("2"));

        var delta2 = await leaf.GetDeltaSinceAsync(delta1.Version);
        Assert.That(delta2.IsEmpty, Is.False);
        Assert.That(delta2.Entries, Has.Count.EqualTo(1));
        Assert.That(delta2.Entries.ContainsKey("second"), Is.True);
    }

    [Test]
    public async Task GetDeltaSinceAsync_includes_tombstones()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var delta1 = await leaf.GetDeltaSinceAsync(new VersionVector());

        await leaf.DeleteAsync("k");

        var delta2 = await leaf.GetDeltaSinceAsync(delta1.Version);
        Assert.That(delta2.IsEmpty, Is.False);
        Assert.That(delta2.Entries["k"].IsTombstone, Is.True);
    }

    [Test]
    public async Task Delta_version_advances_monotonically()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        await leaf.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var v1 = (await leaf.GetDeltaSinceAsync(new VersionVector())).Version;

        await leaf.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var v2 = (await leaf.GetDeltaSinceAsync(new VersionVector())).Version;

        Assert.That(v2.IsNewerThan(v1), Is.True);
    }
}
