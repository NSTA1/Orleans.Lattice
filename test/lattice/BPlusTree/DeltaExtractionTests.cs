using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(ClusterCollection.Name)]
public class DeltaExtractionTests(ClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task GetDeltaSinceAsync_returns_all_entries_for_empty_version()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await leaf.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var delta = await leaf.GetDeltaSinceAsync(new VersionVector());

        Assert.False(delta.IsEmpty);
        Assert.Equal(2, delta.Entries.Count);
        Assert.True(delta.Entries.ContainsKey("a"));
        Assert.True(delta.Entries.ContainsKey("b"));
    }

    [Fact]
    public async Task GetDeltaSinceAsync_returns_empty_when_up_to_date()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("x", Encoding.UTF8.GetBytes("val"));

        // First delta: get everything.
        var delta1 = await leaf.GetDeltaSinceAsync(new VersionVector());
        Assert.False(delta1.IsEmpty);

        // Second delta using the version from the first: should be empty.
        var delta2 = await leaf.GetDeltaSinceAsync(delta1.Version);
        Assert.True(delta2.IsEmpty);
    }

    [Fact]
    public async Task GetDeltaSinceAsync_returns_only_new_entries()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("first", Encoding.UTF8.GetBytes("1"));

        var delta1 = await leaf.GetDeltaSinceAsync(new VersionVector());

        await leaf.SetAsync("second", Encoding.UTF8.GetBytes("2"));

        var delta2 = await leaf.GetDeltaSinceAsync(delta1.Version);
        Assert.False(delta2.IsEmpty);
        Assert.Single(delta2.Entries);
        Assert.True(delta2.Entries.ContainsKey("second"));
    }

    [Fact]
    public async Task GetDeltaSinceAsync_includes_tombstones()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        await leaf.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var delta1 = await leaf.GetDeltaSinceAsync(new VersionVector());

        await leaf.DeleteAsync("k");

        var delta2 = await leaf.GetDeltaSinceAsync(delta1.Version);
        Assert.False(delta2.IsEmpty);
        Assert.True(delta2.Entries["k"].IsTombstone);
    }

    [Fact]
    public async Task Delta_version_advances_monotonically()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        await leaf.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var v1 = (await leaf.GetDeltaSinceAsync(new VersionVector())).Version;

        await leaf.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var v2 = (await leaf.GetDeltaSinceAsync(new VersionVector())).Version;

        Assert.True(v2.IsNewerThan(v1));
    }
}
