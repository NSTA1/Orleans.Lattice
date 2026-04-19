using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// FX-012 regression: <c>CountAsync</c> and <c>CountPerShardAsync</c> must not
/// over-count keys during concurrent shard splits. Before the fix, reducing
/// per-shard counts without participating in the F-032 shard-map
/// reconciliation path would double-count keys in migrating virtual slots
/// between the destination shard's commit and the source shard's
/// tombstone settle.
/// </summary>
[TestFixture]
public class CountAsyncSplitRegressionTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task CountAsync_matches_true_live_count_after_split()
    {
        var treeId = $"fx012-count-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int keyCount = 400;
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        // Force a split commit via the coordinator and confirm that a
        // subsequent CountAsync returns the true live count (not the
        // summed-per-shard count, which would double-count migrating
        // slots during the settle window).
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        var count = await tree.CountAsync();
        Assert.That(count, Is.EqualTo(keyCount),
            "CountAsync must reconcile per-slot ownership across a mid-count split");
    }

    [Test]
    public async Task CountPerShardAsync_matches_true_live_count_after_split()
    {
        var treeId = $"fx012-pershard-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int keyCount = 400;
        for (int i = 0; i < keyCount; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(0);
        await split.RunSplitPassAsync();

        var perShard = await tree.CountPerShardAsync();
        Assert.That(perShard.Sum(), Is.EqualTo(keyCount),
            "CountPerShardAsync must reconcile per-slot ownership across a mid-count split");
    }
}
