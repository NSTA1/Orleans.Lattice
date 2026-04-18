using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class OperationStatusIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task IsMergeCompleteAsync_returns_true_when_no_merge_initiated()
    {
        var treeId = $"status-merge-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var result = await tree.IsMergeCompleteAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsSnapshotCompleteAsync_returns_true_when_no_snapshot_initiated()
    {
        var treeId = $"status-snap-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var result = await tree.IsSnapshotCompleteAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsResizeCompleteAsync_returns_true_when_no_resize_initiated()
    {
        var treeId = $"status-resize-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var result = await tree.IsResizeCompleteAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsMergeCompleteAsync_returns_true_after_merge_completes()
    {
        var sourceTree = $"status-merge-src-{Guid.NewGuid():N}";
        var targetTree = $"status-merge-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        await source.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await target.SetAsync("_init", [0]);
        await target.DeleteAsync("_init");

        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        var result = await target.IsMergeCompleteAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsSnapshotCompleteAsync_returns_true_after_snapshot_completes()
    {
        var sourceTree = $"status-snap-src-{Guid.NewGuid():N}";
        var destTree = $"status-snap-dst-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        await source.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await source.SnapshotAsync(destTree, SnapshotMode.Offline);
        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.RunSnapshotPassAsync();

        var result = await source.IsSnapshotCompleteAsync();
        Assert.That(result, Is.True);
    }
}
