using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
[Category("Integration")]
public sealed class BootstrapShadowCopyIntegrationTests
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
    public async Task Bootstrap_copy_keeps_original_view_until_complete()
    {
        var treeName = $"bootstrap-shadow-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await tree.SetAsync("existing", [1]);

        var operationId = Guid.NewGuid().ToString("N");
        var shadowTreeId = await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();

        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True,
            "The online snapshot must be held before imported state is applied.");
        Assert.That(await tree.GetAsync("existing"), Is.EqualTo(new byte[] { 1 }));
        Assert.That(await tree.GetAsync("imported"), Is.Null,
            "The logical tree must not expose rows staged on the shadow copy.");

        using (LatticeBootstrapShadowRouteContext.BeginScope(treeName, shadowTreeId))
        {
            await tree.SetAsync("imported", [2]);
            Assert.That(await tree.GetAsync("imported"), Is.EqualTo(new byte[] { 2 }),
                "Bootstrap apply must read its own staged state.");
        }

        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        await apply.ApplySetAsync(
            "incremental",
            [3],
            new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
            "source",
            sourceVectorClock: null,
            expiresAtTicks: 0);

        Assert.That(await tree.GetAsync("imported"), Is.Null,
            "Readers outside bootstrap apply must remain on the original tree until cutover.");
        Assert.That(await tree.GetAsync("incremental"), Is.EqualTo(new byte[] { 3 }),
            "A committed incremental apply lands on the original tree, which mirrors it to the shadow copy (#5694).");

        await resize.CompleteBootstrapCopyAsync(operationId);

        Assert.That(await tree.GetAsync("existing"), Is.EqualTo(new byte[] { 1 }));
        Assert.That(await tree.GetAsync("imported"), Is.EqualTo(new byte[] { 2 }),
            "The alias must expose the complete imported view after cutover.");
        Assert.That(await tree.GetAsync("incremental"), Is.EqualTo(new byte[] { 3 }),
            "Incremental updates staged during bootstrap must survive the alias cutover.");
    }

    [Test]
    public async Task Bootstrap_copy_abort_discards_staged_rows_and_keeps_original_view()
    {
        var treeName = $"bootstrap-shadow-abort-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await tree.SetAsync("existing", [1]);

        var operationId = Guid.NewGuid().ToString("N");
        var shadowTreeId = await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();

        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True);
        using (LatticeBootstrapShadowRouteContext.BeginScope(treeName, shadowTreeId))
        {
            await tree.SetAsync("partial-import", [2]);
        }

        await resize.AbortBootstrapCopyAsync(operationId);

        Assert.That(await tree.GetAsync("existing"), Is.EqualTo(new byte[] { 1 }));
        Assert.That(await tree.GetAsync("partial-import"), Is.Null,
            "An aborted copy must not publish any partially imported rows.");
    }

    [Test]
    public async Task Committed_stream_applies_during_hold_survive_bootstrap_copy_abort()
    {
        // Issue #5694: the sender's cursor and the receiver's live dedupe cache
        // both treat these applies as delivered, so nothing re-ships them after
        // the copy is discarded.
        var treeName = $"bootstrap-shadow-abort-live-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        await tree.SetAsync("existing", [1]);
        await tree.SetAsync("doomed", [1]);

        var operationId = Guid.NewGuid().ToString("N");
        var shadowTreeId = await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True);

        using (LatticeBootstrapShadowRouteContext.BeginScope(treeName, shadowTreeId))
        {
            await tree.SetAsync("partial-import", [2]);
        }

        var hlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };
        await apply.ApplySetAsync("live-set", [3], hlc, "source", sourceVectorClock: null, expiresAtTicks: 0);
        await apply.ApplyMergeManyAsync(
        [
            new ApplyMergeItem { Key = "live-merge-a", Value = [4], SourceHlc = hlc, OriginClusterId = "source" },
            new ApplyMergeItem { Key = "live-merge-b", Value = [5], SourceHlc = hlc, OriginClusterId = "source" },
        ]);
        await apply.ApplyDeleteAsync("doomed", hlc, "source", sourceVectorClock: null);

        await resize.AbortBootstrapCopyAsync(operationId);

        Assert.That(await tree.GetAsync("partial-import"), Is.Null,
            "An aborted copy must not publish any partially imported rows.");
        Assert.That(await tree.GetAsync("live-set"), Is.EqualTo(new byte[] { 3 }),
            "A committed stream apply delivered during the hold must survive the copy's abort.");
        Assert.That(await tree.GetAsync("live-merge-a"), Is.EqualTo(new byte[] { 4 }));
        Assert.That(await tree.GetAsync("live-merge-b"), Is.EqualTo(new byte[] { 5 }));
        Assert.That(await tree.GetAsync("doomed"), Is.Null,
            "A committed stream delete delivered during the hold must survive the copy's abort.");
    }
}
