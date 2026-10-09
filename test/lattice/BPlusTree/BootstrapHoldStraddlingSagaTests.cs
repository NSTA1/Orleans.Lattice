using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression for issue 4791: a replicated atomic batch whose prepares straddle a bootstrap
/// hold (one prepare already on the old tree, the rest routed to the staging tree) must not
/// become partially visible to old-tree readers when its terminal decision lands mid-hold.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BootstrapHoldStraddlingSagaTests
{
    private const string Origin = "straddle-origin";
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

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static readonly string[] Keys = ["k0", "k1", "k2", "k3"];

    private static async Task PrepareAsync(IReplicationApplyGrain apply, Guid txid, int index)
    {
        await apply.ApplyPreparedSetAsync(
            Keys[index],
            [(byte)(index + 1)],
            Hlc(5_000 + index),
            Origin,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: Keys.Length,
            atomicBatchIndex: index);
    }

    // Production ships one terminal per distinct source WAL shard the batch touched, each
    // carrying the distinct-shard count (ReplicationApplier). Mirror that fan-out here.
    private static async Task TerminalAsync(IReplicationApplyGrain apply, Guid txid, bool committed)
    {
        var shards = Keys
            .Select(k => LatticeSharding.GetShardIndex(k, LatticeConstants.DefaultShardCount))
            .Distinct()
            .ToArray();
        foreach (var shard in shards)
        {
            await apply.ApplyTxTerminalAsync(txid, committed, shardIndex: shard, Hlc(6_000), Origin, atomicShardCount: shards.Length);
        }
    }

    private static async Task<int> VisibleCountAsync(ILattice tree)
    {
        var visible = 0;
        foreach (var key in Keys)
        {
            if (await tree.GetAsync(key) is not null) visible++;
        }
        return visible;
    }

    private async Task<(ILattice Tree, ITreeResizeGrain Resize, IReplicationApplyGrain Apply, string OperationId, Guid Txid)> StraddleAsync(string prefix)
    {
        var treeName = $"{prefix}-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        await tree.SetAsync("existing", [9]);

        var txid = Guid.NewGuid();
        await PrepareAsync(apply, txid, 2);

        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True);

        foreach (var index in new[] { 0, 1, 3 })
        {
            await PrepareAsync(apply, txid, index);
        }

        return (tree, resize, apply, operationId, txid);
    }

    [Test]
    public async Task Straddling_saga_is_never_partially_visible_to_old_tree_readers_during_hold()
    {
        var (tree, resize, apply, operationId, txid) = await StraddleAsync("straddle-commit");

        await TerminalAsync(apply, txid, committed: true);

        // The terminal landed on the staging tree only; the old tree's mirrored
        // prepares must resolve from the recorded decision for old-tree readers.
        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length),
            "An old-tree reader must see the whole committed batch during the hold.");

        await resize.CompleteBootstrapCopyAsync(operationId);
        await TerminalAsync(apply, txid, committed: true);

        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length),
            "The committed batch must be fully visible after cutover.");
    }

    [Test]
    public async Task Straddling_saga_mirror_copies_dedupe_on_the_staging_tree_after_cutover()
    {
        var (tree, resize, apply, operationId, txid) = await StraddleAsync("straddle-dedupe");

        // Redeliver the hold-time prepares: each is staged again on both trees and
        // forwarded again from the old tree to the staging tree.
        foreach (var index in new[] { 0, 1, 3 })
        {
            await PrepareAsync(apply, txid, index);
        }

        await TerminalAsync(apply, txid, committed: true);
        await resize.CompleteBootstrapCopyAsync(operationId);
        await TerminalAsync(apply, txid, committed: true);

        for (var i = 0; i < Keys.Length; i++)
        {
            Assert.That(await tree.GetAsync(Keys[i]), Is.EqualTo(new[] { (byte)(i + 1) }), Keys[i]);
        }

        var scanned = new List<string>();
        await foreach (var key in tree.KeysAsync())
        {
            scanned.Add(key);
        }
        Assert.That(scanned, Is.EquivalentTo(Keys.Append("existing")),
            "Duplicated mirror copies must collapse to one entry per key.");

        // No duplicate pending entry may linger and shadow a later write.
        await tree.SetAsync("k0", [42]);
        Assert.That(await tree.GetAsync("k0"), Is.EqualTo(new byte[] { 42 }));

        // A prepare redelivered after the decision settles against it.
        await PrepareAsync(apply, txid, 1);
        Assert.That(await tree.GetAsync("k1"), Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task Baseline_no_hold_saga_is_visible_after_terminal()
    {
        var treeName = $"baseline-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        var txid = Guid.NewGuid();
        for (var i = 0; i < Keys.Length; i++) await PrepareAsync(apply, txid, i);
        await TerminalAsync(apply, txid, committed: true);
        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length));
    }

    [Test]
    public async Task Control_non_straddling_saga_applied_in_hold_is_visible_after_cutover()
    {
        var treeName = $"control-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        await tree.SetAsync("existing", [9]);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        var txid = Guid.NewGuid();
        for (var i = 0; i < Keys.Length; i++) await PrepareAsync(apply, txid, i);
        await TerminalAsync(apply, txid, committed: true);
        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(0).Or.EqualTo(Keys.Length),
            "Old-tree readers must see none or all of the batch during the hold.");
        await resize.CompleteBootstrapCopyAsync(operationId);
        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length),
            "The staging tree must hold the whole batch after cutover.");
    }

    [Test]
    public async Task Hold_time_prepares_settled_after_decision_are_never_partially_visible_to_old_tree_readers()
    {
        var treeName = $"settled-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        await tree.SetAsync("existing", [9]);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();

        // k1 arrives before the decision and is staged as pending; the decision then lands,
        // so k0, k2 and k3 settle directly as committed writes.
        var txid = Guid.NewGuid();
        await PrepareAsync(apply, txid, 1);
        await TerminalAsync(apply, txid, committed: true);
        foreach (var index in new[] { 0, 2, 3 })
        {
            await PrepareAsync(apply, txid, index);
        }

        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(0).Or.EqualTo(Keys.Length),
            "Old-tree readers must see none or all of the batch during the hold.");

        await resize.CompleteBootstrapCopyAsync(operationId);
        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length),
            "The committed batch must be fully visible after cutover.");
    }

    [Test]
    public async Task Straddling_saga_decision_is_published_after_hold_abort()
    {
        var (tree, resize, apply, operationId, txid) = await StraddleAsync("straddle-abort");

        await TerminalAsync(apply, txid, committed: true);
        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length));

        // Abort discards the staging tree; the old tree must still hold the whole batch.
        await resize.AbortBootstrapCopyAsync(operationId);
        await TerminalAsync(apply, txid, committed: true);

        Assert.That(await VisibleCountAsync(tree), Is.EqualTo(Keys.Length),
            "After abort the committed batch must be wholly visible on the old tree.");
    }
}
