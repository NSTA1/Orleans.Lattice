using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4730: an entry in a tree's cross-tree barrier index must never
/// outlive the barrier state it points to. A decided barrier withdraws its
/// entries best effort; one left behind, and then cleared by the barrier's
/// retention, read as undecided for ever, so every later import of the tree
/// stayed read-fenced. The fence check now asks each indexed barrier itself,
/// and a barrier withdraws its entries before its retention clears it.
/// </summary>
public partial class CrossTreeImportBarrierIntegrationTests
{
    /// <summary>The receiver's retention reminder name (<c>LatticeCrossTreeReceiverGrain</c>).</summary>
    private const string BarrierRetentionReminder = "cross-tree-receiver-retention";

    [Test]
    public async Task A_plain_import_with_no_indexed_barrier_lifts_its_fence()
    {
        const string tree = "xtib-stale-ctl";
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("k", [7]);

        await StartBootstrapAsync(tree);
        var phase = await AwaitPhaseAsync(tree, LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(await ReadAsync(tree, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 7 })));
        });
    }

    [Test]
    public async Task An_index_entry_whose_barrier_holds_no_state_does_not_pin_the_import_fence()
    {
        // The state a decided barrier leaves once its best-effort withdrawal
        // failed and its retention cleared it - and equally a barrier whose open
        // write failed after it indexed itself: an entry under the tree whose
        // barrier holds no state, so it reads undecided.
        const string tree = "xtib-stale-entry";
        var staleKey = LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, "xtib-stale-entry-op");
        var index = _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(tree);
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("k", [7]);
        await index.AddAsync(staleKey);

        await StartBootstrapAsync(tree);
        var phase = await AwaitPhaseAsync(tree, LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the stale entry does not hold the import");
            Assert.That(await ReadAsync(tree, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 7 })));
            Assert.That(await index.GetAsync(), Does.Not.Contain(staleKey), "the import prunes the stale entry");
        });
    }

    [Test]
    public async Task An_index_entry_whose_barrier_decided_but_did_not_withdraw_does_not_pin_the_import_fence()
    {
        // A decided barrier whose withdrawal failed, before its retention runs.
        const string treeA = "xtib-stale-decided-a";
        const string treeB = "xtib-stale-decided-b";
        const string operationId = "xtib-stale-decided-op";
        var key = LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId);
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        Assert.That(await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental), Is.EqualTo(LatticeBootstrapState.LiveIncremental),
            "precondition: the barrier decides");

        var index = _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA);
        await index.AddAsync(key);
        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(await index.GetAsync(), Does.Not.Contain(key), "the decided barrier withdraws its left-behind entry");
        });
    }

    [Test]
    public async Task A_barrier_whose_retention_expires_withdraws_its_index_entries_before_it_clears()
    {
        const string treeA = "xtib-stale-ttl-a";
        const string treeB = "xtib-stale-ttl-b";
        const string operationId = "xtib-stale-ttl-op";
        var key = LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId);
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        Assert.That(await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental), Is.EqualTo(LatticeBootstrapState.LiveIncremental),
            "precondition: the barrier decides");

        // Its best-effort withdrawal failed: both entries are left behind.
        var indexA = _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA);
        var indexB = _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB);
        await indexA.AddAsync(key);
        await indexB.AddAsync(key);

        await Barrier(operationId).AsReference<IRemindable>().ReceiveReminder(BarrierRetentionReminder, default);
        var status = await Barrier(operationId).GetStatusAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(status.Opened, Is.False, "precondition: the retention compacted the barrier");
            Assert.That(status.Decided, Is.True, "the retention keeps a decided tombstone");
            Assert.That(await Barrier(operationId).GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));
            Assert.That(await indexA.GetAsync(), Does.Not.Contain(key), "no entry outlives the compacted barrier");
            Assert.That(await indexB.GetAsync(), Does.Not.Contain(key), "no entry outlives the compacted barrier");
        });
    }

    /// <summary>Decides the operation's barrier on site B, then runs its retention.</summary>
    private async Task DecideThenExpireAsync(string treeA, string treeB, string operationId)
    {
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        Assert.That(await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental), Is.EqualTo(LatticeBootstrapState.LiveIncremental),
            "precondition: the barrier decides");
        await Barrier(operationId).AsReference<IRemindable>().ReceiveReminder(BarrierRetentionReminder, default);
    }

    [Test]
    public async Task A_terminal_re_shipped_after_the_barrier_retention_does_not_reopen_it()
    {
        // Issue #4730, route 3: a rewind re-ships tree A's retained terminal
        // after the barrier's retention ran. A cleared barrier would reopen on
        // it and wait for ever for tree B, whose terminal was acknowledged long
        // ago, pinning every later import of either tree.
        const string treeA = "xtib-reopen-term-a";
        const string treeB = "xtib-reopen-term-b";
        const string operationId = "xtib-reopen-term-op";
        await DecideThenExpireAsync(treeA, treeB, operationId);

        var txid = (await ExportedCrossTreeDecisionAsync(treeA, operationId)).TransactionId;
        await _siteB.Client.GetGrain<IReplicationApplyGrain>(treeA).ApplyTxTerminalAsync(
            txid, committed: true, ShardOf("k"),
            HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }), SiteAClusterId,
            atomicShardCount: 0, crossTreeOperationId: operationId, crossTreeWaitSet: [treeA, treeB]);
        var status = await Barrier(operationId).GetStatusAsync();

        await StartBootstrapAsync(treeB);
        var phaseB = await AwaitPhaseAsync(treeB, LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(status.Decided, Is.True, "the re-shipped terminal finds the decided tombstone");
            Assert.That(await _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync(),
                Does.Not.Contain(LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId)), "the barrier never re-indexes");
            Assert.That(phaseB, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "a later import of the sibling is not pinned");
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 1 })));
            Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 2 })));
        });
    }

    [Test]
    public async Task A_re_drain_after_the_barrier_retention_whose_export_still_carries_the_decision_lifts_its_fence()
    {
        // Issue #4730, route 3: a re-seed of tree A drains an export that still
        // carries the operation's decision row (the origin keeps it while the
        // cross-tree hold is unreleased) after the barrier's retention ran.
        const string treeA = "xtib-reopen-drain-a";
        const string treeB = "xtib-reopen-drain-b";
        const string operationId = "xtib-reopen-drain-op";
        await DecideThenExpireAsync(treeA, treeB, operationId);
        Assert.That((await ExportAsync(treeA)).Any(e => e.IsDecision && e.CrossTreeOperationId == operationId), Is.True,
            "precondition: the export still carries the decision row");

        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the re-drain's fence lifts");
            Assert.That((await Barrier(operationId).GetStatusAsync()).Decided, Is.True);
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 1 })));
        });
    }

    [Test]
    public async Task An_undecided_barrier_that_waits_for_the_tree_keeps_holding_it_and_its_entry()
    {
        // The perturbation the fix must not break: an opened, undecided barrier
        // that waits for the tree holds its fence and keeps its entry.
        const string treeA = "xtib-stale-open-a";
        const string treeB = "xtib-stale-open-b";
        const string operationId = "xtib-stale-open-op";
        var key = LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId);
        await DeliverTreeBAsync(treeA, treeB, operationId);

        var holdsA = await Barrier(operationId).SettleIndexEntryAsync(treeA);
        var holdsOther = await Barrier(operationId).SettleIndexEntryAsync("xtib-stale-open-unrelated");

        Assert.Multiple(async () =>
        {
            Assert.That(holdsA, Is.True, "an undecided barrier waiting for the tree holds it");
            Assert.That(await _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetAsync(), Does.Contain(key));
            Assert.That(holdsOther, Is.False, "a barrier that does not wait for a tree holds nothing of it");
        });
    }

    [Test]
    public void Settling_an_index_entry_is_serialized_with_the_calls_that_open_or_decide_a_barrier()
    {
        var method = typeof(ILatticeCrossTreeReceiverGrain).GetMethod(nameof(ILatticeCrossTreeReceiverGrain.SettleIndexEntryAsync))!;
        Assert.That(method.GetCustomAttribute<AlwaysInterleaveAttribute>(), Is.Null,
            "an interleaved settle could withdraw the entry an in-flight open is about to rely on");
    }
}
