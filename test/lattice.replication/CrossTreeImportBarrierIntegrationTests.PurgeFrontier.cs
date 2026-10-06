using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4733: a decided barrier tombstone (#4730) is dropped once the origin's
/// advertised cross-tree purge frontier has passed the operation's decision
/// sequence on every participant - and not before, on any one of them.
/// </summary>
public partial class CrossTreeImportBarrierIntegrationTests
{
    /// <summary>
    /// Decides the operation's barrier on site B through tree A's import, whose
    /// decision row carries the participants and decision sequences, then runs
    /// the barrier's retention. Returns site A's decision sequences.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, long>> DecideByImportThenExpireAsync(string treeA, string treeB, string operationId)
    {
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        var sequences = (await ExportedCrossTreeDecisionAsync(treeA, operationId)).CrossTreeDecisionSequences;
        Assert.That(sequences?.Keys, Is.EquivalentTo(new[] { treeA, treeB }), "precondition: the decision row carries every participant's sequence");
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        Assert.That(await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental), Is.EqualTo(LatticeBootstrapState.LiveIncremental),
            "precondition: the barrier decides");
        await Barrier(operationId).AsReference<IRemindable>().ReceiveReminder(BarrierRetentionReminder, default);
        return sequences!;
    }

    private ICrossTreePurgeFrontierGrain PurgeFrontier => _siteB.Client.GetGrain<ICrossTreePurgeFrontierGrain>(SiteAClusterId);

    private async Task<bool> ListedAsync(string tree, string operationId) =>
        (await _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(tree).GetTombstonesAsync())
            .Contains(LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId));

    [Test]
    public async Task A_tombstone_is_kept_until_the_origin_purge_frontier_passes_every_participant()
    {
        const string treeA = "xtpf-every-a";
        const string treeB = "xtpf-every-b";
        const string operationId = "xtpf-every-op";
        var sequences = await DecideByImportThenExpireAsync(treeA, treeB, operationId);
        var listed = await ListedAsync(treeA, operationId) && await ListedAsync(treeB, operationId);

        // Tree A passed; tree B not advertised at all, then advertised short.
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [treeA] = sequences[treeA] });
        var afterA = await Barrier(operationId).GetStatusAsync();
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [treeB] = sequences[treeB] - 1 });
        var afterShortB = await Barrier(operationId).GetStatusAsync();
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [treeB] = sequences[treeB] });
        var afterB = await Barrier(operationId).GetStatusAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(listed, Is.True, "precondition: the tombstone is listed under every participant");
            Assert.That(afterA.Decided, Is.True, "a participant whose frontier was never advertised keeps the tombstone");
            Assert.That(afterShortB.Decided, Is.True, "a frontier below the participant's sequence keeps the tombstone");
            Assert.That(afterB.Decided, Is.False, "the tombstone drops once every participant's frontier passed it");
            Assert.That(afterB.Opened, Is.False);
            Assert.That(await ListedAsync(treeA, operationId) || await ListedAsync(treeB, operationId), Is.False,
                "the dropped tombstone is unlisted");
        });
    }

    [Test]
    public async Task A_tombstone_whose_frontier_already_passed_drops_when_its_retention_runs()
    {
        // The frontier advanced before the retention listed the tombstone: the
        // retention reads it itself, so the tombstone does not wait for a
        // further advance that may never come.
        const string treeA = "xtpf-early-a";
        const string treeB = "xtpf-early-b";
        const string operationId = "xtpf-early-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        var sequences = (await ExportedCrossTreeDecisionAsync(treeA, operationId)).CrossTreeDecisionSequences!;
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        Assert.That(await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [treeA] = sequences[treeA], [treeB] = sequences[treeB] });

        await Barrier(operationId).AsReference<IRemindable>().ReceiveReminder(BarrierRetentionReminder, default);
        var status = await Barrier(operationId).GetStatusAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(status.Decided, Is.False, "the retention drops a tombstone the frontier already passed");
            Assert.That(await ListedAsync(treeA, operationId), Is.False);
        });
    }

    [Test]
    public async Task A_tombstone_of_an_operation_decided_before_sequencing_drops_once_no_such_decision_remains()
    {
        // No sequences: the operation counts as sequence 0 on every
        // participant, which the origin's frontier reaches only once it stores
        // no decision recorded before sequencing.
        const string treeA = "xtpf-legacy-a";
        const string treeB = "xtpf-legacy-b";
        const string operationId = "xtpf-legacy-op";
        var barrier = Barrier(operationId);
        await barrier.RecordDecisionStampsAsync(new Dictionary<string, long>(), null, [treeA, treeB]);
        foreach (var tree in new[] { treeA, treeB })
        {
            var txid = Guid.NewGuid();
            var stamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
            var apply = _siteB.Client.GetGrain<IReplicationApplyGrain>(tree);
            await apply.ApplyPreparedSetAsync(
                "k", [1], stamp, SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0, txid, atomicBatchSize: 0, atomicBatchIndex: 0);
            await apply.ApplyTxTerminalAsync(
                txid, committed: true, ShardOf("k"), HybridLogicalClock.Tick(stamp), SiteAClusterId,
                atomicShardCount: 0, crossTreeOperationId: operationId, crossTreeWaitSet: [treeA, treeB]);
        }

        Assert.That((await barrier.GetStatusAsync()).Decided, Is.True, "precondition: the barrier decides");
        await barrier.AsReference<IRemindable>().ReceiveReminder(BarrierRetentionReminder, default);
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [treeA] = 0 });
        var afterA = await barrier.GetStatusAsync();
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [treeB] = 0 });
        var afterB = await barrier.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(afterA.Decided, Is.True);
            Assert.That(afterB.Decided, Is.False, "frontier 0 on every participant drops a tombstone decided before sequencing");
        });
    }

    [Test]
    public async Task The_purge_frontier_only_advances()
    {
        const string tree = "xtpf-monotone";
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [tree] = 9 });
        await PurgeFrontier.AdvanceAsync(new Dictionary<string, long> { [tree] = 4 });

        Assert.That((await PurgeFrontier.GetAsync())[tree], Is.EqualTo(9));
    }
}
