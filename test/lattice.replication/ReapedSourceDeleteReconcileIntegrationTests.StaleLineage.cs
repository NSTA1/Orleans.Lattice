using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4673: after a source restore re-stamps the source tree's lineage and
/// the receiver drains the new lineage, a batch the source read under the old
/// lineage - a push still in flight, or a retry - must not apply. Applied, it
/// plants a source-origin row the new lineage never held, and the receiver,
/// aligned with the new lineage, would fabricate its delete on the next
/// reconcile. Each delayed batch is delivered the way the push path delivers
/// it: the source lineage gate, then the real applier. Runs the real bootstrap
/// coordinator, tree frontier and applier in two real clusters.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    /// <summary>Delivers <paramref name="record"/> as a push stamped with <paramref name="stamped"/>.</summary>
    private static async Task<ReplicationSourceLineageGate.Verdict> PushStampedAsync(TestCluster receiver, WalRecord record, Guid? stamped)
    {
        var tree = record.TreeId!;
        var epoch = await receiver.Client.GetGrain<IReplicationTreeFrontierGrain>(tree)
            .ObserveAsync(SiteAClusterId, null, CancellationToken.None);
        var verdict = await ReplicationSourceLineageGate.CheckAsync(
            receiver.Client, tree, SiteAClusterId, stamped, epoch, NullLogger.Instance);
        if (verdict == ReplicationSourceLineageGate.Verdict.Apply)
        {
            await Applier(receiver).ApplyBatchAsync([record], CancellationToken.None);
        }

        return verdict;
    }

    private async Task<Guid> RestampSourceLineageAsync(string tree)
    {
        var registry = _siteA.Client.GetLatticeRegistry();
        var entry = await registry.GetEntryAsync(tree);
        var restored = Guid.NewGuid();
        await registry.UpdateAsync(tree, entry! with { Lineage = restored });
        return restored;
    }

    private static WalRecord StaleWrite(string tree, string key) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [9],
        Timestamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }),
        OriginClusterId = SiteAClusterId,
    };

    [Test]
    public async Task A_batch_read_under_the_pre_restore_lineage_is_refused_once_the_receiver_drained_the_restored_lineage()
    {
        const string tree = "rsdr-stale-lineage";
        const string ghost = "dropped-by-the-restore";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var preRestore = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // The source restores the tree: its lineage is re-stamped, and the
        // receiver re-bootstraps from the restored contents.
        await RestampSourceLineageAsync(tree);
        await BootstrapSiteBAsync(tree);

        // A push the source read before the restore arrives late.
        var verdict = await PushStampedAsync(_siteB, StaleWrite(tree, ghost), preRestore);
        var appliedAfterRealign = await siteB.GetAsync(ghost);

        var outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(verdict, Is.EqualTo(ReplicationSourceLineageGate.Verdict.RefuseLineage));
            Assert.That(appliedAfterRealign, Is.Null,
                "a write the source read under the pre-restore lineage must not land on a copy drained from the restored one");
            Assert.That(await siteB.GetAsync(ghost), Is.Null, "and no later reconcile has a stale source row to delete");
            Assert.That(await siteB.GetAsync("kept"), Is.EqualTo(new byte[] { 1 }));
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled),
                "precondition: the receiver is aligned with the restored lineage, so its reconcile runs");
        });
    }

    [Test]
    public async Task A_batch_of_the_drained_lineage_applies()
    {
        const string tree = "rsdr-current-lineage";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var current = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        var verdict = await PushStampedAsync(_siteB, StaleWrite(tree, "live"), current);

        Assert.Multiple(async () =>
        {
            Assert.That(verdict, Is.EqualTo(ReplicationSourceLineageGate.Verdict.Apply));
            Assert.That(await _siteB.Client.GetGrain<ILattice>(tree).GetAsync("live"), Is.EqualTo(new byte[] { 9 }));
        });
    }

    [Test]
    public async Task A_pre_restore_batch_arriving_after_a_coordinated_restore_cutover_is_refused()
    {
        const string tree = "rsdr-cutover-lineage";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var preRestore = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // The coordinated restore cuts the receiver over to its restored copy,
        // which re-stamps its own lineage and so re-mints its tree frontier.
        await _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(tree)
            .OnLineageChangingAsync(Guid.NewGuid(), CancellationToken.None);

        var verdict = await PushStampedAsync(_siteB, StaleWrite(tree, "pre-restore"), preRestore);

        // The re-seed the refusal forces drains the source again, which records
        // the drain under the new frontier epoch: the refusal clears, no loop.
        await BootstrapSiteBAsync(tree);
        var current = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;
        var afterReseed = await PushStampedAsync(_siteB, StaleWrite(tree, "post-reseed"), current);

        Assert.Multiple(async () =>
        {
            Assert.That(verdict, Is.EqualTo(ReplicationSourceLineageGate.Verdict.RefuseLineage),
                "a drain from before the cutover no longer describes the restored copy");
            Assert.That(await _siteB.Client.GetGrain<ILattice>(tree).GetAsync("pre-restore"), Is.Null);
            Assert.That(afterReseed, Is.EqualTo(ReplicationSourceLineageGate.Verdict.Apply),
                "the next stable drain re-records the drain epoch, so the source's current batches apply again");
            Assert.That(await _siteB.Client.GetGrain<ILattice>(tree).GetAsync("post-reseed"), Is.EqualTo(new byte[] { 9 }));
        });
    }
}
