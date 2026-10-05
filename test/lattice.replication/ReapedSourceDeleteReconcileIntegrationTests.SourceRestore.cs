using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The source-restore contract for the re-bootstrap reconcile (issues #4537 and
/// #4549). A unilateral, uncoordinated restore of the source's tree loses rows
/// without deleting them and gives the tree a new lineage. A receiver that then
/// re-bootstraps cannot tell a lost row from a deleted one, so it must keep
/// every source-origin row the export no longer carries and synthesise no delete.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    [Test]
    public async Task A_unilateral_source_restore_that_drops_source_rows_leaves_them_on_the_receiver_and_synthesises_no_delete()
    {
        const string tree = "rsdr-unilateral-restore";
        const string restored = "rsdr-unilateral-restore-copy";
        const string kept = "kept";
        const string lost = "lost-by-the-restore";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(kept, new byte[] { 1 });
        await siteA.SetAsync(lost, new byte[] { 2 });
        await BootstrapSiteBAsync(tree);
        Assert.That(await siteB.GetAsync(lost), Is.EqualTo(new byte[] { 2 }), "precondition: the receiver holds the row");

        // The source is restored, unilaterally, onto another binding: the tree
        // takes a new lineage, and the row is gone from it with no tombstone the
        // receiver could ever be shipped - which is all a restore that lost the
        // row looks like from the receiver.
        var registry = _siteA.Client.GetLatticeRegistry();
        var before = (await registry.GetEntryAsync(tree))!.Lineage;
        await _siteA.Client.GetGrain<ILattice>(restored).SetAsync(kept, new byte[] { 1 });
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await registry.SetAliasAsync(tree, restored);
        }

        await siteA.DeleteAsync(lost);
        await ReapSourceTombstonesAsync(tree);

        Assert.That((await registry.GetEntryAsync(tree))!.Lineage, Is.Not.EqualTo(before), "precondition: the lineage changed");
        var exported = await ExportSiteAAsync(tree);
        Assert.That(exported.Select(e => e.Key), Has.No.Member(lost), "precondition: the export no longer carries the lost row");

        var outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedLineageMismatch),
                "the receiver's copy is aligned with the old lineage and the export orphans one of its rows, so it skips");
            Assert.That(outcomes, Has.No.Member(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
            Assert.That(await siteB.GetAsync(lost), Is.EqualTo(new byte[] { 2 }),
                "the receiver cannot prove the row was deleted in the lineage it is aligned with, so it keeps it");
            Assert.That(await siteB.GetAsync(kept), Is.EqualTo(new byte[] { 1 }));
        });
    }
}
