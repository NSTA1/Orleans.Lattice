using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4684, the two premises a barrier's judgement of an unnaming import
/// rests on. The barrier takes such an import as the tree's arrival only when
/// it knows the operation's decision stamps, carried on whatever opened it - a
/// shipped terminal or an imported decision row - and only for an import the
/// source served under the cross-tree hold.
/// </summary>
public partial class CrossTreeImportBarrierIntegrationTests
{
    [Test]
    public async Task A_barrier_opened_by_an_imported_decision_row_judges_a_sibling_import_against_its_stamps()
    {
        // Tree A is imported from an export that opened before the operation
        // existed, so its rows are pre-saga and name the operation nowhere. The
        // operation then decides, stamped with tree A's export epoch, and tree B
        // is imported: its decision row opens the barrier and carries the
        // stamps. Judged against them tree A's import is not past its decision,
        // so tree A stays pending; a barrier that lost the row's stamps would
        // take every import as one from after the decision and decide on it.
        const string treeA = "xtib-rowstamp-a";
        const string treeB = "xtib-rowstamp-b";
        const string operationId = "xtib-rowstamp-op";
        await _siteA.Client.GetGrain<ILattice>(treeA).SetAsync("k", [7]);
        await StartBootstrapAsync(treeA);
        var aPhase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);
        var aDrained = await Coordinator(treeA).GetDrainedExportEpochAsync(SiteAClusterId);

        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        var row = await ExportedCrossTreeDecisionAsync(treeB, operationId);
        Assert.Multiple(() =>
        {
            Assert.That(aPhase, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "precondition: tree A was imported first");
            Assert.That(row.CrossTreeDecisionStamps, Is.Not.Null.And.ContainKey(treeA), "precondition: the decision row carries the stamps");
            Assert.That(row.CrossTreeDecisionStamps![treeA], Is.GreaterThanOrEqualTo(aDrained!.Value),
                "precondition: tree A's import opened before the decision");
        });

        await StartBootstrapAsync(treeB);
        var bPhase = await AwaitPhaseAsync(treeB, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        await Task.Delay(HeldWindow);

        var barrier = await Barrier(operationId).GetStatusAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(bPhase, Is.Not.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(barrier.Opened, Is.True, "tree B's decision row opened the barrier");
            Assert.That(barrier.Decided, Is.False, "an import from before the decision decides nothing");
            Assert.That(barrier.ArrivedTrees, Is.EqualTo(new[] { treeB }), "tree A stays pending in the barrier");
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 7 })), "tree A is still pre-saga");
            Assert.That((await ReadAsync(treeB, "k")).Fenced, Is.True, "tree B is not served while its barrier waits");
        });
    }

    [Test]
    public async Task An_import_not_served_under_the_cross_tree_hold_is_never_taken_as_an_arrival()
    {
        // The case the liveness test above it settles, except that the source
        // served tree A's export without the cross-tree hold: it could have
        // purged a decision row the export needed. The receiver records no
        // import, so the barrier tree B's stamped terminal opens waits for tree
        // A's own terminal rather than deciding on the import.
        const string treeA = "xtib-unhonoured-a";
        const string treeB = "xtib-unhonoured-b";
        const string operationId = "xtib-unhonoured-op";
        await _siteA.Client.GetGrain<ILattice>(treeA).SetAsync("k", [1]);
        var stamps = new Dictionary<string, long> { [treeA] = await SiteAEpochAsync(treeA), [treeB] = await SiteAEpochAsync(treeB) };

        PausableTransport.Unhonoured[treeA] = true;
        try
        {
            await StartBootstrapAsync(treeA);
            var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);
            var import = await _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetImportAsync(SiteAClusterId);
            await DeliverStampedTreeBAsync(treeA, treeB, operationId, stamps);

            var barrier = await Barrier(operationId).GetStatusAsync();
            Assert.Multiple(async () =>
            {
                Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
                Assert.That(import, Is.Null, "an import the source did not serve under the hold is not recorded");
                Assert.That(barrier.Decided, Is.False, "the barrier does not decide on that import");
                Assert.That(barrier.ArrivedTrees, Is.EqualTo(new[] { treeB }));
                Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo((false, (byte[]?)null)), "tree B stays pre-saga");
            });
        }
        finally
        {
            PausableTransport.Unhonoured.TryRemove(treeA, out _);
        }
    }
}
