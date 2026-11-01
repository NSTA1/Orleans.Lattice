using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #5534: a bootstrap of a tree whose merge mode is not last-writer-wins
/// (the OR-Map <c>sys-replication-config</c> tree among them) has no foreign
/// deletes to reconcile, so a stable export must neither install the drop floor
/// nor owe a reconcile retry. An owed reconcile withholds the completed export
/// epoch, so a sender waiting for a re-seed was never released and re-requested
/// the re-seed forever.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [TestCase(LatticeMergeMode.OrMap)]
    [TestCase(LatticeMergeMode.OrSet)]
    [TestCase(LatticeMergeMode.PnCounter)]
    public async Task A_stable_export_of_a_non_lww_tree_owes_no_reconcile_and_echoes_its_epoch(LatticeMergeMode mode)
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)mode);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, factory, provider, reminders, _, hwm, _) = Create(fake, mergeResolver: resolver);
        SnapshotStream? stream = null;
        async IAsyncEnumerable<SnapshotEntry> Entries()
        {
            await Task.Yield();
            yield return new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(5) };
            stream!.CloseGeneration = StableGeneration;
            stream.SourceFrontier = ExportedFrontier(StableGeneration.Lineage);
        }

        stream = new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), Entries())
        {
            OpenGeneration = StableGeneration,
            OpenFrontier = ExportedFrontier(StableGeneration.Lineage),
            ExportEpoch = 7,
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(fake.State.ReconcileOwedBySource.GetValueOrDefault(SourceCluster), Is.False,
                "a non-LWW tree has no foreign deletes to reconcile, so a stable export owes no retry");
        });
        await hwm.DidNotReceive().SetBootstrapFloorAsync(
            Arg.Any<IReadOnlyDictionary<string, HybridLogicalClock>>(),
            Arg.Any<IReadOnlyDictionary<string, HybridLogicalClock[]>>(),
            Arg.Any<CancellationToken>());

        var epoch = Guid.NewGuid();
        fake.State.FrontierEpoch = epoch;
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.PinAsync(default, default!, default!, default).ReturnsForAnyArgs(true);
        factory.GetGrain<IReplicationTreeFrontierGrain>(Tree).Returns(frontier);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive").Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(await grain.GetCompletedExportEpochAsync(SourceCluster, epoch), Is.EqualTo(7),
                "the sender's re-seed past epoch 2 must be answered, or it re-requests the re-seed forever");
        });
    }
}
