using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4534: the coordinator records the export epoch of the snapshot it
/// drains and, once the bootstrap reaches
/// <see cref="LatticeBootstrapState.LiveIncremental"/>, keeps it per source so
/// the receive path can echo it to a sender waiting for a re-seed.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public async Task ProcessNextPhase_records_the_export_epoch_of_the_snapshot_it_drains()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, _, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SnapshotStream(Tree, Hlc(10), new VersionVector(), Stream()) { ExportEpoch = 7 }));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.SnapshotExportEpoch, Is.EqualTo(7));
    }

    [Test]
    public async Task Completed_bootstrap_keeps_its_export_epoch_per_source_and_never_lowers_it()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotExportEpoch = 7;
        var (grain, _, _, _, reminders, _, _, _) = Create(fake);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));
        Assert.That(await grain.GetCompletedExportEpochAsync(SourceCluster), Is.Null);

        await grain.ProcessNextPhaseAsync();

        Assert.That(await grain.GetCompletedExportEpochAsync(SourceCluster), Is.EqualTo(7));

        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotExportEpoch = 3;
        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetCompletedExportEpochAsync(SourceCluster), Is.EqualTo(7));
            Assert.That(await grain.GetCompletedExportEpochAsync("another-source"), Is.Null);
        });
    }
}
