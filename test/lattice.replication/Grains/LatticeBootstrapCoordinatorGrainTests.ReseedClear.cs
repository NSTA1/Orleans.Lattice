using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4533: a re-seed request is consumed only by a drain whose export
/// postdates it, and a bootstrap that completes with the request unconsumed
/// does not echo its epoch, so the sender keeps withholding saga records until
/// a drain has cleared the stale pending buckets.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public async Task Drain_from_an_export_after_the_reseed_request_consumes_it()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        fake.State.ReseedAfterEpochs[SourceCluster] = 5;
        var (grain, _, _, provider, _, _, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SnapshotStream(Tree, Hlc(10), new VersionVector(), Stream()) { ExportEpoch = 7 }));

        Assert.That(await grain.IsReseedPendingAsync(SourceCluster), Is.True, "precondition");
        await grain.ProcessNextPhaseAsync();

        Assert.That(await grain.IsReseedPendingAsync(SourceCluster), Is.False,
            "a drain from an export after the request clears the stale buckets and consumes it");
    }

    [Test]
    public async Task Bootstrap_completing_with_an_unconsumed_reseed_request_does_not_echo_its_epoch()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotExportEpoch = 7;
        // Recorded after this bootstrap's drain had finished.
        fake.State.ReseedAfterEpochs[SourceCluster] = 6;
        var (grain, _, _, _, reminders, _, _, _) = Create(fake);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetCompletedExportEpochAsync(SourceCluster), Is.Null,
                "the echo would release the sender's saga records over stale pending buckets");
            Assert.That(await grain.IsReseedPendingAsync(SourceCluster), Is.True, "the next drain still owes the clear");
        });
    }
}
