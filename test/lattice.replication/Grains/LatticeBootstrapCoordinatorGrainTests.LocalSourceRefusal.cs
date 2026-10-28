using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #5109: when the coordinator drains through the local bootstrap
/// snapshot source (no cross-cluster transport registered), a bootstrap that
/// names a foreign source cluster must fail closed. The local tree's own
/// export must not be applied as that source's contents, must not record the
/// local lineage as the source's drained lineage, and must not consume a
/// pending re-seed request from that source.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private static SnapshotStream LocalSelfExport() =>
        new(Tree, Hlc(10), new VersionVector(), Stream(
            new SnapshotEntry { Key = "local-k", Value = new byte[] { 9 }, Timestamp = Hlc(5) }))
        {
            OpenGeneration = StableGeneration,
            ExportEpoch = 7,
        };

    /// <summary>
    /// Routes the harness's bootstrap source through a real
    /// <see cref="LocalBootstrapSnapshotSource"/> over a substitute local
    /// provider, so the coordinator drains exactly as it does on a silo with
    /// no <see cref="IRemoteSnapshotTransport"/>.
    /// </summary>
    private static ISnapshotProvider RouteThroughLocalSource(IBootstrapSnapshotSource harnessSource, string localClusterId)
    {
        var localProvider = Substitute.For<ISnapshotProvider>();
        localProvider.ExportAsync(Tree, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(LocalSelfExport()));
        var real = new LocalBootstrapSnapshotSource(localProvider, LocalBootstrapSnapshotSourceTests.Options(localClusterId));
        harnessSource.ExportAsync(Tree, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call => real.ExportAsync(
                (string)call[0], (string)call[1], (HybridLogicalClock)call[2], (CancellationToken)call[3]));
        return localProvider;
    }

    [Test]
    public async Task A_foreign_source_bootstrap_through_the_local_source_fails_without_completing_or_consuming_the_reseed()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        fake.State.ReseedAfterEpochs[SourceCluster] = 5;
        var (grain, _, _, provider, reminders, apply, _, _) = Create(fake);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));
        var localProvider = RouteThroughLocalSource(provider, localClusterId: "test-cluster");

        Assert.That(await grain.IsReseedPendingAsync(SourceCluster), Is.True, "precondition");

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<LatticeBootstrapSourceMismatchException>());

        Assert.Multiple(async () =>
        {
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(fake.State.InProgress, Is.False);
            Assert.That(await grain.GetDrainedLineageAsync(SourceCluster), Is.Null,
                "the local lineage must not be recorded as the foreign source's drained lineage");
            Assert.That(await grain.GetCompletedExportEpochAsync(SourceCluster), Is.Null);
            Assert.That(await grain.IsReseedPendingAsync(SourceCluster), Is.True,
                "a self-export must not consume the foreign source's re-seed request");
            await localProvider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default(string)!, default, default);
            await apply.DidNotReceiveWithAnyArgs().ApplyAsync(default!, default);
        });
    }

    [Test]
    public async Task A_same_cluster_bootstrap_through_the_local_source_still_drains()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        fake.State.ReseedAfterEpochs[SourceCluster] = 5;
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        var localProvider = RouteThroughLocalSource(provider, localClusterId: SourceCluster);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(fake.State.Phase, Is.Not.EqualTo(LatticeBootstrapState.Failed));
            Assert.That((await grain.GetDrainedLineageAsync(SourceCluster))?.Lineage, Is.EqualTo(StableGeneration.Lineage));
            Assert.That(await grain.IsReseedPendingAsync(SourceCluster), Is.False);
            await localProvider.Received(1).ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
            await apply.Received(1).ApplyAsync(
                Arg.Is<WalRecord>(r => IsBootstrapSet(r, "local-k", Hlc(5), SourceCluster, LatticeMergeMode.LwwRegister)),
                Arg.Any<CancellationToken>());
        });
    }
}