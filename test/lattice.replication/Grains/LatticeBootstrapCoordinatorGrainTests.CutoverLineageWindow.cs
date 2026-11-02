using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #5534: a bootstrap's shadow-copy cutover re-mints the receiver's tree
/// frontier epoch before the coordinator records the new epoch against the
/// lineage it drained. <see cref="ILatticeBootstrapCoordinatorGrain.GetDrainedLineageAsync"/>
/// interleaves with that cutover, so a stamped batch from the source it just
/// drained, arriving inside the window, read the old epoch, was refused as
/// "replaced", and the source took the receiver off the log again: another
/// re-seed, whose own cutover reopened the window.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public async Task A_batch_from_the_drained_source_during_the_cutover_is_retried_not_refused_for_its_lineage()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        var lineage = Guid.NewGuid();
        var drainEpoch = Guid.NewGuid();
        var cutoverEpoch = Guid.NewGuid();
        fake.State.UseShadowCopy = true;
        fake.State.ShadowCopyTreeId = $"{Tree}/shadow";
        fake.State.FrontierEpoch = drainEpoch;
        fake.State.DrainedLineageBySource[SourceCluster] = lineage;
        fake.State.DrainedFrontierEpochBySource[SourceCluster] = drainEpoch;
        var (grain, _, factory, _, _, _, _, _) = Create(fake);

        // Hold the cutover part-way: the alias has swapped (so the frontier
        // epoch has moved) but the coordinator has not yet recorded it.
        var resize = factory.GetGrain<ITreeResizeGrain>(Tree);
        var swapped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completeCopy = resize.CompleteBootstrapCopyAsync("warm-up");
        resize.CompleteBootstrapCopyAsync(Arg.Any<string>()).Returns(async _ =>
        {
            swapped.TrySetResult();
            await release.Task;
            await completeCopy;
        });
        var pump = grain.ProcessNextPhaseAsync();
        await swapped.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var inWindow = await grain.GetDrainedLineageAsync(SourceCluster);
        var verdict = await ReplicationSourceLineageGate.CheckAsync(
            Substitute.For<IGrainFactory>().WithCoordinator(Tree, grain),
            Tree,
            SourceCluster,
            lineage,
            cutoverEpoch,
            NullLogger.Instance);

        release.TrySetResult();
        try
        {
            await pump;
        }
        catch
        {
            // The handoff's later steps are not under test here.
        }

        Assert.Multiple(() =>
        {
            Assert.That(inWindow?.CutoverPending, Is.True,
                "the coordinator must report that its cutover has not yet recorded the new epoch");
            Assert.That(verdict, Is.EqualTo(ReplicationSourceLineageGate.Verdict.RefuseTransient),
                "a batch from the source just drained must be retried, never refused for a lineage it has drained");
        });
    }

    [Test]
    public async Task A_settled_drain_reports_no_pending_cutover_and_a_replaced_tree_still_refuses_the_lineage()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        var lineage = Guid.NewGuid();
        var drainEpoch = Guid.NewGuid();
        fake.State.Phase = LatticeBootstrapState.LiveIncremental;
        fake.State.SourceClusterId = SourceCluster;
        fake.State.DrainedLineageBySource[SourceCluster] = lineage;
        fake.State.DrainedFrontierEpochBySource[SourceCluster] = drainEpoch;
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        var record = await grain.GetDrainedLineageAsync(SourceCluster);
        var factory = Substitute.For<IGrainFactory>().WithCoordinator(Tree, grain);

        Assert.Multiple(async () =>
        {
            Assert.That(record?.CutoverPending, Is.False);
            Assert.That(
                await ReplicationSourceLineageGate.CheckAsync(factory, Tree, SourceCluster, lineage, drainEpoch, NullLogger.Instance),
                Is.EqualTo(ReplicationSourceLineageGate.Verdict.Apply));
            Assert.That(
                await ReplicationSourceLineageGate.CheckAsync(factory, Tree, SourceCluster, lineage, Guid.NewGuid(), NullLogger.Instance),
                Is.EqualTo(ReplicationSourceLineageGate.Verdict.RefuseLineage),
                "once no cutover is pending, contents replaced since the drain still refuse the stale lineage");
        });
    }
}

internal static class CoordinatorFactoryExtensions
{
    public static IGrainFactory WithCoordinator(this IGrainFactory factory, string tree, ILatticeBootstrapCoordinatorGrain grain)
    {
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree).Returns(grain);
        return factory;
    }
}
