using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #5534: the re-seed responder reads the completed export epoch and the
/// coordinator status (both interleave with a running bootstrap) and then calls
/// <see cref="LatticeBootstrapCoordinatorGrain.BootstrapForReseedAsync"/>, which
/// does not interleave and so can run only once the bootstrap it raced has
/// completed. A request that completion already answers must neither be
/// re-armed nor start another bootstrap: the redundant import re-minted the
/// receiver's frontier epoch after the sender settled on the first one, and the
/// sender read it as a new gap, so the regions re-seeded each other repeatedly.
/// Any request the completion does not answer, under the tree's current
/// frontier epoch, still re-bootstraps.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private const long AnsweredExportEpoch = 7;

    [Test]
    public async Task A_reseed_request_the_completed_bootstrap_answers_is_neither_recorded_nor_restarted()
    {
        var (grain, fake) = CreateCompleted(out var epoch);
        SetCurrentFrontierEpoch(grain.Factory, epoch);

        await grain.Grain.BootstrapForReseedAsync(SourceCluster, 2, start: true, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.Grain.IsReseedPendingAsync(SourceCluster), Is.False,
                "the completion already answers the request, so it must not be re-armed");
            Assert.That(fake.State.InProgress, Is.False, "an answered request must not start another bootstrap");
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(await grain.Grain.GetCompletedExportEpochAsync(SourceCluster, epoch), Is.EqualTo(AnsweredExportEpoch),
                "the sender's next push is answered by the echo instead");
        });
    }

    [Test]
    public async Task A_reseed_request_for_the_completed_epoch_or_later_is_still_recorded()
    {
        var (grain, _) = CreateCompleted(out var epoch);
        SetCurrentFrontierEpoch(grain.Factory, epoch);

        await grain.Grain.BootstrapForReseedAsync(SourceCluster, AnsweredExportEpoch, start: false, CancellationToken.None);

        Assert.That(await grain.Grain.IsReseedPendingAsync(SourceCluster), Is.True,
            "an export at the requested epoch does not postdate the request, so it does not answer it");
    }

    [Test]
    public async Task A_reseed_request_after_the_frontier_epoch_moved_is_still_recorded()
    {
        var (grain, _) = CreateCompleted(out _);
        // The tree's contents were replaced after the completion: it no longer vouches for them.
        SetCurrentFrontierEpoch(grain.Factory, Guid.NewGuid());

        await grain.Grain.BootstrapForReseedAsync(SourceCluster, 2, start: false, CancellationToken.None);

        Assert.That(await grain.Grain.IsReseedPendingAsync(SourceCluster), Is.True,
            "a completion installed under an earlier frontier epoch must not stall a needed re-seed");
    }

    [Test]
    public async Task A_reseed_request_while_a_reconcile_is_owed_is_still_recorded()
    {
        var (grain, fake) = CreateCompleted(out var epoch);
        fake.State.ReconcileOwedBySource[SourceCluster] = true;
        SetCurrentFrontierEpoch(grain.Factory, epoch);

        await grain.Grain.BootstrapForReseedAsync(SourceCluster, 2, start: false, CancellationToken.None);

        Assert.That(await grain.Grain.IsReseedPendingAsync(SourceCluster), Is.True,
            "an owed reconcile withholds the completion, so it answers nothing (#5116)");
    }

    [Test]
    public async Task A_reseed_request_is_still_recorded_when_the_frontier_cannot_be_read()
    {
        var (grain, _) = CreateCompleted(out _);
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.GetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ReplicationTreeFrontierSnapshot>(new TimeoutException("frontier unreachable")));
        grain.Factory.GetGrain<IReplicationTreeFrontierGrain>(Tree).Returns(frontier);

        await grain.Grain.BootstrapForReseedAsync(SourceCluster, 2, start: false, CancellationToken.None);

        Assert.That(await grain.Grain.IsReseedPendingAsync(SourceCluster), Is.True,
            "without the current epoch the completion cannot be shown to answer the request");
    }

    private static ((LatticeBootstrapCoordinatorGrain Grain, IGrainFactory Factory) Grain, FakePersistentState<BootstrapCoordinatorState> State)
        CreateCompleted(out Guid epoch)
    {
        epoch = Guid.NewGuid();
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        fake.State.InProgress = false;
        fake.State.Phase = LatticeBootstrapState.LiveIncremental;
        fake.State.SourceClusterId = SourceCluster;
        fake.State.OperationId = "test-op";
        fake.State.CompletedExportEpochs[SourceCluster] = AnsweredExportEpoch;
        fake.State.CompletedExportLineages[SourceCluster] = epoch;
        var (grain, _, factory, _, _, _, _, _) = Create(fake);
        return ((grain, factory), fake);
    }

    private static void SetCurrentFrontierEpoch(IGrainFactory factory, Guid epoch)
    {
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.GetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ReplicationTreeFrontierSnapshot { Epoch = epoch }));
        factory.GetGrain<IReplicationTreeFrontierGrain>(Tree).Returns(frontier);
    }
}
