using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the single-tree restore path's replication-enrollment gate. The
/// inbound cross-cluster saga control channel authorizes the <b>origin cluster</b>
/// only, so <see cref="SagaControlRequest.TargetTree"/> is a peer-supplied field:
/// a peer holding an accepted mesh credential picks the tree the shadow cutover
/// overwrites. The participant must therefore re-derive the target's replication
/// enrollment locally and refuse a tree this peer is not enrolled for, exactly as
/// the sibling <b>set</b> path already does through its hosted-member filter.
/// </summary>
public partial class RestoreParticipantTests
{
    private const string UnenrolledTree = "sys-auth-policies";

    private static SagaControlRequest RequestForTree(string tree) => new()
    {
        SagaId = SagaId,
        TargetTree = tree,
        ManifestId = BackupId,
        CoordinatorClusterId = CoordinatorCluster,
    };

    [Test]
    public async Task PrepareAsync_targetTreeNotReplicated_votesAbortWithoutBuilding()
    {
        var engine = HealthyEngine(UnenrolledTree);
        var participant = Participant(
            engine,
            FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            membership: MembershipFor(TargetTree),
            options: OptionsFor("site-b"));

        var result = await participant.PrepareAsync(RequestForTree(UnenrolledTree));

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(result.Detail, Does.Contain(UnenrolledTree));
        });

        await engine.DidNotReceive().BuildShadowAsync(
            Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        await engine.DidNotReceive().ProbeAdmissionAsync(
            Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CommitAsync_targetTreeNotReplicated_doesNotSwapTheAlias()
    {
        var engine = HealthyEngine(UnenrolledTree);
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(
            engine,
            FactoryFor(fence),
            membership: MembershipFor(TargetTree),
            options: OptionsFor("site-b"));

        await participant.CommitAsync(RequestForTree(UnenrolledTree));

        await engine.DidNotReceive().CommitShadowAsync(
            Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>());
        await engine.DidNotReceive().BuildShadowAsync(
            Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
        await fence.Received().LiftAsync();
    }

    [Test]
    public async Task AbortAsync_targetTreeNotReplicated_doesNotRevertTheAlias()
    {
        var engine = HealthyEngine(UnenrolledTree);
        var restoreService = Substitute.For<ILatticeBackupRestoreService>();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(
            engine,
            FactoryFor(fence),
            restoreService: restoreService,
            membership: MembershipFor(TargetTree),
            options: OptionsFor("site-b"));

        await participant.AbortAsync(RequestForTree(UnenrolledTree));

        await restoreService.DidNotReceive().RevertRestoreAsync(
            Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>());
        await engine.DidNotReceive().DeleteShadowAsync(
            Arg.Any<string>(), Arg.Any<CancellationToken>());
        await fence.Received().LiftAsync();
    }

    [Test]
    public async Task PrepareAsync_targetTreeReplicated_stillBuilds()
    {
        var engine = HealthyEngine();
        var participant = Participant(
            engine,
            FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            membership: MembershipFor(TargetTree),
            options: OptionsFor("site-b"));

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit));
        await engine.Received(1).BuildShadowAsync(
            Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PrepareAsync_membershipSeamNotWired_isUnchanged()
    {
        var engine = HealthyEngine(UnenrolledTree);
        var participant = Participant(
            engine,
            FactoryFor(Substitute.For<ISagaWriteFenceGrain>()));

        var result = await participant.PrepareAsync(RequestForTree(UnenrolledTree));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit),
            "a host without the membership seam (direct unit tests) keeps the previous behaviour");
    }
}
