using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the group-atomic <b>commit</b> and <b>abort</b> paths for a set
/// restore. Commit engages a single write fence over every hosted member, flips
/// each member's shadow, and unblocks writes; a reactivation that lost the built
/// cache re-derives every member idempotently before flipping. Abort reverts and
/// garbage collects every prepared member (or, cache-lost, resolves and GCs each
/// shadow id without a rebuild) and then lifts the shared fence. Both also honour
/// the host filter: a cluster that hosts none of the set's trees flips nothing on
/// commit and merely lifts its defensive fence on abort. These invariants keep a
/// set all-or-nothing across clusters and leak no shadow storage on rollback.
/// </summary>
public partial class RestoreParticipantTests
{
    [Test]
    public async Task CommitSetAsync_cache_lost_rebuilds_every_member_then_flips_the_group()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence), setResolver: TwoMemberResolver());

        // No prepare ran on this activation, so commit must rebuild both members.
        await participant.CommitAsync(SetRequestFor(SetId));

        await engine.Received(2).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        await fence.Received(1).EngageAsync(Arg.Any<SagaWriteFenceRequest>());
        await engine.Received(2).CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>());
        await fence.Received(1).UnblockWritesAsync();
        Assert.That(participant.TryGetLocalSetResult(SetId, out _), Is.True, "the rebuilt group is cached");
    }

    [Test]
    public async Task CommitSetAsync_hosts_no_member_returns_without_engaging_the_fence()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(
            engine, FactoryFor(fence),
            setResolver: TwoMemberResolver(),
            membership: MembershipFor(),
            options: OptionsFor("foreign-cluster"));

        await participant.CommitAsync(SetRequestFor(SetId, CoordinatorCluster));

        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
        await engine.DidNotReceive().BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AbortSetAsync_after_prepare_reverts_and_deletes_every_member_then_lifts()
    {
        var engine = HealthyEngine();
        var restoreService = Substitute.For<ILatticeBackupRestoreService>();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(
            engine, FactoryFor(fence),
            restoreService: restoreService,
            setResolver: TwoMemberResolver());

        var prepared = await participant.PrepareAsync(SetRequestFor(SetId));
        Assert.That(prepared.Vote, Is.EqualTo(SagaVote.Commit), "the group prepared so the cache is populated");

        await participant.AbortAsync(SetRequestFor(SetId));

        await restoreService.Received(2).RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>());
        await engine.Received(2).DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await fence.Received(1).LiftAsync();
        Assert.That(participant.TryGetLocalSetResult(SetId, out _), Is.False, "the aborted group is evicted");
    }

    [Test]
    public async Task AbortSetAsync_cache_lost_resolves_and_gcs_each_member_then_lifts()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence), setResolver: TwoMemberResolver());

        // No prepare ran, so abort resolves each member's shadow id and GCs it with
        // no rebuild and no revert (no commit can precede an abort).
        await participant.AbortAsync(SetRequestFor(SetId));

        await engine.DidNotReceive().BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        await engine.Received(2).DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public async Task AbortSetAsync_hosts_no_member_lifts_fence_and_returns()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(
            engine, FactoryFor(fence),
            setResolver: TwoMemberResolver(),
            membership: MembershipFor(),
            options: OptionsFor("foreign-cluster"));

        await participant.AbortAsync(SetRequestFor(SetId, CoordinatorCluster));

        await fence.Received(1).LiftAsync();
        await engine.DidNotReceive().DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
