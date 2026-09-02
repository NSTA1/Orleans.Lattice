using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the group-atomic <b>prepare</b> path a restore participant takes when
/// the saga carries a <see cref="SagaControlRequest.SetId"/>: it expands the set into
/// its member trees, filters to the subset this cluster hosts, then admits and builds
/// every hosted member's shadow unfenced. The invariant proven here is all-or-nothing:
/// if any member is infeasible or fails to build, every member built so far is garbage
/// collected and the whole set votes abort, so a set never commits some trees while
/// aborting others. The per-cluster host filter is also exercised in both directions
/// (a replicated member is hosted; the coordinator hosts every member; a foreign,
/// non-coordinator cluster hosts none), since it decides which members the group
/// operates on.
/// </summary>
public partial class RestoreParticipantTests
{
    private const string TreeA = "set-orders@site";
    private const string TreeB = "set-audit@site";
    private const string SetId = "restore-set-nightly";

    private static ILatticeBackupSetResolver TwoMemberResolver() =>
        ResolverFor(new BackupSetMember("bkp-" + TreeA, TreeA), new BackupSetMember("bkp-" + TreeB, TreeB));

    [Test]
    public async Task PrepareSetAsync_all_members_feasible_votes_commit_and_caches_group()
    {
        var engine = HealthyEngine();
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit), "every hosted member prepared");
        await engine.Received(2).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        Assert.That(participant.TryGetLocalSetResult(SetId, out var built), Is.True);
        Assert.That(built, Has.Count.EqualTo(2), "the built group is cached for reuse by commit");
    }

    [Test]
    public async Task PrepareSetAsync_member_infeasible_gcs_built_members_and_votes_abort()
    {
        // TreeA builds; TreeB is capacity-refused, so the whole set aborts and the
        // already-built TreeA shadow plus the never-built TreeB shadow are both GC'd.
        var engine = HealthyEngine();
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            capacity: CapacityExcept(TreeB),
            setResolver: TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(result.Detail, Does.Contain("infeasible"));
        await engine.Received(2).DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(participant.TryGetLocalSetResult(SetId, out _), Is.False, "an aborted set caches no group");
    }

    [Test]
    public void PrepareSetAsync_admission_probe_cancelled_rethrows_cancellation()
    {
        var engine = HealthyEngine();
        engine.ProbeAdmissionAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver());

        Assert.That(
            async () => await participant.PrepareAsync(SetRequestFor(SetId)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PrepareSetAsync_admission_probe_faulted_gcs_and_votes_abort()
    {
        var engine = HealthyEngine();
        engine.ProbeAdmissionAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("catalog unreachable"));
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(result.Detail, Does.Contain("admission probe failed"));
        // No member built, so the whole group is resolved-and-GC'd by id.
        await engine.Received(2).DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void PrepareSetAsync_shadow_build_cancelled_rethrows_cancellation()
    {
        var engine = HealthyEngine();
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver());

        Assert.That(
            async () => await participant.PrepareAsync(SetRequestFor(SetId)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PrepareSetAsync_shadow_build_faulted_gcs_and_votes_abort()
    {
        var engine = HealthyEngine();
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("capacity exhausted"));
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(result.Detail, Does.Contain("build failed"));
    }

    [Test]
    public async Task PrepareSetAsync_coordinator_hosts_every_member_even_when_none_replicated()
    {
        // A member the membership seam reports NOT replicated is still hosted because
        // this cluster is the coordinator, which is where local-only members live.
        var engine = HealthyEngine();
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver(),
            membership: MembershipFor(),
            options: OptionsFor(CoordinatorCluster));

        var result = await participant.PrepareAsync(SetRequestFor(SetId, CoordinatorCluster));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit));
        await engine.Received(2).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PrepareSetAsync_non_coordinator_hosts_only_replicated_members()
    {
        // A foreign cluster hosts only the members it replicates; the local-only member
        // is filtered out, so exactly one shadow is built.
        var engine = HealthyEngine();
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            setResolver: TwoMemberResolver(),
            membership: MembershipFor(TreeA),
            options: OptionsFor("foreign-cluster"));

        var result = await participant.PrepareAsync(SetRequestFor(SetId, CoordinatorCluster));

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit));
        await engine.Received(1).BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>());
        Assert.That(participant.TryGetLocalSetResult(SetId, out var built), Is.True);
        Assert.That(built, Has.Count.EqualTo(1), "only the replicated member is hosted here");
    }
}
