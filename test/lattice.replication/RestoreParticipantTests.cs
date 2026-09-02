using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="RestoreParticipant"/>, the first internal
/// <see cref="ISagaParticipant"/>. Verifies the prepare build is unfenced, the
/// commit engages the cutover fence before the atomic swap and unblocks writes
/// after, admission and capacity refusals vote abort without building, a
/// transient build failure retries within budget, and abort reverts, garbage
/// collects the shadow, and lifts the fence.
/// </summary>
[TestFixture]
public partial class RestoreParticipantTests
{
    private const string SagaId = "saga-1";
    private const string TargetTree = "orders";
    private const string BackupId = "backup-1";
    private const string CoordinatorCluster = "site-a";

    private static SagaControlRequest Request() => new()
    {
        SagaId = SagaId,
        TargetTree = TargetTree,
        ManifestId = BackupId,
        CoordinatorClusterId = CoordinatorCluster,
    };

    private static (RestoreParticipant participant, FakeCoordinatedRestoreEngine engine, ISagaWriteFenceGrain fence)
        CreateParticipant(bool canHost = true, FakeCoordinatedRestoreEngine? engine = null)
    {
        engine ??= new FakeCoordinatedRestoreEngine { TargetTree = TargetTree };

        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(canHost));

        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(fence);

        var participant = new RestoreParticipant(
            engine, engine, capacity, factory, NullLogger<RestoreParticipant>.Instance);
        return (participant, engine, fence);
    }

    [Test]
    public async Task PrepareAsync_replicated_target_builds_shadow_without_fencing()
    {
        var (participant, engine, fence) = CreateParticipant();

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit));
        Assert.That(engine.BuildCount, Is.EqualTo(1));
        Assert.That(engine.CommitCount, Is.EqualTo(0), "the build must not swap the alias");
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
    }

    [Test]
    public async Task PrepareAsync_infeasible_target_votes_abort_without_building()
    {
        var (participant, engine, _) = CreateParticipant(canHost: false);

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(engine.BuildCount, Is.EqualTo(0), "an infeasible target is refused before any build");
    }

    [Test]
    public async Task PrepareAsync_admission_probe_failure_votes_abort()
    {
        var engine = new FakeCoordinatedRestoreEngine
        {
            TargetTree = TargetTree,
            ProbeFailure = new InvalidOperationException("catalog unreachable"),
        };
        var (participant, _, _) = CreateParticipant(engine: engine);

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(engine.BuildCount, Is.EqualTo(0));
    }

    [Test]
    public async Task PrepareAsync_transient_build_failure_retries_then_commits()
    {
        var engine = new FakeCoordinatedRestoreEngine
        {
            TargetTree = TargetTree,
            TransientBuildFailures = 2,
        };
        var (participant, _, _) = CreateParticipant(engine: engine);

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit));
        Assert.That(engine.BuildCount, Is.EqualTo(3), "two transient failures then a success");
    }

    [Test]
    public async Task PrepareAsync_permanent_build_failure_gcs_shadow_and_votes_abort()
    {
        var engine = new FakeCoordinatedRestoreEngine
        {
            TargetTree = TargetTree,
            BuildFailure = new InvalidOperationException("capacity exhausted"),
        };
        var (participant, _, _) = CreateParticipant(engine: engine);

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(engine.BuildCount, Is.EqualTo(3), "the build is retried within the bounded budget");
        Assert.That(engine.DeleteCount, Is.GreaterThanOrEqualTo(1), "any partial shadow is garbage collected");
    }

    [Test]
    public async Task PrepareAsync_precondition_failure_votes_abort_without_retry()
    {
        var engine = new FakeCoordinatedRestoreEngine
        {
            TargetTree = TargetTree,
            BuildFailure = new LatticeRestoreValidationException("base backup missing"),
        };
        var (participant, _, _) = CreateParticipant(engine: engine);

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(engine.BuildCount, Is.EqualTo(1), "a precondition failure is permanent and not retried");
        Assert.That(engine.DeleteCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task CommitAsync_engages_fence_swaps_then_unblocks_writes()
    {
        var (participant, engine, fence) = CreateParticipant();
        await participant.PrepareAsync(Request());

        await participant.CommitAsync(Request());

        Assert.That(engine.CommitCount, Is.EqualTo(1), "the alias is swapped exactly once");
        await fence.Received(1).EngageAsync(Arg.Is<SagaWriteFenceRequest>(
            r => r.SagaId == SagaId && r.Trees.Contains(TargetTree)));
        await fence.Received(1).UnblockWritesAsync();
        Received.InOrder(() =>
        {
            fence.EngageAsync(Arg.Any<SagaWriteFenceRequest>());
            fence.UnblockWritesAsync();
        });
    }

    [Test]
    public async Task CommitAsync_without_prepared_state_rebuilds_then_commits()
    {
        var (participant, engine, fence) = CreateParticipant();

        // Commit without a prior prepare models a reactivation that lost its
        // in-memory built-shadow cache; the idempotent build resumes the shadow.
        await participant.CommitAsync(Request());

        Assert.That(engine.BuildCount, Is.EqualTo(1));
        Assert.That(engine.CommitCount, Is.EqualTo(1));
        await fence.Received(1).UnblockWritesAsync();
    }

    [Test]
    public async Task AbortAsync_after_prepare_reverts_gcs_shadow_and_lifts_fence()
    {
        var (participant, engine, fence) = CreateParticipant();
        await participant.PrepareAsync(Request());

        await participant.AbortAsync(Request());

        Assert.That(engine.RevertCount, Is.EqualTo(1));
        Assert.That(engine.DeleteCount, Is.GreaterThanOrEqualTo(1), "the shadow is garbage collected");
        await fence.Received(1).LiftAsync();
        Assert.That(engine.CommitCount, Is.EqualTo(0), "abort never commits");
    }

    [Test]
    public async Task AbortAsync_without_prepared_state_gcs_by_resolved_id_and_lifts()
    {
        var (participant, engine, fence) = CreateParticipant();

        await participant.AbortAsync(Request());

        Assert.That(engine.RevertCount, Is.EqualTo(0), "nothing was committed, so nothing is reverted");
        Assert.That(engine.DeleteCount, Is.EqualTo(1), "the shadow is resolved by id and garbage collected");
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public async Task AbortAsync_is_idempotent()
    {
        var (participant, engine, fence) = CreateParticipant();
        await participant.PrepareAsync(Request());

        await participant.AbortAsync(Request());
        await participant.AbortAsync(Request());

        // The second abort finds no prepared state and garbage collects by id
        // again (idempotent at the engine); the fence lift is a no-op.
        await fence.Received(2).LiftAsync();
        Assert.That(engine.DeleteCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task GetStatusAsync_returns_none()
    {
        var (participant, _, _) = CreateParticipant();

        var phase = await participant.GetStatusAsync(Request());

        Assert.That(phase, Is.EqualTo(SagaPhase.None));
    }
}
