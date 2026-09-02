using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the single-tree abort compensation helpers, which must be robust to
/// every failure of their two collaborators (the restore service and the engine)
/// because they run on the rollback path where a second fault must not mask the
/// first. The contract proven here: a revert is skipped when no restore service is
/// wired and swallowed when it faults, a shadow garbage-collect is skipped when the
/// shadow id cannot be resolved or is empty and swallowed when the delete faults,
/// and in every non-cancellation case the write fence is still lifted so writes are
/// never left blocked. Cooperative cancellation is the one fault that must propagate
/// rather than be swallowed, so the coordinator can retry compensation.
/// </summary>
public partial class RestoreParticipantTests
{
    [Test]
    public async Task AbortAsync_null_restore_service_skips_revert_but_deletes_shadow()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence), restoreService: null);

        await participant.PrepareAsync(Request());
        await participant.AbortAsync(Request());

        await engine.Received(1).DeleteShadowAsync(TargetTree + "-shadow", Arg.Any<CancellationToken>());
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public void AbortAsync_revert_cancelled_rethrows_cancellation()
    {
        var engine = HealthyEngine();
        var restoreService = Substitute.For<ILatticeBackupRestoreService>();
        restoreService.RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()), restoreService: restoreService);

        Assert.That(async () =>
        {
            await participant.PrepareAsync(Request());
            await participant.AbortAsync(Request());
        }, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task AbortAsync_revert_faulted_is_swallowed_and_fence_still_lifts()
    {
        var engine = HealthyEngine();
        var restoreService = Substitute.For<ILatticeBackupRestoreService>();
        restoreService.RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("registry unreachable"));
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence), restoreService: restoreService);

        await participant.PrepareAsync(Request());
        await participant.AbortAsync(Request());

        await fence.Received(1).LiftAsync();
        await engine.Received(1).DeleteShadowAsync(TargetTree + "-shadow", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AbortAsync_cache_lost_resolve_shadow_faulted_is_swallowed()
    {
        var engine = HealthyEngine();
        engine.ResolveShadowTreeId(Arg.Any<LatticeRestoreRequest>())
            .Throws(new InvalidOperationException("cannot resolve shadow"));
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence));

        // No prepare, so abort re-derives the shadow id; a resolve fault is non-fatal.
        await participant.AbortAsync(Request());

        await engine.DidNotReceive().DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public async Task AbortAsync_cache_lost_empty_shadow_id_skips_delete()
    {
        var engine = HealthyEngine();
        engine.ResolveShadowTreeId(Arg.Any<LatticeRestoreRequest>()).Returns(string.Empty);
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence));

        await participant.AbortAsync(Request());

        await engine.DidNotReceive().DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public void AbortAsync_cache_lost_delete_shadow_cancelled_rethrows_cancellation()
    {
        var engine = HealthyEngine();
        engine.DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var participant = Participant(engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()));

        Assert.That(
            async () => await participant.AbortAsync(Request()),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task AbortAsync_cache_lost_delete_shadow_faulted_is_swallowed_and_fence_lifts()
    {
        var engine = HealthyEngine();
        engine.DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("storage unreachable"));
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence));

        await participant.AbortAsync(Request());

        await fence.Received(1).LiftAsync();
    }
}
