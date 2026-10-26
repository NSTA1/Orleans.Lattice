using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class CrossClusterSagaParticipantGrainTests
{
    [Test]
    public async Task Prepare_duplicate_cannot_rebind_existing_saga_to_another_coordinator()
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([participant]);
        await grain.PrepareAsync(Request());
        var writes = state.WriteCount;

        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await grain.PrepareAsync(Request() with { CoordinatorClusterId = "other-peer" }));

        Assert.That(state.State.CoordinatorClusterId, Is.EqualTo(Request().CoordinatorClusterId));
        Assert.That(state.WriteCount, Is.EqualTo(writes));
        Assert.That(participant.PrepareCount, Is.EqualTo(1));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Terminal_retry_after_reactivation_still_requires_recorded_identity(bool commit)
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([participant]);
        await grain.PrepareAsync(Request());
        if (commit) await grain.CommitAsync(Request());
        else await grain.AbortAsync(Request());
        var (reactivated, _, _) = CreateGrain([participant], state);
        var writes = state.WriteCount;

        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            if (commit) await reactivated.CommitAsync(Request() with { CoordinatorClusterId = "other-peer" });
            else await reactivated.AbortAsync(Request() with { CoordinatorClusterId = "other-peer" });
        });

        if (commit) await reactivated.CommitAsync(Request());
        else await reactivated.AbortAsync(Request());
        Assert.That(state.WriteCount, Is.EqualTo(writes));
        Assert.That(participant.CommitCount, Is.EqualTo(commit ? 1 : 0));
        Assert.That(participant.AbortCount, Is.EqualTo(commit ? 0 : 1));
    }

    [TestCase("coordinator")]
    [TestCase("tree")]
    [TestCase("manifest")]
    [TestCase("set")]
    [TestCase("saga")]
    public async Task Status_request_with_different_persisted_identity_is_denied(string field)
    {
        var (grain, state, _) = CreateGrain([new RecordingSagaParticipant()]);
        var original = Request() with { SetId = "set-1" };
        await grain.PrepareAsync(original);
        var writes = state.WriteCount;
        var changed = field switch
        {
            "coordinator" => original with { CoordinatorClusterId = "other-peer" },
            "tree" => original with { TargetTree = "other-tree" },
            "manifest" => original with { ManifestId = "other-manifest" },
            "set" => original with { SetId = null },
            _ => original with { SagaId = "other-saga" },
        };

        Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await grain.GetStatusAsync(changed));

        var status = await grain.GetStatusAsync(original);
        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(state.WriteCount, Is.EqualTo(writes));
        });
    }

    [Test]
    public async Task Status_request_before_prepare_answers_any_coordinator()
    {
        var (grain, _, _) = CreateGrain([]);

        var status = await grain.GetStatusAsync(Request() with { CoordinatorClusterId = "any-peer" });

        Assert.That(status.Phase, Is.EqualTo(SagaPhase.None));
    }

    [Test]
    public async Task Abort_before_prepare_persists_identity_for_idempotent_retry()
    {
        var (grain, state, _) = CreateGrain([]);
        var original = Request() with { SetId = "set-1" };
        await grain.AbortAsync(original);
        Assert.That(state.State.TargetTree, Is.EqualTo(original.TargetTree));
        Assert.That(state.State.ManifestId, Is.EqualTo(original.ManifestId));
        Assert.That(state.State.SetId, Is.EqualTo(original.SetId));
        Assert.That(state.State.CoordinatorClusterId, Is.EqualTo(original.CoordinatorClusterId));
        await grain.AbortAsync(original);
        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await grain.AbortAsync(original with { CoordinatorClusterId = "other-peer" }));
    }

    [TestCase("coordinator", true)]
    [TestCase("tree", true)]
    [TestCase("manifest", true)]
    [TestCase("set", true)]
    [TestCase("saga", true)]
    [TestCase("coordinator", false)]
    [TestCase("tree", false)]
    [TestCase("manifest", false)]
    [TestCase("set", false)]
    [TestCase("saga", false)]
    public async Task Terminal_request_with_different_persisted_identity_is_denied_before_side_effects(
        string field, bool commit)
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([participant]);
        var original = Request() with { SetId = "set-1" };
        await grain.PrepareAsync(original);
        var deadline = state.State.FenceDeadlineTicks;
        var writes = state.WriteCount;
        var changed = field switch
        {
            "coordinator" => original with { CoordinatorClusterId = "other-peer" },
            "tree" => original with { TargetTree = "other-tree" },
            "manifest" => original with { ManifestId = "other-manifest" },
            "set" => original with { SetId = null },
            _ => original with { SagaId = "other-saga" },
        };

        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            if (commit) await grain.CommitAsync(changed);
            else await grain.AbortAsync(changed);
        });

        Assert.Multiple(() =>
        {
            Assert.That(participant.CommitCount, Is.Zero);
            Assert.That(participant.AbortCount, Is.Zero);
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(state.State.FenceDeadlineTicks, Is.EqualTo(deadline));
            Assert.That(state.WriteCount, Is.EqualTo(writes));
        });

        // A refused request must not poison the owning coordinator's retry.
        if (commit) await grain.CommitAsync(original);
        else await grain.AbortAsync(original);
        Assert.That(state.State.Phase, Is.EqualTo(commit ? SagaPhase.Committed : SagaPhase.Aborted));
    }
}
