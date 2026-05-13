using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the Class B "persisted / in-memory divergence on
/// write failure (idempotency-guarded)" anti-pattern on
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeReshardGrain"/>. Each
/// test arranges the grain, forces <c>FakePersistentState&lt;T&gt;.ThrowOnWrite</c>,
/// asserts the failing call rethrows, then asserts every mutated field on
/// <c>state.State</c> matches its pre-call snapshot - i.e. the in-memory
/// activation no longer diverges from what storage and any future
/// reactivation observe.
/// </summary>
public partial class TreeReshardGrainTests
{
    [Test]
    public void ReshardAsync_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Default harness gives a non-empty tree (CountAsync=1) and an idle
        // resize, so ReshardAsync(4) on a 2-shard tree drives the full
        // validation path through to the persisted state mutation.
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);

        Assume.That(state.State.InProgress, Is.False);
        Assume.That(state.State.Complete, Is.False);
        Assume.That(state.State.OperationId, Is.Null);
        Assume.That(state.State.Phase, Is.EqualTo(ReshardPhase.None));
        Assume.That(state.State.TargetShardCount, Is.EqualTo(0));

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.ReshardAsync(4));

        // Without the snapshot-and-restore fix, InProgress / OperationId /
        // Phase / TargetShardCount all remain at their post-mutation values
        // on the failing activation. The next ReshardAsync call from the
        // same activation would hit the idempotency guard at L68-73
        // (`if (state.State.InProgress) { if (TargetShardCount == newShardCount) return; throw; }`)
        // - returning success for the same target without ever having
        // persisted the reshard, or throwing for a different target the
        // user genuinely wants to start. Either way the transient storage
        // failure becomes a permanent split-brain until activation recycles.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.OperationId, Is.Null);
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.None));
            Assert.That(state.State.TargetShardCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void Finalise_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange the grain into the Migrating->Complete handoff point that
        // FinaliseAsync expects: InProgress=true, Phase=Complete, target set.
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ReshardPhase.Complete;
        state.State.TargetShardCount = 4;
        state.State.OperationId = "op-fin-test";

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.FinaliseAsync());

        // Without the fix, InProgress=false / Complete=true / Phase=None
        // would survive the throw in memory while disk still holds
        // InProgress=true / Complete=false / Phase=Complete. The grain
        // exposes IsIdleAsync as `!InProgress`, which would then lie to
        // callers (returning true on the dirty in-memory value), and
        // RunReshardPassAsync would short-circuit at its `!InProgress`
        // guard - halting the reshard on this activation while disk-loaded
        // reactivations would continue to see "in progress" and resume.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Complete));
            Assert.That(state.State.TargetShardCount, Is.EqualTo(4));
        });
    }
}
