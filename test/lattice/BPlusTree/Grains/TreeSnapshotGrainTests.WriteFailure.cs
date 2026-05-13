using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the Class B "persisted / in-memory divergence on
/// write failure (idempotency-guarded)" anti-pattern on
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeSnapshotGrain"/>. Each
/// test arranges the grain, forces <c>FakePersistentState&lt;T&gt;.ThrowOnWrite</c>,
/// asserts the failing call rethrows, then asserts every mutated field on
/// <c>state.State</c> matches its pre-call snapshot - i.e. the in-memory
/// activation no longer diverges from what storage and any future
/// reactivation observe.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    [Test]
    public void Snapshot_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: registry calls in InitiateSnapshotStateAsync succeed; only
        // the final WriteStateAsync at L152 throws.
        var (grain, state, _, _, _) = CreateGrain();

        Assume.That(state.State.InProgress, Is.False);
        Assume.That(state.State.DestinationTreeId, Is.Null);
        Assume.That(state.State.OperationId, Is.Null);
        Assume.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Lock));

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await grain.SnapshotAsync(DestTreeId, SnapshotMode.Online));

        // Without the snapshot-and-restore fix, InProgress / DestinationTreeId /
        // OperationId / Mode / Phase / ShardCount / Complete / LogicalTreeId
        // remain at their post-mutation values on the failing activation.
        // The next SnapshotAsync call from the same activation would hit the
        // idempotency guard at L73-84:
        //   `if (state.State.InProgress) { if (matching params) return; throw; }`
        // returning success for the same destination without ever having
        // persisted the snapshot, or throwing for a different destination
        // the user genuinely wants to start. Either way the transient storage
        // failure becomes a permanent split-brain until activation recycles.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.DestinationTreeId, Is.Null);
            Assert.That(state.State.OperationId, Is.Null);
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Lock));
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(state.State.ShardRetries, Is.EqualTo(0));
            Assert.That(state.State.ShardCount, Is.EqualTo(0));
            Assert.That(state.State.MaxLeafKeys, Is.Null);
            Assert.That(state.State.MaxInternalChildren, Is.Null);
            Assert.That(state.State.LogicalTreeId, Is.EqualTo(""));
        });
    }

    [Test]
    public void Complete_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange the grain into the mid-flight state CompleteSnapshotAsync
        // expects to flip: InProgress=true, Phase=Copy, cursor past the last
        // shard, destination set. CompleteSnapshotAsync is internal and
        // unit-test-exposed; it mutates 5 fields and persists.
        var (grain, state, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = ShardCount;
        state.State.ShardRetries = 3;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-complete-test";
        state.State.ShardCount = ShardCount;
        state.State.Mode = SnapshotMode.Online;

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.CompleteSnapshotAsync());

        // Without the fix, InProgress=false / Complete=true / Phase=Lock /
        // NextShardIndex=0 / ShardRetries=0 would survive the throw in
        // memory while disk still holds the mid-flight values. The grain
        // exposes IsIdleAsync as `!InProgress`, which would then lie to
        // callers (returning true on the dirty in-memory value), and
        // RunSnapshotPassAsync would short-circuit at its `!InProgress`
        // guard - halting the snapshot on this activation while disk-loaded
        // reactivations would see "in progress" and resume.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(state.State.NextShardIndex, Is.EqualTo(ShardCount));
            Assert.That(state.State.ShardRetries, Is.EqualTo(3));
        });
    }

    [Test]
    public void Abort_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange the grain into a mid-flight snapshot AbortAsync would
        // tear down: InProgress=true, OperationId matches the caller, with
        // a full set of mutated fields populated.
        var (grain, state, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 1;
        state.State.ShardRetries = 2;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-abort-test";
        state.State.Mode = SnapshotMode.Online;
        state.State.ShardCount = ShardCount;
        state.State.MaxLeafKeys = 64;
        state.State.MaxInternalChildren = 32;
        state.State.LogicalTreeId = "logical-tree";

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await grain.AbortAsync("op-abort-test"));

        // Without the fix, every mutated field would survive the throw in
        // memory while disk still holds the mid-flight values. The next
        // AbortAsync retry from the same activation would hit the
        // idempotency guard at L488 `if (!InProgress) return` and silently
        // no-op against the dirty in-memory state - permanently failing to
        // tear down the snapshot until activation recycles. Similarly the
        // L492 `if (!OperationId.Equals(operationId)) return` guard on a
        // dirty in-memory OperationId=null would silently no-op every
        // subsequent abort from any caller.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
            Assert.That(state.State.ShardRetries, Is.EqualTo(2));
            Assert.That(state.State.DestinationTreeId, Is.EqualTo(DestTreeId));
            Assert.That(state.State.OperationId, Is.EqualTo("op-abort-test"));
            Assert.That(state.State.MaxLeafKeys, Is.EqualTo(64));
            Assert.That(state.State.MaxInternalChildren, Is.EqualTo(32));
            Assert.That(state.State.LogicalTreeId, Is.EqualTo("logical-tree"));
        });
    }
}
