using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Class B "persisted / in-memory divergence on failing <c>WriteStateAsync</c>" regressions
/// for <see cref="Orleans.Lattice.BPlusTree.Grains.TreeResizeGrain"/>. Every mutating site
/// must snapshot the affected fields, attempt the persist, and restore the in-memory state
/// (and rethrow) when the storage call fails. Otherwise an idempotency guard (the
/// <c>InProgress</c> short-circuit in <c>ResizeAsync</c>, the <c>!InProgress &amp;&amp; !Complete</c>
/// guard in <c>UndoResizeAsync</c>, the <c>!InProgress return</c> guard at the top of
/// <c>RunResizePassAsync</c> / <c>ProcessNextPhaseAsync</c>, or the phase guard inside
/// the switch) short-circuits every retry against the dirty in-memory value and the resize
/// stays wedged until the activation recycles.
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public void InitiateResize_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();

        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevNewMaxLeafKeys = state.State.NewMaxLeafKeys;
        var prevNewMaxInternalChildren = state.State.NewMaxInternalChildren;
        var prevOperationId = state.State.OperationId;
        var prevShardCount = state.State.ShardCount;
        var prevComplete = state.State.Complete;
        var prevSnapshotTreeId = state.State.SnapshotTreeId;
        var prevOldPhysicalTreeId = state.State.OldPhysicalTreeId;
        var prevOldRegistryEntry = state.State.OldRegistryEntry;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.InitiateResizeStateAsync(256, 64));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.EqualTo(prevInProgress),
                "InProgress must revert so the ResizeAsync idempotency guard does not short-circuit retries.");
            Assert.That(state.State.Phase, Is.EqualTo(prevPhase));
            Assert.That(state.State.NewMaxLeafKeys, Is.EqualTo(prevNewMaxLeafKeys));
            Assert.That(state.State.NewMaxInternalChildren, Is.EqualTo(prevNewMaxInternalChildren));
            Assert.That(state.State.OperationId, Is.EqualTo(prevOperationId));
            Assert.That(state.State.ShardCount, Is.EqualTo(prevShardCount));
            Assert.That(state.State.Complete, Is.EqualTo(prevComplete));
            Assert.That(state.State.SnapshotTreeId, Is.EqualTo(prevSnapshotTreeId));
            Assert.That(state.State.OldPhysicalTreeId, Is.EqualTo(prevOldPhysicalTreeId));
            Assert.That(state.State.OldRegistryEntry, Is.EqualTo(prevOldRegistryEntry));
            Assert.That(state.WriteCount, Is.Zero, "Failed write must not be counted as a successful persist.");
        });
    }

    [Test]
    public void UndoResize_after_swap_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();

        var oldEntry = new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 };
        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        state.State.OperationId = "op1";
        state.State.ShardCount = ShardCount;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.State.OldRegistryEntry = oldEntry;

        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevSnapshotTreeId = state.State.SnapshotTreeId;
        var prevOldPhysicalTreeId = state.State.OldPhysicalTreeId;
        var prevOldRegistryEntry = state.State.OldRegistryEntry;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.UndoResizeAsync());
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.EqualTo(prevInProgress));
            Assert.That(state.State.Complete, Is.EqualTo(prevComplete),
                "Complete must revert so the UndoResizeAsync top guard does not refuse retries.");
            Assert.That(state.State.SnapshotTreeId, Is.EqualTo(prevSnapshotTreeId));
            Assert.That(state.State.OldPhysicalTreeId, Is.EqualTo(prevOldPhysicalTreeId));
            Assert.That(state.State.OldRegistryEntry, Is.EqualTo(prevOldRegistryEntry));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void CompleteResize_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        state.State.OldPhysicalTreeId = TreeId;
        state.State.OperationId = "op1";

        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.CompleteResizeAsync());
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.EqualTo(prevInProgress),
                "InProgress must revert so the coordinator can re-attempt completion.");
            Assert.That(state.State.Complete, Is.EqualTo(prevComplete));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }
}
