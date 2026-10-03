using NUnit.Framework;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4419: a cleared leaf must not write its row back.
/// <para>
/// <see cref="BPlusLeafGrain.ClearGrainStateAsync"/> deletes the leaf's row and
/// then deactivates the activation. A checkpoint advance still pending in that
/// activation was flushed by the deactivation's checkpoint barrier onto the
/// freshly cleared, default state, which wrote the row back: a row holding only
/// the checkpoint, with no tree id, no siblings and no parent. Nothing references
/// such a row and nothing clears it again. On a live deployment 8,385 of them had
/// accumulated, every one carrying exactly that shape, and each pinned the
/// snapshot storage the leaf owned before #4393.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_cleared_leaf_with_a_pending_checkpoint_does_not_write_its_row_back_on_deactivation()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, _, _) = CreateLeafForFlushTail(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingFlushTailCheckpointAsync(grain, state, wal);

        await grain.ClearGrainStateAsync();
        var writesAfterClear = state.WriteCount;

        // The deactivation the clear requests: the same barriers production runs,
        // including the checkpoint flush that used to write the pending advance.
        await DeactivateLeafAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesAfterClear),
                "THE ASSERTION. Nothing may be written after the clear; before the fix the deactivation's "
                + "checkpoint flush wrote the pending advance onto the cleared state");
            Assert.That(state.RecordExists, Is.False, "the leaf's row stays deleted");
            Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.Not.True,
                "the discarded advance must not be stamped onto the cleared state either");
        });
    }

    [Test]
    public async Task No_write_on_a_cleared_activation_can_recreate_the_leaf_row()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, _, _) = CreateLeafForFlushTail(wal.Coordinator);
        await ActivateAsync(grain);

        await grain.ClearGrainStateAsync();
        var writesAfterClear = state.WriteCount;

        // Any persist on this activation after the clear - here a topology write an
        // interleaved caller could still deliver before the deactivation lands -
        // would re-create the row as an unreferenced stub with no tree id. It must
        // fail rather than write.
        Assert.That(
            async () => await grain.SetNextSiblingAsync(GrainId.Create("leaf", Guid.NewGuid().ToString("N"))),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("cleared"));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesAfterClear));
            Assert.That(state.RecordExists, Is.False);
        });
    }
}
