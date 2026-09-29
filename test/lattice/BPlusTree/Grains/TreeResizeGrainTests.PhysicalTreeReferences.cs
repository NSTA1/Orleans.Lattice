using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <c>TreeResizeGrain.ReferencesPhysicalTreeAsync</c> (issue #3930):
/// the WAL GC discards a retired resize copy only when this coordinator no
/// longer names it, so a copy an undo can still recover must always be named.
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task ReferencesPhysicalTree_names_the_old_physical_tree_of_a_completed_resize()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.Complete = true;
        state.State.OldPhysicalTreeId = $"{TreeId}/resized/op1";
        state.State.SnapshotTreeId = $"{TreeId}/resized/op2";

        Assert.That(await grain.ReferencesPhysicalTreeAsync($"{TreeId}/resized/op1"), Is.True, "the copy an undo would recover");
        Assert.That(await grain.ReferencesPhysicalTreeAsync($"{TreeId}/resized/op2"), Is.True, "the live destination");
        Assert.That(await grain.ReferencesPhysicalTreeAsync($"{TreeId}/resized/op0"), Is.False, "an older copy no undo reaches");
    }

    [Test]
    public async Task ReferencesPhysicalTree_names_nothing_after_an_undo_reset_the_state()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OperationId = "undo-drain";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-drain";
        Assert.That(await grain.ReferencesPhysicalTreeAsync($"{TreeId}/resized/undo-drain"), Is.True, "precondition");

        await grain.UndoResizeAsync();

        Assert.That(await grain.ReferencesPhysicalTreeAsync($"{TreeId}/resized/undo-drain"), Is.False);
    }

    [Test]
    public void ReferencesPhysicalTree_rejects_a_null_id()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(() => grain.ReferencesPhysicalTreeAsync(null!));
    }
}
