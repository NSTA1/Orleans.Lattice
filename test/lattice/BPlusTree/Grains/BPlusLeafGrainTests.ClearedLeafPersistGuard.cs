using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4419: a leaf whose storage row was cleared (or never written) and
/// that carries no <c>TreeId</c> must not be resurrected as a TreeId-less
/// <c>leaf</c> stub row by a stray RPC. Nothing reclaims such a row: it is
/// unreachable from any shard root and carries no tree id for a per-tree sweep
/// to find it by. <c>PersistAsync</c> therefore refuses the write on an
/// activation where <c>!RecordExists &amp;&amp; TreeId is null</c>; the only
/// legitimate seeds (<c>SetTreeIdAsync</c>, <c>InitializeSiblingAsync</c>)
/// assign the tree id before they persist and are unaffected.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateRowlessTreelessLeaf()
    {
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        return (CreateGrain(state), state);
    }

    private static void AssertNoStubRowWritten(FakePersistentState<LeafNodeState> state, int expectedWrites = 0)
    {
        Assert.That(state.WriteCount, Is.EqualTo(expectedWrites), "a TreeId-less leaf with no row must not be persisted (#4419)");
        Assert.That(state.RecordExists, Is.False);
    }

    [Test]
    public async Task Cleared_leaf_SetNextSiblingAsync_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "next"));

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_SetPrevSiblingAsync_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        await grain.SetPrevSiblingAsync(GrainId.Create("leaf", "prev"));

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_SetShardIndexAsync_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        await grain.SetShardIndexAsync(3);

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_SetKeyRangeAsync_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        await grain.SetKeyRangeAsync("a", "m");

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_checkpoint_reassert_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();
        var projection = (ILeafProjection)grain;
        var current = await projection.GetCheckpointOffsetAsync();

        // An idempotent re-assert (offset == current, including the -1
        // never-checkpointed sentinel) is a force-flush signal: it persists
        // even without a pending advance, which is the projection writer
        // #4419 names.
        await projection.SetCheckpointOffsetAsync(current);

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_FlushCheckpointAsync_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();
        var projection = (ILeafProjection)grain;
        var current = await projection.GetCheckpointOffsetAsync();
        await projection.SetCheckpointOffsetAsync(Math.Max(current, 0) + 1);

        await projection.FlushCheckpointAsync();

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_after_ClearStateAsync_does_not_persist_on_a_stray_sibling_splice()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "seeded-tree";
        var grain = CreateGrain(state);
        await state.ClearStateAsync();
        var writesBefore = state.WriteCount;

        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "stray"));
        await grain.SetPrevSiblingAsync(GrainId.Create("leaf", "stray"));

        AssertNoStubRowWritten(state, writesBefore);
    }

    [Test]
    public async Task Cleared_leaf_SetTreeIdAsync_seed_still_persists()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        await grain.SetTreeIdAsync("seed-tree");

        Assert.That(state.WriteCount, Is.EqualTo(1), "the topology seed assigns TreeId before persisting and must not be refused");
        Assert.That(state.State.TreeId, Is.EqualTo("seed-tree"));
    }

    [Test]
    public async Task Cleared_leaf_guard_does_not_refuse_an_existing_row_without_TreeId()
    {
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = true };
        var grain = CreateGrain(state);

        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "next"));

        Assert.That(state.WriteCount, Is.EqualTo(1), "a stored row without a TreeId is a legacy row, not a stub; its updates must persist");
    }

    [Test]
    public async Task Cleared_leaf_guard_does_not_refuse_a_rowless_leaf_that_carries_a_TreeId()
    {
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        state.State.TreeId = "seeded-tree";
        var grain = CreateGrain(state);

        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "next"));

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }
}
