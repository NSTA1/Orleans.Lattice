using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4419: a leaf whose storage row was cleared (or never written) and
/// that carries no <c>TreeId</c> must not be resurrected as a TreeId-less
/// <c>leaf</c> stub row by a stray RPC. Nothing reclaims such a row: it is
/// unreachable from any shard root and carries no tree id for a per-tree sweep
/// to find it by. <c>PersistAsync</c> therefore refuses the write on an
/// activation where <c>!RecordExists &amp;&amp; TreeId is null</c> AND the
/// leaf holds nothing (no entries, no moved-away mask, no unresolved replay
/// work). A sibling being seeded by <c>InitializeSiblingAsync</c>, and an
/// unbound leaf that holds data (the tolerated unbound-donor state, #1744),
/// still persist so no acknowledged data is dropped.
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

        // A rowless leaf not being created fails closed rather than binding
        // (issue #4654); either way no stub row is written.
        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetShardIndexAsync(3));

        AssertNoStubRowWritten(state);
    }

    [Test]
    public async Task Cleared_leaf_SetKeyRangeAsync_does_not_persist_a_stub_row()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetKeyRangeAsync("a", "m"));

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

        // A seed is a create path, so it carries a create intent (issue #4654).
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            await grain.SetTreeIdAsync("seed-tree");
        }

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

        // A rowless leaf carrying a tree id in memory is one being created.
        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            await grain.SetNextSiblingAsync(GrainId.Create("leaf", "next"));
        }

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Unbound_split_sibling_init_persists_and_keeps_moved_entries()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();
        using var createIntent = LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain));

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = null!,
            ShardIndex = 4,
            LowKeyInclusive = "m",
            HighKeyExclusive = "z",
            NextSibling = null,
            PrevSibling = null,
            MovedAwaySlots = new[] { 3, 9 },
            MovedAwayVirtualShardCount = 16,
        });
        var writesAfterSeed = state.WriteCount;
        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), HybridLogicalClock.Tick(default)),
        });
        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "next"));

        Assert.That(writesAfterSeed, Is.GreaterThanOrEqualTo(1), "the sibling seed of an unbound donor (#1744) must persist its row");
        Assert.That(state.WriteCount, Is.GreaterThan(writesAfterSeed), "a populated unbound sibling must persist later updates");
        Assert.That(state.State.MovedAwaySlots, Is.EquivalentTo(new[] { 3, 9 }));
        Assert.That(state.State.NextSibling, Is.EqualTo(GrainId.Create("leaf", "next")));
        Assert.That(grain.EntriesForTest.ContainsKey("k1"), Is.True);
    }

    [Test]
    public async Task Unbound_rowless_leaf_data_write_is_persisted_by_the_next_persist()
    {
        var (grain, state) = CreateRowlessTreelessLeaf();

        // Only a leaf being created may take a write while rowless (issue #4654).
        using var createIntent = LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain));

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), HybridLogicalClock.Tick(default)),
        });
        await grain.SetNextSiblingAsync(GrainId.Create("leaf", "next"));

        Assert.That(state.WriteCount, Is.GreaterThan(0), "an acknowledged data write on an unbound leaf must not be dropped by the #4419 guard");
        Assert.That(state.State.NextSibling, Is.EqualTo(GrainId.Create("leaf", "next")));
        Assert.That(grain.EntriesForTest.ContainsKey("k1"), Is.True);
    }
}
