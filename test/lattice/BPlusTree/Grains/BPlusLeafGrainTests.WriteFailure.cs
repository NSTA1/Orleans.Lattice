using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the "persisted / in-memory divergence on
/// <c>WriteStateAsync</c> failure" anti-pattern (bug-hunter Class B,
/// idempotency-guarded shape) on the leaf-init quad:
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.SetTreeIdAsync"/>,
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.SetShardIndexAsync"/>,
/// and <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.SetKeyRangeAsync"/>.
/// Each method mutates <c>state.State</c> in memory before awaiting the
/// inner <c>state.WriteStateAsync()</c> (via <c>PersistAsync</c>), and each
/// guards the public entrypoint with an idempotency short-circuit
/// (<c>if (state.State.TreeId is not null) return;</c>, etc.). If the
/// persist call throws, the in-memory mutation survives, and every
/// subsequent retry on the same activation hits the guard and silently
/// returns - leaving this leaf permanently divergent from storage until
/// the activation is recycled. Cycle 2 fixed the same shape on
/// <c>BPlusInternalGrain</c>; this cycle bundles the leaf-init siblings.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public void SetTreeId_reverts_in_memory_TreeId_when_WriteStateAsync_throws()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        Assert.That(state.State.TreeId, Is.Null,
            "Test precondition: fresh leaf has no TreeId.");

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.SetTreeIdAsync("my-tree"));

        // If this fails, the idempotency guard at the top of
        // SetTreeIdAsync (`if (state.State.TreeId is not null) return;`)
        // short-circuits every retry, permanently divorcing the leaf's
        // in-memory TreeId from storage. ResolveCommitLogWriter() then
        // routes WAL appends against a tree id no shard root believes
        // owns this leaf.
        Assert.That(state.State.TreeId, Is.Null,
            "TreeId mutated in memory survived a failing WriteStateAsync; "
            + "the idempotency guard now short-circuits every retry, "
            + "leaving the leaf permanently divergent from storage.");
    }

    [Test]
    public void SetShardIndex_reverts_in_memory_ShardIndex_when_WriteStateAsync_throws()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        Assert.That(state.State.ShardIndex, Is.Null,
            "Test precondition: fresh leaf has no ShardIndex.");

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.SetShardIndexAsync(3));

        // If this fails, the idempotency guard
        // (`if (state.State.ShardIndex is not null) return;`)
        // short-circuits every retry, leaving the activation stamping
        // ShardIndex=3 on every foreground commit while every peer (or
        // a future reactivation) believes this leaf still has no owning
        // shard. The replay-time ownership filter on the cross-shard
        // fanout regression gate then silently drops legitimate records.
        Assert.That(state.State.ShardIndex, Is.Null,
            "ShardIndex mutated in memory survived a failing WriteStateAsync; "
            + "the idempotency guard now short-circuits every retry, "
            + "leaving the leaf permanently divergent from storage.");
    }

    [Test]
    public void SetKeyRange_reverts_in_memory_KeyRange_when_WriteStateAsync_throws()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        Assert.That(state.State.LowKeyInclusive, Is.Null,
            "Test precondition: fresh leaf has no LowKeyInclusive.");
        Assert.That(state.State.HighKeyExclusive, Is.Null,
            "Test precondition: fresh leaf has no HighKeyExclusive.");

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.SetKeyRangeAsync("monkey", "panda"));

        // If this fails, the idempotency guard
        // (`if (state.State.LowKeyInclusive is not null) return;`)
        // short-circuits every retry. The activation continues to
        // serve range-scans gated on the in-memory bounds while the
        // persisted state has open bounds; a peer reading from
        // storage would route the same key to a different sibling.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.LowKeyInclusive, Is.Null,
                "LowKeyInclusive mutated in memory survived a failing "
                + "WriteStateAsync; the idempotency guard now short-circuits "
                + "every retry, leaving the leaf's range bounds permanently "
                + "divergent from storage.");
            Assert.That(state.State.HighKeyExclusive, Is.Null,
                "HighKeyExclusive mutated in memory survived a failing "
                + "WriteStateAsync; the leaf's range bounds are permanently "
                + "divergent from storage.");
        });
    }
}