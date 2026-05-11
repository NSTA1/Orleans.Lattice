using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="BPlusLeafGrain.SetShardIndexAsync(int)"/> and
/// the foreground commit-site stamping of the
/// <see cref="LatticeMutation.ShardIndex"/> slot.
/// <para>
/// The shard-root coordinator calls <c>SetShardIndexAsync</c> exactly
/// once per leaf (next to <c>SetTreeIdAsync</c>) so the leaf can stamp
/// its owning chain-shard index onto every mutation it commits to the
/// WAL. At activation-time replay the leaf consults the same persisted
/// slot to filter out records authored by sibling chain shards sharing
/// a WAL partition (the cross-shard fanout regression gate).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task SetShardIndexAsync_persists_value_on_first_call()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetShardIndexAsync(3);

        Assert.That(state.State.ShardIndex, Is.EqualTo(3));
    }

    [Test]
    public async Task SetShardIndexAsync_writes_state_exactly_once_on_first_call()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var before = state.WriteCount;
        await grain.SetShardIndexAsync(2);

        Assert.That(state.WriteCount - before, Is.EqualTo(1));
    }

    [Test]
    public async Task SetShardIndexAsync_is_idempotent_when_already_set()
    {
        // Idempotency contract: a re-call (e.g. from a defensive
        // re-seed in a future code path) must not silently
        // overwrite the persisted value. Both the value and the
        // write-count must remain stable.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetShardIndexAsync(1);
        var writesAfterFirst = state.WriteCount;

        await grain.SetShardIndexAsync(99);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardIndex, Is.EqualTo(1), "first-call value preserved");
            Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst), "no extra persist on subsequent call");
        });
    }

    [Test]
    public async Task ShardIndex_defaults_to_null_before_first_seed_call()
    {
        var state = new FakePersistentState<LeafNodeState>();
        _ = CreateGrain(state);

        // The persisted slot is null until the shard root calls
        // SetShardIndexAsync. The apply-time filter treats null as
        // "legacy / unowned" and bypasses the ownership check — see
        // the materialiser tests for the behavioural side.
        Assert.That(state.State.ShardIndex, Is.Null);
        await Task.CompletedTask;
    }

    // -- foreground commit-site stamping (observable via mutation observer) --

    [Test]
    public async Task SetAsync_stamps_persisted_ShardIndex_onto_outgoing_mutation()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-stamp");
        await grain.SetShardIndexAsync(4);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(observer.Mutations[0].ShardIndex, Is.EqualTo(4));
    }

    [Test]
    public async Task SetAsync_stamps_zero_when_ShardIndex_unset_for_legacy_compat()
    {
        // Falls back to 0 for the V1 single-shard test path where
        // SetShardIndexAsync has not yet been called (every chain
        // shard is shard 0). Receivers without a persisted shard
        // index treat the slot as unconstrained.
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-stamp-legacy");
        // Note: deliberately do NOT call SetShardIndexAsync.

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(observer.Mutations[0].ShardIndex, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteAsync_stamps_persisted_ShardIndex_onto_outgoing_mutation()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-stamp-del");
        await grain.SetShardIndexAsync(7);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        var deleteMutation = observer.Mutations[^1];
        Assert.Multiple(() =>
        {
            Assert.That(deleteMutation.Kind, Is.EqualTo(MutationKind.Delete));
            Assert.That(deleteMutation.ShardIndex, Is.EqualTo(7));
        });
    }
}
