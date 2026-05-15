using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- MarkSlotsMovedAwayAsync: validation ---

    [Test]
    public void MarkSlotsMovedAway_null_slots_throws()
    {
        var grain = CreateGrain();
        Assert.That(
            async () => await grain.MarkSlotsMovedAwayAsync(null!, 16),
            Throws.ArgumentNullException);
    }

    [Test]
    public void MarkSlotsMovedAway_non_positive_vsc_throws()
    {
        var grain = CreateGrain();
        Assert.That(
            async () => await grain.MarkSlotsMovedAwayAsync(new[] { 0 }, 0),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(
            async () => await grain.MarkSlotsMovedAwayAsync(new[] { 0 }, -1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task MarkSlotsMovedAway_empty_array_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync(Array.Empty<int>(), 16);

        Assert.That(state.State.MovedAwaySlots, Is.Null);
        Assert.That(state.State.MovedAwayVirtualShardCount, Is.Null);
    }

    // --- MarkSlotsMovedAwayAsync: persistence and idempotency ---

    [Test]
    public async Task MarkSlotsMovedAway_persists_sorted_distinct_slots()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.MarkSlotsMovedAwayAsync(new[] { 3, 3, 7, 7, 11 }, 16);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 3, 7, 11 }));
        Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(16));
    }

    [Test]
    public async Task MarkSlotsMovedAway_merges_into_existing_set()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.MarkSlotsMovedAwayAsync(new[] { 1, 5 }, 16);
        await grain.MarkSlotsMovedAwayAsync(new[] { 3, 5, 9 }, 16);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 1, 3, 5, 9 }));
        Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(16));
    }

    [Test]
    public async Task MarkSlotsMovedAway_is_idempotent_when_all_slots_already_present()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.MarkSlotsMovedAwayAsync(new[] { 2, 4, 6 }, 16);
        var clockAfterFirst = state.State.Clock;
        var versionAfterFirst = state.State.Version.Clone();

        await grain.MarkSlotsMovedAwayAsync(new[] { 4, 6 }, 16);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 2, 4, 6 }));
        Assert.That(state.State.Clock, Is.EqualTo(clockAfterFirst));
        Assert.That(state.State.Version.DominatesOrEquals(versionAfterFirst), Is.True);
        Assert.That(versionAfterFirst.DominatesOrEquals(state.State.Version), Is.True);
    }

    [Test]
    public async Task MarkSlotsMovedAway_advances_clock_and_publishes_version()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, replicaId: "leaf-A");

        var clockBefore = state.State.Clock;
        await grain.MarkSlotsMovedAwayAsync(new[] { 5 }, 16);

        Assert.That(state.State.Clock, Is.Not.EqualTo(clockBefore));
        // The published per-replica clock equals the new state Clock.
        Assert.That(state.State.Version.Entries.Count, Is.GreaterThanOrEqualTo(1));
        Assert.That(state.State.Version.Entries.Values, Does.Contain(state.State.Clock));
    }

    // --- StateDelta carries moved-away metadata ---

    [Test]
    public async Task GetDeltaSince_propagates_moved_away_fields()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await grain.MarkSlotsMovedAwayAsync(new[] { 2, 8 }, 16);

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());
        Assert.That(delta.MovedAwaySlots, Is.EqualTo(new[] { 2, 8 }));
        Assert.That(delta.MovedAwayVsc, Is.EqualTo(16));
    }

    [Test]
    public async Task GetDeltaSince_returns_null_moved_away_when_unmarked()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());
        Assert.That(delta.MovedAwaySlots, Is.Null);
        Assert.That(delta.MovedAwayVsc, Is.Null);
    }

    // --- Read seal: GetAsync / GetWithVersionAsync / ExistsAsync / GetManyAsync ---

    [Test]
    public async Task Get_returns_null_for_moved_away_slot_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var slot = ShardMap.GetVirtualSlot("k1", 16);
        await grain.MarkSlotsMovedAwayAsync(new[] { slot }, 16);

        Assert.That(await grain.GetAsync("k1"), Is.Null);
    }

    [Test]
    public async Task GetWithVersion_returns_empty_for_moved_away_slot_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var slot = ShardMap.GetVirtualSlot("k1", 16);
        await grain.MarkSlotsMovedAwayAsync(new[] { slot }, 16);

        var result = await grain.GetWithVersionAsync("k1");
        Assert.That(result.Value, Is.Null);
        Assert.That(result.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task Exists_returns_false_for_moved_away_slot_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var slot = ShardMap.GetVirtualSlot("k1", 16);
        await grain.MarkSlotsMovedAwayAsync(new[] { slot }, 16);

        Assert.That(await grain.ExistsAsync("k1"), Is.False);
    }

    [Test]
    public async Task GetMany_skips_moved_away_keys_and_returns_others()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("alpha", Encoding.UTF8.GetBytes("a"));
        await grain.SetAsync("bravo", Encoding.UTF8.GetBytes("b"));
        await grain.SetAsync("charlie", Encoding.UTF8.GetBytes("c"));

        const int vsc = 16;
        var movedSlot = ShardMap.GetVirtualSlot("bravo", vsc);
        await grain.MarkSlotsMovedAwayAsync(new[] { movedSlot }, vsc);

        var result = await grain.GetManyAsync(new List<string> { "alpha", "bravo", "charlie" });

        Assert.That(result.ContainsKey("bravo"), Is.False);
        // alpha and charlie are only guaranteed to be in the result if
        // they did not hash to the moved slot; assert by recomputing.
        if (ShardMap.GetVirtualSlot("alpha", vsc) != movedSlot)
            Assert.That(result.ContainsKey("alpha"), Is.True);
        if (ShardMap.GetVirtualSlot("charlie", vsc) != movedSlot)
            Assert.That(result.ContainsKey("charlie"), Is.True);
    }

    [Test]
    public async Task Read_seal_does_not_affect_keys_outside_moved_slots()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        const int vsc = 16;
        var s1 = ShardMap.GetVirtualSlot("k1", vsc);
        var s2 = ShardMap.GetVirtualSlot("k2", vsc);
        // Pick a slot that does not equal either key's slot so neither
        // is sealed.
        var unrelatedSlot = 0;
        while (unrelatedSlot == s1 || unrelatedSlot == s2) unrelatedSlot++;

        await grain.MarkSlotsMovedAwayAsync(new[] { unrelatedSlot }, vsc);

        Assert.That(await grain.GetAsync("k1"), Is.Not.Null);
        Assert.That(await grain.GetAsync("k2"), Is.Not.Null);
    }
}