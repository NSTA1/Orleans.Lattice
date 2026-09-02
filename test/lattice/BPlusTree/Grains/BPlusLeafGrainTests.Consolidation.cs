using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the leaf-side half of online shard consolidation: lifting the
/// moved-away seal a split wrote, so a survivor shard serves the virtual slots
/// it has just reclaimed.
/// <para>
/// The seal is sticky by design - a source leaf must never surface its orphan
/// snapshot after a slot has migrated - so these tests pin both that the lift
/// works for the reclaimed slots and that it is scoped: unrelated slots, a
/// different slot space, and a leaf that never sealed anything are all left
/// exactly as they were.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const int ConsolidationVirtualShardCount = 16;

    // --- Validation ---

    [Test]
    public void UnmarkSlotsMovedAway_throws_on_null_slots()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.UnmarkSlotsMovedAwayAsync(null!, ConsolidationVirtualShardCount));
    }

    [Test]
    public void UnmarkSlotsMovedAway_throws_on_non_positive_virtual_shard_count()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.UnmarkSlotsMovedAwayAsync([1], 0));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.UnmarkSlotsMovedAwayAsync([1], -1));
    }

    // --- The lift ---

    [Test]
    public async Task UnmarkSlotsMovedAway_removes_only_the_requested_slots()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1, 3, 5], ConsolidationVirtualShardCount);

        await grain.UnmarkSlotsMovedAwayAsync([1, 5], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 3 }).AsCollection);
        Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(ConsolidationVirtualShardCount));
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_retains_the_slot_space_stamp_as_the_lift_signal()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([2, 4], ConsolidationVirtualShardCount);

        await grain.UnmarkSlotsMovedAwayAsync([2, 4], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.Null);
        Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(ConsolidationVirtualShardCount),
            "A stamp with no sealed slot is the unambiguous wire signal that a seal was lifted. "
            + "A leaf that never sealed anything carries no stamp, so a cache can tell the two apart "
            + "and drop its own stale seal instead of refusing reclaimed keys forever.");
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_advances_the_delivery_cursor_so_at_head_caches_see_the_lift()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1], ConsolidationVirtualShardCount);
        var cursorAfterSeal = grain.CurrentDeliveryCursor;

        await grain.UnmarkSlotsMovedAwayAsync([1], ConsolidationVirtualShardCount);

        Assert.That(grain.CurrentDeliveryCursor.Sequence, Is.GreaterThan(cursorAfterSeal.Sequence),
            "A cache already at head takes a stripped-envelope fast path that would drop the lift "
            + "signal; advancing the cursor takes it off that path for exactly one refresh.");
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_delivers_the_lift_signal_to_an_at_head_reader()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("some-key", Encoding.UTF8.GetBytes("v"));
        await grain.MarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);

        // Catch a reader up to head, as a warm LeafCacheGrain would be.
        var atHead = (await grain.GetDeltaSinceCursorAsync(default)).DeliveryCursor;

        await grain.UnmarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);
        var afterLift = await grain.GetDeltaSinceCursorAsync(atHead);

        Assert.That(afterLift.MovedAwaySlots, Is.Null,
            "The leaf holds no sealed slot after the lift.");
        Assert.That(afterLift.MovedAwayVsc, Is.EqualTo(ConsolidationVirtualShardCount),
            "The retained stamp must reach the reader, or a warm cache keeps refusing reclaimed keys.");
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_restores_read_visibility_for_a_reclaimed_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        const string key = "folded-key";
        await grain.SetAsync(key, Encoding.UTF8.GetBytes("v1"));

        var slot = ShardMap.GetVirtualSlot(key, ConsolidationVirtualShardCount);
        await grain.MarkSlotsMovedAwayAsync([slot], ConsolidationVirtualShardCount);

        Assert.That(await grain.GetAsync(key), Is.Null,
            "Precondition: the seal hides the value from every read entrypoint.");

        await grain.UnmarkSlotsMovedAwayAsync([slot], ConsolidationVirtualShardCount);

        var value = await grain.GetAsync(key);
        Assert.That(value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("v1"),
            "After the lift the leaf must serve the key the survivor now owns.");
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_advances_the_leaf_version_so_caches_observe_the_lift()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1], ConsolidationVirtualShardCount);
        var clockAfterSeal = state.State.Clock;

        await grain.UnmarkSlotsMovedAwayAsync([1], ConsolidationVirtualShardCount);

        Assert.That(state.State.Clock, Is.GreaterThan(clockAfterSeal),
            "Without a version advance a LeafCacheGrain would keep refusing the reclaimed keys.");
    }

    // --- Scoping and idempotence ---

    [Test]
    public async Task UnmarkSlotsMovedAway_is_a_no_op_on_a_leaf_that_never_sealed_anything()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var writesBefore = state.WriteCount;

        await grain.UnmarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.Null);
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
            "A leaf holding no seal must not pay a storage write for an unrelated fold.");
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_is_a_no_op_for_an_empty_slot_set()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1], ConsolidationVirtualShardCount);
        var writesBefore = state.WriteCount;

        await grain.UnmarkSlotsMovedAwayAsync([], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 1 }).AsCollection);
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_is_a_no_op_when_no_requested_slot_is_sealed()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1], ConsolidationVirtualShardCount);
        var writesBefore = state.WriteCount;

        await grain.UnmarkSlotsMovedAwayAsync([2, 4], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 1 }).AsCollection);
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_ignores_a_different_virtual_shard_count()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);

        await grain.UnmarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount * 2);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 1, 3 }).AsCollection,
            "Slot indices are only meaningful under the count they were recorded with.");
    }

    [Test]
    public async Task UnmarkSlotsMovedAway_is_idempotent_when_re_driven()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.MarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);

        await grain.UnmarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);
        var writesAfterFirst = state.WriteCount;
        await grain.UnmarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.Null);
        Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst),
            "Re-driving a completed lift after a crash must cost nothing.");
    }

    [Test]
    public async Task Seal_and_lift_round_trip_leaves_the_leaf_resealable()
    {
        // A consolidated shard can be split again later, so the seal must be
        // re-appliable after a lift rather than one-shot.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.MarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);
        await grain.UnmarkSlotsMovedAwayAsync([1, 3], ConsolidationVirtualShardCount);
        await grain.MarkSlotsMovedAwayAsync([3], ConsolidationVirtualShardCount);

        Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { 3 }).AsCollection);
        Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(ConsolidationVirtualShardCount));
    }
}
