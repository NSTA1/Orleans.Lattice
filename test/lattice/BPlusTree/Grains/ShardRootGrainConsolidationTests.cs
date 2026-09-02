using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the shard-root entry seams that online shard consolidation adds:
/// <c>ReclaimSlotsAsync</c> on the survivor and <c>AbortSplitAsync</c> on a
/// donor whose fold is abandoned before the routing map flips.
/// <para>
/// The reclaim seam carries the operation's sharpest correctness obligation.
/// A survivor is usually the shard the donor was originally split out of, so
/// it still refuses the very slots it is about to own again. Without the
/// reclaim, re-pointing the map at it would make every folded key permanently
/// unreachable - the map sends the reader to the survivor, the survivor's gate
/// sends it back to the retired donor, and the reader loops. These tests pin
/// both halves of the lift: the shard-level record and the per-leaf seal.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainConsolidationTests
{
    private const string TreeId = "consolidation-tree";
    private const int SurvivorShardIndex = 0;
    private const int DonorShardIndex = 1;
    private const int VirtualShardCount = 16;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness(int shardIndex = SurvivorShardIndex, int leafChainLength = 1)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{shardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "leaf-0");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 2, factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(Task.CompletedTask);

        // A single-element chain by default; longer chains loop the same stub
        // a bounded number of times so the walk is exercised without needing
        // distinct leaf identities.
        var remaining = leafChainLength - 1;
        leaf.GetNextSiblingAsync().Returns(_ =>
            Task.FromResult<GrainId?>(remaining-- > 0 ? GrainId.Create("leaf", $"leaf-{remaining}") : null));

        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Leaf = leaf, State = state };
    }

    private static void SealSlots(Harness harness, params int[] slots)
    {
        foreach (var slot in slots) harness.State.State.MovedAwaySlots[slot] = DonorShardIndex;
        harness.State.State.MovedAwayVirtualShardCount = VirtualShardCount;
    }

    // --- ReclaimSlotsAsync: validation ---

    [Test]
    public void ReclaimSlots_throws_on_null_slots()
    {
        var harness = CreateHarness();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => harness.Grain.ReclaimSlotsAsync(null!, VirtualShardCount));
    }

    [Test]
    public void ReclaimSlots_throws_on_non_positive_virtual_shard_count()
    {
        var harness = CreateHarness();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => harness.Grain.ReclaimSlotsAsync([1], 0));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => harness.Grain.ReclaimSlotsAsync([1], -3));
    }

    [Test]
    public void ReclaimSlots_throws_when_a_slot_is_outside_the_virtual_slot_space()
    {
        var harness = CreateHarness();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => harness.Grain.ReclaimSlotsAsync([VirtualShardCount], VirtualShardCount));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => harness.Grain.ReclaimSlotsAsync([-1], VirtualShardCount));
    }

    [Test]
    public async Task ReclaimSlots_is_a_no_op_for_an_empty_slot_set()
    {
        var harness = CreateHarness();
        SealSlots(harness, 1, 3);

        var reclaimed = await harness.Grain.ReclaimSlotsAsync([], VirtualShardCount);

        Assert.That(reclaimed, Is.Zero);
        Assert.That(harness.State.State.MovedAwaySlots, Has.Count.EqualTo(2));
    }

    // --- ReclaimSlotsAsync: the lift ---

    [Test]
    public async Task ReclaimSlots_removes_the_shard_level_seal_for_the_requested_slots()
    {
        var harness = CreateHarness();
        SealSlots(harness, 1, 3, 5);

        var reclaimed = await harness.Grain.ReclaimSlotsAsync([1, 5], VirtualShardCount);

        Assert.That(reclaimed, Is.EqualTo(2));
        Assert.That(harness.State.State.MovedAwaySlots.Keys, Is.EquivalentTo(new[] { 3 }));
        Assert.That(harness.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount),
            "Slots remain sealed, so the recorded virtual shard count must survive.");
    }

    [Test]
    public async Task ReclaimSlots_clears_the_recorded_virtual_shard_count_when_the_last_slot_is_lifted()
    {
        var harness = CreateHarness();
        SealSlots(harness, 1, 3);

        await harness.Grain.ReclaimSlotsAsync([1, 3], VirtualShardCount);

        Assert.That(harness.State.State.MovedAwaySlots, Is.Empty);
        Assert.That(harness.State.State.MovedAwayVirtualShardCount, Is.Null,
            "A shard with no sealed slot must not keep a stale slot-space stamp.");
    }

    [Test]
    public async Task ReclaimSlots_lifts_the_seal_on_every_leaf_of_the_chain()
    {
        var harness = CreateHarness(leafChainLength: 4);
        SealSlots(harness, 1, 3);

        await harness.Grain.ReclaimSlotsAsync([1, 3], VirtualShardCount);

        await harness.Leaf.Received(4).UnmarkSlotsMovedAwayAsync(
            Arg.Is<int[]>(s => s.Length == 2 && s[0] == 1 && s[1] == 3), VirtualShardCount);
    }

    [Test]
    public async Task ReclaimSlots_persists_before_touching_the_leaves()
    {
        // Ordering matters on crash recovery: a shard that persisted the
        // reclaim but has not yet reached every leaf is re-driven by the
        // coordinator's idempotent retry, whereas the reverse order could
        // leave leaves serving slots the shard record still rejects.
        var harness = CreateHarness(leafChainLength: 2);
        SealSlots(harness, 1);

        var writesAtFirstLeafCall = -1;
        harness.Leaf
            .When(l => l.UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>()))
            .Do(_ => writesAtFirstLeafCall = writesAtFirstLeafCall < 0 ? harness.State.WriteCount : writesAtFirstLeafCall);

        await harness.Grain.ReclaimSlotsAsync([1], VirtualShardCount);

        Assert.That(writesAtFirstLeafCall, Is.GreaterThanOrEqualTo(1),
            "The shard-level reclaim must be durable before any leaf seal is lifted.");
    }

    // --- ReclaimSlotsAsync: idempotence and safety ---

    [Test]
    public async Task ReclaimSlots_is_idempotent_when_re_driven()
    {
        var harness = CreateHarness();
        SealSlots(harness, 1, 3);

        var first = await harness.Grain.ReclaimSlotsAsync([1, 3], VirtualShardCount);
        var second = await harness.Grain.ReclaimSlotsAsync([1, 3], VirtualShardCount);

        Assert.That(first, Is.EqualTo(2));
        Assert.That(second, Is.Zero, "Re-driving a completed reclaim must be a clean no-op.");
        Assert.That(harness.State.State.MovedAwaySlots, Is.Empty);
    }

    [Test]
    public async Task ReclaimSlots_is_a_no_op_on_a_shard_that_never_sealed_anything()
    {
        var harness = CreateHarness();

        var reclaimed = await harness.Grain.ReclaimSlotsAsync([1, 3], VirtualShardCount);

        Assert.That(reclaimed, Is.Zero);
        await harness.Leaf.DidNotReceive().UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public async Task ReclaimSlots_leaves_slots_recorded_under_a_different_slot_space_untouched()
    {
        var harness = CreateHarness();
        SealSlots(harness, 1, 3);

        var reclaimed = await harness.Grain.ReclaimSlotsAsync([1, 3], VirtualShardCount * 2);

        Assert.That(reclaimed, Is.Zero,
            "Slot indices are only meaningful under the count they were recorded with.");
        Assert.That(harness.State.State.MovedAwaySlots, Has.Count.EqualTo(2));
    }

    [Test]
    public void ReclaimSlots_refuses_a_slot_an_active_split_is_migrating_away()
    {
        var harness = CreateHarness();
        SealSlots(harness, 1);
        harness.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.Drain,
            ShadowTargetShardIndex = 7,
            MovedSlots = [1, 3],
            VirtualShardCount = VirtualShardCount,
        };

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.Grain.ReclaimSlotsAsync([1], VirtualShardCount));
        Assert.That(ex!.Message, Does.Contain("adaptive split"));
    }

    [Test]
    public async Task ReclaimSlots_admits_slots_an_unrelated_active_split_does_not_touch()
    {
        var harness = CreateHarness();
        SealSlots(harness, 5);
        harness.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.Drain,
            ShadowTargetShardIndex = 7,
            MovedSlots = [1, 3],
            VirtualShardCount = VirtualShardCount,
        };

        var reclaimed = await harness.Grain.ReclaimSlotsAsync([5], VirtualShardCount);

        Assert.That(reclaimed, Is.EqualTo(1));
    }

    // --- AbortSplitAsync ---

    [Test]
    public async Task AbortSplit_is_a_no_op_when_no_migration_is_in_flight()
    {
        var harness = CreateHarness(shardIndex: DonorShardIndex);

        await harness.Grain.AbortSplitAsync();

        Assert.That(harness.State.State.SplitInProgress, Is.Null);
        Assert.That(harness.State.WriteCount, Is.Zero);
    }

    [Test]
    public Task AbortSplit_clears_a_migration_in_the_shadow_write_phase()
        => AssertAbortClearsAsync(ShardSplitPhase.BeginShadowWrite);

    [Test]
    public Task AbortSplit_clears_a_migration_in_the_drain_phase()
        => AssertAbortClearsAsync(ShardSplitPhase.Drain);

    [Test]
    public Task AbortSplit_clears_a_migration_in_the_swap_phase()
        => AssertAbortClearsAsync(ShardSplitPhase.Swap);

    private static async Task AssertAbortClearsAsync(ShardSplitPhase phase)
    {
        var harness = CreateHarness(shardIndex: DonorShardIndex);
        harness.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = phase,
            ShadowTargetShardIndex = SurvivorShardIndex,
            MovedSlots = [1, 3],
            VirtualShardCount = VirtualShardCount,
        };

        await harness.Grain.AbortSplitAsync();

        Assert.That(harness.State.State.SplitInProgress, Is.Null);
        Assert.That(harness.State.State.MovedAwaySlots, Is.Empty,
            "An abandoned fold must not leave the permanent retirement record CompleteSplitAsync would write.");
    }

    [Test]
    public void AbortSplit_refuses_a_migration_in_the_reject_phase()
        => AssertAbortRefused(ShardSplitPhase.Reject);

    [Test]
    public void AbortSplit_refuses_a_migration_in_the_complete_phase()
        => AssertAbortRefused(ShardSplitPhase.Complete);

    private static void AssertAbortRefused(ShardSplitPhase phase)
    {
        var harness = CreateHarness(shardIndex: DonorShardIndex);
        harness.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = phase,
            ShadowTargetShardIndex = SurvivorShardIndex,
            MovedSlots = [1, 3],
            VirtualShardCount = VirtualShardCount,
        };

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => harness.Grain.AbortSplitAsync());
        Assert.That(ex!.Message, Does.Contain(phase.ToString()));
        Assert.That(harness.State.State.SplitInProgress, Is.Not.Null,
            "A refused abort must leave the migration record intact.");
    }

    // --- The unreachable-key hazard the reclaim exists to prevent ---

    [Test]
    public async Task Reclaimed_slots_stop_being_refused_by_the_shard_read_gate()
    {
        // The survivor of a fold is usually the shard the donor was split out
        // of. Before the reclaim it refuses those slots; after it must serve
        // them, or the folded keys are unreachable from every route.
        var harness = CreateHarness();
        const string key = "reclaimed-key";
        var slot = ShardMap.GetVirtualSlot(key, VirtualShardCount);
        SealSlots(harness, slot);

        Assert.That(harness.Grain.IsSlotMovedAway(key), Is.True,
            "Precondition: the survivor still refuses the slot it is about to own again.");

        await harness.Grain.ReclaimSlotsAsync([slot], VirtualShardCount);

        Assert.That(harness.Grain.IsSlotMovedAway(key), Is.False,
            "After the reclaim the survivor must serve the slot the routing map now sends it.");
    }
}
