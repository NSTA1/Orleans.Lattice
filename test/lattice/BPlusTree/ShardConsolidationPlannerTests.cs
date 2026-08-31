using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Tests for <see cref="ShardConsolidationPlanner"/>, the pure decision half
/// of online shard consolidation: which physical shards may be folded
/// together, which virtual slots that moves, and which pair a healing driver
/// should fold next.
/// </summary>
[TestFixture]
public class ShardConsolidationPlannerTests
{
    private static ShardMap Map(params int[] slots) => new() { Slots = slots };

    // --- AreAdjacent ---

    [Test]
    public void AreAdjacent_returns_true_for_consecutive_referenced_shards()
    {
        var map = Map(0, 1, 2, 0, 1, 2);

        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 1, 2), Is.True);
        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 0, 1), Is.True);
    }

    [Test]
    public void AreAdjacent_is_symmetric()
    {
        var map = Map(0, 1, 2, 0, 1, 2);

        Assert.That(
            ShardConsolidationPlanner.AreAdjacent(map, 2, 1),
            Is.EqualTo(ShardConsolidationPlanner.AreAdjacent(map, 1, 2)));
    }

    [Test]
    public void AreAdjacent_returns_false_when_a_referenced_shard_lies_between()
    {
        var map = Map(0, 1, 2, 0, 1, 2);

        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 0, 2), Is.False,
            "Shard 1 lies between 0 and 2 in the referenced set.");
    }

    [Test]
    public void AreAdjacent_ignores_gaps_left_by_unreferenced_indices()
    {
        // Physical index 1 was allocated by a split and later fully drained,
        // so nothing routes to it any more. 0 and 2 are then neighbours.
        var map = Map(0, 2, 0, 2);

        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 0, 2), Is.True);
    }

    [Test]
    public void AreAdjacent_returns_false_when_either_shard_is_unreferenced()
    {
        var map = Map(0, 1, 0, 1);

        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 1, 5), Is.False);
        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 7, 1), Is.False);
    }

    [Test]
    public void AreAdjacent_returns_false_for_the_same_shard()
    {
        var map = Map(0, 1, 0, 1);

        Assert.That(ShardConsolidationPlanner.AreAdjacent(map, 1, 1), Is.False);
    }

    [Test]
    public void AreAdjacent_throws_on_null_map()
    {
        Assert.Throws<ArgumentNullException>(() => ShardConsolidationPlanner.AreAdjacent(null!, 0, 1));
    }

    // --- CountOwnedSlots ---

    [Test]
    public void CountOwnedSlots_counts_only_the_requested_physical_shard()
    {
        var map = Map(0, 1, 1, 2, 1, 0);

        Assert.That(ShardConsolidationPlanner.CountOwnedSlots(map, 0), Is.EqualTo(2));
        Assert.That(ShardConsolidationPlanner.CountOwnedSlots(map, 1), Is.EqualTo(3));
        Assert.That(ShardConsolidationPlanner.CountOwnedSlots(map, 2), Is.EqualTo(1));
        Assert.That(ShardConsolidationPlanner.CountOwnedSlots(map, 9), Is.EqualTo(0));
    }

    [Test]
    public void CountOwnedSlots_throws_on_null_map()
    {
        Assert.Throws<ArgumentNullException>(() => ShardConsolidationPlanner.CountOwnedSlots(null!, 0));
    }

    // --- TryPlan ---

    [Test]
    public void TryPlan_returns_the_donor_slots_in_ascending_order()
    {
        var map = Map(0, 1, 0, 1, 0, 1);

        var planned = ShardConsolidationPlanner.TryPlan(map, 1, 0, out var plan, out _);

        Assert.That(planned, Is.True);
        Assert.That(plan.DonorShardIndex, Is.EqualTo(1));
        Assert.That(plan.SurvivorShardIndex, Is.EqualTo(0));
        Assert.That(plan.DonorSlots, Is.EqualTo(new[] { 1, 3, 5 }).AsCollection);
        Assert.That(plan.VirtualShardCount, Is.EqualTo(6));
    }

    [Test]
    public void TryPlan_reports_already_consolidated_when_the_donor_owns_no_slot()
    {
        var map = Map(0, 1, 0, 1);

        var planned = ShardConsolidationPlanner.TryPlan(map, 4, 0, out _, out var reason);

        Assert.That(planned, Is.False);
        Assert.That(reason, Does.Contain("already consolidated"));
    }

    [Test]
    public void TryPlan_refuses_a_survivor_outside_the_routing_map()
    {
        var map = Map(0, 1, 0, 1);

        var planned = ShardConsolidationPlanner.TryPlan(map, 1, 9, out _, out var reason);

        Assert.That(planned, Is.False);
        Assert.That(reason, Does.Contain("not part of the routing map"));
    }

    [Test]
    public void TryPlan_refuses_a_non_adjacent_pair()
    {
        var map = Map(0, 1, 2, 0, 1, 2);

        var planned = ShardConsolidationPlanner.TryPlan(map, 2, 0, out _, out var reason);

        Assert.That(planned, Is.False);
        Assert.That(reason, Does.Contain("not adjacent"));
    }

    [Test]
    public void TryPlan_refuses_donor_equal_to_survivor()
    {
        var map = Map(0, 1, 0, 1);

        var planned = ShardConsolidationPlanner.TryPlan(map, 1, 1, out _, out var reason);

        Assert.That(planned, Is.False);
        Assert.That(reason, Does.Contain("different physical shards"));
    }

    [Test]
    public void TryPlan_refuses_negative_indices()
    {
        var map = Map(0, 1, 0, 1);

        Assert.That(ShardConsolidationPlanner.TryPlan(map, -1, 0, out _, out var r1), Is.False);
        Assert.That(r1, Does.Contain("non-negative"));
        Assert.That(ShardConsolidationPlanner.TryPlan(map, 0, -2, out _, out var r2), Is.False);
        Assert.That(r2, Does.Contain("non-negative"));
    }

    [Test]
    public void TryPlan_refuses_an_empty_map()
    {
        var planned = ShardConsolidationPlanner.TryPlan(Map(), 0, 1, out _, out var reason);

        Assert.That(planned, Is.False);
        Assert.That(reason, Does.Contain("empty"));
    }

    [Test]
    public void TryPlan_throws_on_null_map()
    {
        Assert.Throws<ArgumentNullException>(
            () => ShardConsolidationPlanner.TryPlan(null!, 0, 1, out _, out _));
    }

    // --- TryPlanNext ---

    [Test]
    public void TryPlanNext_returns_false_for_a_single_shard_map()
    {
        var map = Map(0, 0, 0, 0);

        Assert.That(ShardConsolidationPlanner.TryPlanNext(map, out _), Is.False);
    }

    [Test]
    public void TryPlanNext_returns_false_for_an_empty_map()
    {
        Assert.That(ShardConsolidationPlanner.TryPlanNext(Map(), out _), Is.False);
    }

    [Test]
    public void TryPlanNext_picks_the_cheapest_adjacent_pair()
    {
        // Shard 0 owns 4 slots, shard 1 owns 1, shard 2 owns 1.
        // The (1,2) pair is the cheapest to fold.
        var map = Map(0, 0, 0, 0, 1, 2);

        var planned = ShardConsolidationPlanner.TryPlanNext(map, out var plan);

        Assert.That(planned, Is.True);
        Assert.That(new[] { plan.DonorShardIndex, plan.SurvivorShardIndex },
            Is.EquivalentTo(new[] { 1, 2 }));
    }

    [Test]
    public void TryPlanNext_retires_the_lighter_shard_of_the_pair()
    {
        // Adjacent pair (0,1): shard 0 owns 3 slots, shard 1 owns 1.
        var map = Map(0, 0, 0, 1);

        var planned = ShardConsolidationPlanner.TryPlanNext(map, out var plan);

        Assert.That(planned, Is.True);
        Assert.That(plan.DonorShardIndex, Is.EqualTo(1));
        Assert.That(plan.SurvivorShardIndex, Is.EqualTo(0));
    }

    [Test]
    public void TryPlanNext_breaks_a_tie_by_retiring_the_higher_index()
    {
        // Both shards own two slots; the low index survives so the healed map
        // drifts back toward the dense identity shape.
        var map = Map(0, 1, 0, 1);

        var planned = ShardConsolidationPlanner.TryPlanNext(map, out var plan);

        Assert.That(planned, Is.True);
        Assert.That(plan.DonorShardIndex, Is.EqualTo(1));
        Assert.That(plan.SurvivorShardIndex, Is.EqualTo(0));
    }

    [Test]
    public void TryPlanNext_is_deterministic_across_repeated_calls()
    {
        var map = Map(0, 0, 1, 2, 2, 3);

        ShardConsolidationPlanner.TryPlanNext(map, out var first);
        ShardConsolidationPlanner.TryPlanNext(map, out var second);

        Assert.That(second.DonorShardIndex, Is.EqualTo(first.DonorShardIndex));
        Assert.That(second.SurvivorShardIndex, Is.EqualTo(first.SurvivorShardIndex));
        Assert.That(second.DonorSlots, Is.EqualTo(first.DonorSlots).AsCollection);
    }

    [Test]
    public void TryPlanNext_repeated_application_converges_to_a_single_shard()
    {
        // Model a badly over-split tree and fold it repeatedly, exactly as a
        // healing driver would, asserting the shard count strictly decreases
        // every round and terminates rather than oscillating.
        var slots = new int[32];
        for (var i = 0; i < slots.Length; i++) slots[i] = i % 8;
        var map = Map(slots);

        var rounds = 0;
        var previousCount = map.GetPhysicalShardIndices().Count;
        Assert.That(previousCount, Is.EqualTo(8));

        while (ShardConsolidationPlanner.TryPlanNext(map, out var plan))
        {
            var next = (int[])map.Slots.Clone();
            foreach (var slot in plan.DonorSlots) next[slot] = plan.SurvivorShardIndex;
            map = Map(next);

            var count = map.GetPhysicalShardIndices().Count;
            Assert.That(count, Is.LessThan(previousCount),
                "Every fold must strictly reduce the physical shard count.");
            previousCount = count;

            rounds++;
            Assert.That(rounds, Is.LessThan(32), "Planner must terminate, not oscillate.");
        }

        Assert.That(map.GetPhysicalShardIndices().Count, Is.EqualTo(1));
    }

    [Test]
    public void TryPlanNext_preserves_every_virtual_slot_across_a_full_fold_sequence()
    {
        var slots = new int[64];
        for (var i = 0; i < slots.Length; i++) slots[i] = i % 16;
        var map = Map(slots);

        while (ShardConsolidationPlanner.TryPlanNext(map, out var plan))
        {
            var next = (int[])map.Slots.Clone();
            foreach (var slot in plan.DonorSlots) next[slot] = plan.SurvivorShardIndex;
            map = Map(next);

            Assert.That(map.Slots.Length, Is.EqualTo(64),
                "Consolidation never changes the virtual slot space, only its physical targets.");
            foreach (var target in map.Slots)
                Assert.That(target, Is.GreaterThanOrEqualTo(0),
                    "Every virtual slot must always route somewhere - no slot is ever orphaned.");
        }
    }

    [Test]
    public void TryPlanNext_throws_on_null_map()
    {
        Assert.Throws<ArgumentNullException>(() => ShardConsolidationPlanner.TryPlanNext(null!, out _));
    }
}
