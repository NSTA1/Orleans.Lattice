using System.Buffers;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The immutable plan for one online shard consolidation: which physical
/// shard is retired, which absorbs it, and exactly which virtual slots move.
/// Produced by <see cref="ShardConsolidationPlanner"/> and handed to the
/// coordinator, so the decision of <i>what</i> to consolidate is separable -
/// and separately testable - from the machinery that performs it.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardConsolidationPlan)]
[Immutable]
internal readonly record struct ShardConsolidationPlan
{
    /// <summary>Physical shard index to retire from the routing map.</summary>
    [Id(0)] public int DonorShardIndex { get; init; }

    /// <summary>Physical shard index that absorbs the donor's virtual slots.</summary>
    [Id(1)] public int SurvivorShardIndex { get; init; }

    /// <summary>
    /// Sorted, distinct virtual slots currently routed to the donor. These are
    /// the slots the consolidation re-points onto the survivor.
    /// </summary>
    [Id(2)] public int[] DonorSlots { get; init; }

    /// <summary>
    /// Virtual shard count of the map the plan was computed against. Every
    /// slot index in <see cref="DonorSlots"/> is meaningful only under this
    /// count, so the coordinator carries it verbatim for the whole operation.
    /// </summary>
    [Id(3)] public int VirtualShardCount { get; init; }
}

/// <summary>
/// Pure planning helpers for online shard consolidation: adjacency under a
/// tree's routing map, validation of a caller-chosen pair, and selection of
/// the next pair a healing driver should fold.
/// <para>
/// <b>Adjacency.</b> A virtual-slot routing map has no geometry, so
/// "adjacent" is defined on the sorted sequence of <i>physical shard indices
/// the map actually references</i>: two shards are adjacent when no third
/// referenced physical shard index lies strictly between them. That makes
/// adjacency total, deterministic, and cheap to evaluate, and it lets a
/// driver walk a damaged tree's shard list folding neighbours pairwise until
/// the count comes back down.
/// </para>
/// <para>
/// Every method here is allocation-free except <see cref="TryPlan"/>, which
/// allocates the single donor-slot array that defines the operation.
/// </para>
/// </summary>
internal static class ShardConsolidationPlanner
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="donorShardIndex"/>
    /// and <paramref name="survivorShardIndex"/> are adjacent under
    /// <paramref name="map"/> - both are referenced by the map and no other
    /// referenced physical shard index lies strictly between them.
    /// <para>
    /// Runs one linear scan of the slot array and allocates nothing, so it is
    /// safe to call repeatedly while a driver searches for a foldable pair.
    /// </para>
    /// </summary>
    /// <param name="map">The tree's current routing map.</param>
    /// <param name="donorShardIndex">Physical shard index proposed for retirement.</param>
    /// <param name="survivorShardIndex">Physical shard index proposed to absorb the donor.</param>
    public static bool AreAdjacent(ShardMap map, int donorShardIndex, int survivorShardIndex)
    {
        ArgumentNullException.ThrowIfNull(map);

        if (donorShardIndex == survivorShardIndex) return false;

        var low = donorShardIndex < survivorShardIndex ? donorShardIndex : survivorShardIndex;
        var high = donorShardIndex < survivorShardIndex ? survivorShardIndex : donorShardIndex;

        var sawLow = false;
        var sawHigh = false;
        var slots = map.Slots;
        for (var i = 0; i < slots.Length; i++)
        {
            var physical = slots[i];
            if (physical == low) sawLow = true;
            else if (physical == high) sawHigh = true;
            else if (physical > low && physical < high) return false;
        }

        return sawLow && sawHigh;
    }

    /// <summary>
    /// Counts the virtual slots <paramref name="map"/> routes to
    /// <paramref name="physicalShardIndex"/>. Allocation-free.
    /// </summary>
    /// <param name="map">The tree's current routing map.</param>
    /// <param name="physicalShardIndex">Physical shard index to count slots for.</param>
    public static int CountOwnedSlots(ShardMap map, int physicalShardIndex)
    {
        ArgumentNullException.ThrowIfNull(map);

        var count = 0;
        var slots = map.Slots;
        for (var i = 0; i < slots.Length; i++)
        {
            if (slots[i] == physicalShardIndex) count++;
        }
        return count;
    }

    /// <summary>
    /// Validates a caller-chosen donor/survivor pair against
    /// <paramref name="map"/> and, when the pair is foldable, produces the
    /// plan describing exactly which virtual slots move.
    /// <para>
    /// Returns <see langword="false"/> with a human-readable
    /// <paramref name="reason"/> when the pair is not foldable. A donor that
    /// already owns no slot is <i>not</i> an error - it reports
    /// <see langword="false"/> with the "already consolidated" reason, which
    /// is what makes re-consolidating a finished pair a clean no-op rather
    /// than a fault.
    /// </para>
    /// </summary>
    /// <param name="map">The tree's current routing map.</param>
    /// <param name="donorShardIndex">Physical shard index to retire.</param>
    /// <param name="survivorShardIndex">Physical shard index to absorb the donor.</param>
    /// <param name="plan">The resulting plan when this method returns <see langword="true"/>.</param>
    /// <param name="reason">Why the pair is not foldable, when this method returns <see langword="false"/>.</param>
    public static bool TryPlan(
        ShardMap map,
        int donorShardIndex,
        int survivorShardIndex,
        out ShardConsolidationPlan plan,
        out string reason)
    {
        ArgumentNullException.ThrowIfNull(map);

        plan = default;

        if (map.Slots.Length == 0)
        {
            reason = "The shard map is empty.";
            return false;
        }

        if (donorShardIndex < 0 || survivorShardIndex < 0)
        {
            reason = "Shard indices must be non-negative.";
            return false;
        }

        if (donorShardIndex == survivorShardIndex)
        {
            reason = "Donor and survivor must be different physical shards.";
            return false;
        }

        var donorSlotCount = CountOwnedSlots(map, donorShardIndex);
        if (donorSlotCount == 0)
        {
            reason = "The donor shard already owns no virtual slot; it is already consolidated.";
            return false;
        }

        if (CountOwnedSlots(map, survivorShardIndex) == 0)
        {
            reason = "The survivor shard owns no virtual slot and is not part of the routing map.";
            return false;
        }

        if (!AreAdjacent(map, donorShardIndex, survivorShardIndex))
        {
            reason = "The donor and survivor shards are not adjacent under the routing map.";
            return false;
        }

        var donorSlots = new int[donorSlotCount];
        var write = 0;
        var slots = map.Slots;
        for (var i = 0; i < slots.Length; i++)
        {
            if (slots[i] == donorShardIndex) donorSlots[write++] = i;
        }

        plan = new ShardConsolidationPlan
        {
            DonorShardIndex = donorShardIndex,
            SurvivorShardIndex = survivorShardIndex,
            DonorSlots = donorSlots,
            VirtualShardCount = slots.Length,
        };
        reason = "";
        return true;
    }

    /// <summary>
    /// Selects the next adjacent pair a healing driver should fold, or
    /// returns <see langword="false"/> when the map has fewer than two
    /// physical shards and nothing can be consolidated.
    /// <para>
    /// The rule is deterministic so repeated passes converge instead of
    /// oscillating: among all adjacent pairs, pick the one whose combined
    /// slot count is smallest (folding the cheapest pair first keeps each
    /// operation short and keeps the tree balanced); break ties on the lower
    /// pair. Within the chosen pair the shard owning fewer slots is the
    /// donor, breaking ties in favour of retiring the <i>higher</i> physical
    /// index, because the identity map assigns the low indices and keeping
    /// them makes the healed map converge back toward its original shape.
    /// </para>
    /// </summary>
    /// <param name="map">The tree's current routing map.</param>
    /// <param name="plan">The selected plan when this method returns <see langword="true"/>.</param>
    public static bool TryPlanNext(ShardMap map, out ShardConsolidationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(map);

        plan = default;

        var slots = map.Slots;
        if (slots.Length == 0) return false;

        var physical = map.GetPhysicalShardIndices();
        if (physical.Count < 2) return false;

        // Count every shard's slots in a single pass. The per-pair
        // CountOwnedSlots shape this replaces was O(shards x slots), which on
        // exactly the badly-over-split trees this planner exists to heal - a
        // thousand-plus physical shards over four thousand virtual slots - is
        // millions of comparisons per planning call. The counts are rented
        // rather than allocated so a repeated planning sweep costs nothing.
        var maxIndex = physical[physical.Count - 1];
        var counts = ArrayPool<int>.Shared.Rent(maxIndex + 1);
        try
        {
            Array.Clear(counts, 0, maxIndex + 1);
            for (var i = 0; i < slots.Length; i++)
            {
                var target = slots[i];
                if (target >= 0 && target <= maxIndex) counts[target]++;
            }

            var bestFirst = -1;
            var bestSecond = -1;
            var bestFirstCount = 0;
            var bestSecondCount = 0;
            var bestCombined = int.MaxValue;

            // physical is sorted ascending and distinct, so consecutive
            // entries are exactly the adjacent pairs under the adjacency
            // definition.
            for (var i = 1; i < physical.Count; i++)
            {
                var previousCount = counts[physical[i - 1]];
                var currentCount = counts[physical[i]];
                var combined = previousCount + currentCount;
                if (combined >= bestCombined) continue;

                bestCombined = combined;
                bestFirst = physical[i - 1];
                bestSecond = physical[i];
                bestFirstCount = previousCount;
                bestSecondCount = currentCount;
            }

            if (bestFirst < 0) return false;

            // Retire the lighter shard; on a tie retire the higher index so
            // the surviving map drifts back toward the dense low-index
            // identity shape.
            var donor = bestSecondCount <= bestFirstCount ? bestSecond : bestFirst;
            var survivor = donor == bestSecond ? bestFirst : bestSecond;

            return TryPlan(map, donor, survivor, out plan, out _);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(counts);
        }
    }
}
