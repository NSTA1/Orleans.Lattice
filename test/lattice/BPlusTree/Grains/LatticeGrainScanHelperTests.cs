using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the helper methods on <see cref="LatticeGrain"/>:
/// <c>GroupSlotsByOwner</c>, <c>ComputeOwnerDiff</c>, and
/// <c>BuildOwnedSlotMap</c>. These pure functions underpin the
/// strongly-consistent scan reconciliation and the per-slot count routing.
/// </summary>
[TestFixture]
public class LatticeGrainScanHelperTests
{
    // ============================================================================
    // GroupSlotsByOwner
    // ============================================================================

    [Test]
    public void GroupSlotsByOwner_returns_empty_when_slot_set_empty()
    {
        var map = ShardMap.CreateDefault(8, 4);
        var result = LatticeGrain.GroupSlotsByOwner([], map);
        Assert.That(result, Is.Empty);
    }

    [Test]
    public void GroupSlotsByOwner_groups_slots_by_owning_physical_shard()
    {
        // Identity map of 8 virtual slots over 4 physical shards:
        // slots 0,4 → shard 0; 1,5 → 1; 2,6 → 2; 3,7 → 3.
        var map = ShardMap.CreateDefault(8, 4);

        var result = LatticeGrain.GroupSlotsByOwner([0, 1, 4, 5], map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 0, 1 }));
        Assert.That(result[0], Is.EquivalentTo(new[] { 0, 4 }));
        Assert.That(result[1], Is.EquivalentTo(new[] { 1, 5 }));
    }

    [Test]
    public void GroupSlotsByOwner_silently_drops_out_of_range_slots()
    {
        var map = ShardMap.CreateDefault(8, 4);

        var result = LatticeGrain.GroupSlotsByOwner([0, 8, 100, -1], map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 0 }));
        Assert.That(result[0], Is.EquivalentTo(new[] { 0 }));
    }

    [Test]
    public void GroupSlotsByOwner_collapses_all_slots_to_one_owner_when_map_remaps_uniformly()
    {
        var map = new ShardMap { Slots = [9, 9, 9, 9] };
        var result = LatticeGrain.GroupSlotsByOwner([0, 1, 2, 3], map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 9 }));
        Assert.That(result[9], Is.EquivalentTo(new[] { 0, 1, 2, 3 }));
    }

    // ============================================================================
    // GroupSlotsByOwner - fused ascending ordering
    //
    // The callers feed each bucket straight to GetSortedKeysBatchForSlotsAsync /
    // GetSortedEntriesBatchForSlotsAsync, which require ascending slots. The
    // grouping pass emits that order itself, so these tests pin the guarantee
    // that the removed copy-then-sort step used to provide.
    // ============================================================================

    [Test]
    public void GroupSlotsByOwner_emits_each_owner_bucket_in_ascending_slot_order()
    {
        // Every slot lands on shard 0, so the single bucket must carry the
        // whole set ascending regardless of HashSet enumeration order.
        var map = new ShardMap { Slots = [0, 0, 0, 0, 0, 0, 0, 0] };

        var result = LatticeGrain.GroupSlotsByOwner([5, 2, 7, 1, 4], map);

        Assert.That(result[0], Is.EqualTo(new[] { 1, 2, 4, 5, 7 }));
    }

    [Test]
    public void GroupSlotsByOwner_emits_ascending_buckets_on_the_sparse_fallback()
    {
        // Owner 9 is beyond the slot array length, forcing the hashing
        // fallback; it must produce the same ascending order as the dense arm.
        var map = new ShardMap { Slots = [9, 9, 9, 9] };

        var result = LatticeGrain.GroupSlotsByOwner([3, 0, 2, 1], map);

        Assert.That(result[9], Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void GroupSlotsByOwner_emits_ascending_buckets_for_negative_owners()
    {
        // A negative owner also routes to the hashing fallback.
        var map = new ShardMap { Slots = [-1, 0, -1, 0] };

        var result = LatticeGrain.GroupSlotsByOwner([3, 2, 1, 0], map);

        Assert.That(result[-1], Is.EqualTo(new[] { 0, 2 }));
        Assert.That(result[0], Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void GroupSlotsByOwner_matches_the_group_then_sort_shape_it_replaced()
    {
        // Differential test: the fused pass must agree with the original
        // "group into lists, then copy-and-sort each list" shape on the same
        // input, which is what makes the change output-identical.
        var map = ShardMap.CreateDefault(64, 5);
        var rng = new Random(20260905);
        var slots = new HashSet<int>();
        for (var i = 0; i < 200; i++)
            slots.Add(rng.Next(-10, 80));

        var actual = LatticeGrain.GroupSlotsByOwner(slots, map);
        var expected = GroupThenSortReference(slots, map);

        Assert.That(actual.Keys, Is.EquivalentTo(expected.Keys));
        foreach (var (owner, bucket) in expected)
            Assert.That(actual[owner], Is.EqualTo(bucket), $"owner {owner}");
    }

    /// <summary>
    /// The pre-optimisation shape: group into per-owner lists through a
    /// dictionary, then copy each list into a sorted array.
    /// </summary>
    private static Dictionary<int, int[]> GroupThenSortReference(HashSet<int> slots, ShardMap map)
    {
        var byOwner = new Dictionary<int, List<int>>();
        foreach (var s in slots)
        {
            if ((uint)s >= (uint)map.Slots.Length) continue;
            var owner = map.Slots[s];
            if (!byOwner.TryGetValue(owner, out var list))
            {
                list = [];
                byOwner[owner] = list;
            }
            list.Add(s);
        }

        var result = new Dictionary<int, int[]>(byOwner.Count);
        foreach (var (owner, list) in byOwner)
        {
            var arr = list.ToArray();
            Array.Sort(arr);
            result[owner] = arr;
        }
        return result;
    }

    // ============================================================================
    // ComputeOwnerDiff
    // ============================================================================

    [Test]
    public void ComputeOwnerDiff_returns_null_when_versions_equal()
    {
        var a = ShardMap.CreateDefault(8, 4);
        var b = ShardMap.CreateDefault(8, 4);
        // Both default identity maps have Version=0; treat as identical.
        Assert.That(LatticeGrain.ComputeOwnerDiff(a, b), Is.Null);
    }

    [Test]
    public void ComputeOwnerDiff_returns_null_when_versions_match_even_if_slots_differ()
    {
        // Defensive: the fast-path version equality short-circuits without
        // looking at slot contents. Documented contract.
        var a = new ShardMap { Slots = [0, 1, 2, 3], Version = 5 };
        var b = new ShardMap { Slots = [9, 9, 9, 9], Version = 5 };
        Assert.That(LatticeGrain.ComputeOwnerDiff(a, b), Is.Null);
    }

    [Test]
    public void ComputeOwnerDiff_returns_changed_slot_indices_when_versions_differ()
    {
        var a = new ShardMap { Slots = [0, 1, 2, 3, 0, 1, 2, 3], Version = 1 };
        var b = new ShardMap { Slots = [0, 1, 4, 3, 0, 1, 4, 3], Version = 2 };

        var diff = LatticeGrain.ComputeOwnerDiff(a, b);

        Assert.That(diff, Is.Not.Null);
        Assert.That(diff!, Is.EquivalentTo(new[] { 2, 6 }));
    }

    [Test]
    public void ComputeOwnerDiff_handles_empty_diff_when_only_version_changed()
    {
        var a = new ShardMap { Slots = [0, 1, 2, 3], Version = 1 };
        var b = new ShardMap { Slots = [0, 1, 2, 3], Version = 2 };

        var diff = LatticeGrain.ComputeOwnerDiff(a, b);

        // Versions differ → we must inspect; nothing actually changed → empty/null.
        Assert.That(diff, Is.Null.Or.Empty);
    }

    [Test]
    public void ComputeOwnerDiff_handles_different_lengths_by_comparing_overlapping_prefix()
    {
        var a = new ShardMap { Slots = [0, 1, 2, 3], Version = 1 };
        var b = new ShardMap { Slots = [9, 1, 2], Version = 2 };

        var diff = LatticeGrain.ComputeOwnerDiff(a, b);

        Assert.That(diff, Is.Not.Null);
        Assert.That(diff!, Is.EquivalentTo(new[] { 0 }));
    }

    // ============================================================================
    // BuildOwnedSlotMap
    //
    // The partitioner buckets virtual slots by owning physical shard through
    // dense owner-indexed arrays, falling back to hashed dictionaries for the
    // negative or pathologically sparse owners no production path emits. Both
    // arms must produce identical, ascending-ordered results.
    // ============================================================================

    [Test]
    public void BuildOwnedSlotMap_returns_empty_for_an_empty_map()
    {
        var result = LatticeGrain.BuildOwnedSlotMap(new ShardMap { Slots = [] });

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void BuildOwnedSlotMap_partitions_an_identity_map_by_owner()
    {
        // Identity map of 8 virtual slots over 4 physical shards:
        // slots 0,4 -> shard 0; 1,5 -> 1; 2,6 -> 2; 3,7 -> 3.
        var result = LatticeGrain.BuildOwnedSlotMap(ShardMap.CreateDefault(8, 4));

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 0, 1, 2, 3 }));
        Assert.That(result[0], Is.EqualTo(new[] { 0, 4 }));
        Assert.That(result[1], Is.EqualTo(new[] { 1, 5 }));
        Assert.That(result[2], Is.EqualTo(new[] { 2, 6 }));
        Assert.That(result[3], Is.EqualTo(new[] { 3, 7 }));
    }

    [Test]
    public void BuildOwnedSlotMap_emits_each_owner_bucket_in_ascending_slot_order()
    {
        // Deliberately unsorted ownership so a bucket built back-to-front
        // would be detected.
        var map = new ShardMap { Slots = [2, 0, 1, 0, 2, 2, 1, 0] };

        var result = LatticeGrain.BuildOwnedSlotMap(map);

        Assert.That(result[0], Is.EqualTo(new[] { 1, 3, 7 }));
        Assert.That(result[1], Is.EqualTo(new[] { 2, 6 }));
        Assert.That(result[2], Is.EqualTo(new[] { 0, 4, 5 }));
    }

    [Test]
    public void BuildOwnedSlotMap_omits_physical_shards_that_own_no_slot()
    {
        // Shard 1 is referenced by no slot, so it must not appear as a key
        // (an empty int[] would make callers fan out to an idle shard).
        var map = new ShardMap { Slots = [0, 2, 0, 2] };

        var result = LatticeGrain.BuildOwnedSlotMap(map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 0, 2 }));
        Assert.That(result[0], Is.EqualTo(new[] { 0, 2 }));
        Assert.That(result[2], Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void BuildOwnedSlotMap_collapses_to_one_owner_when_the_map_remaps_uniformly()
    {
        var map = new ShardMap { Slots = [3, 3, 3, 3] };

        var result = LatticeGrain.BuildOwnedSlotMap(map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 3 }));
        Assert.That(result[3], Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void BuildOwnedSlotMap_falls_back_for_owners_too_sparse_for_dense_buckets()
    {
        // maxOwner (9) >= slot count (4) routes through the hashed fallback.
        var map = new ShardMap { Slots = [9, 4, 9, 4] };

        var result = LatticeGrain.BuildOwnedSlotMap(map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { 4, 9 }));
        Assert.That(result[9], Is.EqualTo(new[] { 0, 2 }));
        Assert.That(result[4], Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void BuildOwnedSlotMap_falls_back_for_negative_owners()
    {
        // A negative owner cannot index a dense bucket; the fallback still
        // partitions it rather than throwing.
        var map = new ShardMap { Slots = [0, -1, 0, -1, 1] };

        var result = LatticeGrain.BuildOwnedSlotMap(map);

        Assert.That(result.Keys, Is.EquivalentTo(new[] { -1, 0, 1 }));
        Assert.That(result[-1], Is.EqualTo(new[] { 1, 3 }));
        Assert.That(result[0], Is.EqualTo(new[] { 0, 2 }));
        Assert.That(result[1], Is.EqualTo(new[] { 4 }));
    }

    [Test]
    public void BuildOwnedSlotMap_covers_every_slot_exactly_once_across_a_realistic_map()
    {
        // 4096 virtual slots over 16 physical shards - the production shape.
        var map = ShardMap.CreateDefault(4096, 16);

        var result = LatticeGrain.BuildOwnedSlotMap(map);

        var seen = new List<int>();
        foreach (var (owner, owned) in result)
        {
            Assert.That(owned, Is.Ordered.Ascending, $"shard {owner} bucket is not ascending");
            foreach (var slot in owned)
            {
                Assert.That(map.Slots[slot], Is.EqualTo(owner));
            }
            seen.AddRange(owned);
        }

        seen.Sort();
        Assert.That(seen, Is.EqualTo(Enumerable.Range(0, 4096).ToArray()));
    }

    // ============================================================================
    // BuildOwnedSlotMap(int[]) - the overload the snapshot cursor reuses
    // ============================================================================

    [Test]
    public void BuildOwnedSlotMap_slot_array_overload_agrees_with_the_shard_map_overload()
    {
        var map = ShardMap.CreateDefault(4096, 16);

        var fromMap = LatticeGrain.BuildOwnedSlotMap(map);
        var fromSlots = LatticeGrain.BuildOwnedSlotMap(map.Slots);

        Assert.That(fromSlots.Keys, Is.EquivalentTo(fromMap.Keys));
        foreach (var (owner, owned) in fromMap)
            Assert.That(fromSlots[owner], Is.EqualTo(owned), $"shard {owner}");
    }

    [Test]
    public void BuildOwnedSlotMap_slot_array_overload_matches_the_cursor_shape_it_replaced()
    {
        // Differential test: the snapshot cursor's own per-shard ownership
        // partition previously grew a Dictionary<int, List<int>> and copied
        // each list out. The shared dense pass must agree with it exactly,
        // including on the sparse and negative-owner fallbacks.
        int[][] cases =
        [
            [],
            [0, 0, 0, 0],
            [9, 9, 9, 9],
            [0, -1, 0, -1, 1],
            ShardMap.CreateDefault(4096, 16).Slots,
            ShardMap.CreateDefault(64, 5).Slots,
        ];

        foreach (var slots in cases)
        {
            var actual = LatticeGrain.BuildOwnedSlotMap(slots);
            var expected = CursorOwnedSlotsReference(slots);

            Assert.That(actual.Keys, Is.EquivalentTo(expected.Keys));
            foreach (var (shard, owned) in expected)
                Assert.That(actual[shard], Is.EqualTo(owned), $"shard {shard}");
        }
    }

    /// <summary>
    /// The pre-optimisation shape from <c>LatticeCursorGrain.Snapshot</c>:
    /// a forward scan into per-shard lists, then a copy of each list.
    /// </summary>
    private static Dictionary<int, int[]> CursorOwnedSlotsReference(int[] slots)
    {
        var lists = new Dictionary<int, List<int>>();
        for (var slot = 0; slot < slots.Length; slot++)
        {
            var shard = slots[slot];
            if (!lists.TryGetValue(shard, out var list))
            {
                list = new List<int>();
                lists[shard] = list;
            }
            list.Add(slot);
        }

        var result = new Dictionary<int, int[]>(lists.Count);
        foreach (var (shard, list) in lists)
            result[shard] = list.ToArray();
        return result;
    }
}
