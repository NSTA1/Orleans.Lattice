using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the helper methods on <see cref="LatticeGrain"/>:
/// <c>GroupSlotsByOwner</c>, <c>ToSortedArray</c>, <c>ComputeOwnerDiff</c>, and
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
    // ToSortedArray
    // ============================================================================

    [Test]
    public void ToSortedArray_returns_empty_when_input_empty()
    {
        Assert.That(LatticeGrain.ToSortedArray([]), Is.Empty);
    }

    [Test]
    public void ToSortedArray_returns_ascending_copy_of_input()
    {
        var input = new List<int> { 5, 2, 8, 1, 4 };
        var result = LatticeGrain.ToSortedArray(input);

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 4, 5, 8 }));
        // Must be a copy - input is unchanged.
        Assert.That(input, Is.EqualTo(new List<int> { 5, 2, 8, 1, 4 }));
    }

    [Test]
    public void ToSortedArray_preserves_duplicates()
    {
        var result = LatticeGrain.ToSortedArray([3, 1, 3, 1, 2]);
        Assert.That(result, Is.EqualTo(new[] { 1, 1, 2, 3, 3 }));
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
}
