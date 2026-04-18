using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the F-011 helper methods on <see cref="LatticeGrain"/>:
/// <c>GroupSlotsByOwner</c>, <c>ToSortedArray</c>, and <c>ComputeOwnerDiff</c>.
/// These pure functions underpin the strongly-consistent scan reconciliation.
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
        // Must be a copy — input is unchanged.
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
}
