using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins <see cref="ShardRootGrain.FoldRowsIntoUnion"/> - the single-probe
/// cross-leaf snapshot union - against the
/// <see cref="SortedDictionary{TKey, TValue}"/>-plus-parallel-mode-map shape it
/// replaces, so the two are shown to materialise byte-for-byte identical
/// baselines.
/// </summary>
/// <remarks>
/// The replacement is output-identical on two counts that a single happy-path
/// test would not catch: the key order (a sorted tree yields ascending ordinal
/// order inherently, the flat map only after one explicit sort), and the
/// per-key merge mode on a donor-orphan collision, where the mode must follow
/// whichever value the LWW merge kept - including a null incoming mode, which
/// clears a previously stored one.
/// </remarks>
public class SnapshotBaselineUnionFoldTests
{
    private static LeafSnapshotRow Row(string key, long ticks, LatticeMergeMode? mode, byte payload)
        => new(
            key,
            LwwValue<byte[]>.Create([payload], new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 }),
            mode);

    /// <summary>The pre-change shape, kept verbatim as the differential oracle.</summary>
    private static List<LeafSnapshotRow> MaterialiseWithSortedTree(
        IReadOnlyList<IReadOnlyList<LeafSnapshotRow>> leaves)
    {
        var union = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        var unionModes = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);

        foreach (var rows in leaves)
        {
            foreach (var row in rows)
            {
                if (union.TryGetValue(row.Key, out var existing))
                {
                    var merged = LwwValue<byte[]>.Merge(existing, row.Value);
                    union[row.Key] = merged;
                    if (ReferenceEquals(merged.Value, row.Value.Value)
                        || merged.Timestamp.Equals(row.Value.Timestamp))
                    {
                        if (row.MergeMode is { } incoming)
                            unionModes[row.Key] = incoming;
                        else
                            unionModes.Remove(row.Key);
                    }
                }
                else
                {
                    union[row.Key] = row.Value;
                    if (row.MergeMode is { } mode)
                        unionModes[row.Key] = mode;
                }
            }
        }

        var materialised = new List<LeafSnapshotRow>(union.Count);
        foreach (var (key, value) in union)
        {
            var mode = unionModes.TryGetValue(key, out var m) ? m : (LatticeMergeMode?)null;
            materialised.Add(new LeafSnapshotRow(key, value, mode));
        }

        return materialised;
    }

    /// <summary>The shipped shape, driving the real production fold.</summary>
    private static List<LeafSnapshotRow> MaterialiseWithFlatFold(
        IReadOnlyList<IReadOnlyList<LeafSnapshotRow>> leaves)
    {
        var union = new Dictionary<string, (LwwValue<byte[]> Value, LatticeMergeMode? Mode)>(
            StringComparer.Ordinal);

        foreach (var rows in leaves)
            ShardRootGrain.FoldRowsIntoUnion([.. rows], union);

        var orderedKeys = new string[union.Count];
        union.Keys.CopyTo(orderedKeys, 0);
        Array.Sort(orderedKeys, StringComparer.Ordinal);

        var materialised = new List<LeafSnapshotRow>(orderedKeys.Length);
        foreach (var key in orderedKeys)
        {
            var (value, mergeMode) = union[key];
            materialised.Add(new LeafSnapshotRow(key, value, mergeMode));
        }

        return materialised;
    }

    private static void AssertSameBaseline(IReadOnlyList<IReadOnlyList<LeafSnapshotRow>> leaves)
    {
        var expected = MaterialiseWithSortedTree(leaves);
        var actual = MaterialiseWithFlatFold(leaves);

        Assert.That(actual.Select(r => r.Key), Is.EqualTo(expected.Select(r => r.Key)));
        Assert.That(actual.Select(r => r.MergeMode), Is.EqualTo(expected.Select(r => r.MergeMode)));
        Assert.That(
            actual.Select(r => r.Value.Timestamp),
            Is.EqualTo(expected.Select(r => r.Value.Timestamp)));
        Assert.That(
            actual.Select(r => r.Value.Value),
            Is.EqualTo(expected.Select(r => r.Value.Value)));
    }

    [Test]
    public void Disjoint_leaves_materialise_in_ascending_ordinal_order()
    {
        // Leaves arrive in whatever order the frozen set enumerates, which is
        // not key order; the sort must impose it.
        AssertSameBaseline(
        [
            [Row("k/030", 30, null, 3), Row("k/010", 10, LatticeMergeMode.OrSet, 1)],
            [Row("k/020", 20, null, 2), Row("k/000", 5, null, 0)],
        ]);
    }

    [Test]
    public void Ordinal_ordering_is_preserved_for_keys_that_differ_by_case_and_separator()
    {
        // StringComparer.Ordinal is not culture ordering: '/' (0x2F) sorts
        // before digits, and upper case sorts before lower case.
        AssertSameBaseline(
        [
            [Row("a/b", 10, null, 1), Row("a-b", 11, null, 2), Row("A/b", 12, null, 3)],
            [Row("ab", 13, null, 4), Row("a/B", 14, null, 5)],
        ]);
    }

    [Test]
    public void A_donor_orphan_collision_keeps_the_highest_timestamp_and_its_mode()
    {
        AssertSameBaseline(
        [
            [Row("dup", 10, LatticeMergeMode.OrSet, 1)],
            [Row("dup", 20, LatticeMergeMode.PnCounter, 2)],
        ]);
    }

    [Test]
    public void A_losing_incoming_row_leaves_the_existing_mode_untouched()
    {
        AssertSameBaseline(
        [
            [Row("dup", 20, LatticeMergeMode.OrSet, 1)],
            [Row("dup", 10, LatticeMergeMode.PnCounter, 2)],
        ]);
    }

    [Test]
    public void A_winning_incoming_row_with_a_null_mode_clears_the_stored_mode()
    {
        // The pre-change shape did this with unionModes.Remove; the tuple form
        // assigns the null. Both must materialise a null mode.
        AssertSameBaseline(
        [
            [Row("dup", 10, LatticeMergeMode.OrSet, 1)],
            [Row("dup", 20, null, 2)],
        ]);
        Assert.That(
            MaterialiseWithFlatFold(
            [
                new[] { Row("dup", 10, LatticeMergeMode.OrSet, 1) },
                new[] { Row("dup", 20, null, 2) },
            ]).Single().MergeMode,
            Is.Null);
    }

    [Test]
    public void A_losing_incoming_row_with_a_null_mode_does_not_clear_the_stored_mode()
    {
        AssertSameBaseline(
        [
            [Row("dup", 20, LatticeMergeMode.OrSet, 1)],
            [Row("dup", 10, null, 2)],
        ]);
        Assert.That(
            MaterialiseWithFlatFold(
            [
                new[] { Row("dup", 20, LatticeMergeMode.OrSet, 1) },
                new[] { Row("dup", 10, null, 2) },
            ]).Single().MergeMode,
            Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void An_empty_leaf_set_materialises_an_empty_baseline()
    {
        AssertSameBaseline([]);
        Assert.That(MaterialiseWithFlatFold([]), Is.Empty);
    }

    [Test]
    public void A_wide_overlapping_leaf_set_agrees_with_the_prior_shape()
    {
        // 8 leaves x 64 rows with a deliberate 4-row overlap between adjacent
        // leaves, exercising both branches of the fold at volume.
        var leaves = new List<IReadOnlyList<LeafSnapshotRow>>();
        for (var leaf = 0; leaf < 8; leaf++)
        {
            var rows = new List<LeafSnapshotRow>(64);
            for (var r = 0; r < 64; r++)
            {
                var ordinal = (leaf * 64) + r - (r < 4 ? 4 : 0);
                rows.Add(Row(
                    $"k/{ordinal:D5}",
                    1_000 + ordinal + (r % 2),
                    (r % 3) switch
                    {
                        0 => LatticeMergeMode.OrSet,
                        1 => LatticeMergeMode.GCounter,
                        _ => null,
                    },
                    (byte)(r % 251)));
            }

            leaves.Add(rows);
        }

        AssertSameBaseline(leaves);
    }
}
