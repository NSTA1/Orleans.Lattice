using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LeafReplayOwnership"/>, which pairs a replaying
/// leaf's judgement with the filter it pushes down to storage (issue #3565).
/// The safety argument for the push-down is one implication - every record the
/// filter excludes is one the judge rejects - so it is checked here directly,
/// over the full grid of ownership shapes a leaf can hold, rather than inferred
/// from the replay tests that depend on it.
/// </summary>
[TestFixture]
public sealed class LeafReplayOwnershipTests
{
    private const int LeafShard = 1;

    private static readonly ShardMap Map = ShardMap.CreateDefault(64, 4);

    private static readonly string[] Keys =
        ["a", "m", "m-owned", "mz", "n", "z", "\u00FCber", .. Enumerable.Range(0, 48).Select(i => $"k{i:D3}")];

    private static readonly MutationKind[] Kinds =
    [
        MutationKind.Set, MutationKind.Delete, MutationKind.Tombstone,
        MutationKind.DeleteRange, MutationKind.TxCommit, MutationKind.TxAbort,
    ];

    private static LeafNodeState State(int? shardIndex, string? low, string? high) => new()
    {
        ShardIndex = shardIndex,
        LowKeyInclusive = low,
        HighKeyExclusive = high,
    };

    private static IEnumerable<(LeafNodeState State, ShardMap? Map)> Ownerships()
    {
        foreach (var shard in new int?[] { null, LeafShard })
        {
            foreach (var (low, high) in new (string?, string?)[] { (null, null), ("m", "n"), ("k010", "k030"), (null, "k020") })
            {
                yield return (State(shard, low, high), null);
                yield return (State(shard, low, high), Map);
            }
        }
    }

    [Test]
    public void Every_record_the_filter_excludes_is_one_the_judge_rejects()
    {
        var checkedRecords = 0;
        Assert.Multiple(() =>
        {
            foreach (var (state, map) in Ownerships())
            {
                var ownership = LeafReplayOwnership.Capture(state, map);
                foreach (var kind in Kinds)
                {
                    foreach (var key in Keys)
                    {
                        // The stamp is varied too: without a map the judge reads
                        // it, and the filter must not assume it matches.
                        foreach (var stamp in new[] { 0, LeafShard, 3 })
                        {
                            var mutation = new LatticeMutation { TreeId = "t", Kind = kind, Key = key, ShardIndex = stamp };
                            if (ownership.Filter.Excludes(kind, key))
                            {
                                Assert.That(
                                    ownership.ShouldApply(mutation),
                                    Is.False,
                                    $"{kind} '{key}' (stamp {stamp}) is excluded by the filter but applied by the judge "
                                    + $"(shard {state.ShardIndex}, [{state.LowKeyInclusive}, {state.HighKeyExclusive}), map {(map is null ? "none" : "set")}).");
                            }

                            checkedRecords++;
                        }
                    }
                }
            }
        });

        Assert.That(checkedRecords, Is.GreaterThan(1000));
    }

    [Test]
    public void With_a_shard_map_the_filter_excludes_every_key_scoped_record_the_judge_rejects()
    {
        // The converse, which is what makes the shard axis worth pushing down:
        // under a map the judge ignores the stamp, so the filter can decide
        // exactly what the judge decides for a key-scoped record.
        var ownership = LeafReplayOwnership.Capture(State(LeafShard, "k010", "k040"), Map);

        Assert.Multiple(() =>
        {
            foreach (var kind in new[] { MutationKind.Set, MutationKind.Delete, MutationKind.Tombstone })
            {
                foreach (var key in Keys)
                {
                    var mutation = new LatticeMutation { TreeId = "t", Kind = kind, Key = key, ShardIndex = 3 };
                    Assert.That(
                        ownership.Filter.Excludes(kind, key),
                        Is.EqualTo(!ownership.ShouldApply(mutation)),
                        $"{kind} '{key}'.");
                }
            }
        });
    }

    [Test]
    public void The_filter_carries_the_shard_axis_only_when_the_judge_resolves_shards_through_a_map()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafReplayOwnership.Capture(State(LeafShard, "m", "n"), Map).Filter.HasShardConstraint, Is.True);
            Assert.That(
                LeafReplayOwnership.Capture(State(LeafShard, "m", "n"), null).Filter.HasShardConstraint,
                Is.False,
                "Without a map the judge falls back to the stamp, which storage does not read, so the axis stays with the judge.");
            Assert.That(
                LeafReplayOwnership.Capture(State(null, "m", "n"), Map).Filter.HasShardConstraint,
                Is.False,
                "A slot-less leaf owns every shard.");
            Assert.That(LeafReplayOwnership.Capture(State(null, null, null), null).Filter.IsUnbounded, Is.True);
            Assert.That(
                LeafReplayOwnership.Capture(State(LeafShard, null, null), new ShardMap { Slots = [] }).Filter.IsUnbounded,
                Is.True,
                "An empty map defines no ownership, so nothing is pushed down.");
        });
    }

    [TestCase("a", "c", "d", "g", false, TestName = "RangeOverlaps_range_wholly_below_the_leaf_is_disjoint")]
    [TestCase("h", "k", "d", "g", false, TestName = "RangeOverlaps_range_wholly_above_the_leaf_is_disjoint")]
    [TestCase("a", "d", "d", "g", false, TestName = "RangeOverlaps_end_equal_to_low_is_disjoint")]
    [TestCase("g", "k", "d", "g", false, TestName = "RangeOverlaps_start_equal_to_high_is_disjoint")]
    [TestCase("a", "e", "d", "g", true, TestName = "RangeOverlaps_range_straddling_low_overlaps")]
    [TestCase("f", "k", "d", "g", true, TestName = "RangeOverlaps_range_straddling_high_overlaps")]
    [TestCase("e", "f", "d", "g", true, TestName = "RangeOverlaps_range_inside_the_leaf_overlaps")]
    [TestCase("a", "z", "d", "g", true, TestName = "RangeOverlaps_range_covering_the_leaf_overlaps")]
    [TestCase("d", "d\u0000", "d", "g", true, TestName = "RangeOverlaps_single_key_range_at_low_overlaps")]
    [TestCase("a", "c", null, "g", true, TestName = "RangeOverlaps_unbounded_low_overlaps_a_range_below_high")]
    [TestCase("h", "k", null, "g", false, TestName = "RangeOverlaps_unbounded_low_is_still_disjoint_above_high")]
    [TestCase("h", "k", "d", null, true, TestName = "RangeOverlaps_unbounded_high_overlaps_a_range_above_low")]
    [TestCase("a", "c", "d", null, false, TestName = "RangeOverlaps_unbounded_high_is_still_disjoint_below_low")]
    [TestCase("a", "c", null, null, true, TestName = "RangeOverlaps_unbounded_leaf_overlaps_everything")]
    [TestCase("a", null, "d", "g", true, TestName = "RangeOverlaps_open_ended_range_below_high_overlaps")]
    [TestCase("h", null, "d", "g", false, TestName = "RangeOverlaps_open_ended_range_starting_at_or_above_high_is_disjoint")]
    [TestCase("e", "c", "a", "z", false, TestName = "RangeOverlaps_inverted_range_is_empty_and_so_disjoint")]
    public void RangeOverlaps_decides_half_open_intersection(
        string start, string? end, string? low, string? high, bool expected)
    {
        Assert.That(LeafReplayOwnership.RangeOverlaps(start, end, low, high), Is.EqualTo(expected));
    }

    [Test]
    public void IsDisjointRangeDelete_is_true_only_for_a_range_delete_outside_the_captured_bounds()
    {
        var ownership = LeafReplayOwnership.Capture(State(LeafShard, "d", "g"), Map);

        Assert.Multiple(() =>
        {
            Assert.That(ownership.IsDisjointRangeDelete(RangeDelete("a", "c")), Is.True);
            Assert.That(ownership.IsDisjointRangeDelete(RangeDelete("g", "k")), Is.True);
            Assert.That(ownership.IsDisjointRangeDelete(RangeDelete("a", "e")), Is.False, "A straddling range overlaps.");
            Assert.That(ownership.IsDisjointRangeDelete(RangeDelete("f", "k")), Is.False, "A straddling range overlaps.");

            // Only range deletes are ever consumed: every other kind keeps its
            // existing judgement, whatever its key.
            foreach (var kind in Kinds.Where(k => k != MutationKind.DeleteRange))
            {
                var outside = new LatticeMutation { TreeId = "t", Kind = kind, Key = "a", EndExclusiveKey = "c" };
                Assert.That(ownership.IsDisjointRangeDelete(outside), Is.False, $"{kind} is not a range delete.");
            }
        });

        Assert.That(
            LeafReplayOwnership.Capture(State(null, null, null), null).IsDisjointRangeDelete(RangeDelete("a", "c")),
            Is.False,
            "An unbounded leaf owns every key, so no range delete is disjoint from it.");
    }

    [Test]
    public void A_range_delete_judged_disjoint_covers_no_key_the_leaf_owns()
    {
        // The safety half of issue #3601, checked by brute force: consuming a
        // range delete without applying it is correct only if it cannot
        // tombstone any key the leaf holds, i.e. no key lies in both ranges.
        string?[] bounds = [null, "a", "k010", "k020", "k030", "m", "n", "z"];
        var checkedPairs = 0;

        Assert.Multiple(() =>
        {
            foreach (var low in bounds)
            {
                foreach (var high in bounds)
                {
                    var ownership = LeafReplayOwnership.Capture(State(null, low, high), null);
                    foreach (var start in bounds.OfType<string>())
                    {
                        foreach (var end in bounds)
                        {
                            if (!ownership.IsDisjointRangeDelete(RangeDelete(start, end)))
                                continue;

                            foreach (var key in Keys.Append(start))
                            {
                                var inDelete = string.CompareOrdinal(key, start) >= 0
                                    && (end is null || string.CompareOrdinal(key, end) < 0);
                                var inLeaf = (low is null || string.CompareOrdinal(key, low) >= 0)
                                    && (high is null || string.CompareOrdinal(key, high) < 0);
                                Assert.That(
                                    inDelete && inLeaf,
                                    Is.False,
                                    $"[{start}, {end}) was judged disjoint from [{low}, {high}) but both contain '{key}'.");
                            }

                            checkedPairs++;
                        }
                    }
                }
            }
        });

        Assert.That(checkedPairs, Is.GreaterThan(50), "The grid must actually exercise disjoint ranges.");
    }

    private static LatticeMutation RangeDelete(string start, string? end) => new()
    {
        TreeId = "t",
        Kind = MutationKind.DeleteRange,
        Key = start,
        EndExclusiveKey = end,
    };

    [Test]
    public void Capture_rejects_a_null_state()
    {
        Assert.That(() => LeafReplayOwnership.Capture(null!, null), Throws.ArgumentNullException);
    }
}
