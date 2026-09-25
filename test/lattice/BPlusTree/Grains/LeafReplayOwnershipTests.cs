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

    [Test]
    public void Capture_rejects_a_null_state()
    {
        Assert.That(() => LeafReplayOwnership.Capture(null!, null), Throws.ArgumentNullException);
    }
}
