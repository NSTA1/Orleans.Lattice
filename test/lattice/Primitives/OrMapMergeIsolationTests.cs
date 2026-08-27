using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression for the merge-purity contract on
/// <see cref="OrMap{TKey, TValue}"/>. PR #1705 made
/// <see cref="OrMap{TKey, TValue}.Clone"/> deep, which protects the
/// <em>left</em> operand of the static
/// <see cref="OrMap{TKey, TValue}.Merge"/> (it is
/// <c>left.Clone().MergeFrom(right)</c>) - but
/// <see cref="OrMap{TKey, TValue}.MergeFrom"/> itself adopted the
/// <em>other</em> side's <see cref="OrMapEntry{TValue}"/> objects (and
/// therefore their nested value CRDTs) by reference at three sites. A later
/// same-dot fold resolves the collision in place via
/// <c>existing.Value.MergeFrom(...)</c>, so it wrote straight through the
/// alias into the map that was merged in - a merge that mutates its argument.
/// <para>
/// These pin all three adoption sites (key absent locally, key present with a
/// new dot below the linear-dedup threshold, and the same above it) plus the
/// fan-out shape that turns the aliasing into an observable divergence: one
/// snapshot folded into two targets must leave both targets agreeing on what
/// the snapshot authored.
/// </para>
/// </summary>
[TestFixture]
public class OrMapMergeIsolationTests
{
    // OrMap.LinearDedupThreshold - the combined-entry-count crossover above
    // which MergeFrom indexes the local side by dot instead of scanning it.
    private const int LinearDedupThreshold = 16;

    private static PnCounter Counter(long increment)
    {
        var counter = new PnCounter();
        counter.Increment("author", increment);
        return counter;
    }

    private static OrMap<string, PnCounter> MapWith(string key, string replicaId, long increment)
    {
        var map = new OrMap<string, PnCounter>();
        map.Set(key, replicaId, Counter(increment));
        return map;
    }

    [Test]
    public void MergeFrom_when_the_key_is_absent_locally_does_not_alias_the_other_side_entries()
    {
        // `other` authors dot (r1, 1) = 5 under "k".
        var other = MapWith("k", "r1", 5);
        var map = new OrMap<string, PnCounter>();

        // Key absent locally: the adoption site that installs other's entry list.
        map.MergeFrom(other);

        // A second map authoring the SAME dot (r1, 1) with a divergent value
        // drives the in-place same-dot fold on the entry `map` just adopted.
        map.MergeFrom(MapWith("k", "r1", 9));

        Assert.That(other.Get("k")!.Value, Is.EqualTo(5),
            "MergeFrom must not retain the other side's entry objects: a later same-dot fold wrote through the alias");
    }

    [Test]
    public void MergeFrom_when_the_key_is_present_and_the_dot_is_new_does_not_alias_the_other_side_entries()
    {
        // Local map already holds the key under a different dot, so the adoption
        // takes the small-list branch that appends other's entry object.
        var map = MapWith("k", "local", 1);
        var other = MapWith("k", "r1", 5);

        map.MergeFrom(other);
        map.MergeFrom(MapWith("k", "r1", 9));

        Assert.That(other.Get("k")!.Value, Is.EqualTo(5),
            "the small-list adoption branch must copy the entry, not append the other side's instance");
    }

    [Test]
    public void MergeFrom_when_the_combined_entry_count_exceeds_the_linear_threshold_does_not_alias_the_other_side_entries()
    {
        // Push the combined entry count for "k" past LinearDedupThreshold so the
        // merge takes the dictionary-indexed branch.
        var map = new OrMap<string, PnCounter>();
        for (var i = 0; i < LinearDedupThreshold + 2; i++)
        {
            map.Set("k", "filler-" + i, Counter(1));
        }

        var other = MapWith("k", "r1", 5);

        map.MergeFrom(other);
        map.MergeFrom(MapWith("k", "r1", 9));

        Assert.That(other.Get("k")!.Value, Is.EqualTo(5),
            "the dictionary-indexed adoption branch must copy the entry, not append the other side's instance");
    }

    [Test]
    public void Merge_when_one_snapshot_is_folded_into_two_targets_leaves_the_snapshot_unmodified()
    {
        // The fan-out shape: a single snapshot shipped to two shards. If the
        // first fold aliases the snapshot's entries, the second shard observes
        // a value the snapshot never authored.
        var snapshot = MapWith("k", "r1", 5);

        var shard1 = OrMap<string, PnCounter>.Merge(new OrMap<string, PnCounter>(), snapshot);
        shard1.MergeFrom(MapWith("k", "r1", 9));

        var shard2 = OrMap<string, PnCounter>.Merge(new OrMap<string, PnCounter>(), snapshot);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Get("k")!.Value, Is.EqualTo(5),
                "Merge must be pure in its right operand");
            Assert.That(shard2.Get("k")!.Value, Is.EqualTo(5),
                "a second target fed the same snapshot must observe only what the snapshot authored");
        });
    }

    [Test]
    public void Set_when_the_caller_mutates_the_value_afterwards_does_not_change_the_map()
    {
        var map = new OrMap<string, PnCounter>();
        var counter = Counter(5);

        map.Set("k", "r1", counter);
        counter.Increment("author", 100);

        Assert.That(map.Get("k")!.Value, Is.EqualTo(5),
            "Set must snapshot the caller's value on ingress; the caller keeps a live handle otherwise");
    }
}
