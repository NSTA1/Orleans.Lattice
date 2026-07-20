using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class OrMapTests
{
    private static OrSet SetOf(params string[] elements)
    {
        var s = new OrSet();
        for (var i = 0; i < elements.Length; i++)
        {
            s.Add(System.Text.Encoding.UTF8.GetBytes(elements[i]), "seed", i + 1);
        }
        return s;
    }

    private static IReadOnlyList<string> Elements(OrSet? s)
    {
        if (s is null) return Array.Empty<string>();
        return s.Elements().Select(static b => System.Text.Encoding.UTF8.GetString(b)).OrderBy(x => x, StringComparer.Ordinal).ToArray();
    }

    [Test]
    public void New_map_is_empty_and_bottom()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.IsEmpty, Is.True);
        Assert.That(m.IsBottom, Is.True);
        Assert.That(m.Count, Is.EqualTo(0));
        Assert.That(m.Keys(), Is.Empty);
    }

    [Test]
    public void Set_throws_on_null_arguments()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(() => m.Set(null!, "r1", new OrSet()), Throws.ArgumentNullException);
        Assert.That(() => m.Set("k", null!, new OrSet()), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => m.Set("k", "", new OrSet()), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => m.Set("k", "r1", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Remove_throws_on_null_key()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(() => m.Remove(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ContainsKey_and_Get_throw_on_null_key()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(() => m.ContainsKey(null!), Throws.ArgumentNullException);
        Assert.That(() => m.Get(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Set_then_Get_returns_merged_value()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("tags", "r1", SetOf("a"));
        Assert.That(m.ContainsKey("tags"), Is.True);
        Assert.That(Elements(m.Get("tags")), Is.EquivalentTo(new[] { "a" }));
        Assert.That(m.Count, Is.EqualTo(1));
        Assert.That(m.IsEmpty, Is.False);
    }

    [Test]
    public void Get_single_live_entry_returns_independent_copy()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("tags", "r1", SetOf("a"));

        var first = m.Get("tags");
        Assert.That(Elements(first), Is.EquivalentTo(new[] { "a" }));

        // The single-live-entry fast path returns a defensive clone: mutating it
        // must not change the stored value, and a subsequent Get is unaffected.
        first!.Add(System.Text.Encoding.UTF8.GetBytes("mutation"), "rogue", 1);
        Assert.That(Elements(m.Get("tags")), Is.EquivalentTo(new[] { "a" }));
    }

    [Test]
    public void Get_returns_null_for_absent_key()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.Get("missing"), Is.Null);
        Assert.That(m.ContainsKey("missing"), Is.False);
    }

    [Test]
    public void Remove_tombstones_observed_dots_and_returns_true()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("k", "r1", SetOf("a"));
        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.ContainsKey("k"), Is.False);
        Assert.That(m.Get("k"), Is.Null);
        Assert.That(m.IsEmpty, Is.True);
        Assert.That(m.IsBottom, Is.True);
    }

    [Test]
    public void Remove_returns_false_when_key_absent()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.Remove("missing"), Is.False);
    }

    [Test]
    public void Remove_is_idempotent()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("k", "r1", SetOf("a"));
        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.Remove("k"), Is.False);
    }

    [Test]
    public void Add_wins_against_concurrent_remove_on_other_replica()
    {
        // Replica r1 sets and removes; replica r2 sets concurrently
        // (no causal observation of r1's writes). Merge keeps r2's add.
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("alpha"));
        a.Remove("k");

        var b = new OrMap<string, OrSet>();
        b.Set("k", "r2", SetOf("beta"));

        var merged = OrMap<string, OrSet>.Merge(a, b);
        Assert.That(merged.ContainsKey("k"), Is.True);
        Assert.That(Elements(merged.Get("k")), Is.EquivalentTo(new[] { "beta" }));
    }

    [Test]
    public void Concurrent_sets_on_same_key_recursively_merge_values()
    {
        // Two replicas concurrently write OrSet values under "k" -
        // OR-Map's value semantics fold them via OrSet.MergeFrom.
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("alpha"));

        var b = new OrMap<string, OrSet>();
        b.Set("k", "r2", SetOf("beta"));

        var merged = OrMap<string, OrSet>.Merge(a, b);
        Assert.That(Elements(merged.Get("k")), Is.EquivalentTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("alpha"));
        var b = new OrMap<string, OrSet>();
        b.Set("k", "r2", SetOf("beta"));

        var ab = OrMap<string, OrSet>.Merge(a, b);
        var ba = OrMap<string, OrSet>.Merge(b, a);
        Assert.That(Elements(ab.Get("k")), Is.EqualTo(Elements(ba.Get("k"))));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("alpha"));
        var b = new OrMap<string, OrSet>();
        b.Set("k", "r2", SetOf("beta"));
        var c = new OrMap<string, OrSet>();
        c.Set("k", "r3", SetOf("gamma"));

        var left = OrMap<string, OrSet>.Merge(OrMap<string, OrSet>.Merge(a, b), c);
        var right = OrMap<string, OrSet>.Merge(a, OrMap<string, OrSet>.Merge(b, c));
        Assert.That(Elements(left.Get("k")), Is.EqualTo(Elements(right.Get("k"))));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("alpha"));
        var once = OrMap<string, OrSet>.Merge(a, a);
        var twice = OrMap<string, OrSet>.Merge(once, a);
        Assert.That(Elements(twice.Get("k")), Is.EqualTo(Elements(once.Get("k"))));
        Assert.That(twice.Count, Is.EqualTo(once.Count));
    }

    [Test]
    public void Merge_throws_on_null_arguments()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(() => OrMap<string, OrSet>.Merge(null!, m), Throws.ArgumentNullException);
        Assert.That(() => OrMap<string, OrSet>.Merge(m, null!), Throws.ArgumentNullException);
        Assert.That(() => m.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_is_independent_of_original()
    {
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("alpha"));
        var copy = a.Clone();
        copy.Set("k", "r2", SetOf("beta"));
        Assert.That(Elements(a.Get("k")), Is.EquivalentTo(new[] { "alpha" }));
        Assert.That(Elements(copy.Get("k")), Is.EquivalentTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void Set_with_pn_counter_value_recursively_merges()
    {
        var a = new OrMap<string, PnCounter>();
        var pa = new PnCounter();
        pa.Increment("r1", 3);
        a.Set("k", "r1", pa);

        var b = new OrMap<string, PnCounter>();
        var pb = new PnCounter();
        pb.Increment("r2", 5);
        b.Set("k", "r2", pb);

        var merged = OrMap<string, PnCounter>.Merge(a, b);
        Assert.That(merged.Get("k")!.Value, Is.EqualTo(8));
    }

    [Test]
    public void Keys_returns_only_live_keys_in_deterministic_order()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("b", "r1", SetOf("x"));
        m.Set("a", "r1", SetOf("y"));
        m.Set("c", "r1", SetOf("z"));
        m.Remove("b");

        Assert.That(m.Keys(), Is.EqualTo(new[] { "a", "c" }));
    }

    [Test]
    public void Set_after_remove_resurrects_key_with_fresh_dot()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("k", "r1", SetOf("alpha"));
        m.Remove("k");
        Assert.That(m.ContainsKey("k"), Is.False);
        m.Set("k", "r1", SetOf("beta"));
        Assert.That(m.ContainsKey("k"), Is.True);
        Assert.That(Elements(m.Get("k")), Is.EquivalentTo(new[] { "beta" }));
    }

    [Test]
    public void Count_reflects_only_live_keys()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.Count, Is.EqualTo(0));
        m.Set("a", "r1", SetOf("x"));
        m.Set("b", "r1", SetOf("y"));
        Assert.That(m.Count, Is.EqualTo(2));
        m.Remove("a");
        Assert.That(m.Count, Is.EqualTo(1));
        m.Remove("b");
        Assert.That(m.Count, Is.EqualTo(0));
    }

    [Test]
    public void IsBottom_is_true_after_every_key_tombstoned()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("a", "r1", SetOf("x"));
        Assert.That(m.IsBottom, Is.False);
        m.Remove("a");
        Assert.That(m.IsBottom, Is.True);
        Assert.That(m.IsEmpty, Is.True);
    }

    [Test]
    public void ContainsKey_is_false_after_remove_and_true_again_after_resurrect()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("k", "r1", SetOf("x"));
        Assert.That(m.ContainsKey("k"), Is.True);
        m.Remove("k");
        Assert.That(m.ContainsKey("k"), Is.False);
        m.Set("k", "r1", SetOf("y"));
        Assert.That(m.ContainsKey("k"), Is.True);
    }

    [Test]
    public void ContainsKey_returns_false_for_absent_key()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.ContainsKey("nope"), Is.False);
    }

    [Test]
    public void Get_returns_null_when_only_tombstoned_entries_remain()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("k", "r1", SetOf("x"));
        m.Remove("k");
        Assert.That(m.Get("k"), Is.Null);
    }

    [Test]
    public void Remove_returns_false_when_every_observed_dot_already_tombstoned()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("k", "r1", SetOf("x"));
        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.Remove("k"), Is.False);
    }

    [Test]
    public void MergeFrom_unions_disjoint_keys_when_both_sides_share_a_key()
    {
        // Targets the same-key, both-sides-non-empty branch in MergeFrom
        // (the small-list/linear path) with disjoint dots so the
        // "append to local list" code path is exercised end-to-end.
        var a = new OrMap<string, OrSet>();
        a.Set("k", "r1", SetOf("x"));

        var b = new OrMap<string, OrSet>();
        b.Set("k", "r2", SetOf("y"));

        a.MergeFrom(b);
        Assert.That(Elements(a.Get("k")), Is.EquivalentTo(new[] { "x", "y" }));
    }

    [Test]
    public void MergeFrom_with_more_than_threshold_entries_still_converges()
    {
        // Targets the hash-set path in MergeFrom by pushing the per-key
        // entry count past the linear-vs-hash crossover threshold (16).
        var a = new OrMap<string, OrSet>();
        var b = new OrMap<string, OrSet>();
        for (var i = 0; i < 12; i++) a.Set("k", $"r{i}", SetOf($"a{i}"));
        for (var i = 0; i < 12; i++) b.Set("k", $"s{i}", SetOf($"b{i}"));

        a.MergeFrom(b);

        var live = Elements(a.Get("k"));
        Assert.That(live.Count, Is.EqualTo(24));
        Assert.That(a.ContainsKey("k"), Is.True);
    }

    [Test]
    public void Remove_with_more_than_threshold_tombstones_dedupes_correctly()
    {
        // Push the per-key tombstone list past the linear-vs-hash
        // crossover so the HashSet code path in Remove is exercised.
        var m = new OrMap<string, OrSet>();
        for (var i = 0; i < 20; i++) m.Set("k", $"r{i}", SetOf("x"));
        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.Remove("k"), Is.False);
        Assert.That(m.ContainsKey("k"), Is.False);
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(() => m.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Keys_returns_empty_for_empty_map()
    {
        var m = new OrMap<string, OrSet>();
        Assert.That(m.Keys(), Is.Empty);
    }

    [Test]
    public void Keys_returns_empty_when_every_key_tombstoned()
    {
        var m = new OrMap<string, OrSet>();
        m.Set("a", "r1", SetOf("x"));
        m.Remove("a");
        Assert.That(m.Keys(), Is.Empty);
    }

    [Test]
    public void Same_dot_collision_on_both_sides_folds_value_via_MergeFrom()
    {
        // Build two maps whose Adds list contains a literal duplicate
        // dot for the same key but with different value snapshots; the
        // merge must fold the values via TValue.MergeFrom rather than
        // double-count them.
        var a = new OrMap<string, OrSet>
        {
            Adds =
            {
                ["k"] = new()
                {
                    new OrMapEntry<OrSet> { ReplicaId = "r1", Counter = 1, Value = SetOf("alpha") },
                },
            },
        };
        var b = new OrMap<string, OrSet>
        {
            Adds =
            {
                ["k"] = new()
                {
                    new OrMapEntry<OrSet> { ReplicaId = "r1", Counter = 1, Value = SetOf("beta") },
                },
            },
        };

        a.MergeFrom(b);

        // Single dot survives; its value is the union of the two snapshots.
        Assert.That(a.Adds["k"].Count, Is.EqualTo(1));
        Assert.That(Elements(a.Get("k")), Is.EquivalentTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void OrMapEntry_default_value_is_initialised_for_reference_value_types()
    {
        // Sanity-check that the OrMapEntry default ctor leaves Value
        // non-null when TValue is itself a CRDT class. This is the
        // contract that lets OrMap.MergeFrom call existing.Value.MergeFrom
        // without a null guard at the same-dot collision site.
        var entry = new OrMapEntry<OrSet> { ReplicaId = "r1", Counter = 1, Value = new OrSet() };
        Assert.That(entry.Value, Is.Not.Null);
    }
}
