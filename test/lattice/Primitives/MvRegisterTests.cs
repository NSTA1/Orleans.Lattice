using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class MvRegisterTests
{
    private static byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);
    private static string S(byte[] b) => System.Text.Encoding.UTF8.GetString(b);

    private static IReadOnlyList<string> ValuesAsStrings(MvRegister r)
        => r.Values().Select(static b => System.Text.Encoding.UTF8.GetString(b)).ToArray();

    [Test]
    public void New_register_is_empty()
    {
        var r = new MvRegister();
        Assert.That(r.IsEmpty, Is.True);
        Assert.That(r.Count, Is.EqualTo(0));
        Assert.That(r.Values(), Is.Empty);
    }

    [Test]
    public void Set_stores_single_value_on_one_replica()
    {
        var r = new MvRegister();
        r.Set("r1", B("alpha"));
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(ValuesAsStrings(r), Is.EquivalentTo(new[] { "alpha" }));
        Assert.That(r.Context["r1"], Is.EqualTo(1));
    }

    [Test]
    public void Sequential_set_on_same_replica_drops_prior_value()
    {
        var r = new MvRegister();
        r.Set("r1", B("a"));
        r.Set("r1", B("b"));
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(ValuesAsStrings(r), Is.EquivalentTo(new[] { "b" }));
        Assert.That(r.Context["r1"], Is.EqualTo(2));
    }

    [Test]
    public void Set_throws_on_null_value()
    {
        var r = new MvRegister();
        Assert.That(() => r.Set("r1", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Set_throws_on_empty_replica_id()
    {
        var r = new MvRegister();
        Assert.That(() => r.Set("", B("x")), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => r.Set(null!, B("x")), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Concurrent_writes_from_different_replicas_survive_merge()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));

        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var merged = MvRegister.Merge(a, b);
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "alpha", "beta" }));
        Assert.That(merged.Context["r1"], Is.EqualTo(1));
        Assert.That(merged.Context["r2"], Is.EqualTo(1));
    }

    [Test]
    public void Write_after_merge_observes_and_supersedes_prior_dots()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var merged = MvRegister.Merge(a, b);
        // r1 observes the merged context and writes a new value.
        merged.Set("r1", B("gamma"));
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "gamma" }));
        Assert.That(merged.Context["r1"], Is.EqualTo(2));
        Assert.That(merged.Context["r2"], Is.EqualTo(1));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var once = MvRegister.Merge(a, b);
        var twice = MvRegister.Merge(once, b);
        Assert.That(ValuesAsStrings(twice), Is.EquivalentTo(ValuesAsStrings(once)));
        Assert.That(twice.Context, Is.EquivalentTo(once.Context));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var b = new MvRegister();
        b.Set("r2", B("beta"));

        var ab = MvRegister.Merge(a, b);
        var ba = MvRegister.Merge(b, a);
        Assert.That(ValuesAsStrings(ab), Is.EquivalentTo(ValuesAsStrings(ba)));
        Assert.That(ab.Context, Is.EquivalentTo(ba.Context));
    }

    [Test]
    public void Merge_with_self_is_identity()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var copy = MvRegister.Merge(a, a);
        Assert.That(ValuesAsStrings(copy), Is.EquivalentTo(new[] { "alpha" }));
        Assert.That(copy.Context["r1"], Is.EqualTo(1));
    }

    [Test]
    public void Sequential_write_then_merge_with_stale_drops_stale_value()
    {
        var a = new MvRegister();
        a.Set("r1", B("a1"));
        // Snapshot a's stale state for later merge.
        var stale = a.Clone();
        a.Set("r1", B("a2"));

        var merged = MvRegister.Merge(a, stale);
        // The stale dot is dominated by a's post-write context (counter 2)
        // and must not be re-introduced.
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "a2" }));
    }

    [Test]
    public void Merge_throws_on_null_left()
    {
        Assert.That(() => MvRegister.Merge(null!, new MvRegister()), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_throws_on_null_right()
    {
        Assert.That(() => MvRegister.Merge(new MvRegister(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        Assert.That(() => new MvRegister().MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_produces_independent_copy()
    {
        var a = new MvRegister();
        a.Set("r1", B("a1"));
        var copy = a.Clone();
        copy.Set("r1", B("a2"));
        Assert.That(ValuesAsStrings(a), Is.EquivalentTo(new[] { "a1" }));
        Assert.That(ValuesAsStrings(copy), Is.EquivalentTo(new[] { "a2" }));
    }

    [Test]
    public void Values_returns_deterministic_order()
    {
        var a = new MvRegister();
        a.Set("rB", B("beta"));
        var b = new MvRegister();
        b.Set("rA", B("alpha"));
        var merged = MvRegister.Merge(a, b);
        // Ordered ascending by (ReplicaId, Counter): rA before rB.
        Assert.That(ValuesAsStrings(merged), Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void Three_way_concurrent_merge_keeps_all_three_values()
    {
        var a = new MvRegister();
        a.Set("r1", B("a"));
        var b = new MvRegister();
        b.Set("r2", B("b"));
        var c = new MvRegister();
        c.Set("r3", B("c"));

        var merged = MvRegister.Merge(MvRegister.Merge(a, b), c);
        Assert.That(ValuesAsStrings(merged), Is.EquivalentTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void Values_single_entry_returns_that_value()
    {
        var r = new MvRegister();
        r.Set("rA", B("solo"));
        Assert.That(ValuesAsStrings(r), Is.EqualTo(new[] { "solo" }));
    }

    [Test]
    public void ValuesShared_single_value_reads_reuse_cached_snapshot()
    {
        var r = new MvRegister();
        r.Set("r1", B("alpha"));
        var first = r.ValuesShared();
        var second = r.ValuesShared();
        // The single-value snapshot is cached and handed to repeated internal
        // reads between writes, so no fresh array is allocated. It is reached
        // through the internal aliasing view; the public Values copies out of it
        // per read (see Values_returns_a_fresh_projection_per_read), because the
        // cached arrays are the register's own stored buffers.
        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void Values_returns_a_fresh_projection_per_read()
    {
        var r = new MvRegister();
        r.Set("r1", B("alpha"));

        var first = r.Values();
        var second = r.Values();

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.Not.SameAs(first),
                "the public projection must not hand two readers the same instance");
            Assert.That(ValuesAsStrings(r), Is.EqualTo(new[] { "alpha" }),
                "though the two reads must agree on content");
        });
    }

    [Test]
    public void Values_snapshot_is_invalidated_by_set()
    {
        var r = new MvRegister();
        r.Set("r1", B("alpha"));
        var before = r.ValuesShared();
        r.Set("r1", B("beta"));
        var after = r.ValuesShared();
        Assert.That(after, Is.Not.SameAs(before));
        Assert.That(ValuesAsStrings(r), Is.EquivalentTo(new[] { "beta" }));
    }

    [Test]
    public void Values_snapshot_is_invalidated_by_merge_from()
    {
        var a = new MvRegister();
        a.Set("r1", B("alpha"));
        var before = a.ValuesShared();

        var b = new MvRegister();
        b.Set("r2", B("beta"));
        a.MergeFrom(b);

        var after = a.ValuesShared();
        Assert.That(after, Is.Not.SameAs(before));
        Assert.That(ValuesAsStrings(a), Is.EquivalentTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void Values_snapshot_is_invalidated_by_merge_delta()
    {
        var local = new MvRegister();
        local.Set("r1", B("a1"));
        var before = local.ValuesShared();

        // A later write on r1 the local side has not seen supersedes a1.
        var newer = local.Clone();
        newer.Set("r1", B("a2"));
        local.MergeDelta(DeltaFrom(newer));

        var after = local.ValuesShared();
        Assert.That(after, Is.Not.SameAs(before));
        Assert.That(ValuesAsStrings(local), Is.EquivalentTo(new[] { "a2" }));
    }

    [Test]
    public void Set_reuses_entries_list_in_steady_state()
    {
        var r = new MvRegister();
        r.Set("r1", B("a"));
        var list = r.Entries;
        // A same-replica steady-state write compacts in place: the drop of
        // the observed prior entry and the append reuse the existing list.
        r.Set("r1", B("b"));
        Assert.That(r.Entries, Is.SameAs(list));
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(ValuesAsStrings(r), Is.EquivalentTo(new[] { "b" }));
    }

    [Test]
    public void Values_ordering_matches_orderby_replicaid_ordinal_then_counter()
    {
        // Hand-build a transient multi-value state with entries deliberately
        // inserted out of order, including two dots on the same replica (so
        // the Counter tie-break is exercised) and replica ids whose ordinal
        // order differs from a culture-aware order. The optimised Array.Sort
        // path must reproduce the exact composite key the former LINQ chain
        // (OrderBy ReplicaId ordinal, ThenBy Counter) produced.
        var r = new MvRegister();
        r.Entries.AddRange(new[]
        {
            new MvRegisterEntry { ReplicaId = "rB", Counter = 2, Value = B("rB-2") },
            new MvRegisterEntry { ReplicaId = "rA", Counter = 5, Value = B("rA-5") },
            new MvRegisterEntry { ReplicaId = "rB", Counter = 1, Value = B("rB-1") },
            new MvRegisterEntry { ReplicaId = "rA", Counter = 1, Value = B("rA-1") },
        });

        var expected = r.Entries
            .OrderBy(static e => e.ReplicaId, StringComparer.Ordinal)
            .ThenBy(static e => e.Counter)
            .Select(static e => S(e.Value))
            .ToArray();

        Assert.That(ValuesAsStrings(r), Is.EqualTo(expected));
        Assert.That(ValuesAsStrings(r), Is.EqualTo(new[] { "rA-1", "rA-5", "rB-1", "rB-2" }));
    }

    // ===== MergeDelta =====
    // MergeDelta folds a typed delta directly into the receiver. It must be
    // observationally identical to building a transient MvRegister from the
    // delta's Entries + Context and calling MergeFrom (the form the direct
    // fold replaced), for every overlap shape.

    private static MvRegisterDelta DeltaFrom(MvRegister source) => new()
    {
        Entries = source.Entries.ToArray(),
        Context = new Dictionary<string, long>(source.Context, StringComparer.Ordinal),
    };

    private static MvRegister ViaMergeFrom(MvRegister local, MvRegisterDelta delta)
    {
        var other = new MvRegister
        {
            Entries = delta.Entries?.ToList() ?? [],
            Context = delta.Context is null
                ? []
                : new Dictionary<string, long>(delta.Context, StringComparer.Ordinal),
        };
        var copy = local.Clone();
        copy.MergeFrom(other);
        return copy;
    }

    private static void AssertSameState(MvRegister expected, MvRegister actual)
    {
        Assert.That(ValuesAsStrings(actual), Is.EqualTo(ValuesAsStrings(expected)));
        Assert.That(actual.Context, Is.EquivalentTo(expected.Context));
    }

    [Test]
    public void MergeDelta_concurrent_delta_keeps_both_values()
    {
        var local = new MvRegister();
        local.Set("r1", B("alpha"));
        var remote = new MvRegister();
        remote.Set("r2", B("beta"));
        var delta = DeltaFrom(remote);

        var expected = ViaMergeFrom(local, delta);
        local.MergeDelta(delta);

        AssertSameState(expected, local);
        Assert.That(ValuesAsStrings(local), Is.EquivalentTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public void MergeDelta_dominating_context_drops_superseded_local_value()
    {
        var local = new MvRegister();
        local.Set("r1", B("a1"));
        // A later write on r1 (counter 2) that the local side has not seen.
        var newer = local.Clone();
        newer.Set("r1", B("a2"));
        var delta = DeltaFrom(newer);

        var expected = ViaMergeFrom(local, delta);
        local.MergeDelta(delta);

        AssertSameState(expected, local);
        Assert.That(ValuesAsStrings(local), Is.EquivalentTo(new[] { "a2" }));
    }

    [Test]
    public void MergeDelta_is_idempotent()
    {
        var local = new MvRegister();
        local.Set("r1", B("alpha"));
        var remote = new MvRegister();
        remote.Set("r2", B("beta"));
        var delta = DeltaFrom(remote);

        local.MergeDelta(delta);
        var afterOnce = local.Clone();
        local.MergeDelta(delta);

        AssertSameState(afterOnce, local);
    }

    [Test]
    public void MergeDelta_empty_delta_is_noop()
    {
        var local = new MvRegister();
        local.Set("r1", B("alpha"));
        var before = local.Clone();

        local.MergeDelta(MvRegisterDelta.Empty);

        AssertSameState(before, local);
    }

    [Test]
    public void MergeDelta_default_delta_with_null_collections_is_noop()
    {
        var local = new MvRegister();
        local.Set("r1", B("alpha"));
        var before = local.Clone();

        local.MergeDelta(default);

        AssertSameState(before, local);
    }

    [Test]
    public void MergeDelta_context_only_delta_supersedes_without_new_entries()
    {
        var local = new MvRegister();
        local.Set("r1", B("a1"));
        // Delta carries only a context that has observed r1@1, with no entries:
        // the local value must be dropped (observed-and-superseded) and no new
        // value introduced.
        var delta = new MvRegisterDelta
        {
            Entries = System.Array.Empty<MvRegisterEntry>(),
            Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 1 },
        };

        var expected = ViaMergeFrom(local, delta);
        local.MergeDelta(delta);

        AssertSameState(expected, local);
        Assert.That(local.IsEmpty, Is.True);
        Assert.That(local.Context["r1"], Is.EqualTo(1));
    }

    [Test]
    public void MergeDelta_shared_dot_survives_even_when_context_dominates()
    {
        // The delta carries the same dot r1@1 as the local side and a context
        // that also lists r1@1. Structural presence of the dot must keep the
        // entry alive (it has not been superseded), matching MergeFrom.
        var local = new MvRegister();
        local.Set("r1", B("alpha"));
        var delta = DeltaFrom(local);

        var expected = ViaMergeFrom(local, delta);
        local.MergeDelta(delta);

        AssertSameState(expected, local);
        Assert.That(ValuesAsStrings(local), Is.EquivalentTo(new[] { "alpha" }));
    }
}
