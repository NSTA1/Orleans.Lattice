using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Coverage for <see cref="OrMap{TKey, TValue}"/>'s linear-versus-hash dedup
/// crossover, and for the legacy-payload context fold.
/// <para>
/// Three separate sites in the map switch strategy at the same
/// <c>LinearDedupThreshold</c> (16): tombstoning in <c>Remove</c>, live-entry
/// resolution in <c>Get</c>, and tombstone union in <c>MergeFrom</c>. Each has a
/// small-list linear scan that avoids allocating a hash set, and a large-list
/// hash path that avoids the quadratic scan. They are pure optimisations, so the
/// property that matters is that <b>both strategies produce identical results</b>
/// - a divergence would be a convergence bug that only appears once a key
/// accumulates enough replicas, which is precisely the case no small fixture
/// would catch.
/// </para>
/// <para>
/// Every test below is therefore paired: the same scenario is run once under the
/// threshold and once over it, and the two are asserted to agree. The existing
/// <c>OrMapTests</c> family covers the small-list side of these paths
/// incidentally; nothing exercised the crossover.
/// </para>
/// </summary>
[TestFixture]
public class OrMapDedupThresholdTests
{
    /// <summary>
    /// Mirrors the private <c>OrMap.LinearDedupThreshold</c>. Asserted against
    /// observed behaviour by <see cref="The_dedup_threshold_is_where_this_fixture_assumes_it_is"/>
    /// so this fixture cannot silently stop straddling the crossover if the
    /// constant moves.
    /// </summary>
    private const int LinearDedupThreshold = 16;

    private static OrSet SetOf(params string[] elements)
    {
        var s = new OrSet();
        for (var i = 0; i < elements.Length; i++)
        {
            s.Add(System.Text.Encoding.UTF8.GetBytes(elements[i]), "seed", i + 1);
        }
        return s;
    }

    private static IReadOnlyList<string> Elements(OrSet? s) =>
        s is null
            ? Array.Empty<string>()
            : s.Elements()
                .Select(static b => System.Text.Encoding.UTF8.GetString(b))
                .OrderBy(static x => x, StringComparer.Ordinal)
                .ToArray();

    /// <summary>
    /// Writes <paramref name="key"/> once from each of <paramref name="replicas"/>
    /// distinct replicas, so the key accumulates that many live dots.
    /// </summary>
    private static OrMap<string, OrSet> MapWithDots(string key, int replicas, string valuePrefix = "v")
    {
        var m = new OrMap<string, OrSet>();
        for (var i = 0; i < replicas; i++)
        {
            m.Set(key, $"r{i}", SetOf($"{valuePrefix}{i}"));
        }
        return m;
    }

    [Test]
    public void The_dedup_threshold_is_where_this_fixture_assumes_it_is()
    {
        // Guards every "small" / "large" split below. The threshold is private,
        // so it is pinned behaviourally: a tombstone list at exactly the
        // threshold still takes the linear path, and one dot more crosses over.
        // Both must produce the same tombstone count, which is the invariant the
        // crossover exists to preserve.
        var atThreshold = MapWithDots("k", LinearDedupThreshold);
        var overThreshold = MapWithDots("k", LinearDedupThreshold + 1);

        atThreshold.Remove("k");
        overThreshold.Remove("k");

        Assert.Multiple(() =>
        {
            Assert.That(atThreshold.Tombstones["k"], Has.Count.EqualTo(LinearDedupThreshold));
            Assert.That(overThreshold.Tombstones["k"], Has.Count.EqualTo(LinearDedupThreshold + 1));
        });
    }

    // --- Remove: re-tombstoning an already-tombstoned key ---

    [Test]
    public void Remove_dedups_against_a_small_tombstone_list_and_adds_only_new_dots()
    {
        // First Remove tombstones r0's dot. The second write adds a dot from a
        // new replica, so the second Remove must skip the already-tombstoned dot
        // and add exactly the new one.
        var m = MapWithDots("k", replicas: 1);
        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.Tombstones["k"], Has.Count.EqualTo(1));

        m.Set("k", "r-late", SetOf("late"));

        Assert.That(m.Remove("k"), Is.True,
            "A newly observed dot must be reported as newly tombstoned.");
        Assert.That(m.Tombstones["k"], Has.Count.EqualTo(2),
            "The already-tombstoned dot must not be duplicated.");
    }

    [Test]
    public void Remove_dedups_against_a_large_tombstone_list_and_adds_only_new_dots()
    {
        // Same scenario over the threshold, so the hash-set dedup runs instead.
        var m = MapWithDots("k", replicas: LinearDedupThreshold + 1);
        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.Tombstones["k"], Has.Count.EqualTo(LinearDedupThreshold + 1));

        m.Set("k", "r-late", SetOf("late"));

        Assert.That(m.Remove("k"), Is.True);
        Assert.That(m.Tombstones["k"], Has.Count.EqualTo(LinearDedupThreshold + 2),
            "The hash path must dedup exactly as the linear path does.");
    }

    [Test]
    public void Remove_reports_no_change_when_every_dot_is_already_tombstoned()
    {
        // The negative half of the pair above, on both sides of the crossover:
        // a repeat Remove with no intervening write adds nothing and says so.
        var small = MapWithDots("k", replicas: 2);
        var large = MapWithDots("k", replicas: LinearDedupThreshold + 1);
        small.Remove("k");
        large.Remove("k");

        Assert.Multiple(() =>
        {
            Assert.That(small.Remove("k"), Is.False);
            Assert.That(small.Tombstones["k"], Has.Count.EqualTo(2));
            Assert.That(large.Remove("k"), Is.False);
            Assert.That(large.Tombstones["k"], Has.Count.EqualTo(LinearDedupThreshold + 1));
        });
    }

    // --- Get: resolving live entries against a large tombstone list ---

    [Test]
    public void Get_resolves_the_same_value_either_side_of_the_tombstone_crossover()
    {
        // A key whose tombstone list is large but which still has two live dots:
        // the resolution must skip every tombstoned dot and fold the survivors,
        // exactly as the small-list path does.
        var large = BuildResurrected(LinearDedupThreshold + 1);
        var small = BuildResurrected(2);

        Assert.Multiple(() =>
        {
            Assert.That(Elements(large.Get("k")), Is.EqualTo(new[] { "live-a", "live-b" }),
                "Only the two live dots may contribute to the resolved value.");
            Assert.That(Elements(small.Get("k")), Is.EqualTo(Elements(large.Get("k"))),
                "The hash and linear resolution paths must agree.");
            Assert.That(large.ContainsKey("k"), Is.True);
        });

        static OrMap<string, OrSet> BuildResurrected(int tombstonedReplicas)
        {
            var m = MapWithDots("k", tombstonedReplicas, valuePrefix: "dead");
            m.Remove("k");

            // Written after the removal, so these dots are not tombstoned and
            // the key is live again (add-wins).
            m.Set("k", "live-1", SetOf("live-a"));
            m.Set("k", "live-2", SetOf("live-b"));
            return m;
        }
    }

    [Test]
    public void Get_returns_null_when_every_dot_is_tombstoned_either_side_of_the_crossover()
    {
        // Positive control's counterpart: with no surviving dot the fold never
        // seeds, so the key resolves to nothing on both paths.
        var large = MapWithDots("k", LinearDedupThreshold + 1);
        var small = MapWithDots("k", 2);
        large.Remove("k");
        small.Remove("k");

        Assert.Multiple(() =>
        {
            Assert.That(large.Get("k"), Is.Null);
            Assert.That(small.Get("k"), Is.Null);
            Assert.That(large.ContainsKey("k"), Is.False);
        });
    }

    // --- MergeFrom: unioning tombstone lists ---

    [Test]
    public void MergeFrom_unions_small_tombstone_lists_without_duplicating_shared_dots()
    {
        // Both sides tombstone the same key, sharing the dots they both
        // observed. The union must contain each dot once.
        var (local, incoming) = BuildDivergentTombstones(sharedReplicas: 2);

        local.MergeFrom(incoming);

        Assert.That(local.Tombstones["k"], Is.Unique);
        Assert.That(local.Tombstones["k"], Has.Count.EqualTo(4),
            "Two shared dots plus one distinct dot per side.");
    }

    [Test]
    public void MergeFrom_unions_large_tombstone_lists_without_duplicating_shared_dots()
    {
        // The same union over the threshold, so the hash-set path runs.
        var (local, incoming) = BuildDivergentTombstones(sharedReplicas: LinearDedupThreshold);

        local.MergeFrom(incoming);

        Assert.That(local.Tombstones["k"], Is.Unique);
        Assert.That(local.Tombstones["k"], Has.Count.EqualTo(LinearDedupThreshold + 2),
            "The hash union must dedup the shared dots exactly as the linear union does.");
    }

    [Test]
    public void MergeFrom_tombstone_union_is_commutative_across_the_crossover()
    {
        // The property the two tests above exist to protect: a CRDT merge must
        // not depend on direction, and must not depend on which side of the
        // dedup crossover the lists happen to fall.
        var (a1, b1) = BuildDivergentTombstones(sharedReplicas: LinearDedupThreshold);
        var (a2, b2) = BuildDivergentTombstones(sharedReplicas: LinearDedupThreshold);

        a1.MergeFrom(b1);
        b2.MergeFrom(a2);

        Assert.That(
            b2.Tombstones["k"].OrderBy(d => d.ReplicaId, StringComparer.Ordinal).ToArray(),
            Is.EqualTo(a1.Tombstones["k"].OrderBy(d => d.ReplicaId, StringComparer.Ordinal).ToArray()));
    }

    /// <summary>
    /// Builds two maps that tombstone the same key over
    /// <paramref name="sharedReplicas"/> dots they both observed, plus one dot
    /// each that only that side observed.
    /// </summary>
    private static (OrMap<string, OrSet> local, OrMap<string, OrSet> incoming) BuildDivergentTombstones(
        int sharedReplicas)
    {
        var seed = MapWithDots("k", sharedReplicas);
        var local = seed.Clone();
        var incoming = seed.Clone();

        local.Set("k", "only-local", SetOf("l"));
        incoming.Set("k", "only-incoming", SetOf("i"));
        local.Remove("k");
        incoming.Remove("k");
        return (local, incoming);
    }

    // --- Legacy payload context fold ---

    [Test]
    public void MergeFrom_rebuilds_the_context_from_a_legacy_payloads_tombstone_dots()
    {
        // A payload written before the Context cache existed carries dots but an
        // empty Context. Folding it must recover the per-replica maxima from its
        // dots - including its TOMBSTONE dots, not just its adds. Missing the
        // tombstone half would leave the context failing to dominate a
        // tombstoned replica's counter, so a later Set on that replica could
        // mint a counter that collides with an already-tombstoned dot and be
        // born invisible.
        var legacy = new OrMap<string, OrSet>
        {
            Tombstones = new Dictionary<string, List<OrSetDot>>
            {
                ["gone"] = [new OrSetDot { ReplicaId = "ghost", Counter = 42 }],
            },
        };
        Assume.That(legacy.Context, Is.Empty, "The legacy shape under test has no context.");

        var target = new OrMap<string, OrSet>();
        target.MergeFrom(legacy);

        Assert.That(target.Context.TryGetValue("ghost", out var counter), Is.True,
            "The tombstoned replica must appear in the rebuilt context.");
        Assert.That(counter, Is.EqualTo(42));
    }

    [Test]
    public void A_replica_resurrected_after_a_legacy_merge_mints_a_counter_above_its_tombstone()
    {
        // The consequence the fold above protects, asserted end to end: a write
        // from the tombstoned replica must land live rather than collide with
        // its own tombstoned dot.
        var legacy = new OrMap<string, OrSet>
        {
            Tombstones = new Dictionary<string, List<OrSetDot>>
            {
                ["k"] = [new OrSetDot { ReplicaId = "ghost", Counter = 42 }],
            },
        };

        var target = new OrMap<string, OrSet>();
        target.MergeFrom(legacy);
        target.Set("k", "ghost", SetOf("reborn"));

        Assert.That(Elements(target.Get("k")), Is.EqualTo(new[] { "reborn" }),
            "A counter minted at or below the tombstoned dot would be born invisible.");
    }
}
