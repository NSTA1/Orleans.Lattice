using System.Text;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class OrMapProvenanceDecoderTests
{
    private static OrMapProvenanceDecoder Decoder => OrMapProvenanceDecoder.Instance;

    private static OrMapDeltaEntry<string, OrFlag> Add(string key, string replica, long counter) =>
        new() { Key = key, ReplicaId = replica, Counter = counter, Value = new OrFlag() };

    private static OrMapDeltaTombstone<string> Tomb(string key, string replica, long counter) =>
        new() { Key = key, ReplicaId = replica, Counter = counter };

    private static OrMapDelta<string, OrFlag> Delta(
        OrMapDeltaEntry<string, OrFlag>[]? adds = null,
        OrMapDeltaTombstone<string>[]? tombstones = null) => new()
    {
        Adds = adds ?? Array.Empty<OrMapDeltaEntry<string, OrFlag>>(),
        Tombstones = tombstones ?? Array.Empty<OrMapDeltaTombstone<string>>(),
    };

    private static string KeyOf(CrdtMemberChange change) => Encoding.UTF8.GetString(change.Element);

    [Test]
    public void Mode_is_ormap()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.OrMap));
    }

    [Test]
    public void DecodeDeltas_null_throws()
    {
        Assert.That(() => Decoder.DecodeDeltas(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeState_null_throws()
    {
        Assert.That(() => Decoder.DecodeState(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeDeltas_empty_sequence_yields_no_events()
    {
        Assert.That(Decoder.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>()), Is.Empty);
    }

    [Test]
    public void DecodeState_empty_map_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new OrMap<string, OrFlag>()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_add_yields_added_with_key_bytes_and_dot()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(adds: new[] { Add("k1", "r1", 3) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(KeyOf(events[0]), Is.EqualTo("k1"));
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Ordinal, Is.EqualTo(3L));
        });
    }

    [Test]
    public void DecodeDeltas_tombstone_yields_removed()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(tombstones: new[] { Tomb("k1", "r1", 3) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
            Assert.That(KeyOf(events[0]), Is.EqualTo("k1"));
        });
    }

    [Test]
    public void DecodeDeltas_preserves_remove_then_readd_order()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Add("k", "r1", 1) })),
            new CrdtProvenanceDelta(Delta(tombstones: new[] { Tomb("k", "r1", 1) })),
            new CrdtProvenanceDelta(Delta(adds: new[] { Add("k", "r1", 2) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            CrdtMemberChangeKind.Added,
            CrdtMemberChangeKind.Removed,
            CrdtMemberChangeKind.Added,
        }));
    }

    [Test]
    public void DecodeState_concurrent_adds_for_same_key_both_survive()
    {
        var a = new OrMap<string, OrFlag>();
        a.Set("k", "r1", new OrFlag());
        var b = new OrMap<string, OrFlag>();
        b.Set("k", "r2", new OrFlag());
        var merged = OrMap<string, OrFlag>.Merge(a, b);

        var events = Decoder.DecodeState(merged);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added && KeyOf(e) == "k"), Is.True);
        Assert.That(events.Select(e => e.ReplicaId), Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeState_remove_then_readd_shows_both_events()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("k", "r1", new OrFlag());   // dot (r1, 1)
        map.Remove("k");                     // tombstones (r1, 1)
        map.Set("k", "r1", new OrFlag());   // fresh dot (r1, 2)

        var events = Decoder.DecodeState(map);

        // The tombstoned add stays in the add set, so a re-add surfaces both
        // the original add, its removal, and the new live add - all under "k".
        Assert.That(events.All(e => KeyOf(e) == "k"), Is.True);
        var adds = events.Where(e => e.Kind == CrdtMemberChangeKind.Added).Select(e => e.Ordinal);
        var removes = events.Where(e => e.Kind == CrdtMemberChangeKind.Removed).Select(e => e.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(adds, Is.EquivalentTo(new[] { 1L, 2L }));
            Assert.That(removes, Is.EquivalentTo(new[] { 1L }));
        });
    }

    [Test]
    public void DecodeState_groups_by_key_deterministically()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("kB", "r1", new OrFlag());
        map.Set("kA", "r1", new OrFlag());

        var first = Decoder.DecodeState(map);
        var second = Decoder.DecodeState(map);

        Assert.That(first.Select(KeyOf), Is.EqualTo(second.Select(KeyOf)));
        Assert.That(first.Select(KeyOf), Is.EqualTo(new[] { "kA", "kB" }));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("k", "r1", new OrFlag());

        var events = Decoder.DecodeState(map);

        Assert.That(events, Is.Not.Empty, "the 'always null' claim is only meaningful over a non-empty decode");
        Assert.That(events.All(e => e.WallClock is null), Is.True);
    }

    // ---- current-value (live keys only) path ----

    private static string KeyOf(CrdtMemberValue member) => Encoding.UTF8.GetString(member.Element);

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_empty_map_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new OrMap<string, OrFlag>()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_yields_one_member_per_live_key_sorted()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("kB", "r1", new OrFlag());
        map.Set("kA", "r1", new OrFlag());

        var members = Decoder.DecodeCurrentValue(map);

        Assert.That(members.Select(KeyOf), Is.EqualTo(new[] { "kA", "kB" }));
    }

    [Test]
    public void DecodeCurrentValue_excludes_removed_key()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("kept", "r1", new OrFlag());
        map.Set("dropped", "r1", new OrFlag());
        map.Remove("dropped");

        var members = Decoder.DecodeCurrentValue(map);

        // The removed key's add dot lingers under a tombstone but must not surface
        // in the current value.
        Assert.That(members.Select(KeyOf), Is.EqualTo(new[] { "kept" }));
    }

    // ---- tombstone counter index (shared-replica fast path) ----
    //
    // Above a threshold a key's membership test switches from a linear scan of
    // its tombstone list to a sorted counter index, licensed only when every
    // tombstone for that key carries one replica id. These cover both sides of
    // that licence plus the shape a counter-only test would get wrong.

    /// <summary>Churns one key past the index threshold, then re-adds it.</summary>
    private static OrMap<string, OrFlag> ChurnedMap(int cycles)
    {
        var map = new OrMap<string, OrFlag>();
        for (var i = 0; i < cycles; i++)
        {
            map.Set("churned", "r1", new OrFlag());
            map.Remove("churned");
        }

        map.Set("churned", "r1", new OrFlag());
        return map;
    }

    [Test]
    public void DecodeCurrentValue_indexed_key_yields_only_the_live_dot()
    {
        var map = ChurnedMap(cycles: 12);

        var members = Decoder.DecodeCurrentValue(map);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(KeyOf(members[0]), Is.EqualTo("churned"));
            Assert.That(members[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(members[0].Ordinal, Is.EqualTo(13L));
        });
    }

    [Test]
    public void DecodeCurrentValue_indexed_key_fully_removed_yields_nothing()
    {
        var map = ChurnedMap(cycles: 12);
        map.Remove("churned");

        Assert.That(Decoder.DecodeCurrentValue(map), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_keeps_live_dot_whose_counter_collides_across_replicas()
    {
        // r1's dots 1..10 are all tombstoned; r2's dots 1..5 are live and reuse
        // the same counters. A counter-only membership test would wrongly bury
        // them, so the index is licensed only behind a replica-id guard.
        var churned = new OrMap<string, OrFlag>();
        for (var i = 0; i < 10; i++) churned.Set("k", "r1", new OrFlag());
        churned.Remove("k");

        var other = new OrMap<string, OrFlag>();
        for (var i = 0; i < 5; i++) other.Set("k", "r2", new OrFlag());

        var merged = OrMap<string, OrFlag>.Merge(churned, other);

        var members = Decoder.DecodeCurrentValue(merged);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(KeyOf(members[0]), Is.EqualTo("k"));
            Assert.That(members[0].ReplicaId, Is.EqualTo("r2"));
            Assert.That(members[0].Ordinal, Is.EqualTo(5L));
        });
    }

    [Test]
    public void DecodeCurrentValue_multi_replica_tombstones_still_resolve_the_live_dot()
    {
        // Two replicas' tombstones merge into one list, so the shared-replica
        // precondition fails and the linear scan must carry the key.
        var a = new OrMap<string, OrFlag>();
        for (var i = 0; i < 6; i++) a.Set("k", "r1", new OrFlag());
        a.Remove("k");

        var b = new OrMap<string, OrFlag>();
        for (var i = 0; i < 6; i++) b.Set("k", "r2", new OrFlag());
        b.Remove("k");

        var merged = OrMap<string, OrFlag>.Merge(a, b);
        merged.Set("k", "r3", new OrFlag());

        var members = Decoder.DecodeCurrentValue(merged);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].ReplicaId, Is.EqualTo("r3"));
    }

    // ---- delta key surrogate memo ----
    //
    // A dot group for one key encodes that key's surrogate once and shares the
    // array across the group's events. These pin the element bytes for both the
    // grouped shape the memo hits and the interleaved shape it must miss.

    [Test]
    public void DecodeDeltas_grouped_dots_carry_their_own_key_bytes()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                adds: new[] { Add("kA", "r1", 1), Add("kA", "r1", 2), Add("kB", "r1", 1) },
                tombstones: new[] { Tomb("kB", "r1", 1), Tomb("kB", "r2", 1), Tomb("kC", "r1", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(KeyOf), Is.EqualTo(new[] { "kA", "kA", "kB", "kB", "kB", "kC" }));
    }

    [Test]
    public void DecodeDeltas_interleaved_dots_carry_their_own_key_bytes()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                adds: new[] { Add("kA", "r1", 1), Add("kB", "r1", 1), Add("kA", "r1", 2), Add("kB", "r1", 2) },
                tombstones: new[] { Tomb("kC", "r1", 1), Tomb("kD", "r1", 1), Tomb("kC", "r1", 2) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(KeyOf), Is.EqualTo(new[] { "kA", "kB", "kA", "kB", "kC", "kD", "kC" }));
    }

    [Test]
    public void DecodeDeltas_shared_key_bytes_are_never_carried_across_keys()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[]
            {
                Add("short", "r1", 1),
                Add("a-much-longer-key", "r1", 1),
                Add("short", "r1", 2),
            })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.Multiple(() =>
        {
            Assert.That(events.Select(KeyOf), Is.EqualTo(new[] { "short", "a-much-longer-key", "short" }));
            Assert.That(events[0].Element, Is.Not.SameAs(events[1].Element));
        });
    }
}
