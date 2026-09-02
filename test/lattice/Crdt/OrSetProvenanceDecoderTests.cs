namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class OrSetProvenanceDecoderTests
{
    private static readonly byte[] Apple = "apple"u8.ToArray();
    private static readonly byte[] Banana = "banana"u8.ToArray();

    private static OrSetProvenanceDecoder Decoder => OrSetProvenanceDecoder.Instance;

    private static OrSetDelta Delta(OrSetDeltaDot[]? adds = null, OrSetDeltaDot[]? removes = null) => new()
    {
        Adds = adds ?? Array.Empty<OrSetDeltaDot>(),
        Removes = removes ?? Array.Empty<OrSetDeltaDot>(),
    };

    private static OrSetDeltaDot Dot(byte[] element, string replica, long counter) =>
        new() { Element = element, ReplicaId = replica, Counter = counter };

    // ---- shape / guards ----

    [Test]
    public void Mode_is_orset()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
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
        var events = Decoder.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>());
        Assert.That(events, Is.Empty);
    }

    [Test]
    public void DecodeState_empty_set_yields_no_events()
    {
        var events = Decoder.DecodeState(new OrSet());
        Assert.That(events, Is.Empty);
    }

    // ---- delta-sequence path ----

    [Test]
    public void DecodeDeltas_single_add_yields_one_added_event()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(adds: new[] { Dot(Apple, "r1", 7) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        var e = events[0];
        Assert.Multiple(() =>
        {
            Assert.That(e.Element, Is.EqualTo(Apple));
            Assert.That(e.Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(e.ReplicaId, Is.EqualTo("r1"));
            Assert.That(e.Ordinal, Is.EqualTo(7L));
        });
    }

    [Test]
    public void DecodeDeltas_single_remove_yields_one_removed_event()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(removes: new[] { Dot(Apple, "r1", 3) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
        Assert.That(events[0].Ordinal, Is.EqualTo(3L));
    }

    [Test]
    public void DecodeDeltas_preserves_operation_order_across_deltas()
    {
        // add -> remove -> re-add, each as its own delta in causal order.
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Dot(Apple, "r1", 1) })),
            new CrdtProvenanceDelta(Delta(removes: new[] { Dot(Apple, "r1", 1) })),
            new CrdtProvenanceDelta(Delta(adds: new[] { Dot(Apple, "r1", 2) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => (e.Kind, e.Ordinal)), Is.EqualTo(new[]
        {
            (CrdtMemberChangeKind.Added, 1L),
            (CrdtMemberChangeKind.Removed, 1L),
            (CrdtMemberChangeKind.Added, 2L),
        }));
    }

    [Test]
    public void DecodeDeltas_concurrent_adds_from_two_replicas_both_represented()
    {
        // Both adds in one delta but authored by different replicas: neither
        // is dropped (no last-writer-wins collapse).
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Dot(Apple, "r1", 1), Dot(Apple, "r2", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
        Assert.That(events.Select(e => e.ReplicaId), Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeDeltas_adds_precede_removes_within_a_single_delta()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                adds: new[] { Dot(Apple, "r1", 2) },
                removes: new[] { Dot(Apple, "r1", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            CrdtMemberChangeKind.Added,
            CrdtMemberChangeKind.Removed,
        }));
    }

    [Test]
    public void DecodeDeltas_associates_wall_clock_when_supplied()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 12345, Counter = 2 };
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Dot(Apple, "r1", 1) }), hlc),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events[0].WallClock, Is.EqualTo(hlc));
    }

    [Test]
    public void DecodeDeltas_exposes_causal_order_only_when_no_wall_clock()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Dot(Apple, "r1", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events[0].WallClock, Is.Null);
    }

    [Test]
    public void DecodeDeltas_skips_dots_with_null_element()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[]
            {
                new OrSetDeltaDot { Element = null!, ReplicaId = "r1", Counter = 1 },
                Dot(Apple, "r1", 2),
            })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Ordinal, Is.EqualTo(2L));
    }

    // ---- folded-state fallback path ----

    [Test]
    public void DecodeState_single_add_yields_one_added_event()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 5);

        var events = Decoder.DecodeState(set);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Element, Is.EqualTo(Apple));
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Ordinal, Is.EqualTo(5L));
        });
    }

    [Test]
    public void DecodeState_concurrent_adds_both_represented()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Add(Apple, "r2", 1);

        var events = Decoder.DecodeState(set);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
        Assert.That(events.Select(e => e.ReplicaId), Is.EqualTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeState_removed_then_readded_shows_both_events_in_causal_order()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Remove(Apple);          // tombstones dot (r1, 1)
        set.Add(Apple, "r1", 2);    // re-add with a fresh dot

        var events = Decoder.DecodeState(set);

        Assert.That(events.Select(e => (e.Kind, e.Ordinal)), Is.EqualTo(new[]
        {
            (CrdtMemberChangeKind.Added, 1L),
            (CrdtMemberChangeKind.Removed, 1L),
            (CrdtMemberChangeKind.Added, 2L),
        }));
    }

    [Test]
    public void DecodeState_orders_within_element_by_causal_ordinal()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 3);
        set.Add(Apple, "r1", 1);
        set.Add(Apple, "r1", 2);

        var events = Decoder.DecodeState(set);

        // The old unbounded representation emitted all three hand-authored
        // same-replica adds. Compaction retains the newest dot, matching the
        // accessor contract that counters only move forward.
        Assert.That(events.Select(e => e.Ordinal), Is.EqualTo(new[] { 3L }));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Remove(Apple);

        var events = Decoder.DecodeState(set);

        Assert.That(events.All(e => e.WallClock is null), Is.True);
    }

    [Test]
    public void DecodeState_cross_element_order_is_deterministic()
    {
        var set = new OrSet();
        set.Add(Banana, "r1", 1);
        set.Add(Apple, "r1", 1);

        var events = Decoder.DecodeState(set);

        // Elements are ordered by the ordinal sort of their internal (base64)
        // keys, which is stable across replicas.
        var first = Convert.ToBase64String(events[0].Element);
        var second = Convert.ToBase64String(events[1].Element);
        Assert.That(string.CompareOrdinal(first, second), Is.LessThan(0));
    }

    [Test]
    public void DecodeState_pure_remove_element_yields_removed_event()
    {
        // An element present only in tombstones (its adds tombstoned away) is
        // still surfaced from the folded state.
        var set = new OrSet();
        set.Tombstones["YQ=="] = new List<OrSetDot> { new() { ReplicaId = "r1", Counter = 1 } };

        var events = Decoder.DecodeState(set);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
    }

    // ---- current-value (live members only) path ----

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_empty_set_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new OrSet()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_single_add_yields_one_live_member()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 5);

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(members[0].Element, Is.EqualTo(Apple));
            Assert.That(members[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(members[0].Ordinal, Is.EqualTo(5L));
        });
    }

    [Test]
    public void DecodeCurrentValue_fully_removed_element_is_excluded()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Add(Banana, "r1", 2);
        set.Remove(Apple); // tombstones every add dot for Apple

        var members = Decoder.DecodeCurrentValue(set);

        // Only the surviving element remains; the fully-removed one is absent
        // even though its add dot still lingers under a tombstone.
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Element, Is.EqualTo(Banana));
    }

    [Test]
    public void DecodeCurrentValue_removed_then_readded_is_live()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Remove(Apple);       // tombstones dot (r1, 1)
        set.Add(Apple, "r1", 2); // fresh live dot

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Element, Is.EqualTo(Apple));
        Assert.That(members[0].Ordinal, Is.EqualTo(2L),
            "the representative dot is the surviving (highest-ordinal) add");
    }

    [Test]
    public void DecodeCurrentValue_picks_highest_surviving_dot_as_representative()
    {
        var set = new OrSet();
        set.Add(Apple, "r1", 1);
        set.Add(Apple, "r1", 3);
        set.Add(Apple, "r2", 2);

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Ordinal, Is.EqualTo(3L));
        Assert.That(members[0].ReplicaId, Is.EqualTo("r1"));
    }
}
