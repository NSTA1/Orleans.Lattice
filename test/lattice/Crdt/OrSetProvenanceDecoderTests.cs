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

        Assert.That(events, Is.Not.Empty, "the 'always null' claim is only meaningful over a non-empty decode");
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
    // ---- dot-index path (above the threshold that replaces the linear scan) ----
    //
    // Above the threshold the membership tests take a replica-plus-counter
    // index instead of scanning the whole dot list. These pin the two
    // preconditions the index rests on: that it is only taken when the list
    // carries a single replica, and that a counter match on a different replica
    // is never mistaken for a hit.

    private const int AboveIndexThreshold = 12;

    private static string Key(byte[] element) => Convert.ToBase64String(element);

    [Test]
    public void DecodeCurrentValue_indexed_element_excludes_covered_dots_and_keeps_the_survivor()
    {
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= AboveIndexThreshold; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
            tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        adds.Add(new OrSetDot { ReplicaId = "r1", Counter = AboveIndexThreshold + 1 });
        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Ordinal, Is.EqualTo((long)AboveIndexThreshold + 1));
    }

    [Test]
    public void DecodeCurrentValue_indexed_element_keeps_a_live_dot_whose_counter_collides_across_replicas()
    {
        // Every tombstone is on r1, so the index IS taken. The r2 dot shares a
        // tombstoned counter, and a counter-only test without the replica guard
        // would wrongly cancel it.
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= AboveIndexThreshold; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
            tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        adds.Add(new OrSetDot { ReplicaId = "r2", Counter = 1 });
        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].ReplicaId, Is.EqualTo("r2"));
        Assert.That(members[0].Ordinal, Is.EqualTo(1L));
    }

    [Test]
    public void DecodeCurrentValue_multi_replica_tombstones_keep_the_scan_semantics()
    {
        // The precondition fails, so the coverage scan is kept. Every add is
        // covered by a tombstone on its own replica, so nothing survives.
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= AboveIndexThreshold; i++)
        {
            var replica = (i % 2) == 0 ? "r1" : "r2";
            adds.Add(new OrSetDot { ReplicaId = replica, Counter = i });
            tombs.Add(new OrSetDot { ReplicaId = replica, Counter = i });
        }

        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        Assert.That(Decoder.DecodeCurrentValue(set), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_indexed_element_cancels_a_dot_below_a_higher_tombstone()
    {
        // Cancellation is coverage-based: a tombstone at counter N cancels every
        // dot from the same replica at or below N, not only its exact equal.
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= AboveIndexThreshold; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        for (var i = 1; i < AboveIndexThreshold; i++)
        {
            tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = AboveIndexThreshold + 5 });
        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        Assert.That(Decoder.DecodeCurrentValue(set), Is.Empty);
    }

    [Test]
    public void DecodeState_churned_element_synthesizes_only_the_compacted_away_adds()
    {
        // The add list is above the threshold and single-replica, so the exact
        // containment test takes the sorted counter index. A tombstone whose dot
        // is still in the add list must not be synthesized; one whose dot was
        // compacted away must be.
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= AboveIndexThreshold; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
            tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        // Compacted away from the add list, so its Added half must be synthesized.
        tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = 500 });
        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        var events = Decoder.DecodeState(set);

        Assert.That(
            events.Count(e => e.Kind == CrdtMemberChangeKind.Added),
            Is.EqualTo(AboveIndexThreshold + 1),
            "the compacted-away tombstone contributes one synthesized Added event");
        Assert.That(
            events.Count(e => e.Kind == CrdtMemberChangeKind.Removed),
            Is.EqualTo(AboveIndexThreshold + 1));
    }

    [Test]
    public void DecodeState_churned_element_does_not_match_a_tombstone_across_replicas()
    {
        // The add list is single-replica (r1), so the index is taken. The r2
        // tombstone shares a counter with an r1 add; a counter-only test would
        // call it present and skip the synthesized Added event it is owed.
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        for (var i = 1; i <= AboveIndexThreshold; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = new List<OrSetDot>
        {
            new() { ReplicaId = "r2", Counter = 1 },
            new() { ReplicaId = "r2", Counter = 2 },
        };

        var events = Decoder.DecodeState(set);

        Assert.That(
            events.Count(e => e.Kind == CrdtMemberChangeKind.Added),
            Is.EqualTo(AboveIndexThreshold + 2),
            "neither r2 tombstone is present in the r1 add list, so both synthesize an Added event");
    }

    // ---- result capacity ----
    //
    // These two lanes pin the allocation shape of the decode buffer. They read
    // Capacity rather than Count deliberately: the event CONTENT is already
    // pinned by the synthesis tests above, and a capacity regression is exactly
    // the kind of change those tests cannot see.

    [Test]
    public void DecodeState_presizes_an_uncompacted_set_exactly_and_never_widens_it()
    {
        // The common path. Every dot yields exactly one event, so the initial
        // presize is already exact and the lazy widening must not fire - a
        // change that reserved the worst case up front would show here as a
        // capacity above the event count.
        var set = new OrSet();
        var adds = new List<OrSetDot>();
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= 64; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
            tombs.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        var events = Decoder.DecodeState(set);

        Assert.That(events, Is.InstanceOf<List<CrdtMemberChange>>());
        var list = (List<CrdtMemberChange>)events;
        Assert.That(list.Capacity, Is.EqualTo(list.Count),
            "an uncompacted decode must keep its exact presize - no widening, no slack");
    }

    [Test]
    public void DecodeState_widens_a_compacted_set_to_the_exact_ceiling_not_a_doubling()
    {
        // The compacted path. Synthesis overflows the presize, so the buffer
        // must grow - but to total + tombstoneDots, the provable ceiling, and
        // not to List<T>'s default 2 * total. With a mostly-add set the two
        // differ by nearly the whole add-dot count, which is the saving.
        const int addCount = 256;
        const int tombCount = 8;

        var set = new OrSet();
        var adds = new List<OrSetDot>();
        for (var i = 1; i <= addCount; i++)
        {
            adds.Add(new OrSetDot { ReplicaId = "r1", Counter = i });
        }

        // Tombstones from a different replica, so none of them is present in the
        // r1 add list and every one synthesizes its Added half.
        var tombs = new List<OrSetDot>();
        for (var i = 1; i <= tombCount; i++)
        {
            tombs.Add(new OrSetDot { ReplicaId = "r2", Counter = i });
        }

        set.Adds[Key(Apple)] = adds;
        set.Tombstones[Key(Apple)] = tombs;

        var events = Decoder.DecodeState(set);

        const int total = addCount + tombCount;
        const int ceiling = total + tombCount;

        Assert.That(events, Is.InstanceOf<List<CrdtMemberChange>>());
        var list = (List<CrdtMemberChange>)events;

        Assert.Multiple(() =>
        {
            Assert.That(list.Count, Is.EqualTo(total + tombCount),
                "every r2 tombstone contributes a synthesized Added plus its Removed");
            Assert.That(list.Capacity, Is.EqualTo(ceiling),
                "the widening must be an exact-size reallocation, not a doubling to 2 * total");
            Assert.That(list.Capacity, Is.LessThan(2 * total),
                "a doubling would reserve this much - that is the allocation being removed");
        });
    }
}
