using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the generalised <see cref="CrdtShapeRegistry"/> and
/// per-mode <see cref="CrdtShape"/> factory helpers.
/// </summary>
[TestFixture]
public class CrdtShapeRegistryTests
{
    [Test]
    public void Constructor_prepopulates_closed_shape_modes()
    {
        var r = new CrdtShapeRegistry();
        Assert.That(r.TryGet("any", LatticeMergeMode.OrSet), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.PnCounter), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.VersionVector), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.MvRegister), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.Sequence), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.OrFlag), Is.Not.Null);
    }

    [Test]
    public void TryGet_returns_null_for_unregistered_ormap_tree()
    {
        var r = new CrdtShapeRegistry();
        Assert.That(r.TryGet("missing", LatticeMergeMode.OrMap), Is.Null);
    }

    [Test]
    public void Register_throws_on_null_or_empty_tree()
    {
        var r = new CrdtShapeRegistry();
        var s = CrdtShape.ForOrMap<string, PnCounter>();
        Assert.That(() => r.Register(null!, s), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => r.Register("", s), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Register_throws_on_null_shape()
    {
        var r = new CrdtShapeRegistry();
        Assert.That(() => r.Register("tree", null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Register_resolves_per_tree_ormap_shape()
    {
        var r = new CrdtShapeRegistry();
        var shape = CrdtShape.ForOrMap<string, PnCounter>();
        r.Register("orders", shape);
        Assert.That(r.TryGet("orders", LatticeMergeMode.OrMap), Is.SameAs(shape));
    }

    [Test]
    public void Register_rejects_conflicting_pair_for_same_tree_and_mode()
    {
        var r = new CrdtShapeRegistry();
        r.Register("orders", CrdtShape.ForOrMap<string, PnCounter>());
        Assert.That(
            () => r.Register("orders", CrdtShape.ForOrMap<string, OrSet>()),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Register_is_idempotent_for_same_descriptor_instance()
    {
        var r = new CrdtShapeRegistry();
        var s = CrdtShape.ForOrMap<string, PnCounter>();
        r.Register("orders", s);
        Assert.That(() => r.Register("orders", s), Throws.Nothing);
    }

    [Test]
    public void Per_tree_ormap_does_not_shadow_global_closed_shapes()
    {
        var r = new CrdtShapeRegistry();
        r.Register("orders", CrdtShape.ForOrMap<string, PnCounter>());
        Assert.That(r.TryGet("orders", LatticeMergeMode.OrSet), Is.Not.Null);
        Assert.That(r.TryGet("orders", LatticeMergeMode.OrSet)!.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void ForOrSet_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForOrSet();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        var state = (OrSet)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.Contains(new byte[] { 1 }), Is.True);

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (OrSet)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.Contains(new byte[] { 1 }), Is.True);
    }

    [Test]
    public void ForPnCounter_descriptor_merges_state()
    {
        var shape = CrdtShape.ForPnCounter();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
        var a = (PnCounter)shape.CreateEmpty();
        a.Increment("A", 3);
        var b = (PnCounter)shape.CreateEmpty();
        b.Increment("B", 5);
        shape.MergeStates(a, b);
        Assert.That(a.Value, Is.EqualTo(8));
    }

    [Test]
    public void ForOrMap_descriptor_carries_mode_and_creates_empty()
    {
        var shape = CrdtShape.ForOrMap<string, PnCounter>();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.OrMap));
        var state = (OrMap<string, PnCounter>)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);
    }

    [Test]
    public void ForVersionVector_descriptor_roundtrips_state_and_merges_delta()
    {
        var shape = CrdtShape.ForVersionVector();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.VersionVector));

        var state = (VersionVector)shape.CreateEmpty();
        state.Tick("A");
        var delta = new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                ["B"] = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
            },
        };
        var deltaBytes = JsonLatticeSerializer<VersionVectorDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));

        // Re-serialise / re-hydrate the post-merge state and confirm both
        // entries survived the round trip.
        var stateBytes = shape.SerializeState(state);
        var roundtripped = (VersionVector)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.Entries.ContainsKey("A"), Is.True);
        Assert.That(roundtripped.Entries.ContainsKey("B"), Is.True);
    }

    [Test]
    public void ForMvRegister_descriptor_roundtrips_state_and_merges_other_state()
    {
        var shape = CrdtShape.ForMvRegister();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.MvRegister));

        var a = (MvRegister)shape.CreateEmpty();
        a.Set("A", new byte[] { 1 });
        var b = (MvRegister)shape.CreateEmpty();
        b.Set("B", new byte[] { 2 });
        shape.MergeStates(a, b);

        // After merging two concurrent writes from distinct replicas the
        // MV register must hold both values.
        Assert.That(a.Values().Count, Is.EqualTo(2));
    }

    [Test]
    public void ForRga_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForRga();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.Sequence));

        var state = (Rga)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var head = new OrSetDot { ReplicaId = "A", Counter = 1 };
        var delta = new RgaDelta
        {
            Inserts = new[]
            {
                new RgaDeltaNode { ReplicaId = "A", Counter = 1, ParentDot = Rga.Root, Value = new byte[] { 1 } },
                new RgaDeltaNode { ReplicaId = "A", Counter = 2, ParentDot = head, Value = new byte[] { 2 } },
            },
            Tombstones = Array.Empty<OrSetDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<RgaDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));

        var rendered = state.ToList().Select(t => t.Value[0]).ToArray();
        Assert.That(rendered, Is.EqualTo(new byte[] { 1, 2 }));

        // Re-serialise / re-hydrate the post-merge state and confirm the
        // ordered traversal survives the round trip.
        var stateBytes = shape.SerializeState(state);
        var roundtripped = (Rga)shape.DeserializeState(stateBytes);
        var rendered2 = roundtripped.ToList().Select(t => t.Value[0]).ToArray();
        Assert.That(rendered2, Is.EqualTo(new byte[] { 1, 2 }));
    }

    [Test]
    public void ForRga_descriptor_merges_other_state()
    {
        var shape = CrdtShape.ForRga();
        var a = (Rga)shape.CreateEmpty();
        a.InsertAfter(Rga.Root, "A", new byte[] { 1 });
        var b = (Rga)shape.CreateEmpty();
        b.InsertAfter(Rga.Root, "B", new byte[] { 2 });
        shape.MergeStates(a, b);
        Assert.That(a.Count, Is.EqualTo(2));
    }

    [Test]
    public void ForOrFlag_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForOrFlag();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.OrFlag));

        var state = (OrFlag)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var delta = new OrFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
            Disables = Array.Empty<OrSetDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<OrFlagDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.IsEnabled, Is.True);

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (OrFlag)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.IsEnabled, Is.True);
    }

    [Test]
    public void ForOrFlag_descriptor_merges_other_state_enable_wins()
    {
        var shape = CrdtShape.ForOrFlag();

        // a enables then disables (observing only its own dot); b enables
        // concurrently with a dot a never saw. The state merge must keep
        // the flag enabled (enable-wins).
        var a = (OrFlag)shape.CreateEmpty();
        a.Enable("A", 1);
        a.Disable();
        var b = (OrFlag)shape.CreateEmpty();
        b.Enable("B", 1);

        shape.MergeStates(a, b);
        Assert.That(a.IsEnabled, Is.True);
    }

    // --- OR-Map pre-ship delta coalescing -----------------------------------

    private static OrMapDeltaEntry<string, PnCounter> Add(string key, string replicaId, long counter, string incReplica, long amount)
    {
        var counterValue = new PnCounter();
        counterValue.Increment(incReplica, amount);
        return new OrMapDeltaEntry<string, PnCounter>
        {
            Key = key,
            ReplicaId = replicaId,
            Counter = counter,
            Value = counterValue,
        };
    }

    private static OrMapDeltaTombstone<string> Tombstone(string key, string replicaId, long counter) => new()
    {
        Key = key,
        ReplicaId = replicaId,
        Counter = counter,
    };

    private static OrMapDelta<string, PnCounter> MapDelta(
        OrMapDeltaEntry<string, PnCounter>[]? adds = null,
        OrMapDeltaTombstone<string>[]? tombstones = null) => new()
    {
        Adds = adds ?? Array.Empty<OrMapDeltaEntry<string, PnCounter>>(),
        Tombstones = tombstones ?? Array.Empty<OrMapDeltaTombstone<string>>(),
    };

    private static OrMapDelta<string, PnCounter> Combine(
        OrMapDelta<string, PnCounter> a,
        OrMapDelta<string, PnCounter> b)
    {
        var shape = CrdtShape.ForOrMap<string, PnCounter>();
        return (OrMapDelta<string, PnCounter>)shape.CombineDeltas!(a, b);
    }

    private static OrMap<string, PnCounter> ApplySequential(params OrMapDelta<string, PnCounter>[] deltas)
    {
        var map = new OrMap<string, PnCounter>();
        foreach (var d in deltas)
        {
            map.MergeDelta(d);
        }
        return map;
    }

    private static void AssertLiveEquivalent(OrMap<string, PnCounter> left, OrMap<string, PnCounter> right)
    {
        var leftKeys = left.Keys().OrderBy(k => k, StringComparer.Ordinal).ToArray();
        var rightKeys = right.Keys().OrderBy(k => k, StringComparer.Ordinal).ToArray();
        Assert.That(leftKeys, Is.EqualTo(rightKeys));
        foreach (var key in leftKeys)
        {
            Assert.That(left.Get(key)!.Value, Is.EqualTo(right.Get(key)!.Value), $"value mismatch at key '{key}'");
        }
    }

    [Test]
    public void ForOrMap_descriptor_exposes_non_null_combine_deltas()
    {
        var shape = CrdtShape.ForOrMap<string, PnCounter>();
        Assert.That(shape.CombineDeltas, Is.Not.Null);
        Assert.That(shape.SerializeDelta, Is.Not.Null);
    }

    [Test]
    public void CombineDeltas_distinct_dots_for_same_key_preserves_multi_value()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var b = MapDelta(adds: new[] { Add("k", "A", 2, "Y", 3) });

        var combined = Combine(a, b);

        Assert.That(combined.Adds, Has.Count.EqualTo(2));
        // Both distinct-dot snapshots survive; the live merged value folds both.
        var map = new OrMap<string, PnCounter>();
        map.MergeDelta(combined);
        Assert.That(map.Get("k")!.Value, Is.EqualTo(5));
    }

    [Test]
    public void CombineDeltas_unions_tombstones_deduped()
    {
        var a = MapDelta(tombstones: new[] { Tombstone("k", "A", 1) });
        var b = MapDelta(tombstones: new[] { Tombstone("k", "A", 1), Tombstone("k", "B", 2) });

        var combined = Combine(a, b);

        Assert.That(combined.Tombstones, Has.Count.EqualTo(2));
    }

    [Test]
    public void CombineDeltas_same_dot_value_collision_yields_value_crdt_join()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var b = MapDelta(adds: new[] { Add("k", "A", 1, "Y", 3) });

        var combined = Combine(a, b);

        Assert.That(combined.Adds, Has.Count.EqualTo(1));
        Assert.That(combined.Adds[0].Value.Value, Is.EqualTo(5));
    }

    [Test]
    public void CombineDeltas_same_dot_value_collision_does_not_mutate_source_deltas()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var b = MapDelta(adds: new[] { Add("k", "A", 1, "Y", 3) });

        _ = Combine(a, b);

        // Clone-on-insert means the source snapshots keep their original value.
        Assert.That(a.Adds[0].Value.Value, Is.EqualTo(2));
        Assert.That(b.Adds[0].Value.Value, Is.EqualTo(3));
    }

    [Test]
    public void CombineDeltas_apply_equivalent_for_distinct_dot_adds()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var b = MapDelta(adds: new[] { Add("k", "B", 1, "Y", 3) });

        var combinedMap = ApplySequential(Combine(a, b));
        var sequentialMap = ApplySequential(a, b);

        AssertLiveEquivalent(combinedMap, sequentialMap);
        Assert.That(combinedMap.Get("k")!.Value, Is.EqualTo(5));
    }

    [Test]
    public void CombineDeltas_apply_equivalent_for_add_then_tombstone()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var b = MapDelta(tombstones: new[] { Tombstone("k", "A", 1) });

        var combinedMap = ApplySequential(Combine(a, b));
        var sequentialMap = ApplySequential(a, b);

        AssertLiveEquivalent(combinedMap, sequentialMap);
        Assert.That(combinedMap.ContainsKey("k"), Is.False);
    }

    [Test]
    public void CombineDeltas_apply_equivalent_for_same_dot_value_merge()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var b = MapDelta(adds: new[] { Add("k", "A", 1, "Y", 3) });

        var combinedMap = ApplySequential(Combine(a, b));
        var sequentialMap = ApplySequential(a, b);

        AssertLiveEquivalent(combinedMap, sequentialMap);
        Assert.That(combinedMap.Get("k")!.Value, Is.EqualTo(5));
    }

    [Test]
    public void CombineDeltas_is_commutative_under_apply()
    {
        var a = MapDelta(
            adds: new[] { Add("k", "A", 1, "X", 2) },
            tombstones: new[] { Tombstone("m", "C", 9) });
        var b = MapDelta(adds: new[] { Add("k", "A", 1, "Y", 3), Add("k", "B", 1, "Z", 4) });

        var ab = ApplySequential(Combine(a, b));
        var ba = ApplySequential(Combine(b, a));

        AssertLiveEquivalent(ab, ba);
    }

    [Test]
    public void CombineDeltas_is_idempotent_under_apply()
    {
        var a = MapDelta(
            adds: new[] { Add("k", "A", 1, "X", 2), Add("k", "B", 1, "Y", 3) },
            tombstones: new[] { Tombstone("k", "C", 5) });

        var combinedMap = ApplySequential(Combine(a, a));
        var singleMap = ApplySequential(a);

        AssertLiveEquivalent(combinedMap, singleMap);
    }

    [Test]
    public void CombineDeltas_treats_default_delta_as_empty()
    {
        var a = MapDelta(adds: new[] { Add("k", "A", 1, "X", 2) });
        var combined = Combine(a, default);

        Assert.That(combined.Adds, Is.Not.Null);
        Assert.That(combined.Tombstones, Is.Not.Null);
        Assert.That(combined.Adds, Has.Count.EqualTo(1));
        Assert.That(combined.Tombstones, Is.Empty);
    }
}
