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
        Assert.That(r.TryGet("any", LatticeMergeMode.RwFlag), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.GCounter), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.GSet), Is.Not.Null);
        Assert.That(r.TryGet("any", LatticeMergeMode.RwSet), Is.Not.Null);
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

    [Test]
    public void ForRwFlag_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForRwFlag();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));

        var state = (RwFlag)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var delta = new RwFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
            Disables = Array.Empty<OrSetDot>(),
            Tombstones = Array.Empty<OrSetDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<RwFlagDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.IsEnabled, Is.True);

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (RwFlag)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.IsEnabled, Is.True);
    }

    [Test]
    public void ForRwFlag_descriptor_merges_other_state_remove_wins()
    {
        var shape = CrdtShape.ForRwFlag();

        // a enables then disables (observing its own enable dot); b enables
        // concurrently with a dot that observes neither a's disable. The
        // state merge must keep the flag disabled (remove-wins): a's disable
        // dot is not tombstoned by b's enable, so it survives.
        var a = (RwFlag)shape.CreateEmpty();
        a.Enable("A", 1);
        a.Disable("A", 2);
        var b = (RwFlag)shape.CreateEmpty();
        b.Enable("B", 1);

        shape.MergeStates(a, b);
        Assert.That(a.IsEnabled, Is.False);
    }

    [Test]
    public void ForGCounter_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForGCounter();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.GCounter));

        var state = (GCounter)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var delta = new GCounterDelta
        {
            Increments = new Dictionary<string, long> { ["A"] = 5 },
        };
        var deltaBytes = JsonLatticeSerializer<GCounterDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.Value, Is.EqualTo(5));

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (GCounter)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.Value, Is.EqualTo(5));
    }

    [Test]
    public void ForGCounter_descriptor_merges_state_pointwise_max()
    {
        var shape = CrdtShape.ForGCounter();
        var a = (GCounter)shape.CreateEmpty();
        a.Increment("A", 3);
        var b = (GCounter)shape.CreateEmpty();
        b.Increment("B", 5);
        shape.MergeStates(a, b);
        Assert.That(a.Value, Is.EqualTo(8));
    }

    [Test]
    public void ForGCounter_CombineDeltas_coalesces_by_pointwise_max()
    {
        var shape = CrdtShape.ForGCounter();
        Assert.That(shape.CombineDeltas, Is.Not.Null);

        var a = new GCounterDelta { Increments = new Dictionary<string, long> { ["A"] = 5, ["B"] = 2 } };
        var b = new GCounterDelta { Increments = new Dictionary<string, long> { ["A"] = 3, ["B"] = 9 } };

        var combined = (GCounterDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Increments["A"], Is.EqualTo(5));
        Assert.That(combined.Increments["B"], Is.EqualTo(9));
    }

    [Test]
    public void ForGCounter_CombineDeltas_apply_equivalent_to_sequential_apply()
    {
        var shape = CrdtShape.ForGCounter();
        var a = new GCounterDelta { Increments = new Dictionary<string, long> { ["A"] = 5, ["B"] = 2 } };
        var b = new GCounterDelta { Increments = new Dictionary<string, long> { ["A"] = 3, ["B"] = 9 } };

        var sequential = (GCounter)shape.CreateEmpty();
        shape.MergeDelta(sequential, a);
        shape.MergeDelta(sequential, b);

        var coalesced = (GCounter)shape.CreateEmpty();
        shape.MergeDelta(coalesced, shape.CombineDeltas!(a, b));

        Assert.That(coalesced.Value, Is.EqualTo(sequential.Value));
        Assert.That(coalesced.Increments, Is.EquivalentTo(sequential.Increments));
    }

    [Test]
    public void ForGSet_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForGSet();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.GSet));

        var state = (GSet)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var delta = new GSetDelta { Adds = new[] { new byte[] { 1 }, new byte[] { 2 } } };
        var deltaBytes = JsonLatticeSerializer<GSetDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.Count, Is.EqualTo(2));

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (GSet)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.Count, Is.EqualTo(2));
    }

    [Test]
    public void ForGSet_descriptor_merges_other_state_by_union()
    {
        var shape = CrdtShape.ForGSet();
        var a = (GSet)shape.CreateEmpty();
        a.Add(new byte[] { 1 });
        var b = (GSet)shape.CreateEmpty();
        b.Add(new byte[] { 2 });

        shape.MergeStates(a, b);
        Assert.That(a.Count, Is.EqualTo(2));
    }

    [Test]
    public void ForGSet_CombineDeltas_unions_adds_deduped()
    {
        var shape = CrdtShape.ForGSet();
        var a = new GSetDelta { Adds = new[] { new byte[] { 1 }, new byte[] { 2 } } };
        var b = new GSetDelta { Adds = new[] { new byte[] { 2 }, new byte[] { 3 } } };

        Assert.That(shape.CombineDeltas, Is.Not.Null);
        var combined = (GSetDelta)shape.CombineDeltas!(a, b);
        Assert.That(combined.Adds, Has.Count.EqualTo(3));

        // Applying the combined delta is equivalent to applying both in sequence.
        var viaCombined = (GSet)shape.CreateEmpty();
        viaCombined.MergeDelta(combined);
        var viaSequence = (GSet)shape.CreateEmpty();
        viaSequence.MergeDelta(a);
        viaSequence.MergeDelta(b);
        Assert.That(viaCombined.Elements, Is.EquivalentTo(viaSequence.Elements));
    }

    [Test]
    public void ForRwSet_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForRwSet();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.RwSet));

        var state = (RwSet)shape.CreateEmpty();
        Assert.That(state.IsBottom, Is.True);

        var delta = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = new byte[] { 9 }, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<RwSetDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.Contains(new byte[] { 9 }), Is.True);

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (RwSet)shape.DeserializeState(stateBytes);
        Assert.That(roundtripped.Contains(new byte[] { 9 }), Is.True);
    }

    [Test]
    public void ForRwSet_descriptor_merges_other_state_remove_wins()
    {
        var shape = CrdtShape.ForRwSet();

        // a adds then removes x (observing its own add dot); b adds x
        // concurrently with a dot that observes neither a's remove. The state
        // merge must keep x absent (remove-wins): a's remove dot is not
        // tombstoned by b's add, so it survives.
        var a = (RwSet)shape.CreateEmpty();
        a.Add(new byte[] { 1 }, "A", 1);
        a.Remove(new byte[] { 1 }, "A", 2);
        var b = (RwSet)shape.CreateEmpty();
        b.Add(new byte[] { 1 }, "B", 1);

        shape.MergeStates(a, b);
        Assert.That(a.Contains(new byte[] { 1 }), Is.False);
    }

    [Test]
    public void ForRwSet_CombineDeltas_unions_the_three_dot_lists()
    {
        var shape = CrdtShape.ForRwSet();
        var a = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };
        var b = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = new byte[] { 2 }, ReplicaId = "B", Counter = 1 } },
            Removes = new[] { new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "C", Counter = 3 } },
            Tombstones = new[] { new OrSetDeltaDot { Element = new byte[] { 2 }, ReplicaId = "D", Counter = 5 } },
        };

        Assert.That(shape.CombineDeltas, Is.Not.Null);
        var combined = (RwSetDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Adds, Has.Count.EqualTo(2));
            Assert.That(combined.Removes, Has.Count.EqualTo(1));
            Assert.That(combined.Tombstones, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void ForMaxRegister_descriptor_roundtrips_delta_through_state()
    {
        var shape = CrdtShape.ForMaxRegister();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.MaxRegister));

        var state = (BoundedRegister)shape.CreateEmpty();
        Assert.Multiple(() =>
        {
            Assert.That(state.IsBottom, Is.True);
            Assert.That(state.IsMin, Is.False);
        });

        var delta = new BoundedRegisterDelta { Value = new byte[] { 0x05 }, OrderKey = new byte[] { 0x05 }, HasValue = true };
        var deltaBytes = JsonLatticeSerializer<BoundedRegisterDelta>.Default.Serialize(delta);
        shape.MergeDelta(state, shape.DeserializeDelta(deltaBytes));
        Assert.That(state.OrderKey, Is.EqualTo(new byte[] { 0x05 }));

        var stateBytes = shape.SerializeState(state);
        var roundtripped = (BoundedRegister)shape.DeserializeState(stateBytes);
        Assert.Multiple(() =>
        {
            Assert.That(roundtripped.OrderKey, Is.EqualTo(new byte[] { 0x05 }));
            Assert.That(roundtripped.IsMin, Is.False);
        });
    }

    [Test]
    public void ForRwSet_CombineDeltas_dedupes_identical_dots()
    {
        var shape = CrdtShape.ForRwSet();
        var dot = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "A", Counter = 1 };
        var a = new RwSetDelta
        {
            Adds = new[] { dot },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };
        var b = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };

        var combined = (RwSetDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Adds, Has.Count.EqualTo(1),
            "identical (element, replica, counter) add dots collapse to one");
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_GSet()
    {
        var shape = CrdtShape.ForGSet();
        var state = (GSet)shape.CreateEmpty();
        state.Add(new byte[] { 9 });
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_RwSet()
    {
        var shape = CrdtShape.ForRwSet();
        var state = (RwSet)shape.CreateEmpty();
        for (var i = 0; i < 32; i++)
        {
            state.Add(System.Text.Encoding.UTF8.GetBytes("e-" + i), "R", i + 1);
        }
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void ForMinRegister_descriptor_creates_min_direction_empty()
    {
        var shape = CrdtShape.ForMinRegister();
        Assert.That(shape.Mode, Is.EqualTo(LatticeMergeMode.MinRegister));

        var state = (BoundedRegister)shape.CreateEmpty();
        Assert.That(state.IsMin, Is.True);
    }

    [Test]
    public void ForMaxRegister_descriptor_merges_other_state_keeps_greatest()
    {
        var shape = CrdtShape.ForMaxRegister();
        var a = (BoundedRegister)shape.CreateEmpty();
        a.Set(new byte[] { 0x02 }, new byte[] { 0x02 });
        var b = (BoundedRegister)shape.CreateEmpty();
        b.Set(new byte[] { 0x08 }, new byte[] { 0x08 });

        shape.MergeStates(a, b);
        Assert.That(a.OrderKey, Is.EqualTo(new byte[] { 0x08 }));
    }

    [Test]
    public void ForMinRegister_descriptor_merges_other_state_keeps_least()
    {
        var shape = CrdtShape.ForMinRegister();
        var a = (BoundedRegister)shape.CreateEmpty();
        a.Set(new byte[] { 0x08 }, new byte[] { 0x08 });
        var b = (BoundedRegister)shape.CreateEmpty();
        b.Set(new byte[] { 0x02 }, new byte[] { 0x02 });

        shape.MergeStates(a, b);
        Assert.That(a.OrderKey, Is.EqualTo(new byte[] { 0x02 }));
    }

    [Test]
    public void ForMaxRegister_combine_deltas_keeps_greatest_candidate()
    {
        var shape = CrdtShape.ForMaxRegister();
        var lo = new BoundedRegisterDelta { Value = new byte[] { 0x02 }, OrderKey = new byte[] { 0x02 }, HasValue = true };
        var hi = new BoundedRegisterDelta { Value = new byte[] { 0x08 }, OrderKey = new byte[] { 0x08 }, HasValue = true };

        var combined = (BoundedRegisterDelta)shape.CombineDeltas!(lo, hi);
        var combinedSwapped = (BoundedRegisterDelta)shape.CombineDeltas!(hi, lo);
        Assert.Multiple(() =>
        {
            Assert.That(combined.OrderKey, Is.EqualTo(new byte[] { 0x08 }));
            Assert.That(combinedSwapped.OrderKey, Is.EqualTo(new byte[] { 0x08 }));
        });
    }

    [Test]
    public void ForMinRegister_combine_deltas_keeps_least_candidate()
    {
        var shape = CrdtShape.ForMinRegister();
        var lo = new BoundedRegisterDelta { Value = new byte[] { 0x02 }, OrderKey = new byte[] { 0x02 }, HasValue = true };
        var hi = new BoundedRegisterDelta { Value = new byte[] { 0x08 }, OrderKey = new byte[] { 0x08 }, HasValue = true };

        var combined = (BoundedRegisterDelta)shape.CombineDeltas!(lo, hi);
        Assert.That(combined.OrderKey, Is.EqualTo(new byte[] { 0x02 }));
    }

    [Test]
    public void Constructor_prepopulates_bounded_register_modes()
    {
        var r = new CrdtShapeRegistry();
        Assert.Multiple(() =>
        {
            Assert.That(r.TryGet("any", LatticeMergeMode.MaxRegister), Is.Not.Null);
            Assert.That(r.TryGet("any", LatticeMergeMode.MinRegister), Is.Not.Null);
        });
    }

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

    // ?? streaming state serialiser (deferred CRDT-apply digest lane) ??
    //
    // The deferred CRDT-apply path feeds the projection-digest fold from
    // SerializeStateInto and later materialises the byte[] row from
    // SerializeState. The two lanes MUST be byte-identical or a materialised
    // read's digest contribution would diverge from the one already folded
    // in, corrupting cross-silo digest convergence. These tests pin that
    // invariant for every closed shape that wires the streaming lane.

    private static byte[] StreamState(CrdtShape shape, object state)
    {
        Assert.That(shape.SerializeStateInto, Is.Not.Null,
            "shape under test must wire the streaming serialiser");
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        shape.SerializeStateInto!(state, writer);
        return writer.WrittenSpan.ToArray();
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_OrSet()
    {
        var shape = CrdtShape.ForOrSet();
        var state = (OrSet)shape.CreateEmpty();
        for (var i = 0; i < 32; i++)
        {
            state.Add(System.Text.Encoding.UTF8.GetBytes("e-" + i), "R", i + 1);
        }
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_PnCounter()
    {
        var shape = CrdtShape.ForPnCounter();
        var state = (PnCounter)shape.CreateEmpty();
        state.Increment("A", 7);
        state.Decrement("B", 3);
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_VersionVector()
    {
        var shape = CrdtShape.ForVersionVector();
        var state = (VersionVector)shape.CreateEmpty();
        state.Tick("A");
        state.Tick("B");
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_MvRegister()
    {
        var shape = CrdtShape.ForMvRegister();
        var state = (MvRegister)shape.CreateEmpty();
        state.Set("A", new byte[] { 1, 2, 3 });
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_OrFlag()
    {
        var shape = CrdtShape.ForOrFlag();
        var state = (OrFlag)shape.CreateEmpty();
        state.Enable("A", 1);
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_RwFlag()
    {
        var shape = CrdtShape.ForRwFlag();
        var state = (RwFlag)shape.CreateEmpty();
        state.Enable("A", 1);
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_GCounter()
    {
        var shape = CrdtShape.ForGCounter();
        var state = (GCounter)shape.CreateEmpty();
        state.Increment("A", 7);
        state.Increment("B", 3);
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_empty_OrSet()
    {
        var shape = CrdtShape.ForOrSet();
        var state = shape.CreateEmpty();
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_MaxRegister()
    {
        var shape = CrdtShape.ForMaxRegister();
        var state = (BoundedRegister)shape.CreateEmpty();
        state.Set(new byte[] { 0x05 }, new byte[] { 0x05 });
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_matches_SerializeState_for_MinRegister()
    {
        var shape = CrdtShape.ForMinRegister();
        var state = (BoundedRegister)shape.CreateEmpty();
        state.Set(new byte[] { 0x05 }, new byte[] { 0x05 });
        Assert.That(StreamState(shape, state), Is.EqualTo(shape.SerializeState(state)));
    }

    [Test]
    public void SerializeStateInto_is_null_for_reflection_shapes()
    {
        // Sequence (Rga) and OR-Map use the reflection serialiser and do not
        // expose a streaming lane; the deferred apply path falls back to the
        // eager re-serialise for these shapes.
        Assert.That(CrdtShape.ForRga().SerializeStateInto, Is.Null);
        Assert.That(CrdtShape.ForOrMap<string, PnCounter>().SerializeStateInto, Is.Null);
    }
}
