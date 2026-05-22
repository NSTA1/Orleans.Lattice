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
}
