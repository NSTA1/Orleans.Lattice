using System.Text.Json;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Wire-conformance tests for <see cref="CrdtJsonSerializerContext"/>. The
/// closed-shape CRDT (de)serialisers were moved off the reflection-based
/// <see cref="JsonLatticeSerializer{T}"/> onto System.Text.Json
/// source-generated metadata to drop the per-apply reflection cost from the
/// CRDT delta-apply hot path. The persisted byte[] rows and the replication
/// wire shape must not drift as a result, so every representative instance is
/// asserted to serialise byte-identically through both lanes and to
/// round-trip across the lanes in both directions.
/// </summary>
[TestFixture]
public class CrdtJsonSerializerContextTests
{
    private static byte[] Reflection<T>(T value) =>
        JsonSerializer.SerializeToUtf8Bytes(value, (JsonSerializerOptions?)null);

    private static PnCounter SamplePnCounter()
    {
        var c = new PnCounter();
        c.Increment("replica-a", 7);
        c.Increment("replica-b", 2);
        c.Decrement("replica-a", 3);
        return c;
    }

    private static OrSet SampleOrSet()
    {
        var s = new OrSet();
        s.Add([1, 2, 3], "replica-a", 1);
        s.Add([4, 5], "replica-b", 2);
        s.Add([1, 2, 3], "replica-b", 3);
        s.Remove([4, 5]);
        return s;
    }

    private static VersionVector SampleVersionVector()
    {
        var v = new VersionVector();
        v.Tick("replica-a");
        v.Tick("replica-a");
        v.Tick("replica-b");
        return v;
    }

    private static MvRegister SampleMvRegister()
    {
        var m = new MvRegister();
        m.Set("replica-a", [9, 9]);
        m.Set("replica-b", [8]);
        return m;
    }

    private static OrFlag SampleOrFlag()
    {
        var f = new OrFlag();
        f.Enable("replica-a", 1);
        f.Enable("replica-b", 2);
        f.Disable();
        f.Enable("replica-a", 3);
        return f;
    }

    private static RwFlag SampleRwFlag()
    {
        var f = new RwFlag();
        f.Enable("replica-a", 1);
        f.Disable("replica-b", 2);
        f.Enable("replica-a", 3);
        return f;
    }

    private static PnCounterDelta SamplePnCounterDelta() => new()
    {
        Increments = new Dictionary<string, long> { ["replica-a"] = 7, ["replica-b"] = 2 },
        Decrements = new Dictionary<string, long> { ["replica-a"] = 3 },
    };

    private static OrSetDelta SampleOrSetDelta() => new()
    {
        Adds = new[]
        {
            new OrSetDeltaDot { Element = [1, 2, 3], ReplicaId = "replica-a", Counter = 1 },
            new OrSetDeltaDot { Element = [4, 5], ReplicaId = "replica-b", Counter = 2 },
        },
        Removes = new[]
        {
            new OrSetDeltaDot { Element = [4, 5], ReplicaId = "replica-b", Counter = 2 },
        },
    };

    private static VersionVectorDelta SampleVersionVectorDelta() => new()
    {
        Entries = new Dictionary<string, HybridLogicalClock>
        {
            ["replica-a"] = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 },
            ["replica-b"] = new HybridLogicalClock { WallClockTicks = 456, Counter = 0 },
        },
    };

    private static MvRegisterDelta SampleMvRegisterDelta() => new()
    {
        Entries = new[]
        {
            new MvRegisterEntry { ReplicaId = "replica-a", Counter = 1, Value = [9, 9] },
            new MvRegisterEntry { ReplicaId = "replica-b", Counter = 1, Value = [8] },
        },
        Context = new Dictionary<string, long> { ["replica-a"] = 1, ["replica-b"] = 1 },
    };

    private static OrFlagDelta SampleOrFlagDelta() => new()
    {
        Enables = new[]
        {
            new OrSetDot { ReplicaId = "replica-a", Counter = 1 },
            new OrSetDot { ReplicaId = "replica-b", Counter = 2 },
        },
        Disables = new[]
        {
            new OrSetDot { ReplicaId = "replica-a", Counter = 1 },
        },
    };

    private static RwFlagDelta SampleRwFlagDelta() => new()
    {
        Enables = new[]
        {
            new OrSetDot { ReplicaId = "replica-a", Counter = 1 },
            new OrSetDot { ReplicaId = "replica-b", Counter = 2 },
        },
        Disables = new[]
        {
            new OrSetDot { ReplicaId = "replica-c", Counter = 4 },
        },
        Tombstones = new[]
        {
            new OrSetDot { ReplicaId = "replica-d", Counter = 3 },
        },
    };

    [Test]
    public void PnCounter_state_is_byte_identical_to_reflection()
    {
        var value = SamplePnCounter();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.PnCounter);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void OrSet_state_is_byte_identical_to_reflection()
    {
        var value = SampleOrSet();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.OrSet);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void VersionVector_state_is_byte_identical_to_reflection()
    {
        var value = SampleVersionVector();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.VersionVector);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void MvRegister_state_is_byte_identical_to_reflection()
    {
        var value = SampleMvRegister();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.MvRegister);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void PnCounterDelta_is_byte_identical_to_reflection()
    {
        var value = SamplePnCounterDelta();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.PnCounterDelta);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void OrSetDelta_is_byte_identical_to_reflection()
    {
        var value = SampleOrSetDelta();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.OrSetDelta);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void VersionVectorDelta_is_byte_identical_to_reflection()
    {
        var value = SampleVersionVectorDelta();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.VersionVectorDelta);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void MvRegisterDelta_is_byte_identical_to_reflection()
    {
        var value = SampleMvRegisterDelta();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.MvRegisterDelta);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void OrFlag_state_is_byte_identical_to_reflection()
    {
        var value = SampleOrFlag();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.OrFlag);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void OrFlagDelta_is_byte_identical_to_reflection()
    {
        var value = SampleOrFlagDelta();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.OrFlagDelta);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void RwFlag_state_is_byte_identical_to_reflection()
    {
        var value = SampleRwFlag();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.RwFlag);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void RwFlagDelta_is_byte_identical_to_reflection()
    {
        var value = SampleRwFlagDelta();
        var sourceGen = JsonSerializer.SerializeToUtf8Bytes(value, CrdtJsonSerializerContext.Default.RwFlagDelta);
        Assert.That(sourceGen, Is.EqualTo(Reflection(value)));
    }

    [Test]
    public void Source_gen_reads_legacy_reflection_bytes_for_states()
    {
        // Old persisted rows were written by the reflection serialiser; the
        // shape's source-generated DeserializeState must read them back.
        var registry = new CrdtShapeRegistry();

        var pn = (PnCounter)registry.TryGet("t", LatticeMergeMode.PnCounter)!.DeserializeState(Reflection(SamplePnCounter()));
        Assert.That(pn.Value, Is.EqualTo(SamplePnCounter().Value));

        var or = (OrSet)registry.TryGet("t", LatticeMergeMode.OrSet)!.DeserializeState(Reflection(SampleOrSet()));
        Assert.That(or.Count, Is.EqualTo(SampleOrSet().Count));

        var vv = (VersionVector)registry.TryGet("t", LatticeMergeMode.VersionVector)!.DeserializeState(Reflection(SampleVersionVector()));
        Assert.That(vv.Entries.Count, Is.EqualTo(SampleVersionVector().Entries.Count));

        var mv = (MvRegister)registry.TryGet("t", LatticeMergeMode.MvRegister)!.DeserializeState(Reflection(SampleMvRegister()));
        Assert.That(mv.Entries.Count, Is.EqualTo(SampleMvRegister().Entries.Count));

        var of = (OrFlag)registry.TryGet("t", LatticeMergeMode.OrFlag)!.DeserializeState(Reflection(SampleOrFlag()));
        Assert.That(of.IsEnabled, Is.EqualTo(SampleOrFlag().IsEnabled));

        var rw = (RwFlag)registry.TryGet("t", LatticeMergeMode.RwFlag)!.DeserializeState(Reflection(SampleRwFlag()));
        Assert.That(rw.IsEnabled, Is.EqualTo(SampleRwFlag().IsEnabled));
    }

    [Test]
    public void Source_gen_reads_legacy_reflection_bytes_for_deltas()
    {
        var registry = new CrdtShapeRegistry();

        var pn = (PnCounterDelta)registry.TryGet("t", LatticeMergeMode.PnCounter)!.DeserializeDelta(Reflection(SamplePnCounterDelta()));
        Assert.That(pn.Increments!["replica-a"], Is.EqualTo(7));

        var or = (OrSetDelta)registry.TryGet("t", LatticeMergeMode.OrSet)!.DeserializeDelta(Reflection(SampleOrSetDelta()));
        Assert.That(or.Adds!.Count, Is.EqualTo(2));

        var vv = (VersionVectorDelta)registry.TryGet("t", LatticeMergeMode.VersionVector)!.DeserializeDelta(Reflection(SampleVersionVectorDelta()));
        Assert.That(vv.Entries!.Count, Is.EqualTo(2));

        var mv = (MvRegisterDelta)registry.TryGet("t", LatticeMergeMode.MvRegister)!.DeserializeDelta(Reflection(SampleMvRegisterDelta()));
        Assert.That(mv.Entries!.Count, Is.EqualTo(2));

        var of = (OrFlagDelta)registry.TryGet("t", LatticeMergeMode.OrFlag)!.DeserializeDelta(Reflection(SampleOrFlagDelta()));
        Assert.That(of.Enables!.Count, Is.EqualTo(2));

        var rw = (RwFlagDelta)registry.TryGet("t", LatticeMergeMode.RwFlag)!.DeserializeDelta(Reflection(SampleRwFlagDelta()));
        Assert.That(rw.Enables!.Count, Is.EqualTo(2));
    }

    [Test]
    public void Shape_state_serializer_is_byte_identical_to_reflection()
    {
        // The production path goes through CrdtShape.SerializeState; assert the
        // shipped seam (not just the raw context) stays wire-identical.
        var registry = new CrdtShapeRegistry();

        var pn = SamplePnCounter();
        Assert.That(registry.TryGet("t", LatticeMergeMode.PnCounter)!.SerializeState(pn), Is.EqualTo(Reflection(pn)));

        var or = SampleOrSet();
        Assert.That(registry.TryGet("t", LatticeMergeMode.OrSet)!.SerializeState(or), Is.EqualTo(Reflection(or)));

        var vv = SampleVersionVector();
        Assert.That(registry.TryGet("t", LatticeMergeMode.VersionVector)!.SerializeState(vv), Is.EqualTo(Reflection(vv)));

        var mv = SampleMvRegister();
        Assert.That(registry.TryGet("t", LatticeMergeMode.MvRegister)!.SerializeState(mv), Is.EqualTo(Reflection(mv)));

        var of = SampleOrFlag();
        Assert.That(registry.TryGet("t", LatticeMergeMode.OrFlag)!.SerializeState(of), Is.EqualTo(Reflection(of)));

        var rw = SampleRwFlag();
        Assert.That(registry.TryGet("t", LatticeMergeMode.RwFlag)!.SerializeState(rw), Is.EqualTo(Reflection(rw)));
    }
}
