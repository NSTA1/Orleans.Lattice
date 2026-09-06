using System.Buffers;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Add-only coverage for the <see cref="CrdtShape"/> per-mode factory descriptors,
/// exercising the state-lane closures (create-empty, serialize, deserialize,
/// merge-states, and the streaming <c>SerializeStateInto</c>) for every merge mode,
/// plus the constructor null-guards. These closures are the type-erased seam the
/// leaf grain drives; the round-trips here are deterministic and allocation-only,
/// no cluster required.
/// </summary>
[TestFixture]
public class CrdtShapeStateClosureTests
{
    private static IEnumerable<TestCaseData> AllShapes()
    {
        yield return new TestCaseData(CrdtShape.ForOrSet(), LatticeMergeMode.OrSet).SetName("OrSet");
        yield return new TestCaseData(CrdtShape.ForPnCounter(), LatticeMergeMode.PnCounter).SetName("PnCounter");
        yield return new TestCaseData(CrdtShape.ForVersionVector(), LatticeMergeMode.VersionVector).SetName("VersionVector");
        yield return new TestCaseData(CrdtShape.ForMvRegister(), LatticeMergeMode.MvRegister).SetName("MvRegister");
        yield return new TestCaseData(CrdtShape.ForGCounter(), LatticeMergeMode.GCounter).SetName("GCounter");
        yield return new TestCaseData(CrdtShape.ForOrFlag(), LatticeMergeMode.OrFlag).SetName("OrFlag");
        yield return new TestCaseData(CrdtShape.ForRwFlag(), LatticeMergeMode.RwFlag).SetName("RwFlag");
        yield return new TestCaseData(CrdtShape.ForGSet(), LatticeMergeMode.GSet).SetName("GSet");
        yield return new TestCaseData(CrdtShape.ForMaxRegister(), LatticeMergeMode.MaxRegister).SetName("MaxRegister");
        yield return new TestCaseData(CrdtShape.ForMinRegister(), LatticeMergeMode.MinRegister).SetName("MinRegister");
        yield return new TestCaseData(CrdtShape.ForRwSet(), LatticeMergeMode.RwSet).SetName("RwSet");
        yield return new TestCaseData(CrdtShape.ForRga(), LatticeMergeMode.Sequence).SetName("Rga");
        yield return new TestCaseData(CrdtShape.ForOrMap<string, GCounter>(), LatticeMergeMode.OrMap).SetName("OrMap");
    }

    [TestCaseSource(nameof(AllShapes))]
    public void Factory_producesShape_withExpectedModeAndNonNullClosures(CrdtShape shape, LatticeMergeMode expectedMode)
    {
        Assert.That(shape.Mode, Is.EqualTo(expectedMode));
        Assert.That(shape.DeserializeState, Is.Not.Null);
        Assert.That(shape.DeserializeDelta, Is.Not.Null);
        Assert.That(shape.MergeDelta, Is.Not.Null);
        Assert.That(shape.MergeStates, Is.Not.Null);
        Assert.That(shape.CreateEmpty, Is.Not.Null);
        Assert.That(shape.SerializeState, Is.Not.Null);
    }

    [TestCaseSource(nameof(AllShapes))]
    public void CreateEmpty_thenSerialize_roundTripsAndMergesWithSelf(CrdtShape shape, LatticeMergeMode expectedMode)
    {
        _ = expectedMode;

        var empty = shape.CreateEmpty();
        Assert.That(empty, Is.Not.Null);

        var bytes = shape.SerializeState(empty);
        Assert.That(bytes, Is.Not.Null);

        var roundTripped = shape.DeserializeState(bytes);
        Assert.That(roundTripped, Is.Not.Null);
        Assert.That(roundTripped.GetType(), Is.EqualTo(empty.GetType()));

        // Merging an empty state into another empty state is a well-defined no-op
        // for every mode; it must not throw and must leave a re-serialisable state.
        Assert.That(() => shape.MergeStates(empty, roundTripped), Throws.Nothing);
        Assert.That(shape.SerializeState(empty), Is.Not.Null);
    }

    [TestCaseSource(nameof(AllShapes))]
    public void SerializeStateInto_whenPresent_matchesSerializeState(CrdtShape shape, LatticeMergeMode expectedMode)
    {
        _ = expectedMode;

        if (shape.SerializeStateInto is null)
        {
            Assert.Pass("Shape has no streaming serialisation lane (falls back to SerializeState).");
            return;
        }

        var empty = shape.CreateEmpty();
        var expected = shape.SerializeState(empty);

        var writer = new ArrayBufferWriter<byte>();
        shape.SerializeStateInto(empty, writer);

        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void AtLeastOneShape_exposesTheStreamingSerialisationLane()
    {
        // Anti-vacuity guard for SerializeStateInto_whenPresent_matchesSerializeState:
        // that case Assert.Pass()es whenever a shape has no streaming lane, so if the
        // lane were dropped from every factory the whole matrix would go green without
        // ever comparing a byte. This test fails in exactly that scenario.
        var withStreamingLane = AllShapes()
            .Select(c => (CrdtShape)c.Arguments[0]!)
            .Count(s => s.SerializeStateInto is not null);

        Assert.That(withStreamingLane, Is.GreaterThan(0), "at least one CrdtShape factory must supply SerializeStateInto, otherwise the streaming round-trip matrix passes vacuously");
    }

    // ---- constructor null-guards ----------------------------------------

    private static Func<byte[], object> DeserState => _ => new object();
    private static Func<byte[], object> DeserDelta => _ => new object();
    private static Action<object, object> Merge => (_, _) => { };
    private static Func<object> Create => () => new object();
    private static Func<object, byte[]> Ser => _ => Array.Empty<byte>();

    [Test]
    public void Ctor_nullDeserializeState_throws()
    {
        Assert.That(
            () => new CrdtShape(LatticeMergeMode.GSet, null!, DeserDelta, Merge, Merge, Create, Ser),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullDeserializeDelta_throws()
    {
        Assert.That(
            () => new CrdtShape(LatticeMergeMode.GSet, DeserState, null!, Merge, Merge, Create, Ser),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullMergeDelta_throws()
    {
        Assert.That(
            () => new CrdtShape(LatticeMergeMode.GSet, DeserState, DeserDelta, null!, Merge, Create, Ser),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullMergeStates_throws()
    {
        Assert.That(
            () => new CrdtShape(LatticeMergeMode.GSet, DeserState, DeserDelta, Merge, null!, Create, Ser),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullCreateEmpty_throws()
    {
        Assert.That(
            () => new CrdtShape(LatticeMergeMode.GSet, DeserState, DeserDelta, Merge, Merge, null!, Ser),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullSerializeState_throws()
    {
        Assert.That(
            () => new CrdtShape(LatticeMergeMode.GSet, DeserState, DeserDelta, Merge, Merge, Create, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_validArgs_defaultsOptionalClosuresToNull()
    {
        var shape = new CrdtShape(LatticeMergeMode.GSet, DeserState, DeserDelta, Merge, Merge, Create, Ser);
        Assert.That(shape.SerializeDelta, Is.Null);
        Assert.That(shape.CombineDeltas, Is.Null);
        Assert.That(shape.SerializeStateInto, Is.Null);
    }
}
