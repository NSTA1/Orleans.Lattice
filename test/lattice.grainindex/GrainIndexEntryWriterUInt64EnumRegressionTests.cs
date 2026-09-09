using System.Text.Json;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Regression tests for projecting a property whose type is an enum with a
/// <see cref="ulong"/> underlying type. <c>GrainIndexEntryWriter.WriteFallback</c>
/// previously wrote every enum through <c>Convert.ToInt64</c>, which throws
/// <see cref="OverflowException"/> for a member above <see cref="long.MaxValue"/>
/// (for example <c>ulong.MaxValue</c>), crashing the whole projection. The writer
/// must instead read a ulong-backed enum through its unsigned underlying type and
/// emit the same JSON number a plain <see cref="ulong"/> property would.
/// </summary>
[TestFixture]
public sealed class GrainIndexEntryWriterUInt64EnumRegressionTests
{
    private enum WideEnum : ulong
    {
        Low = 1,
        AboveLongMax = (ulong)long.MaxValue + 1UL,
        Max = ulong.MaxValue,
    }

    private sealed class WideEnumState
    {
        public WideEnum Kind { get; set; }
    }

    private static GrainIndexProjection Project(WideEnum value)
    {
        var definition = new GrainIndexDefinition<ITestStringKeyedGrain, WideEnumState>(
            "wide-enum-test",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [new TypedGrainIndexProperty<WideEnumState, WideEnum>("Kind", static s => s.Kind)]);

        return new GrainIndexProjector<ITestStringKeyedGrain, WideEnumState>(definition)
            .Project("g1", new WideEnumState { Kind = value });
    }

    private static ulong PayloadNumber(GrainIndexProjection projection)
    {
        using var document = JsonDocument.Parse(projection.Entries.Single().Value);
        return document.RootElement.GetProperty("Kind").GetUInt64();
    }

    [Test]
    public void Project_ulong_backed_enum_above_long_max_does_not_overflow()
    {
        // On the unfixed writer this throws OverflowException inside the projection.
        var projection = Project(WideEnum.AboveLongMax);

        Assert.That(PayloadNumber(projection), Is.EqualTo((ulong)long.MaxValue + 1UL));
    }

    [Test]
    public void Project_ulong_backed_enum_max_value_writes_exact_unsigned_number()
    {
        var projection = Project(WideEnum.Max);

        Assert.That(PayloadNumber(projection), Is.EqualTo(ulong.MaxValue));
    }

    [Test]
    public void Project_ulong_backed_enum_small_value_stays_exact()
    {
        var projection = Project(WideEnum.Low);

        Assert.That(PayloadNumber(projection), Is.EqualTo(1UL));
    }
}
