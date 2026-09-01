namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexEntryWriter"/>'s fallback path: properties whose
/// type is not in the writer's explicit fast set (bool, byte, short, int, long,
/// float, double, DateTime*, DateTimeOffset*, Guid, string, enum) pass through
/// <c>WriteFallback</c>, which handles nullable-enum-with-null, IFormattable,
/// a plain <c>ToString</c> fallback, and <c>char</c>.
/// </summary>
[TestFixture]
public sealed class GrainIndexEntryWriterTests
{
    /// <summary>
    /// State type whose properties cover the fallback branches:
    /// <c>char</c>, <c>TimeSpan</c> (IFormattable non-enum), a custom class
    /// (ToString-only fallback), and a nullable enum.
    /// </summary>
    private sealed class FallbackState
    {
        public char Glyph { get; set; } = 'Z';
        public TimeSpan Span { get; set; } = TimeSpan.FromMinutes(3);
        public CustomObject? Tag { get; set; } = new();
        public TestStatus? OptionalStatus { get; set; }
    }

    /// <summary>A custom non-IFormattable type exercising the ToString fallback.</summary>
    private sealed class CustomObject
    {
        public override string ToString() => "custom";
    }

    private static GrainIndexDefinition<ITestStringKeyedGrain, FallbackState> MakeDefinition(
        params TypedGrainIndexProperty<FallbackState, object?>[] properties)
    {
        throw new NotSupportedException("Use the typed overload.");
    }

    private static GrainIndexProjection Project<TProperty>(
        string propName,
        Func<FallbackState, TProperty> accessor,
        FallbackState state)
    {
        var def = new GrainIndexDefinition<ITestStringKeyedGrain, FallbackState>(
            "fallback-test",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [new TypedGrainIndexProperty<FallbackState, TProperty>(propName, accessor)]);

        return new GrainIndexProjector<ITestStringKeyedGrain, FallbackState>(def)
            .Project("g1", state);
    }

    [Test]
    public void A_char_property_produces_an_entry_via_WriteChar()
    {
        // WriteChar (line 224) is invoked for typeof(TProperty) == typeof(char).
        var state = new FallbackState { Glyph = 'X' };
        var projection = Project("Glyph", static s => s.Glyph, state);

        Assert.That(projection.Entries, Has.Count.EqualTo(1),
            "A char property must produce exactly one index entry.");
    }

    [Test]
    public void A_TimeSpan_property_produces_an_entry_via_IFormattable()
    {
        // TimeSpan implements IFormattable, so lines 217-219 fire in WriteFallback.
        var state = new FallbackState { Span = TimeSpan.FromSeconds(42) };
        var projection = Project("Span", static s => s.Span, state);

        Assert.That(projection.Entries, Has.Count.EqualTo(1),
            "An IFormattable property must produce exactly one index entry.");
    }

    [Test]
    public void A_custom_non_IFormattable_property_produces_an_entry_via_ToString()
    {
        // CustomObject does not implement IFormattable, so line 220 fires.
        var state = new FallbackState { Tag = new CustomObject() };
        var projection = Project("Tag", static s => s.Tag, state);

        Assert.That(projection.Entries, Has.Count.EqualTo(1),
            "A non-IFormattable property must fall back to ToString for its entry.");
    }

    [Test]
    public void A_nullable_enum_property_with_a_null_value_writes_the_null_slot()
    {
        // null passed to WriteFallback with a nullable enum type (lines 207-208).
        var state = new FallbackState { OptionalStatus = null };
        var projection = Project("OptionalStatus", static s => s.OptionalStatus, state);

        Assert.That(projection.Entries, Has.Count.EqualTo(1),
            "A null nullable-enum property must still produce one entry for the null slot.");
    }
}
