namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Shared construction helpers for the projection tests, so each fixture states
/// only what it is actually varying.
/// </summary>
internal static class IndexedTestIndex
{
    /// <summary>
    /// A definition over the four properties the projection tests exercise:
    /// two ordered, one ordered nullable, and one with no order-preserving
    /// encoding.
    /// </summary>
    internal static GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> Definition(string name = "Subjects") =>
        new(
            name,
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [
                Property<int>("Age", static s => s.Age),
                Property<string>("Country", static s => s.Country),
                Property<DateTimeOffset?>("LastSeen", static s => s.LastSeen),
                Property<TestStatus>("Status", static s => s.Status),
            ]);

    /// <summary>A definition over a single named property.</summary>
    internal static GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> SingleProperty<TProperty>(
        string name,
        Func<IndexedTestState, TProperty> accessor) =>
        new(
            "Subjects",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [Property(name, accessor)]);

    /// <summary>A definition that projects nothing.</summary>
    internal static GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> Empty() =>
        new("Subjects", StringGrainKeyCodec<ITestStringKeyedGrain>.Instance, []);

    internal static GrainIndexProjector<ITestStringKeyedGrain, IndexedTestState> Projector(
        GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState>? definition = null) =>
        new(definition ?? Definition());

    internal static TypedGrainIndexProperty<IndexedTestState, TProperty> Property<TProperty>(
        string name,
        Func<IndexedTestState, TProperty> accessor) =>
        new(name, accessor);

    /// <summary>The entry for <paramref name="property"/>, which must be present.</summary>
    internal static GrainIndexEntry EntryFor(GrainIndexProjection projection, string property)
    {
        foreach (var entry in projection.Entries)
        {
            if (GrainIndexKeyEncoder.TryParseKey(entry.Key, out var name, out _, out _)
                && string.Equals(name, property, StringComparison.Ordinal))
            {
                return entry;
            }
        }

        throw new InvalidOperationException($"No entry projected for property '{property}'.");
    }
}
