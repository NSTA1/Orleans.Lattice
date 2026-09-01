using NSubstitute;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Builds a populated grain index over <see cref="FakeIndexTree"/>: the entries
/// are produced by the real <see cref="GrainIndexProjector{TGrain, TState}"/>, so
/// the keys and payloads the query planner and executor meet are byte for byte
/// the ones the projection path writes.
/// </summary>
internal sealed class QueryTestIndex
{
    private readonly Dictionary<string, ITestStringKeyedGrain> _grains = new(StringComparer.Ordinal);

    private QueryTestIndex(
        GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> definition,
        FakeIndexTree tree,
        IGrainFactory grainFactory)
    {
        Definition = definition;
        Tree = tree;
        GrainFactory = grainFactory;
        Index = new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, tree.Lattice, grainFactory);
    }

    /// <summary>The definition under query.</summary>
    internal GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> Definition { get; }

    /// <summary>The backing store.</summary>
    internal FakeIndexTree Tree { get; }

    /// <summary>The substituted factory that resolves matched grains.</summary>
    internal IGrainFactory GrainFactory { get; }

    /// <summary>The index under test.</summary>
    internal GrainIndex<ITestStringKeyedGrain, IndexedTestState> Index { get; }

    /// <summary>
    /// Builds an index over the standard definition (Age, Country, LastSeen,
    /// Status), pre-populated with <paramref name="subjects"/>.
    /// </summary>
    internal static QueryTestIndex Create(params (string Key, IndexedTestState State)[] subjects) =>
        Create(IndexedTestIndex.Definition(), subjects);

    /// <summary>
    /// Builds an index over <paramref name="definition"/>, pre-populated with
    /// <paramref name="subjects"/>.
    /// </summary>
    internal static QueryTestIndex Create(
        GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> definition,
        params (string Key, IndexedTestState State)[] subjects)
    {
        var tree = new FakeIndexTree();
        var factory = Substitute.For<IGrainFactory>();
        var index = new QueryTestIndex(definition, tree, factory);

        factory.GetGrain<ITestStringKeyedGrain>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(call => index.GrainFor(call.ArgAt<string>(0)));

        var projector = new GrainIndexProjector<ITestStringKeyedGrain, IndexedTestState>(definition);
        foreach (var subject in subjects)
        {
            var projection = projector.Project(subject.Key, subject.State);
            foreach (var entry in projection.Entries)
            {
                tree.Put(entry.Key, entry.Value);
            }
        }

        return index;
    }

    /// <summary>The stable grain stand-in for <paramref name="key"/>.</summary>
    internal ITestStringKeyedGrain GrainFor(string key)
    {
        if (!_grains.TryGetValue(key, out var grain))
        {
            grain = Substitute.For<ITestStringKeyedGrain>();
            _grains[key] = grain;
        }

        return grain;
    }

    /// <summary>A state instance with the fields the query tests vary.</summary>
    internal static IndexedTestState State(
        int age = 0,
        string country = "",
        DateTimeOffset? lastSeen = null,
        TestStatus status = TestStatus.Unknown,
        bool isActive = false,
        double score = 0.0) =>
        new()
        {
            Age = age,
            Country = country,
            LastSeen = lastSeen,
            Status = status,
            IsActive = isActive,
            Score = score,
        };
}
