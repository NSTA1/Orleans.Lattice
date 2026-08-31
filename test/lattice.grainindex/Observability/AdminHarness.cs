using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Lattice.GrainIndex.Observability;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Lattice.GrainIndex.Tests.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// Assembles a <c>GrainIndexAdmin</c> over an in-memory registry, a substituted
/// backfill activation, and a substituted index tree, so the whole
/// administrative surface is exercised without a cluster.
/// </summary>
/// <remarks>
/// Nothing here schedules anything: the backfill activation is a substitute that
/// returns a status the test chooses, so a control call is proved by what it
/// delegated to rather than by anything that has to happen later.
/// </remarks>
internal sealed class AdminHarness
{
    private readonly GrainIndexDeclarationOptions _declarations = new();
    private readonly Dictionary<string, GrainIndexOptions> _options = new(StringComparer.Ordinal);

    /// <summary>Initialises a harness declaring <paramref name="indexNames"/>, in order.</summary>
    /// <param name="indexNames">The indexes the silo declares.</param>
    internal AdminHarness(params string[] indexNames)
    {
        foreach (var name in indexNames)
        {
            _declarations.Definitions.Add(
                new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
                    name,
                    StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
                    [
                        new TypedGrainIndexProperty<TestGrainState, int>("Age", static s => s.Age),
                        new TypedGrainIndexProperty<TestGrainState, string>("Country", static s => s.Country),
                    ]));

            _options[name] = new GrainIndexOptions { TreeName = GrainIndexTreeNames.ForIndex(name) };
        }

        var monitor = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(call => OptionsFor(call.Arg<string>()));

        Backfill = Substitute.For<IGrainIndexBackfillGrain>();
        Backfill.GetStatusAsync().Returns(_ => Task.FromResult(Status));
        Backfill.PauseAsync().Returns(_ => Task.FromResult(Status));
        Backfill.ResumeAsync().Returns(_ => Task.FromResult(Status));
        Backfill.RestartAsync().Returns(_ => Task.FromResult(Status));
        Backfill.RunBatchAsync()
            .Returns(_ => Task.FromResult(GrainIndexBackfillBatchResult.None(Status.State)));

        Tree = Substitute.For<ILattice>();
        Tree.CountAsync(Arg.Any<CancellationToken>()).Returns(0);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IGrainIndexBackfillGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(Backfill);
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(Tree);

        var keySources = Substitute.For<IGrainKeySourceResolver>();
        keySources.Resolve(Arg.Any<string>()).Returns(_ => KeySource);

        Registry = new FakeGrainIndexRegistryStore();

        Admin = new GrainIndexAdmin(
            Options.Create(_declarations),
            monitor,
            Registry,
            keySources,
            factory);
    }

    /// <summary>The surface under test.</summary>
    internal GrainIndexAdmin Admin { get; }

    /// <summary>The in-memory registry the admin reads.</summary>
    internal FakeGrainIndexRegistryStore Registry { get; }

    /// <summary>The substituted backfill activation every control delegates to.</summary>
    internal IGrainIndexBackfillGrain Backfill { get; }

    /// <summary>The substituted index tree the entry count is read from.</summary>
    internal ILattice Tree { get; }

    /// <summary>The key source the admin resolves, or <c>null</c> for none.</summary>
    internal IGrainKeySource? KeySource { get; set; }

    /// <summary>The status the substituted crawl reports.</summary>
    internal GrainIndexBackfillStatus Status { get; set; } = GrainIndexBackfillStatus.NotStarted("users");

    /// <summary>Stores a registry record for <paramref name="indexName"/>.</summary>
    /// <param name="indexName">The index to register.</param>
    /// <param name="needsBackfill">Whether the record has a backfill outstanding.</param>
    /// <param name="descriptor">The stored descriptor, or <c>null</c> for the live one.</param>
    /// <returns>The stored record.</returns>
    internal GrainIndexRegistryRecord SeedRecord(
        string indexName,
        bool needsBackfill,
        GrainIndexDescriptor? descriptor = null)
    {
        var definition = _declarations.Definitions.First(d => d.Name == indexName);
        var stored = descriptor ?? definition.Describe(OptionsFor(indexName));
        var keyCodecId = GrainIndexKeyCodecIdentity.For(definition.KeyCodec);

        var record = new GrainIndexRegistryRecord(
            stored,
            keyCodecId,
            GrainIndexFingerprint.Compute(stored, keyCodecId),
            needsBackfill);

        Registry.Seed(indexName, record);
        return record;
    }

    private GrainIndexOptions OptionsFor(string indexName) =>
        _options.TryGetValue(indexName, out var options) ? options : new GrainIndexOptions();
}
