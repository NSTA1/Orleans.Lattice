using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Lattice.GrainIndex.Tests.Enrollment;
using Orleans.Lattice.GrainIndex.Tests.Registry;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Assembles a <see cref="GrainIndexBackfillGrain"/> over in-memory stores, a
/// stubbed clock, and a substituted reminder registry, so the crawl's state
/// machine is exercised as a unit test.
/// </summary>
/// <remarks>
/// <para>
/// The harness leaves <see cref="GrainIndexOptions.BackfillEnabled"/> off. That
/// is not a convenience: with the background driver off the grain registers no
/// reminder and no grain timer, so every pass happens exactly when the test asks
/// for one. Nothing here waits on wall-clock time, a scheduler, or a garbage
/// collection.
/// </para>
/// <para>
/// The grain-timer infrastructure is unavailable on a substituted
/// <see cref="IGrainContext"/> anyway, which is the same reason the core
/// tombstone-compaction grain keeps its timer registration out of the paths its
/// unit tests drive.
/// </para>
/// </remarks>
internal sealed class BackfillHarness
{
    /// <summary>The index the harness declares.</summary>
    internal const string IndexName = "backfill-subjects";

    private readonly GrainIndexDeclarationOptions _declarations = new();

    /// <summary>Initialises the harness with a two-property declaration.</summary>
    internal BackfillHarness()
    {
        Definition = new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
            IndexName,
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [
                new TypedGrainIndexProperty<TestGrainState, int>("Age", static s => s.Age),
                new TypedGrainIndexProperty<TestGrainState, string>("Country", static s => s.Country),
            ]);

        _declarations.Definitions.Add(Definition);

        Options = new GrainIndexOptions
        {
            TreeName = GrainIndexTreeNames.ForIndex(IndexName),
            BackfillBatchSize = 2,
            BackfillEnabled = false,
        };

        OptionsMonitor = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        OptionsMonitor.Get(Arg.Any<string>()).Returns(_ => Options);

        Context = Substitute.For<IGrainContext>();
        Context.GrainId.Returns(GrainId.Create("grainindex-backfill", IndexName));

        // The grain-timer extension resolves ITimerRegistry from the
        // activation's services. Substituting it keeps the enabled-driver paths
        // reachable in a unit test while scheduling nothing: no callback ever
        // fires, so a pass still only happens when a test asks for one.
        var activationServices = new ServiceCollection();
        activationServices.AddSingleton(Substitute.For<ITimerRegistry>());
        Context.ActivationServices.Returns(activationServices.BuildServiceProvider());

        Reminders = Substitute.For<IReminderRegistry>();
        KeySources = Substitute.For<IGrainKeySourceResolver>();
        KeySources.Resolve(Arg.Any<string>()).Returns(_ => KeySource);
    }

    /// <summary>The declaration the crawl projects.</summary>
    internal GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState> Definition { get; }

    /// <summary>The per-index options every lookup returns.</summary>
    internal GrainIndexOptions Options { get; }

    /// <summary>The substituted options monitor.</summary>
    internal IOptionsMonitor<GrainIndexOptions> OptionsMonitor { get; }

    /// <summary>The substituted activation context, keyed by the index name.</summary>
    internal IGrainContext Context { get; }

    /// <summary>The substituted reminder registry.</summary>
    internal IReminderRegistry Reminders { get; }

    /// <summary>The substituted key-source lookup, which returns <see cref="KeySource"/>.</summary>
    internal IGrainKeySourceResolver KeySources { get; }

    /// <summary>The key source the crawl walks, or <c>null</c> to register none.</summary>
    internal ListGrainKeySource? KeySource { get; set; }

    /// <summary>The durable checkpoints.</summary>
    internal FakeGrainIndexBackfillStore Checkpoints { get; } = new();

    /// <summary>The index registry, which supplies the declaration fingerprint.</summary>
    internal FakeGrainIndexRegistryStore Registry { get; } = new();

    /// <summary>The seen markers the crawl skips over.</summary>
    internal RecordingEnrollmentStore Enrollments { get; } = new();

    /// <summary>The activator that stands in for touching a grain.</summary>
    internal RecordingBackfillActivator Activator { get; } = new();

    /// <summary>The clock the checkpoint is stamped from.</summary>
    internal StubTimeProvider Time { get; } = new();

    /// <summary>The fingerprint of the harness's declaration under its options.</summary>
    internal GrainIndexFingerprint Fingerprint =>
        GrainIndexFingerprint.Compute(
            Definition.Describe(Options),
            GrainIndexKeyCodecIdentity.For(Definition.KeyCodec));

    /// <summary>Populates the key source with <paramref name="keys"/>.</summary>
    /// <param name="keys">The population to crawl.</param>
    /// <returns>The harness, for chaining.</returns>
    internal BackfillHarness WithKeys(params string[] keys)
    {
        KeySource = new ListGrainKeySource(keys);
        return this;
    }

    /// <summary>
    /// Seeds the registry record the reconciler would have written, so the crawl
    /// has a declaration fingerprint to run under.
    /// </summary>
    /// <param name="needsBackfill">Whether the index owes a backfill.</param>
    /// <param name="fingerprint">The fingerprint to record, defaulting to the harness's own.</param>
    /// <returns>The harness, for chaining.</returns>
    internal BackfillHarness WithRegistryRecord(
        bool needsBackfill = true,
        GrainIndexFingerprint? fingerprint = null)
    {
        var descriptor = Definition.Describe(Options);
        Registry.Seed(
            IndexName,
            new GrainIndexRegistryRecord(
                descriptor,
                GrainIndexKeyCodecIdentity.For(Definition.KeyCodec),
                fingerprint ?? Fingerprint,
                needsBackfill));

        return this;
    }

    /// <summary>Marks <paramref name="grainKey"/> as already indexed.</summary>
    /// <param name="grainKey">The encoded grain key.</param>
    /// <returns>The harness, for chaining.</returns>
    internal BackfillHarness WithEnrolled(string grainKey)
    {
        Enrollments.SeedEnrollment(IndexName, grainKey, GrainIndexProjection.Empty(grainKey));
        return this;
    }

    /// <summary>Builds the grain under test.</summary>
    /// <returns>The grain.</returns>
    internal GrainIndexBackfillGrain CreateGrain() =>
        new(
            Context,
            Reminders,
            Microsoft.Extensions.Options.Options.Create(_declarations),
            OptionsMonitor,
            Checkpoints,
            Registry,
            Enrollments,
            Activator,
            KeySources,
            Time,
            NullLogger<GrainIndexBackfillGrain>.Instance);

    /// <summary>The checkpoint the crawl has persisted, if any.</summary>
    /// <returns>The checkpoint, or <c>null</c>.</returns>
    internal GrainIndexBackfillCheckpoint? StoredCheckpoint() => Checkpoints.Peek(IndexName);
}
