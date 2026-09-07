using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Regression tests for issue #2188: the zero-repository warmup branch must satisfy
/// readiness <b>without</b> asserting that the vector plane served, because that
/// assertion is load-bearing for three other consumers.
/// <para>
/// Every test here is written as a <b>differential across two subjects</b> rather than
/// as a value read off one. The probe (<c>Phase</c> / <c>IsReady</c>) shares its
/// mechanism with the behaviour under test, so a single-subject assertion could pass
/// vacuously if the fixture never drove the warmup at all. Each test therefore pins the
/// startup goal first - <b>the empty box must be ready</b>, which fails loudly if the
/// fixture is broken or if a fix wedges a fresh box - and only then compares the warmed
/// subject against an identically-configured control that never saw the warmup.
/// </para>
/// <para>
/// These tests deliberately reference only API that predates the fix, so the same file
/// compiles and runs against the unfixed tree. That is what makes the red arm of the
/// red/green pair a source-only revert with reconcilable test counts.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextRetrievalWarmupReadinessPhaseTests
{
    private static readonly TimeSpan HoldDown = TimeSpan.FromSeconds(30);

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    /// <summary>
    /// Drives one warmup pass against a store holding <b>no</b> repository at all, and
    /// returns the readiness state it fed. A genuinely empty start, not a restored one:
    /// the tree yields nothing, so no index job resumes and writes to it.
    /// </summary>
    private static async Task<bool> WarmEmptyStoreAsync(RepoContextRetrievalReadinessState readiness)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => NoEntries());
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        var search = new RepoContextSearchService(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            AvailableEmbedder(),
            readiness);

        var warmup = new RepoContextRetrievalWarmup(
            store, search, readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        return await warmup.TryWarmAsync(CancellationToken.None);
    }

    [Test]
    public async Task An_empty_box_does_not_lock_a_keyword_only_host_out_of_its_own_phase()
    {
        // Consequence 1. MarkKeywordOnly() refuses to demote a plane proven serving,
        // which is correct - but the empty-box branch must not manufacture that proof,
        // because there is no path back and the host is then permanently tagged with a
        // phase that does not describe it.
        using var warmed = new RepoContextRetrievalReadinessState(new SettableTimeProvider(), HoldDown);
        using var control = new RepoContextRetrievalReadinessState(new SettableTimeProvider(), HoldDown);

        var ready = await WarmEmptyStoreAsync(warmed);

        // Anti-vacuity: the startup goal must still hold, or the comparison below is
        // meaningless and this fixture is measuring nothing.
        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.True, "A fresh box must not be wedged before its first repository is onboarded.");
            Assert.That(warmed.IsReady, Is.True);
        });

        warmed.MarkKeywordOnly();
        control.MarkKeywordOnly();

        Assert.Multiple(() =>
        {
            Assert.That(
                warmed.Phase,
                Is.EqualTo(control.Phase),
                "Having started empty must not change where a later keyword-only observation lands.");
            Assert.That(
                warmed.Phase,
                Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly),
                "A host with no embedder bound belongs in KeywordOnly, whether or not it started empty.");
        });
    }

    [Test]
    public async Task An_empty_box_stamps_time_to_ready_without_claiming_it_served()
    {
        // Consequence 2. The ready_seconds figure is tagged by the phase first reached
        // and is published exactly once per process, so a `serving` tag here is a
        // monitoring signal that is confidently wrong and can never be corrected.
        string? phaseTag = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.ReadySecondsInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.PhaseTagKey)
                {
                    phaseTag = tag.Value as string;
                }
            }
        });
        listener.Start();

        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider(), HoldDown);
        var ready = await WarmEmptyStoreAsync(readiness);

        Assert.Multiple(() =>
        {
            // Anti-vacuity: the host did reach ready and the stamp did fire, so a
            // passing tag assertion cannot be a measurement that never happened.
            Assert.That(ready, Is.True);
            Assert.That(readiness.TimeToReady, Is.Not.Null, "Reaching ready must publish the time-to-ready figure.");
            Assert.That(phaseTag, Is.Not.Null, "The ready_seconds histogram must have been recorded.");
            Assert.That(
                phaseTag,
                Is.Not.EqualTo(RepoContextRetrievalReadinessState.PhaseServingTag),
                "No query ran, so the once-per-process readiness stamp must not claim the plane served.");
        });
    }

    [Test]
    public async Task An_empty_box_earns_no_fault_hold_down_grace()
    {
        // Consequence 3. The Phase getter applies the hold-down to ServingRaw alone
        // (`if (raw != ServingRaw) return raw;`), so a plane that never served is
        // revoked at once. Latching Serving at startup silently buys a later genuine
        // fault a full grace window that no retrieval earned.
        var warmedClock = new SettableTimeProvider();
        var controlClock = new SettableTimeProvider();
        using var warmed = new RepoContextRetrievalReadinessState(warmedClock, HoldDown);
        using var control = new RepoContextRetrievalReadinessState(controlClock, HoldDown);

        var ready = await WarmEmptyStoreAsync(warmed);

        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.True, "A fresh box must not be wedged before its first repository is onboarded.");
            Assert.That(warmed.IsReady, Is.True);
        });

        // A repository has since been onboarded and a real query reports the plane
        // cannot serve it. The clock does not move, so any readiness surviving this is
        // hold-down grace and nothing else.
        warmed.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        control.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);

        Assert.Multiple(() =>
        {
            Assert.That(
                warmed.IsReady,
                Is.EqualTo(control.IsReady),
                "Having started empty must not buy a fault a grace window a never-served plane does not get.");
            Assert.That(
                warmed.IsReady,
                Is.False,
                "A plane that never served must lose readiness on the first observed fault.");
        });
    }

    private static RepoContextStore Store(IGrainFactory grainFactory)
        => new(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            new RepoContextVectorWriter(
                grainFactory,
                Serializer,
                Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory)),
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

    private static IEmbeddingProvider AvailableEmbedder()
    {
        var space = new EmbeddingSpace("test-model", 3, true);
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Success(space, new[] { new ReadOnlyMemory<float>([1f, 0f, 0f]) }));
        return provider;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> NoEntries()
    {
        await Task.CompletedTask;
        yield break;
    }
}
