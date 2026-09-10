using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Behavioural tests for the retrieval-latency instruments (issue #2624). Each case
/// drives a real retrieval through <see cref="RepoContextSearchService"/>,
/// <see cref="RepoContextBundleService"/>, or <see cref="RepoContextGraphService"/>
/// and asserts on measurements observed through a live
/// <see cref="MeterListener"/> - never on the reporter's internal state - so a test
/// passing means an operator scraping the meter would genuinely see the series.
/// <para>
/// The fixture is <see cref="NonParallelizableAttribute"/> because a
/// <see cref="MeterListener"/> is process-wide: it observes every instrument on the
/// named meter, including ones published by services another fixture built. Scoping
/// by meter name, instrument name, <b>and</b> tag keeps the assertions honest, and
/// serialising the fixture keeps a concurrent fixture's recordings out of the counts.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextRetrievalLatencyTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpace Space = new("test-model", 3, normalized: true);

    private const string RepoId = "acme";
    private const string FilePath = "src/Widget.cs";

    [Test]
    public async Task A_semantic_search_records_one_end_to_end_measurement_tagged_with_the_path_that_answered()
    {
        var key = RepoContextKeys.File(RepoId, FilePath);
        var latency = new RepoContextRetrievalLatencyReporter();
        var service = CreateSearch(
            latency,
            IndexReturning(RepoContextRetrievalPath.SemanticApproximate, key),
            AvailableEmbedder(),
            hydratedKey: key);

        using var measurements = new LatencyMeasurements();
        var result = await service.SearchAsync(RepoId, "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "Guards the premise: the call really did resolve to the approximate path.");
            Assert.That(
                measurements.CallCount(RepoContextRetrievalTool.Search, RepoContextRetrievalPath.SemanticApproximate),
                Is.EqualTo(1),
                "One search call is one end-to-end measurement, tagged with the path that served it. "
                + "Without the path tag a fast keyword answer and a fast approximate answer are the same number, "
                + "and they mean opposite things about the health of the box.");
            Assert.That(
                measurements.TotalCallCount(), Is.EqualTo(1),
                "and it is recorded exactly once, not once per stage.");
        });
    }

    [Test]
    public async Task A_semantic_search_separates_the_embed_hop_from_the_vector_search_from_the_hydration()
    {
        var key = RepoContextKeys.File(RepoId, FilePath);
        var latency = new RepoContextRetrievalLatencyReporter();
        var service = CreateSearch(
            latency,
            IndexReturning(RepoContextRetrievalPath.SemanticExact, key),
            AvailableEmbedder(),
            hydratedKey: key);

        using var measurements = new LatencyMeasurements();
        await service.SearchAsync(RepoId, "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The three stages have entirely different failure modes and different
            // owners - a separate embedding service, the in-process vector plane, and
            // the store of record. A single end-to-end number cannot say which of them
            // is slow, which is precisely the defect being fixed.
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.Embed), Is.EqualTo(1),
                "the network hop to the embedder is measured separately");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.VectorSearch), Is.EqualTo(1),
                "the vector search is measured separately");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.Hydrate), Is.EqualTo(1),
                "hydrating the hits from the store of record is measured separately");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.KeywordScan), Is.Zero,
                "and the keyword scan never ran, so it records nothing");
            Assert.That(
                measurements.StagePaths(), Is.EquivalentTo(new[] { RepoContextRetrievalPath.SemanticExact }),
                "every stage carries the path the enclosing call resolved to, so a stage cost "
                + "can be attributed to the retrieval mode that incurred it");
        });
    }

    [Test]
    public async Task A_keyword_only_host_records_no_embed_stage_but_a_rising_call_total_makes_that_absence_measured()
    {
        var latency = new RepoContextRetrievalLatencyReporter();
        var service = CreateSearch(
            latency, IndexReturning(RepoContextRetrievalPath.SemanticExact), embeddingProvider: null);

        using var measurements = new LatencyMeasurements();
        var result = await service.SearchAsync(RepoId, "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordNoEmbedder),
                "Guards the premise: this host has no embedder bound.");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.Embed), Is.Zero,
                "no embed hop happened, so none is recorded - the stage series is sparse by construction");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.KeywordScan), Is.EqualTo(1),
                "the scan that did happen is recorded");
            Assert.That(
                measurements.CallCount(RepoContextRetrievalTool.Search, RepoContextRetrievalPath.KeywordNoEmbedder),
                Is.EqualTo(1),
                "This is the whole point of the pair. The end-to-end count is the denominator that turns "
                + "a missing embed stage into a MEASURED absence: a zero beside a rising call total is an "
                + "intended keyword-only host, whereas both at zero means no retrieval ran at all. A stage "
                + "instrument on its own cannot tell those apart.");
        });
    }

    [Test]
    public void A_call_that_is_cancelled_before_a_path_is_settled_is_still_recorded_as_unresolved()
    {
        var latency = new RepoContextRetrievalLatencyReporter();
        var embedder = Substitute.For<IEmbeddingProvider>();
        embedder.Space.Returns(Space);
        embedder.IsAvailableAsync(Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new OperationCanceledException());
        var service = CreateSearch(latency, IndexReturning(RepoContextRetrievalPath.SemanticExact), embedder);

        using var measurements = new LatencyMeasurements();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await service.SearchAsync(RepoId, "widget", 10, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements.CallCount(
                    RepoContextRetrievalTool.Search, RepoContextRetrievalLatencyReporter.PathUnresolved),
                Is.EqualTo(1),
                "A call that aborts is recorded rather than dropped, under an explicit 'unresolved' path. "
                + "Dropping it would let a host that is failing every query look identical to an idle one, "
                + "which is the silent-absence shape this instrument exists to rule out.");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.Embed), Is.EqualTo(1),
                "and the embed hop that threw is still timed, because a stage that ran and then failed "
                + "is the case that most needs to be visible");
        });
    }

    [Test]
    public async Task A_context_call_is_counted_once_as_context_and_never_also_as_a_search()
    {
        var key = RepoContextKeys.File(RepoId, FilePath);
        var latency = new RepoContextRetrievalLatencyReporter();
        var bundle = CreateBundle(
            latency,
            IndexReturning(RepoContextRetrievalPath.SemanticExact, key),
            AvailableEmbedder(),
            hydratedKey: key);

        using var measurements = new LatencyMeasurements();
        await bundle.BuildAsync(
            RepoId, "widget", 3, 4000, RepoContextContextDetail.Paths, null, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements.CallCount(RepoContextRetrievalTool.Context, RepoContextRetrievalPath.SemanticExact),
                Is.EqualTo(1),
                "the bundle records its own end-to-end cost, which includes the packing and token counting "
                + "it adds on top of the inner search");
            Assert.That(
                measurements.ToolCallCount(RepoContextRetrievalTool.Search), Is.Zero,
                "and the inner search does NOT also record an end-to-end row. Otherwise a context call would "
                + "inflate the search tool's series and the two tools' latencies would be mutually contaminated, "
                + "leaving neither answerable.");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.Embed), Is.EqualTo(1),
                "the per-stage timings are still published, because a stage costs the same whichever tool asked");
        });
    }

    [Test]
    public async Task A_graph_read_is_timed_on_the_same_instrument_under_an_explicit_not_applicable_path()
    {
        var latency = new RepoContextRetrievalLatencyReporter();
        var graph = CreateGraph(latency);

        using var measurements = new LatencyMeasurements();
        await graph.OutlineAsync(RepoId, FilePath, CancellationToken.None);
        await graph.RelatedAsync(RepoId, FilePath, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Keeping the graph reads on the same instrument is what makes "outline is
            // two orders of magnitude cheaper than search" a comparison an operator can
            // actually run, rather than a claim in a document.
            Assert.That(
                measurements.CallCount(
                    RepoContextRetrievalTool.Outline, RepoContextRetrievalLatencyReporter.PathNotApplicable),
                Is.EqualTo(1));
            Assert.That(
                measurements.CallCount(
                    RepoContextRetrievalTool.Related, RepoContextRetrievalLatencyReporter.PathNotApplicable),
                Is.EqualTo(1),
                "A graph read consults no vector plane, so it carries an explicit 'not_applicable' path "
                + "rather than an absent tag - an untagged series would silently merge into whichever "
                + "path filter an operator happened to omit.");
            Assert.That(
                measurements.StageCount(RepoContextRetrievalStage.VectorSearch), Is.Zero,
                "and no retrieval stage is claimed for a read that ran none");
        });
    }

    [Test]
    public void The_instruments_are_described_and_carry_the_platform_tenant_sentinel()
    {
        using var latency = new RepoContextRetrievalLatencyReporter();
        using var published = new PublishedInstruments();
        using var probe = new RepoContextRetrievalLatencyReporter();
        probe.RecordCall(RepoContextRetrievalTool.Search, RepoContextRetrievalPath.SemanticExact, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(
                published.Description(RepoContextRetrievalLatencyReporter.DurationInstrumentName),
                Does.Contain("cancel").IgnoreCase,
                "The description must state what the instrument can and cannot record. This one is recorded "
                + "on every termination, so it says so rather than leaving a reader to assume it.");
            Assert.That(
                published.Description(RepoContextRetrievalLatencyReporter.StageDurationInstrumentName),
                Does.Contain("only").IgnoreCase.Or.Contain("if and only if").IgnoreCase,
                "and this one is conditional, so its description says so rather than promising more than "
                + "the code delivers.");
            Assert.That(
                published.PlatformTagged(RepoContextRetrievalLatencyReporter.DurationInstrumentName), Is.True,
                "latency is a property of the host process, so it carries the reserved platform tenant value");
        });
    }

    private static RepoContextSearchService CreateSearch(
        RepoContextRetrievalLatencyReporter latency,
        IRepoContextSemanticIndex index,
        IEmbeddingProvider? embeddingProvider,
        string? hydratedKey = null)
    {
        var grainFactory = GrainFactoryFor(hydratedKey);
        return new RepoContextSearchService(
            grainFactory,
            Serializer,
            index,
            StoreFor(grainFactory),
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            latency,
            embeddingProvider);
    }

    private static RepoContextBundleService CreateBundle(
        RepoContextRetrievalLatencyReporter latency,
        IRepoContextSemanticIndex index,
        IEmbeddingProvider? embeddingProvider,
        string? hydratedKey = null)
    {
        var grainFactory = GrainFactoryFor(hydratedKey);
        var search = new RepoContextSearchService(
            grainFactory,
            Serializer,
            index,
            StoreFor(grainFactory),
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            latency,
            embeddingProvider);
        return new RepoContextBundleService(
            search,
            new RepoContextGraphService(
                grainFactory, Serializer, TokenCounter(), new RepoContextWorkspaceGuard([]), latency),
            new RepoContextSessionStore(grainFactory, Serializer),
            grainFactory,
            Serializer,
            TokenCounter(),
            NoOpUsageRecorder.Instance,
            latency);
    }

    private static RepoContextGraphService CreateGraph(RepoContextRetrievalLatencyReporter latency)
        => new(
            GrainFactoryFor(hydratedKey: null),
            Serializer,
            TokenCounter(),
            new RepoContextWorkspaceGuard([]),
            latency);

    private static IRepoContextTokenCounter TokenCounter()
    {
        var counter = Substitute.For<IRepoContextTokenCounter>();
        counter.CountTokens(Arg.Any<string>()).ReturnsForAnyArgs(7);
        return counter;
    }

    private static IGrainFactory GrainFactoryFor(string? hydratedKey)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        // NSubstitute auto-substitutes an array return as an EMPTY array, not null, so
        // an unconfigured GetAsync would hand the graph service zero bytes to
        // deserialize rather than an absent record. Say "absent" explicitly.
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(Task.FromResult<byte[]?>(null));
        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var key = call.ArgAt<string>(0);
                return Task.FromResult(
                    hydratedKey is not null && string.Equals(key, hydratedKey, StringComparison.Ordinal)
                        ? new VersionedValue
                        {
                            Value = Serializer.SerializeToArray(
                                new FileNode { RepoId = RepoId, Path = FilePath }),
                        }
                        : new VersionedValue());
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);
        return grainFactory;
    }

    private static RepoContextStore StoreFor(IGrainFactory grainFactory)
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
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Success(Space, new[] { new ReadOnlyMemory<float>([1f, 0f, 0f]) }));
        return provider;
    }

    private static IRepoContextSemanticIndex IndexReturning(string retrievalPath, params string[] sourceKeys)
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(retrievalPath);

        var matches = new List<RepoContextVectorMatch>(sourceKeys.Length);
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            matches.Add(new RepoContextVectorMatch($"vec-{i}", sourceKeys[i], 1d - (i * 0.1)));
        }

        index.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(matches));
        return index;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }

    /// <summary>
    /// Observes the two retrieval-latency instruments through a real
    /// <see cref="MeterListener"/> and aggregates each measurement by its tags, so the
    /// assertions read what a scraper would read rather than what the reporter thinks
    /// it emitted. Scoped by meter name, instrument name, and tag - a listener is
    /// process-wide and replays every live instrument, so a narrower scope than that
    /// is not sound.
    /// </summary>
    private sealed class LatencyMeasurements : IDisposable
    {
        private readonly Dictionary<(string Tool, string Path), int> _calls = new();
        private readonly Dictionary<(string Stage, string Path), int> _stages = new();
        private readonly MeterListener _listener = new();

        public LatencyMeasurements()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                    && (instrument.Name == RepoContextRetrievalLatencyReporter.DurationInstrumentName
                        || instrument.Name == RepoContextRetrievalLatencyReporter.StageDurationInstrumentName))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) =>
            {
                string? first = null;
                string? path = null;
                var firstKey = instrument.Name == RepoContextRetrievalLatencyReporter.DurationInstrumentName
                    ? RepoContextRetrievalLatencyReporter.ToolTagKey
                    : RepoContextRetrievalLatencyReporter.StageTagKey;

                foreach (var tag in tags)
                {
                    if (tag.Key == firstKey && tag.Value is string f)
                    {
                        first = f;
                    }
                    else if (tag.Key == RepoContextRetrievalLatencyReporter.PathTagKey && tag.Value is string p)
                    {
                        path = p;
                    }
                }

                if (first is null || path is null)
                {
                    return;
                }

                var bucket = instrument.Name == RepoContextRetrievalLatencyReporter.DurationInstrumentName
                    ? _calls
                    : _stages;
                lock (bucket)
                {
                    bucket[(first, path)] = bucket.GetValueOrDefault((first, path)) + 1;
                }
            });
            _listener.Start();
        }

        public int CallCount(string tool, string path)
        {
            lock (_calls)
            {
                return _calls.GetValueOrDefault((tool, path));
            }
        }

        public int ToolCallCount(string tool)
        {
            lock (_calls)
            {
                return _calls.Where(kv => kv.Key.Tool == tool).Sum(kv => kv.Value);
            }
        }

        public int TotalCallCount()
        {
            lock (_calls)
            {
                return _calls.Values.Sum();
            }
        }

        public int StageCount(string stage)
        {
            lock (_stages)
            {
                return _stages.Where(kv => kv.Key.Stage == stage).Sum(kv => kv.Value);
            }
        }

        public IReadOnlyCollection<string> StagePaths()
        {
            lock (_stages)
            {
                return _stages.Keys.Select(k => k.Path).Distinct(StringComparer.Ordinal).ToArray();
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Reads the published metadata (description and tag set) of the retrieval-latency
    /// instruments, so the assertions can check that what the instrument <i>says</i>
    /// matches what the code actually does.
    /// </summary>
    private sealed class PublishedInstruments : IDisposable
    {
        private readonly Dictionary<string, string?> _descriptions = new(StringComparer.Ordinal);
        private readonly HashSet<string> _platformTagged = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public PublishedInstruments()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name != RepoContextUsageRecorder.MeterName)
                {
                    return;
                }

                lock (_descriptions)
                {
                    _descriptions[instrument.Name] = instrument.Description;
                }

                l.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeTenantLabel.TagTenant
                        && tag.Value is string value
                        && value == LatticeTenantLabel.PlatformTenant)
                    {
                        lock (_platformTagged)
                        {
                            _platformTagged.Add(instrument.Name);
                        }
                    }
                }
            });
            _listener.Start();
        }

        public string? Description(string instrumentName)
        {
            lock (_descriptions)
            {
                return _descriptions.GetValueOrDefault(instrumentName);
            }
        }

        public bool PlatformTagged(string instrumentName)
        {
            lock (_platformTagged)
            {
                return _platformTagged.Contains(instrumentName);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
