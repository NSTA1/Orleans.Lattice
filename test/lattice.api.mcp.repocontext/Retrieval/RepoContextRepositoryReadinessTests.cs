using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Protocol;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

[TestFixture]
public sealed class RepoContextRepositoryReadinessTests
{
    private static readonly EmbeddingSpace Space = new("readiness-test", 3, true);

    [Test]
    public void Health_tool_exposes_optional_repo_scope()
    {
        var tool = new RepoContextToolGroup().Tools.Single(t => t.ProtocolTool.Name == "repocontext_health").ProtocolTool;
        Assert.That(tool.InputSchema.GetProperty("properties").TryGetProperty("repoId", out _), Is.True);
        Assert.That(tool.InputSchema.TryGetProperty("required", out var required)
            && required.EnumerateArray().Any(v => v.GetString() == "repoId"), Is.False);
        Assert.That(tool.Annotations!.ReadOnlyHint, Is.True);
    }

    [Test]
    public async Task Health_registered_empty_repository_is_not_reported_serving()
    {
        using var fixture = new Fixture();
        fixture.SetPlane("empty", true, 0, vectors: 0);
        fixture.Runner.GetProgressAsync("empty").Returns(new RepoIndexProgress
        {
            RepoId = "empty", Status = RepoIndexStatus.Completed, Phase = RepoIndexPhase.Done,
        });
        var result = await fixture.HealthAsync("empty");
        Assert.Multiple(() =>
        {
            Assert.That(result.Repository!.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.NothingRegistered));
            Assert.That(result.RetrievalReady, Is.False);
            Assert.That(result.Repository.Ann!.Value.VectorsIndexed, Is.Zero);
        });
        fixture.AssertNoQuery();
    }

    [Test]
    public async Task Health_nonempty_ingest_with_empty_ann_is_not_reported_serving()
    {
        using var fixture = new Fixture();
        fixture.SetPlane("repo", true, 0, vectors: 0);
        var result = await fixture.HealthAsync("repo");
        Assert.That(result.RetrievalReady, Is.False);
        Assert.That(result.Repository!.Reason, Is.EqualTo("ann_contains_no_vectors_in_configured_space"));
    }

    [Test]
    public async Task Content_fault_is_observed_without_a_health_time_scan()
    {
        using var fixture = new Fixture();
        fixture.Embedder.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(false);
        fixture.Content.EntriesAsync().ReturnsForAnyArgs(_ => throw new IOException("test"));
        await fixture.Search.SearchAsync("repo", "fault", 1, CancellationToken.None);
        var before = fixture.Content.ReceivedCalls().Count();
        var result = await fixture.HealthAsync("repo");
        Assert.Multiple(() =>
        {
            Assert.That(result.Repository!.ContentPhase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(result.Repository.ContentReason, Is.EqualTo("content_scan_fault:IOException"));
            Assert.That(fixture.Content.ReceivedCalls().Count(), Is.EqualTo(before));
        });
    }

    [Test]
    public async Task Health_repo_blocked_while_host_serving_and_ingest_completed()
    {
        using var fixture = new Fixture();
        fixture.Host.MarkServing();
        fixture.Host.ObserveArming(RepoContextRetrievalArming.Armed);
        fixture.Breaker.Trip("blocked");
        fixture.Latency.RecordCall(RepoContextRetrievalTool.Search, RepoContextRetrievalPath.SemanticApproximate, TimeSpan.FromMilliseconds(1));

        var context = await RepoContextRequestContexts.CreateAsync(fixture.Services, new CallToolRequestParams
        {
            Name = "repocontext_health",
            Arguments = new Dictionary<string, JsonElement> { ["repoId"] = JsonSerializer.SerializeToElement("blocked") },
        });
        var tool = new RepoContextToolGroup().Tools.Single(t => t.ProtocolTool.Name == "repocontext_health");
        var wire = await tool.InvokeAsync(context, CancellationToken.None);
        using var payload = JsonDocument.Parse(wire.Content.OfType<TextContentBlock>().Single().Text);
        Assert.That(payload.RootElement.GetProperty("retrievalReady").GetBoolean(), Is.False);
        Assert.That(payload.RootElement.GetProperty("repository").GetProperty("reason").GetString(), Is.EqualTo("exact_fallback_suppressed"));
        var result = await fixture.HealthAsync("blocked");
        Assert.Multiple(() =>
        {
            Assert.That(fixture.Host.IsReady, Is.True);
            Assert.That(fixture.Host.Arming, Is.EqualTo(RepoContextRetrievalArming.Armed));
            Assert.That(result.Repository!.Ingest!.Status, Is.EqualTo(RepoIndexStatus.Completed));
            Assert.That(result.RetrievalReady, Is.False);
            Assert.That(result.Repository.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(result.Repository.Reason, Is.EqualTo("exact_fallback_suppressed"));
            Assert.That(result.Repository.BreakerOpen, Is.True);
            Assert.That(result.Repository.Authoritative, Is.False);
        });
        fixture.AssertNoQuery();
    }

    [Test]
    public async Task Health_two_repositories_diverge_and_unarmed_is_serving()
    {
        using var fixture = new Fixture();
        fixture.SetPlane("healthy", true, partitions: 0);
        fixture.Breaker.Trip("blocked");

        var healthy = await fixture.HealthAsync("healthy");
        var blocked = await fixture.HealthAsync("blocked");
        Assert.Multiple(() =>
        {
            Assert.That(healthy.RetrievalReady, Is.True);
            Assert.That(healthy.Repository!.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
            Assert.That(healthy.Repository.Ann!.Value.PartitionsTotal, Is.Zero);
            Assert.That(healthy.Repository.Ann.Value.Generation, Is.EqualTo(7));
            Assert.That(healthy.Repository.AnnCanServe, Is.True);
            Assert.That(blocked.RetrievalReady, Is.False);
            Assert.That(blocked.Repository!.Reason, Is.EqualTo("exact_fallback_suppressed"));
        });
        fixture.AssertNoQuery();
    }

    [Test]
    public async Task Health_exact_budget_refusal_is_the_actual_query_gate()
    {
        using var fixture = new Fixture();
        fixture.Plane.KnownVectorCount("large").Returns(int.MaxValue);
        var result = await fixture.HealthAsync("large");
        Assert.That(result.RetrievalReady, Is.False);
        Assert.That(result.Repository!.Reason, Is.EqualTo("exact_scan_budget_exceeded"));
        fixture.AssertNoQuery();
        var query = await fixture.Search.SearchAsync("large", "query", 1, CancellationToken.None);
        Assert.That(query.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
        Assert.That(fixture.Exact.ReceivedCalls().Any(c => c.GetMethodInfo().Name == "SearchAsync"), Is.False);
    }

    [Test]
    public async Task Health_ingest_failure_is_unknown_not_empty()
    {
        using var fixture = new Fixture();
        fixture.Runner.GetProgressAsync("repo").Returns(Task.FromException<RepoIndexProgress>(new IOException("private diagnostic")));
        var result = await fixture.HealthAsync("repo");
        Assert.Multiple(() =>
        {
            Assert.That(result.Repository!.Ingest, Is.Null);
            Assert.That(result.Repository.IngestReason, Is.EqualTo("ingest_metadata_unavailable:IOException"));
            Assert.That(result.Repository.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(result.Repository.EmbeddingSpace, Is.EqualTo(Space));
        });
    }

    [Test]
    public async Task Health_passive_reads_do_not_consume_a_due_breaker_probe()
    {
        using var fixture = new Fixture();
        fixture.Breaker.Trip("repo");
        fixture.Clock.Advance(TimeSpan.FromMinutes(2));
        var result = await fixture.HealthAsync("repo");
        await fixture.HealthAsync("repo");
        Assert.That(result.Repository!.BreakerProbeDueIn, Is.EqualTo(TimeSpan.Zero));
        Assert.That(fixture.Breaker.Evaluate("repo"), Is.EqualTo(RepoContextExactScanBreakerDecision.Probe));
        fixture.AssertNoQuery();
    }

    [Test]
    public async Task Health_no_arg_payload_is_byte_compatible()
    {
        using var fixture = new Fixture();
        fixture.Host.MarkServing();
        var context = await RepoContextRequestContexts.CreateAsync(fixture.Services);
        var legacy = RepoContextToolHandlers.Health(context);
        var actual = await RepoContextToolHandlers.HealthAsync(context);
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var json = JsonSerializer.Serialize(actual, options);
        Assert.Multiple(() =>
        {
            Assert.That(json, Is.EqualTo(JsonSerializer.Serialize(legacy, options)));
            Assert.That(json, Is.EqualTo("{\"available\":true,\"group\":\"repocontext\",\"status\":\"The Orleans.Lattice repository-context MCP surface is registered and reachable, and semantic retrieval is serving.\",\"retrievalReady\":true,\"retrievalPhase\":\"serving\"}"));
        });
        fixture.AssertNoQuery();
    }

    [Test]
    public async Task Health_unknown_is_not_empty_and_saturation_names_its_blocker()
    {
        using var fixture = new Fixture();
        var unknown = await fixture.HealthAsync("new");
        fixture.Plane.DescribeReadiness("saturated", Arg.Any<EmbeddingSpaceTag>())
            .Returns(new RepoContextSemanticReadiness(false, "ann_open_saturated", Saturated: true, AnnCanServe: false));
        var saturated = await fixture.HealthAsync("saturated");
        Assert.Multiple(() =>
        {
            Assert.That(unknown.Repository!.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(unknown.Repository.Reason, Is.EqualTo("semantic_serving_not_yet_demonstrated"));
            Assert.That(unknown.Repository.VectorCoverage.Count, Is.Null);
            Assert.That(unknown.Repository.VectorCoverage.Pending, Is.True);
            Assert.That(unknown.Repository.Ann, Is.Null);
            Assert.That(unknown.Repository.ContentPhase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(unknown.Repository.ContentReason, Is.EqualTo("content_tree_not_observed"));
            Assert.That(saturated.Repository!.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.SaturatedUnavailable));
            Assert.That(saturated.Repository.Reason, Is.EqualTo("ann_open_saturated"));
            Assert.That(saturated.RetrievalReady, Is.False);
        });
    }

    [Test]
    public void Readiness_empty_and_keyword_only_cover_the_non_semantic_verdicts()
    {
        using var fixture = new Fixture();
        var empty = fixture.Search.DescribeReadiness("empty",
            new RepoIndexProgress { RepoId = "empty", Status = RepoIndexStatus.Completed, Phase = RepoIndexPhase.Done },
            null, RepoContextEmbeddedCount.Exact(0));
        var unknown = fixture.Search.DescribeReadiness("unknown", null, "ingest_metadata_unavailable",
            RepoContextEmbeddedCount.PendingRefresh(null));
        var keyword = fixture.CreateSearch(null).DescribeReadiness("repo", null, null, RepoContextEmbeddedCount.PendingRefresh(null));
        Assert.Multiple(() =>
        {
            Assert.That(empty.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.NothingRegistered));
            Assert.That(empty.Reason, Is.EqualTo("repository_has_no_indexed_files_or_vectors"));
            Assert.That(unknown.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(unknown.IngestReason, Is.EqualTo("ingest_metadata_unavailable"));
            Assert.That(keyword.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly));
            Assert.That(keyword.Reason, Is.EqualTo("no_embedding_provider"));
            Assert.That(Enum.GetValues<RepoContextRetrievalReadinessPhase>(), Has.Length.EqualTo(5),
                "Enroll any new verdict in this fixture rather than silently leaving it uncovered.");
        });
    }

    [Test]
    public async Task Search_observations_are_repository_scoped_and_preserve_fault_hold_down()
    {
        using var fixture = new Fixture();
        var initial = await fixture.Search.SearchAsync("repo", "ready", 1, CancellationToken.None);
        Assert.That(initial.Mode, Is.EqualTo("semantic"), fixture.Snapshot("repo").Reason);
        Assert.That(fixture.Snapshot("repo").Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        fixture.Embedder.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(false);
        await fixture.Search.SearchAsync("repo", "fault", 1, CancellationToken.None);
        Assert.That(fixture.Snapshot("repo").Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        fixture.Clock.Advance(TimeSpan.FromSeconds(30));
        var failed = fixture.Snapshot("repo");
        Assert.Multiple(() =>
        {
            Assert.That(failed.Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(failed.Reason, Is.EqualTo("embedding_provider_unavailable"));
            Assert.That(failed.LastRetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
            Assert.That(failed.LastQueryAt, Is.Not.Null);
            Assert.That(failed.ContentPhase, Is.EqualTo(RepoContextRetrievalReadinessPhase.NothingRegistered));
            Assert.That(failed.ContentReason, Is.EqualTo("content_scan_empty"));
            Assert.That(failed.ContentObservedAt, Is.Not.Null);
            Assert.That(fixture.Snapshot("other").LastRetrievalPath, Is.Null);
        });
        fixture.Embedder.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        await fixture.Search.SearchAsync("repo", "recovered", 1, CancellationToken.None);
        Assert.That(fixture.Snapshot("repo").Verdict, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
    }

    private sealed class Fixture : IDisposable
    {
        public SettableTimeProvider Clock { get; } = new();
        public IEmbeddingProvider Embedder { get; } = Substitute.For<IEmbeddingProvider>();
        public IRepoContextAnnIndex Plane { get; } = Substitute.For<IRepoContextAnnIndex>();
        public IRepoContextSemanticIndex Exact { get; } = Substitute.For<IRepoContextSemanticIndex>();
        public ILattice Content { get; } = Substitute.For<ILattice>();
        public IRepoIndexRunner Runner { get; } = Substitute.For<IRepoIndexRunner>();
        public RepoContextExactScanBreaker Breaker { get; }
        public RepoContextRetrievalReadinessState Host { get; }
        public RepoContextRetrievalLatencyReporter Latency { get; } = new();
        public ServiceProvider Services { get; }
        public RepoContextSearchService Search { get; }
        private readonly Serializer _serializer;
        private readonly ServiceProvider _serializerServices;
        private readonly IGrainFactory _grains = Substitute.For<IGrainFactory>();
        private readonly RepoContextStore _store;
        private readonly AnnRepoContextSemanticIndex _index;
        private readonly RepoContextRetrievalGuardReporter _guards;

        public Fixture()
        {
            var services = new ServiceCollection().AddSerializer().AddLogging();
            _serializerServices = services.BuildServiceProvider();
            _serializer = _serializerServices.GetRequiredService<Serializer>();
            var tree = Substitute.For<ILattice>();
            tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
            tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnsForAnyArgs(_ =>
                Task.FromResult(new VersionedValue
                {
                    Value = _serializer.SerializeToArray(new FileNode { RepoId = "repo", Path = "item.cs" }),
                }));
            _grains.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);
            var writer = new RepoContextVectorWriter(_grains, _serializer, Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(Clock, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(_grains));
            Content.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
            _grains.GetGrain<ILattice>(RepoContextTrees.Content).Returns(Content);
            var runner = Runner;
            runner.GetProgressAsync(Arg.Any<string>()).Returns(call => Task.FromResult(new RepoIndexProgress
            {
                RepoId = call.Arg<string>(), Status = RepoIndexStatus.Completed, Phase = RepoIndexPhase.Done, FilesScanned = 100,
            }));
            _store = new RepoContextStore(_grains, runner, _serializer, writer,
                Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(), Clock);
            var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
            Breaker = new RepoContextExactScanBreaker(Clock);
            Host = new RepoContextRetrievalReadinessState(Clock);
            _guards = new RepoContextRetrievalGuardReporter(Clock);
            _index = new AnnRepoContextSemanticIndex(Plane, Exact, new RepoContextExactScanBudget(options),
                Breaker, _guards, NullLogger<AnnRepoContextSemanticIndex>.Instance, Host);
            Embedder.Space.Returns(Space);
            Embedder.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
            Embedder.EmbedAsync(Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
                .Returns(EmbeddingResult.Success(Space, new[] { new ReadOnlyMemory<float>([1, 0, 0]) }));
            Plane.SearchAsync(Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping));
            Exact.SearchAsync(Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                    [new("vector", RepoContextKeys.File("repo", "item.cs"), 1)]));
            Search = CreateSearch(Embedder);
            services.AddSingleton(Host).AddSingleton(Search).AddSingleton(writer).AddSingleton(runner);
            Services = services.BuildServiceProvider();
        }

        public RepoContextSearchService CreateSearch(IEmbeddingProvider? provider) =>
            new(_grains, _serializer, _index, _store, Clock, NullLogger<RepoContextSearchService>.Instance, Latency, provider, Host);

        public RepoContextRepositoryReadiness Snapshot(string repo) =>
            Search.DescribeReadiness(repo, null, null, RepoContextEmbeddedCount.PendingRefresh(null));

        public void SetPlane(string repo, bool serving, int partitions, int vectors = 10) =>
            Plane.DescribeReadiness(repo, Arg.Any<EmbeddingSpaceTag>()).Returns(new RepoContextSemanticReadiness(
                serving, Progress: new VectorIndexBuildProgress(VectorIndexBuildPhase.Ready, 7, vectors, vectors, partitions, partitions, false),
                AnnCanServe: serving));

        public async Task<RepoContextHealthResult> HealthAsync(string repo) =>
            await RepoContextToolHandlers.HealthAsync(await RepoContextRequestContexts.CreateAsync(Services), repo);

        public void AssertNoQuery()
        {
            Assert.That(Embedder.ReceivedCalls().Any(c => c.GetMethodInfo().Name is "EmbedAsync" or "IsAvailableAsync"), Is.False);
            Assert.That(Exact.ReceivedCalls().Any(c => c.GetMethodInfo().Name == "SearchAsync"), Is.False);
            Assert.That(Plane.ReceivedCalls().Any(c => c.GetMethodInfo().Name == "SearchAsync"), Is.False);
        }

        public void Dispose()
        {
            Services.Dispose();
            _serializerServices.Dispose();
            Host.Dispose();
            Latency.Dispose();
            _guards.Dispose();
        }

        private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
