using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the retrieval-path attribution <see cref="RepoContextSearchService"/>
/// stamps on every result. Each of the five <see cref="RepoContextRetrievalPath"/> values
/// is provoked through its real code path - including both distinct keyword causes - and
/// every case also asserts the legacy <see cref="RepoContextSearchResult.Mode"/> is
/// unchanged, which is what makes the new attribution provably additive.
/// </summary>
[TestFixture]
public sealed class RepoContextSearchServiceRetrievalPathTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpace Space = new("test-model", 3, normalized: true);

    /// <summary>
    /// Builds a service over substituted trees. When <paramref name="hydratedKey"/> is
    /// supplied, that key resolves to a real stored record so a semantic match hydrates
    /// into a hit; every other key is absent. The keyword scan sees an empty corpus, so a
    /// keyword outcome is the terminal empty result and the mode/path split stays sharp.
    /// </summary>
    private static RepoContextSearchService CreateService(
        IRepoContextSemanticIndex index,
        IEmbeddingProvider? embeddingProvider,
        RepoContextRetrievalReadinessState? readiness = null,
        string? hydratedKey = null)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var key = call.ArgAt<string>(0);
                return Task.FromResult(
                    hydratedKey is not null && string.Equals(key, hydratedKey, StringComparison.Ordinal)
                        ? new VersionedValue
                        {
                            Value = Serializer.SerializeToArray(
                                new FileNode { RepoId = "acme", Path = "src/Widget.cs" }),
                        }
                        : new VersionedValue());
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = new RepoContextStore(
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

        return new RepoContextSearchService(
            grainFactory,
            Serializer,
            index,
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            embeddingProvider,
            readiness);
    }

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

    [Test]
    public async Task Search_reports_semantic_exact_when_the_index_declares_exact_search()
    {
        var key = RepoContextKeys.File("acme", "src/Widget.cs");
        var service = CreateService(
            IndexReturning(RepoContextRetrievalPath.SemanticExact, key),
            AvailableEmbedder(),
            hydratedKey: key);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticExact));
            Assert.That(result.Mode, Is.EqualTo("semantic"), "The legacy mode value is unchanged.");
            Assert.That(result.Hits, Is.Not.Empty);
        });
    }

    [Test]
    public async Task Search_reports_semantic_approximate_when_the_index_declares_approximate_search()
    {
        var key = RepoContextKeys.File("acme", "src/Widget.cs");
        var service = CreateService(
            IndexReturning(RepoContextRetrievalPath.SemanticApproximate, key),
            AvailableEmbedder(),
            hydratedKey: key);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate));
            Assert.That(result.Mode, Is.EqualTo("semantic"), "The legacy mode value is unchanged.");
        });
    }

    [Test]
    public async Task Search_fails_closed_to_approximate_when_the_index_declares_nothing_recognisable()
    {
        var key = RepoContextKeys.File("acme", "src/Widget.cs");
        var service = CreateService(
            IndexReturning("brand-new-engine", key),
            AvailableEmbedder(),
            hydratedKey: key);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "An unrecognised declaration must never be promoted to the complete-recall claim.");
            Assert.That(result.Mode, Is.EqualTo("semantic"));
        });
    }

    [Test]
    public async Task Search_reports_no_embedder_when_none_is_bound()
    {
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordNoEmbedder));
            Assert.That(result.Mode, Is.EqualTo("empty"), "The legacy mode value is unchanged.");
        });
    }

    [Test]
    public async Task Search_reports_vector_plane_unavailable_when_the_embedder_is_unreachable()
    {
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(false);
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), provider);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
            Assert.That(result.Mode, Is.EqualTo("empty"), "The legacy mode value is unchanged.");
        });
    }

    [Test]
    public async Task Search_reports_vector_plane_unavailable_when_the_query_embedding_fails()
    {
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Failure(Space, "model server unreachable"));
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), provider);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
    }

    [Test]
    public async Task Search_reports_vector_plane_unavailable_when_the_plane_holds_no_vectors()
    {
        // The plane is still building (cold start, replay, or a re-derivation back-fill):
        // the index answers, but it has nothing to compare in the query's space.
        var service = CreateService(
            IndexReturning(RepoContextRetrievalPath.SemanticExact),
            AvailableEmbedder());

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
            Assert.That(result.Mode, Is.EqualTo("empty"), "The legacy mode value is unchanged.");
        });
    }

    [Test]
    public async Task Search_reports_index_degraded_when_the_semantic_path_throws()
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);
        index.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsyncForAnyArgs(new InvalidOperationException("simulated stale leaf projection activation fault"));
        var service = CreateService(index, AvailableEmbedder());

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded));
            Assert.That(result.Mode, Is.EqualTo("empty"), "The legacy mode value is unchanged.");
        });
    }

    [Test]
    public async Task Search_reports_index_degraded_when_no_ranked_candidate_hydrates()
    {
        // The index ranks candidates but every source key has drifted out of the store of
        // record, so the index no longer reflects its sources.
        var service = CreateService(
            IndexReturning(RepoContextRetrievalPath.SemanticExact, RepoContextKeys.File("acme", "src/Gone.cs")),
            AvailableEmbedder());

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded));
    }

    [Test]
    public async Task Search_carries_the_keyword_cause_onto_a_keyword_mode_result()
    {
        // A keyword answer with real hits must still carry its cause: mode "keyword" alone
        // is exactly the ambiguity this vocabulary exists to remove.
        var fileKey = RepoContextKeys.File("acme", "src/Widget.cs");
        var fileBytes = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "src/Widget.cs" });

        var structural = Substitute.For<ILattice>();
        structural.EntriesAsync().ReturnsForAnyArgs(
            _ => Yield(new KeyValuePair<string, byte[]>(fileKey, fileBytes)));
        var other = Substitute.For<ILattice>();
        other.EntriesAsync().ReturnsForAnyArgs(_ => Empty());

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(other);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural).Returns(structural);

        var store = new RepoContextStore(
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

        var service = new RepoContextSearchService(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            embeddingProvider: null);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("keyword"), "The legacy mode value is unchanged.");
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordNoEmbedder));
            Assert.That(result.Hits, Is.Not.Empty);
        });
    }

    [Test]
    public async Task Search_folds_a_semantic_answer_into_readiness()
    {
        var key = RepoContextKeys.File("acme", "src/Widget.cs");
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var service = CreateService(
            IndexReturning(RepoContextRetrievalPath.SemanticExact, key),
            AvailableEmbedder(),
            readiness,
            hydratedKey: key);

        await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
    }

    [Test]
    public async Task Search_folds_an_absent_embedder_into_keyword_only_readiness()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var service = CreateService(
            Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null, readiness: readiness);

        await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly));
    }

    [Test]
    public async Task Search_folds_a_vector_plane_failure_into_readiness()
    {
        var clock = new SettableTimeProvider();
        using var readiness = new RepoContextRetrievalReadinessState(clock, TimeSpan.FromSeconds(30));
        readiness.MarkServing();

        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(false);
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), provider, readiness);

        await service.SearchAsync("acme", "widget", 10, CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(30));

        Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
    }

    [Test]
    public async Task Search_without_a_readiness_state_still_answers()
    {
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordNoEmbedder));
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Yield(
        params KeyValuePair<string, byte[]>[] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }
}
