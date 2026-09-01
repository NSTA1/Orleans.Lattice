using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// End-to-end retrieval-path attribution with the approximate plane bound as the
/// default semantic index. Every one of the five
/// <see cref="RepoContextRetrievalPath"/> values a response can carry is provoked
/// through the real code path, and so is the mid-build state, which is the case
/// this whole change most has to get right: while the index builds, the exact
/// scan answers with complete recall and the response reports the weaker
/// approximate claim rather than pretending to be a finished index or collapsing
/// to keyword.
/// </summary>
/// <remarks>
/// Every case also asserts the legacy <see cref="RepoContextSearchResult.Mode"/>
/// value, which is what keeps the attribution provably additive: a client reading
/// only <c>mode</c> sees exactly what it saw before.
/// </remarks>
[TestFixture]
public sealed class RepoContextSearchServiceAnnRetrievalPathTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpace Space = new("test-model", 8, normalized: true);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// Builds the search service over substituted trees. The supplied keys resolve
    /// to real stored records so a semantic match hydrates into a hit; every other
    /// key is absent, and the keyword corpus is empty, so a keyword outcome is the
    /// terminal empty result and the mode/path split stays sharp.
    /// </summary>
    private static RepoContextSearchService CreateService(
        IRepoContextSemanticIndex index,
        IEmbeddingProvider? embeddingProvider,
        params string[] hydratedKeys)
    {
        var hydrated = new HashSet<string>(hydratedKeys, StringComparer.Ordinal);
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var key = call.ArgAt<string>(0);
                return Task.FromResult(
                    hydrated.Contains(key)
                        ? new VersionedValue
                        {
                            Value = Serializer.SerializeToArray(
                                new FileNode { RepoId = RepoId, Path = "src/Widget.cs" }),
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
            embeddingProvider);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static IEmbeddingProvider AvailableEmbedder()
    {
        var query = new float[8];
        query[0] = 1f;

        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Success(Space, new[] { new ReadOnlyMemory<float>(query) }));
        return provider;
    }

    private static IRepoContextSemanticIndex ExactReturning(params string[] sourceKeys)
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);

        var matches = new List<RepoContextVectorMatch>(sourceKeys.Length);
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            matches.Add(new RepoContextVectorMatch($"exact-{i}", sourceKeys[i], 1d - (i * 0.1)));
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

    private static AnnRepoContextSemanticIndex Ann(
        IRepoContextAnnIndex plane, IRepoContextSemanticIndex exact)
        => new(plane, exact, NullLogger<AnnRepoContextSemanticIndex>.Instance);

    [Test]
    public async Task A_built_index_answers_and_the_response_reports_semantic_approximate()
    {
        var key = RepoContextKeys.File(RepoId, "src/Widget.cs");
        using var fixture = new AnnPlaneFixture();
        fixture.Seed("vec-000000", key, UnitVector());
        for (var i = 1; i < 32; i++)
        {
            fixture.Seed($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/Other{i}.cs"), Spread(i));
        }

        await fixture.BuildAsync(Ct);
        var service = CreateService(
            Ann(fixture.Registry, ExactReturning(key)), AvailableEmbedder(), key);

        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "The default path is approximate, and the response says so rather than leaving a caller to assume "
                + "the complete-recall guarantee the surface used to make.");
            Assert.That(result.Mode, Is.EqualTo("semantic"), "The legacy mode value is unchanged.");
            Assert.That(result.Hits, Is.Not.Empty);
        });
    }

    [Test]
    public async Task While_the_index_builds_the_exact_scan_answers_and_the_response_under_promises()
    {
        var key = RepoContextKeys.File(RepoId, "src/Widget.cs");

        // Nothing is built: the plane declines every query, which is precisely the
        // state of an existing deployment on its first start after the upgrade.
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(32);

        var exact = ExactReturning(key);
        var service = CreateService(Ann(fixture.Registry, exact), AvailableEmbedder(), key);

        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);
        var reported = fixture.Registry.TryGetProgress(
            AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("semantic"),
                "Retrieval keeps working while the index builds - it must not collapse to keyword.");
            Assert.That(result.Hits, Is.Not.Empty,
                "The exact scan answers, with complete recall, throughout the build.");
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "The declaration is per index and one index serves every repository, so it reports the weaker "
                + "claim. Under-promising an exact answer is safe; over-promising an approximate one is not.");
            Assert.That(result.RetrievalPath, Is.Not.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded),
                "A building index is warming up, never degraded.");
            Assert.That(reported, Is.True,
                "The build state is reported out of band, per repository and embedding space.");
            Assert.That(progress.IsReady, Is.False, "And it reports honestly that the build is not finished.");
        });
    }

    [Test]
    public async Task No_embedder_reports_keyword_no_embedder()
    {
        using var fixture = new AnnPlaneFixture();
        var service = CreateService(Ann(fixture.Registry, ExactReturning()), embeddingProvider: null);

        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordNoEmbedder));
            Assert.That(result.Mode, Is.EqualTo("empty"));
        });
    }

    [Test]
    public async Task An_unavailable_embedder_reports_keyword_vector_plane_unavailable()
    {
        using var fixture = new AnnPlaneFixture();
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(false);

        var service = CreateService(Ann(fixture.Registry, ExactReturning()), provider);
        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
            Assert.That(result.Mode, Is.EqualTo("empty"));
        });
    }

    [Test]
    public async Task An_empty_vector_plane_reports_keyword_vector_plane_unavailable()
    {
        // The plane is built over an empty corpus, so it serves and returns nothing:
        // the "vectors trimmed or re-deriving" signal, not a degraded index.
        using var fixture = new AnnPlaneFixture();
        await fixture.BuildAsync(Ct);

        var service = CreateService(Ann(fixture.Registry, ExactReturning()), AvailableEmbedder());
        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable));
            Assert.That(result.Mode, Is.EqualTo("empty"));
        });
    }

    [Test]
    public async Task A_plane_that_throws_reports_keyword_index_degraded()
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("the index tree could not be materialised"));

        var service = CreateService(Ann(plane, ExactReturning()), AvailableEmbedder());
        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded),
                "A fault on the semantic path is a real capability loss, distinct from an absent embedder.");
            Assert.That(result.Mode, Is.EqualTo("empty"));
        });
    }

    [Test]
    public async Task Ranked_matches_that_no_longer_hydrate_report_keyword_index_degraded()
    {
        var key = RepoContextKeys.File(RepoId, "src/Widget.cs");
        using var fixture = new AnnPlaneFixture();
        fixture.Seed("vec-000000", key, UnitVector());
        for (var i = 1; i < 32; i++)
        {
            fixture.Seed($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/Other{i}.cs"), Spread(i));
        }

        await fixture.BuildAsync(Ct);

        // The plane ranks candidates, but no key resolves in the store of record.
        var service = CreateService(Ann(fixture.Registry, ExactReturning(key)), AvailableEmbedder());
        var result = await service.SearchAsync(RepoId, "widget", 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded),
                "An index that ranks candidates none of which hydrate has drifted from its sources.");
            Assert.That(result.Mode, Is.EqualTo("empty"));
        });
    }

    private static float[] UnitVector()
    {
        var vector = new float[Space.Dimension];
        vector[0] = 1f;
        return vector;
    }

    private static float[] Spread(int ordinal)
    {
        var angle = 2d * Math.PI * ordinal / 32d;
        var vector = new float[Space.Dimension];
        vector[0] = (float)Math.Cos(angle);
        vector[1] = (float)Math.Sin(angle);
        return vector;
    }
}
