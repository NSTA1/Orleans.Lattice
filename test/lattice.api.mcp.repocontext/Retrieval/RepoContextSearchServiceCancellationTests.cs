using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage of <see cref="RepoContextSearchService"/>'s cancellation contract and
/// its keyword-scan bounds.
/// <para>
/// The service degrades every fault to keyword recall and every keyword fault to
/// the terminal empty result, so its one hard rule is that cancellation is
/// <b>never</b> swallowed by either guard - on the semantic path, on the keyword
/// fallback, or inside the per-tree isolation that keeps one unenumerable tree
/// from sinking the whole scan.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextSearchServiceCancellationTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpace Space = new("test-model", 3, true);

    /// <summary>Mirrors the private <c>RepoContextSearchService.MaxKeywordScan</c> bound.</summary>
    private const int MaxKeywordScan = 5000;

    [Test]
    public void A_cancelled_semantic_path_propagates_rather_than_degrading_to_keyword()
    {
        // The semantic guard degrades every other fault to keyword recall. It must
        // not do that for cancellation, or a cancelled request would silently pay
        // for a whole keyword scan it was told to abandon.
        using var cts = new CancellationTokenSource();
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticApproximate);
        index.SearchAsync(
                Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<Task<IReadOnlyList<RepoContextVectorMatch>>>(_ =>
            {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });

        var service = CreateService(index, AvailableEmbedder(), Tree([]));

        Assert.That(
            async () => await service.SearchAsync(RepoId, "widget", 10, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void A_cancelled_keyword_scan_propagates_through_the_per_tree_isolation()
    {
        // The per-tree guard exists so one unenumerable tree degrades the corpus
        // instead of sinking the fallback - but a cancelled scan must still
        // surface, through both that guard and the outer keyword guard.
        using var cts = new CancellationTokenSource();
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ =>
        {
            cts.Cancel();
            throw new OperationCanceledException(cts.Token);
        });

        // No embedder, so the query goes straight to the keyword fallback.
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null, tree);

        Assert.That(
            async () => await service.SearchAsync(RepoId, "widget", 10, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task An_unenumerable_tree_degrades_the_corpus_instead_of_sinking_the_scan()
    {
        // The non-cancellation half of the same guard: a terminal per-tree fault
        // (a leaf projection awaiting an operator rebuild, say) keeps whatever the
        // healthy trees yielded rather than failing the search.
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ThrowsForAnyArgs(
            new InvalidOperationException("simulated terminal projection fault"));

        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null, tree);

        var result = await service.SearchAsync(RepoId, "widget", 10, CancellationToken.None);

        Assert.That(result.Mode, Is.EqualTo("empty"));
        Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordNoEmbedder));
    }

    [Test]
    public async Task A_query_with_no_searchable_tokens_returns_empty_without_scanning()
    {
        // Tokenising to nothing means there is no term to rank on, so the scan is
        // skipped entirely rather than materialising the whole corpus to score it
        // against an empty token set.
        var tree = Tree([]);
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null, tree);

        var result = await service.SearchAsync(RepoId, "   ", 10, CancellationToken.None);

        Assert.That(result.Hits, Is.Empty);
        Assert.That(result.Mode, Is.EqualTo("empty"));
        Assert.That(tree.ReceivedCalls(), Is.Empty, "no tree is enumerated when the query has no tokens");
    }

    [Test]
    public async Task The_keyword_scan_stops_at_its_candidate_bound()
    {
        // The scan is bounded by candidate count, not by page count, so a
        // repository larger than the bound stops mid-page rather than
        // materialising the whole corpus into memory to rank it.
        var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var fileSerializer = Serializer;
        for (var i = 0; i < MaxKeywordScan + 200; i++)
        {
            var path = $"src/Widget{i:D5}.cs";
            records[RepoContextKeys.File(RepoId, path)] =
                fileSerializer.SerializeToArray(new FileNode { RepoId = RepoId, Path = path });
        }

        var tree = Tree(records);
        var service = CreateService(Substitute.For<IRepoContextSemanticIndex>(), embeddingProvider: null, tree);

        var result = await service.SearchAsync(RepoId, "widget", 5, CancellationToken.None);

        // The bound caps the candidate set, not the answer: a ranked page still
        // comes back.
        Assert.That(result.Mode, Is.EqualTo("keyword"));
        Assert.That(result.Hits, Has.Count.EqualTo(5));
    }

    private static RepoContextSearchService CreateService(
        IRepoContextSemanticIndex index, IEmbeddingProvider? embeddingProvider, ILattice tree)
    {
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

    /// <summary>An <see cref="ILattice"/> whose entry scan honours the requested key window.</summary>
    private static ILattice Tree(SortedDictionary<string, byte[]> records)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(call => Entries(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        return tree;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        foreach (var pair in records)
        {
            if (startInclusive is not null && string.CompareOrdinal(pair.Key, startInclusive) < 0)
            {
                continue;
            }

            if (endExclusive is not null && string.CompareOrdinal(pair.Key, endExclusive) >= 0)
            {
                break;
            }

            yield return pair;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }
}
