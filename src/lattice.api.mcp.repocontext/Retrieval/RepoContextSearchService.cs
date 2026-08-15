using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The adapter behind the read-only <c>repocontext_search</c> tool. It makes the
/// stored repository context findable by meaning and by structure: it embeds the
/// incoming query, asks the configured <see cref="IRepoContextSemanticIndex"/> for
/// the nearest vectors, and <b>hydrates the canonical records</b> from the store
/// of record for every hit - the index returns identities, never a second copy of
/// the data.
/// <para>
/// <b>Fail-closed and honest.</b> When no embedding provider is configured, the
/// provider is unreachable, an embed call fails, or the repository holds no
/// vectors in the query's embedding space, the service degrades to a deterministic
/// keyword/structural scan over the records the structural walk already captured
/// (<see cref="RepoContextKeywordSearch"/>) rather than throwing. The
/// <see cref="RepoContextSearchResult.Mode"/> tells the caller which path
/// answered.
/// </para>
/// </summary>
internal sealed class RepoContextSearchService
{
    private const int DefaultResultCount = 10;
    private const int MaxResultCount = 100;
    private const int MaxKeywordScan = 5000;

    private readonly IGrainFactory _grainFactory;
    private readonly Orleans.Serialization.Serializer _serializer;
    private readonly IRepoContextSemanticIndex _index;
    private readonly RepoContextStore _store;
    private readonly TimeProvider _timeProvider;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly ILogger<RepoContextSearchService> _logger;

    /// <summary>Creates the search service.</summary>
    /// <param name="grainFactory">The grain factory used to reach the context trees for the keyword scan. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode records during the keyword scan. Must not be <see langword="null"/>.</param>
    /// <param name="index">The semantic index queried on the semantic path. Must not be <see langword="null"/>.</param>
    /// <param name="store">The capture store used to hydrate canonical records for semantic hits. Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock used to project remaining life during the keyword scan. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger used to record fail-closed fallbacks. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (search then uses keyword recall).</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextSearchService(
        IGrainFactory grainFactory,
        Orleans.Serialization.Serializer serializer,
        IRepoContextSemanticIndex index,
        RepoContextStore store,
        TimeProvider timeProvider,
        ILogger<RepoContextSearchService> logger,
        IEmbeddingProvider? embeddingProvider = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(index);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _serializer = serializer;
        _index = index;
        _store = store;
        _timeProvider = timeProvider;
        _logger = logger;
        _embeddingProvider = embeddingProvider;
    }

    /// <summary>
    /// Runs a repository-context search: the semantic path when an embedder and
    /// vectors are available, otherwise a keyword/structural scan.
    /// </summary>
    /// <param name="repoId">The repository to search. Must be non-empty.</param>
    /// <param name="query">The free-text query. Must be non-empty.</param>
    /// <param name="k">The maximum number of hits to return; clamped to [1, 100], defaulting to 10.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>The ranked hits and the mode that produced them.</returns>
    public async Task<RepoContextSearchResult> SearchAsync(
        string repoId, string query, int k, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(query);
        var count = k <= 0 ? DefaultResultCount : Math.Min(k, MaxResultCount);

        IReadOnlyList<RepoContextSearchHit>? semantic;
        try
        {
            semantic = await TrySemanticAsync(repoId, query, count, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Fail-closed contract: any fault on the semantic path (an
            // embed failure, an index/grain activation fault such as a
            // stale leaf projection, or a same-silo copier gap surfacing a
            // non-copyable exception) must degrade to keyword recall rather
            // than propagate out of the read-only search tool.
            _logger.LogWarning(
                ex,
                "repocontext_search for repository {RepoId} falling back to keyword recall: the semantic path threw.",
                repoId);
            semantic = null;
        }

        if (semantic is { Count: > 0 })
        {
            return new RepoContextSearchResult
            {
                RepoId = repoId,
                Query = query,
                Mode = "semantic",
                Hits = semantic,
            };
        }

        IReadOnlyList<RepoContextSearchHit> keyword;
        try
        {
            keyword = await KeywordAsync(repoId, query, count, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Fail-closed contract: if the keyword/structural fallback itself
            // faults (for example the same stale-leaf-projection activation fault
            // that can trip the semantic path, since the keyword scan walks the
            // structural and memory trees), the read-only search tool degrades to
            // the terminal empty result rather than propagating a protocol error.
            _logger.LogWarning(
                ex,
                "repocontext_search for repository {RepoId} returning empty: the keyword fallback threw.",
                repoId);
            keyword = Array.Empty<RepoContextSearchHit>();
        }

        return new RepoContextSearchResult
        {
            RepoId = repoId,
            Query = query,
            Mode = keyword.Count > 0 ? "keyword" : "empty",
            Hits = keyword,
        };
    }

    private async Task<IReadOnlyList<RepoContextSearchHit>?> TrySemanticAsync(
        string repoId, string query, int k, CancellationToken cancellationToken)
    {
        if (_embeddingProvider is null)
        {
            return null;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "repocontext_search for repository {RepoId} falling back to keyword recall: the embedding provider is unavailable.",
                repoId);
            return null;
        }

        var embed = await _embeddingProvider
            .EmbedAsync(new[] { query }, EmbeddingTextType.Query, cancellationToken)
            .ConfigureAwait(false);
        if (!embed.Succeeded || embed.Vectors.Count != 1)
        {
            _logger.LogInformation(
                "repocontext_search for repository {RepoId} falling back to keyword recall: the query embedding did not succeed ({Error}).",
                repoId,
                embed.Error ?? "no vector returned");
            return null;
        }

        var querySpace = EmbeddingSpaceTag.FromSpace(embed.Space);
        // Over-fetch: a source now contributes several chunk (and symbol) vectors, so
        // the top-k raw matches can be several passages of one source. Pull a larger
        // pool and collapse it to k distinct sources, keeping each source's best
        // (highest-scored, hence first) passage - the ranker returns descending score.
        var poolK = Math.Min(Math.Max(k * 8, k), 1000);
        var matches = await _index
            .SearchAsync(repoId, embed.Vectors[0], querySpace, poolK, cancellationToken)
            .ConfigureAwait(false);
        if (matches.Count == 0)
        {
            return null;
        }

        var hits = new List<RepoContextSearchHit>(k);
        var seenSources = new HashSet<string>(StringComparer.Ordinal);
        foreach (var match in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (hits.Count >= k)
            {
                break;
            }

            if (string.IsNullOrEmpty(match.SourceKey) || !seenSources.Add(match.SourceKey))
            {
                continue;
            }

            var entry = await _store.RecallAsync(match.SourceKey, cancellationToken).ConfigureAwait(false);
            if (!entry.Exists)
            {
                continue;
            }

            hits.Add(new RepoContextSearchHit { Score = match.Score, Entry = entry, VectorId = match.VectorId });
        }

        return hits;
    }

    private async Task<IReadOnlyList<RepoContextSearchHit>> KeywordAsync(
        string repoId, string query, int k, CancellationToken cancellationToken)
    {
        var tokens = RepoContextKeywordSearch.Tokenize(query);
        if (tokens.Count == 0)
        {
            return Array.Empty<RepoContextSearchHit>();
        }

        var entries = new List<RepoContextEntryView>();
        await ScanTreeAsync(
            RepoContextTrees.Structural, RepoContextKeys.RepoScanPrefix(repoId), entries, cancellationToken)
            .ConfigureAwait(false);
        await ScanTreeAsync(
            RepoContextTrees.Memory, RepoContextKeys.MemoryPrefix(repoId), entries, cancellationToken)
            .ConfigureAwait(false);

        return RepoContextKeywordSearch.Rank(entries, tokens, k);
    }

    private async Task ScanTreeAsync(
        string treeName, string prefix, List<RepoContextEntryView> entries, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(treeName);
        var now = _timeProvider.GetUtcNow().UtcDateTime;

        string? token = null;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            foreach (var record in page.Records)
            {
                if (!RepoContextKeys.TryParse(record.Key, out var parsed))
                {
                    continue;
                }

                // Keyword search is a bulk enumeration like scan: it cannot cheaply
                // read each entry's expiry, so it projects expiry as "not evaluated"
                // (null) rather than falsely asserting a durable entry.
                entries.Add(RepoContextEntryProjection.Project(
                    parsed, record.Value, _serializer, life: null));

                if (entries.Count >= MaxKeywordScan)
                {
                    return;
                }
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);
    }
}
