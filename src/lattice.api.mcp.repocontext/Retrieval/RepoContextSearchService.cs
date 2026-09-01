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
/// answered, and the additive <see cref="RepoContextSearchResult.RetrievalPath"/>
/// tells it <b>why</b> - separating an intended keyword-only deployment from a real
/// vector-plane capability loss, which <c>mode</c> alone cannot distinguish.
/// </para>
/// <para>
/// <b>It is the readiness seam.</b> Every query funnels through here, so the
/// resolved retrieval path is folded into the shared
/// <see cref="RepoContextRetrievalReadinessState"/> exactly once per call. The
/// host's vector-plane readiness component therefore reports what retrieval
/// actually did rather than what configuration promised.
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
    private readonly RepoContextRetrievalReadinessState? _readiness;
    private readonly ILogger<RepoContextSearchService> _logger;

    /// <summary>Creates the search service.</summary>
    /// <param name="grainFactory">The grain factory used to reach the context trees for the keyword scan. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode records during the keyword scan. Must not be <see langword="null"/>.</param>
    /// <param name="index">The semantic index queried on the semantic path. Must not be <see langword="null"/>.</param>
    /// <param name="store">The capture store used to hydrate canonical records for semantic hits. Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock used to project remaining life during the keyword scan. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger used to record fail-closed fallbacks. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (search then uses keyword recall).</param>
    /// <param name="readiness">The shared vector-plane readiness state each resolved retrieval path is folded into, or <see langword="null"/> for an in-process host that publishes no readiness signal.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextSearchService(
        IGrainFactory grainFactory,
        Orleans.Serialization.Serializer serializer,
        IRepoContextSemanticIndex index,
        RepoContextStore store,
        TimeProvider timeProvider,
        ILogger<RepoContextSearchService> logger,
        IEmbeddingProvider? embeddingProvider = null,
        RepoContextRetrievalReadinessState? readiness = null)
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
        _readiness = readiness;
    }

    /// <summary>
    /// Runs a repository-context search: the semantic path when an embedder and
    /// vectors are available, otherwise a keyword/structural scan.
    /// </summary>
    /// <param name="repoId">The repository to search. Must be non-empty.</param>
    /// <param name="query">The free-text query. Must be non-empty.</param>
    /// <param name="k">The maximum number of hits to return; clamped to [1, 100], defaulting to 10.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>The ranked hits, the mode that produced them, and the precise retrieval path that answered.</returns>
    public async Task<RepoContextSearchResult> SearchAsync(
        string repoId, string query, int k, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(query);
        var count = k <= 0 ? DefaultResultCount : Math.Min(k, MaxResultCount);

        SemanticOutcome semantic;
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
            // than propagate out of the read-only search tool. The fault is a
            // degraded semantic index, not an absent embedder, and the
            // retrieval path says exactly that.
            _logger.LogWarning(
                ex,
                "repocontext_search for repository {RepoId} falling back to keyword recall: the semantic path threw.",
                repoId);
            semantic = SemanticOutcome.IndexDegraded;
        }

        // One observation per call, at the single seam every query funnels through: the
        // resolved path is the authoritative statement of what retrieval could do.
        _readiness?.Observe(semantic.RetrievalPath);

        if (semantic.Hits is { Count: > 0 })
        {
            return new RepoContextSearchResult
            {
                RepoId = repoId,
                Query = query,
                Mode = "semantic",
                RetrievalPath = semantic.RetrievalPath,
                Hits = semantic.Hits,
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
            // Fail-closed backstop. Per-tree isolation in KeywordAsync already
            // degrades a terminal single-tree materialisation fault (for example a
            // stale-leaf-projection activation fault) to the remaining healthy
            // trees, so this outer guard now only catches an unexpected fault in
            // the shared setup or ranking. The read-only search tool degrades to
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
            // The keyword cause is reported even on the terminal empty result, so an
            // operator can still tell an intended keyword-only box from a broken one.
            RetrievalPath = semantic.RetrievalPath,
            Hits = keyword,
        };
    }

    /// <summary>
    /// The outcome of the semantic path: the hits it produced (<see langword="null"/>
    /// when it did not answer) and the <see cref="RepoContextRetrievalPath"/> value
    /// describing what happened. A <see langword="readonly"/> record struct with cached
    /// instances for the three no-answer causes, so reporting a cause costs no
    /// allocation on the per-query path.
    /// </summary>
    /// <param name="Hits">The hydrated hits, or <see langword="null"/> when the semantic path did not answer.</param>
    /// <param name="RetrievalPath">The resolved retrieval-path value.</param>
    private readonly record struct SemanticOutcome(
        IReadOnlyList<RepoContextSearchHit>? Hits,
        string RetrievalPath)
    {
        /// <summary>No embedding provider is bound: an intended keyword-only deployment.</summary>
        internal static SemanticOutcome NoEmbedder { get; } =
            new(null, RepoContextRetrievalPath.KeywordNoEmbedder);

        /// <summary>An embedder is bound but the vector plane could not serve the query.</summary>
        internal static SemanticOutcome VectorPlaneUnavailable { get; } =
            new(null, RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);

        /// <summary>The semantic index ran but is degraded: it threw, or ranked candidates that no longer hydrate.</summary>
        internal static SemanticOutcome IndexDegraded { get; } =
            new(null, RepoContextRetrievalPath.KeywordIndexDegraded);
    }

    private async Task<SemanticOutcome> TrySemanticAsync(
        string repoId, string query, int k, CancellationToken cancellationToken)
    {
        if (_embeddingProvider is null)
        {
            return SemanticOutcome.NoEmbedder;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "repocontext_search for repository {RepoId} falling back to keyword recall: the embedding provider is unavailable.",
                repoId);
            return SemanticOutcome.VectorPlaneUnavailable;
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
            return SemanticOutcome.VectorPlaneUnavailable;
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
            // The index answered but holds nothing to compare in this embedding space:
            // the plane is empty, mid-replay, or re-deriving after a fall-off. That is a
            // vector-plane availability fact, not a degraded index.
            return SemanticOutcome.VectorPlaneUnavailable;
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

            hits.Add(new RepoContextSearchHit
            {
                Score = match.Score,
                Entry = entry,
                VectorId = match.VectorId,
                Reasons = RepoContextSearchReasons.ForSemantic(match.SourceKey),
            });
        }

        if (hits.Count == 0)
        {
            // The index ranked candidates but not one of them still hydrates from the
            // store of record: the index has drifted from its sources.
            return SemanticOutcome.IndexDegraded;
        }

        // Re-validate the bound index's own declaration against the local vocabulary
        // rather than trusting it: an unrecognised value reports the weaker
        // (approximate) recall claim instead of over-promising completeness.
        return new SemanticOutcome(hits, RepoContextRetrievalPath.NormalizeSemantic(_index.RetrievalPath));
    }

    private async Task<IReadOnlyList<RepoContextSearchHit>> KeywordAsync(
        string repoId, string query, int k, CancellationToken cancellationToken)
    {
        var tokens = RepoContextKeywordSearch.Tokenize(query);
        if (tokens.Count == 0)
        {
            return Array.Empty<RepoContextSearchHit>();
        }

        // Each context tree is scanned independently and in isolation: a terminal
        // fault materialising one tree must degrade the keyword corpus to the
        // remaining healthy trees, not sink the whole fallback. The underlying
        // RepoContextPortability scan already recovers transparently from a
        // transient EnumerationAbortedException (silo failover, cold start, idle
        // expiry, scale-down) via its retry budget; this guard is for the
        // orthogonal terminal case - for example a LeafProjectionStaleException
        // when a leaf's durable projection checkpoint has fallen off the WAL and
        // awaits an operator-driven rebuild - which the retry budget rightly does
        // not swallow because retrying cannot recover it.
        var entries = new List<RepoContextEntryView>();
        await TryScanTreeAsync(
            RepoContextTrees.Structural, RepoContextKeys.RepoScanPrefix(repoId), entries, repoId, cancellationToken)
            .ConfigureAwait(false);
        await TryScanTreeAsync(
            RepoContextTrees.Memory, RepoContextKeys.MemoryPrefix(repoId), entries, repoId, cancellationToken)
            .ConfigureAwait(false);

        // Fold the per-file content projection in so keyword search ranks over file
        // body text, not just filenames and symbol names. The content tree is
        // separate from the structural tree, so it is scanned explicitly. The shared
        // MaxKeywordScan bound still caps the total candidate set.
        await TryScanTreeAsync(
            RepoContextTrees.Content, RepoContextKeys.ContentPrefix(repoId), entries, repoId, cancellationToken)
            .ConfigureAwait(false);

        return RepoContextKeywordSearch.Rank(entries, tokens, k);
    }

    /// <summary>
    /// Scans one context tree into <paramref name="entries"/>, isolating a
    /// terminal per-tree materialisation fault so it degrades the keyword corpus
    /// to the remaining healthy trees rather than aborting the whole fallback.
    /// Cancellation is never swallowed. Any entries the tree yielded before it
    /// faulted are retained.
    /// </summary>
    private async Task TryScanTreeAsync(
        string treeName,
        string prefix,
        List<RepoContextEntryView> entries,
        string repoId,
        CancellationToken cancellationToken)
    {
        try
        {
            await ScanTreeAsync(treeName, prefix, entries, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // A single tree failing to materialise (most often a content or leaf
            // projection awaiting an operator-driven rebuild) must not sink the
            // keyword fallback: keep whatever the healthy trees yielded and rank
            // over the narrower corpus.
            _logger.LogWarning(
                ex,
                "repocontext_search keyword scan for repository {RepoId} skipped tree {Tree}: it could not be enumerated.",
                repoId,
                treeName);
        }
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
