using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IRepoContextRetrievalWarmup"/>: it drives the ordinary search
/// path against each indexed repository until the vector plane answers, so the readiness
/// signal is fed by a real semantic query rather than by configuration.
/// </summary>
/// <remarks>
/// It classifies nothing itself. <see cref="RepoContextSearchService"/> resolves the
/// retrieval path and folds it into <see cref="RepoContextRetrievalReadinessState"/>,
/// so warmup and client traffic converge on exactly the same readiness semantics.
/// </remarks>
internal sealed class RepoContextRetrievalWarmup : IRepoContextRetrievalWarmup
{
    /// <summary>
    /// The query text the warmup embeds. Its content is irrelevant to the outcome - the
    /// warmup asserts only that the plane answered, never what it answered - so a fixed
    /// generic phrase keeps the pass deterministic and free of repository knowledge.
    /// </summary>
    internal const string WarmupQuery = "repository context readiness warmup";

    private readonly RepoContextStore _store;
    private readonly RepoContextSearchService _search;
    private readonly RepoContextRetrievalReadinessState _readiness;
    private readonly ILogger<RepoContextRetrievalWarmup> _logger;

    /// <summary>Creates the warmup driver.</summary>
    /// <param name="store">The capture store used to enumerate the indexed repositories. Must not be <see langword="null"/>.</param>
    /// <param name="search">The search service whose resolved retrieval path feeds readiness. Must not be <see langword="null"/>.</param>
    /// <param name="readiness">The shared vector-plane readiness state the pass drives. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextRetrievalWarmup(
        RepoContextStore store,
        RepoContextSearchService search,
        RepoContextRetrievalReadinessState readiness,
        ILogger<RepoContextRetrievalWarmup> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(search);
        ArgumentNullException.ThrowIfNull(readiness);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _search = search;
        _readiness = readiness;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<bool> TryWarmAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Ids only, deliberately. The warmup uses nothing but the repository id,
            // and a full summary carries embeddedVectorCount, which is scanned from
            // the membership tree - the largest tree in the store. Paying that scan
            // here means paying it at startup, while the vector trees are still
            // replaying and that scan is at its slowest; it timed out and failed the
            // warmup on a real deployment (issue #1819).
            var repoIds = await _store.ListRepoIdsAsync(cancellationToken).ConfigureAwait(false);
            if (repoIds.Count == 0)
            {
                // No repository is REGISTERED, so there is nothing this host could be
                // asked to retrieve from. Blocking readiness here would wedge a fresh
                // box before its first repository could ever be onboarded.
                //
                // Read that condition precisely: it is "no repository is listed", NOT
                // "nothing is indexed". A listed repository holding no vectors is a
                // different state and deliberately does NOT come through here - it
                // falls to the loop below, where the search reports
                // KeywordVectorPlaneUnavailable (RepoContextSearchService returns it
                // when the index yields no matches at all) and the host stays
                // not-ready. That is the intended semantics, not an oversight:
                // RepoContextRetrievalPath.KeywordVectorPlaneUnavailable classifies an
                // empty or still-building plane as a real capability loss, so a box
                // that was asked to index something and cannot serve it semantically
                // should not claim to be ready.
                //
                // Widening this guard to "holds no vectors" would also cost what it
                // exists to avoid: a vector count comes from the membership tree, the
                // largest in the store, and paying that scan at startup is what timed
                // out and failed the warmup on a real deployment (issue #1819). See
                // the ids-only comment above.
                _readiness.MarkServing();
                _logger.LogInformation(
                    "Repo-context retrieval warmup: no repositories are registered, so the retrieval plane is ready with nothing to serve.");
                return true;
            }

            foreach (var repoId in repoIds)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // The result is deliberately discarded: the readiness observation the
                // search service makes on the way through is the point of the call.
                _ = await _search
                    .SearchAsync(repoId, WarmupQuery, 1, cancellationToken)
                    .ConfigureAwait(false);

                if (_readiness.IsReady)
                {
                    return true;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Fail closed: a warmup fault never marks the plane ready and never
            // propagates to the host. The next pass retries.
            _logger.LogWarning(
                ex,
                "Repo-context retrieval warmup pass failed; the retrieval plane stays not-ready and the warmup will retry.");
        }

        return _readiness.IsReady;
    }
}
