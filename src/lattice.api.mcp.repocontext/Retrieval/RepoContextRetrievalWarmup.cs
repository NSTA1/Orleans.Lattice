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
            var repos = await _store.ListReposAsync(cancellationToken).ConfigureAwait(false);
            if (repos.Repos.Count == 0)
            {
                // Nothing is indexed, so the vector plane holds nothing it could fail to
                // serve. Blocking readiness here would wedge a fresh box before its first
                // repository could ever be onboarded.
                _readiness.MarkServing();
                _logger.LogInformation(
                    "Repo-context retrieval warmup: no repositories are indexed, so the retrieval plane is ready with nothing to serve.");
                return true;
            }

            foreach (var repo in repos.Repos)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // The result is deliberately discarded: the readiness observation the
                // search service makes on the way through is the point of the call.
                _ = await _search
                    .SearchAsync(repo.RepoId, WarmupQuery, 1, cancellationToken)
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
