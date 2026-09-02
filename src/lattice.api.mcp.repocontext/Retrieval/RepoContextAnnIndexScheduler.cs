using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Arms the durable build coordinator for a repository's approximate index.
/// <para>
/// It exists so the two things that know a repository needs indexing - the startup
/// sweep over the registered repositories, and the self-index grain finishing a
/// vectorising pass - can arm the coordinator without either of them having to
/// resolve the embedding provider, know the key layout, or hold an optional
/// dependency. The embedding space is read from the provider here, in one place,
/// because it is the provider that defines which space is live and therefore which
/// index prefix a repository's vectors belong under.
/// </para>
/// <para>
/// Every call is idempotent and cheap: arming an already-armed coordinator
/// re-registers a reminder that is already registered, and arming a converged one
/// costs a single reload step that opens the persisted index into this process
/// rather than leaving it for the next query to pay for.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexScheduler
{
    private readonly IGrainFactory _grainFactory;
    private readonly RepoContextIndexingOptions _options;
    private readonly IEmbeddingProvider? _embedder;
    private readonly ILogger<RepoContextAnnIndexScheduler> _logger;

    /// <summary>Creates the scheduler.</summary>
    /// <param name="grainFactory">The grain factory used to reach the build coordinators. Must not be <see langword="null"/>.</param>
    /// <param name="options">The indexing options carrying the scheduling switch. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    /// <param name="embedder">The embedding provider defining the live space, or
    /// <see langword="null"/> when no provider is bound - in which case nothing is
    /// ever embedded, so there is no index to schedule.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextAnnIndexScheduler(
        IGrainFactory grainFactory,
        RepoContextIndexingOptions options,
        ILogger<RepoContextAnnIndexScheduler> logger,
        IEmbeddingProvider? embedder = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        _grainFactory = grainFactory;
        _options = options;
        _logger = logger;
        _embedder = embedder;
    }

    /// <summary>
    /// Whether a build can be scheduled at all: the switch is on and an embedding
    /// provider is bound, so a live embedding space exists to index under.
    /// </summary>
    public bool CanSchedule => _options.AnnIndexSchedulingEnabled && _embedder is not null;

    /// <summary>
    /// Arms the build coordinator for one repository's index in the live embedding
    /// space. A no-op when <see cref="CanSchedule"/> is <see langword="false"/>.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns><see langword="true"/> when a coordinator was armed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public async Task<bool> TryArmAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (_embedder is null || !_options.AnnIndexSchedulingEnabled)
        {
            return false;
        }

        cancellationToken.ThrowIfCancellationRequested();
        var space = EmbeddingSpaceTag.FromSpace(_embedder.Space);
        await _grainFactory
            .GetGrain<IRepoContextAnnIndexBuildGrain>(RepoContextAnnIndexKeys.BuildGrainKey(repoId, space))
            .EnsureBuildingAsync(space)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Repo {RepoId}: approximate-index build coordinator armed for space {ModelId}/{Dimension}.",
            repoId,
            space.ModelId,
            space.Dimension);
        return true;
    }
}
