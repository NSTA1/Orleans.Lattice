using Microsoft.Extensions.Logging;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IRepoContextSemanticIndex"/>: it answers from the
/// persisted approximate nearest-neighbour plane, and falls back to the exact
/// scan while that plane is still building.
/// <para>
/// <b>It declares the weaker guarantee, always.</b>
/// <see cref="IRepoContextSemanticIndex.RetrievalPath"/> is a property of the
/// index rather than of a query, and one index serves every repository, so a
/// declaration that tracked the current state would be wrong the moment two
/// repositories were in different states, or the moment a build completed between
/// a search and the read of the property. Declaring
/// <see cref="RepoContextRetrievalPath.SemanticApproximate"/> unconditionally is
/// the only sound choice: it under-promises recall while the exact scan is
/// answering, and never over-promises it once the plane is.
/// </para>
/// <para>
/// <b>Nothing about the fallback is a degradation.</b> While the plane builds,
/// the exact scan answers with complete recall - slower, never worse - so this
/// path must never be confused with
/// <see cref="RepoContextRetrievalPath.KeywordIndexDegraded"/>. The build state
/// itself is reported out of band: as a log line on every transition, and to a
/// host through <see cref="TryGetProgress"/>, which is per repository and
/// embedding space and so carries detail the single per-response value could not.
/// </para>
/// </summary>
internal sealed class AnnRepoContextSemanticIndex : IRepoContextSemanticIndex
{
    private readonly IRepoContextAnnIndex _plane;
    private readonly IRepoContextSemanticIndex _exact;
    private readonly ILogger<AnnRepoContextSemanticIndex> _logger;

    /// <summary>Creates the approximate-first semantic index.</summary>
    /// <param name="plane">The approximate retrieval plane. Must not be <see langword="null"/>.</param>
    /// <param name="exact">The exact scan used while the plane is building, and kept as the correctness oracle. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the fallback report is written to. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public AnnRepoContextSemanticIndex(
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        ILogger<AnnRepoContextSemanticIndex> logger)
    {
        ArgumentNullException.ThrowIfNull(plane);
        ArgumentNullException.ThrowIfNull(exact);
        ArgumentNullException.ThrowIfNull(logger);
        _plane = plane;
        _exact = exact;
        _logger = logger;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Always <see cref="RepoContextRetrievalPath.SemanticApproximate"/>. See the
    /// type remarks for why a state-tracking declaration would be unsound.
    /// </remarks>
    public string RetrievalPath => RepoContextRetrievalPath.SemanticApproximate;

    /// <summary>
    /// The state the last query for a repository and embedding space would be
    /// served from, and the build progress behind it. Returns
    /// <see langword="false"/> when the plane holds no index for the pair yet,
    /// which is itself the honest answer: nothing has been built, so the exact
    /// scan is answering.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <param name="progress">The build progress when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the plane holds an index for the pair.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal bool TryGetProgress(string repoId, EmbeddingSpaceTag space, out VectorIndexBuildProgress progress)
        => _plane.TryGetProgress(repoId, space, out progress);

    /// <inheritdoc />
    public async Task<IReadOnlyList<RepoContextVectorMatch>> SearchAsync(
        string repoId,
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag querySpace,
        int k,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(k);

        var outcome = await _plane
            .SearchAsync(repoId, query, querySpace, k, cancellationToken)
            .ConfigureAwait(false);

        if (outcome.State != RepoContextAnnServingState.Bootstrapping)
        {
            return outcome.Matches;
        }

        // The plane has no usable index for this repository and embedding space
        // yet. The exact scan answers with complete recall in the meantime, which is
        // what keeps an existing deployment serving from its first start on a build
        // that has never indexed it.
        _logger.LogDebug(
            "Repository-context semantic search for {RepoId} in space {ModelId}/{Dimension} served by the exact "
            + "scan: the approximate index is still building.",
            repoId,
            querySpace.ModelId,
            querySpace.Dimension);

        return await _exact
            .SearchAsync(repoId, query, querySpace, k, cancellationToken)
            .ConfigureAwait(false);
    }
}
