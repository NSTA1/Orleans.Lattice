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
/// <para>
/// <b>The fallback is skipped when it provably cannot finish.</b> "Slower, never
/// worse" holds only while the exact gather can complete. Past the corpus size
/// <see cref="RepoContextExactScanBudget"/> derives from the tree's own
/// scan-page budget it cannot: the gather burns a full page-fill ceiling, faults
/// with <see cref="ScanPageStalledException"/>, and ends at keyword recall
/// anyway - having spent that time competing for the very tree the build is
/// streaming, so it delays its own resolution. There the plane answers with no
/// matches instead, which the search service reports as
/// <see cref="RepoContextRetrievalPath.KeywordVectorPlaneUnavailable"/>: the
/// documented cause for a plane that is still building, which is exactly what is
/// true. Below that size nothing changes and the exact gather still answers.
/// </para>
/// <para>
/// <b>And it is skipped when it has already failed to finish, whatever the
/// budget predicted.</b> The budget is a prediction from a corpus size, and that
/// size is <c>0</c> from process start until the build publishes its first
/// progress - the whole interval the skip exists to protect - so on its own the
/// budget fails open through precisely the window it was written for (issue
/// #2231). <see cref="RepoContextExactScanBreaker"/> closes that window without
/// reading any count: one gather faults with
/// <see cref="ScanPageStalledException"/>, that fault is caught and reported as
/// the same no-matches answer the budget's skip produces, and no further gather
/// is started for the repository until the plane serves. A miscounted or
/// not-yet-counted corpus cannot defeat it, because it observes the failure
/// rather than predicting it.
/// </para>
/// </summary>
internal sealed class AnnRepoContextSemanticIndex : IRepoContextSemanticIndex
{
    private readonly IRepoContextAnnIndex _plane;
    private readonly IRepoContextSemanticIndex _exact;
    private readonly RepoContextExactScanBudget _exactScanBudget;
    private readonly RepoContextExactScanBreaker _exactScanBreaker;
    private readonly ILogger<AnnRepoContextSemanticIndex> _logger;

    /// <summary>Creates the approximate-first semantic index.</summary>
    /// <param name="plane">The approximate retrieval plane. Must not be <see langword="null"/>.</param>
    /// <param name="exact">The exact scan used while the plane is building, and kept as the correctness oracle. Must not be <see langword="null"/>.</param>
    /// <param name="exactScanBudget">The budget deciding whether an exact gather can finish under the tree's configured scan-page bounds. Must not be <see langword="null"/>.</param>
    /// <param name="exactScanBreaker">The record of gathers that have already proved they cannot finish. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the fallback report is written to. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public AnnRepoContextSemanticIndex(
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        RepoContextExactScanBudget exactScanBudget,
        RepoContextExactScanBreaker exactScanBreaker,
        ILogger<AnnRepoContextSemanticIndex> logger)
    {
        ArgumentNullException.ThrowIfNull(plane);
        ArgumentNullException.ThrowIfNull(exact);
        ArgumentNullException.ThrowIfNull(exactScanBudget);
        ArgumentNullException.ThrowIfNull(exactScanBreaker);
        ArgumentNullException.ThrowIfNull(logger);
        _plane = plane;
        _exact = exact;
        _exactScanBudget = exactScanBudget;
        _exactScanBreaker = exactScanBreaker;
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
            // The plane answered for itself, so the build that a stalled gather was
            // competing with is no longer holding the tree. Restoring the fallback
            // here is what keeps a trip from outliving its cause: no cooldown to
            // wait out, and a later rebuild re-arms the breaker on its own evidence.
            _exactScanBreaker.Reset(repoId);
            return outcome.Matches;
        }

        if (_exactScanBreaker.IsTripped(repoId))
        {
            // A gather over this repository has already spent a full page-fill
            // ceiling and faulted. Nothing about the plane still building makes the
            // next one cheaper, so it is not started. This is the branch that holds
            // when the corpus is uncounted and the budget below therefore fails
            // open - the state the whole bootstrap window is in.
            _logger.LogDebug(
                "Repository-context semantic search for {RepoId} in space {ModelId}/{Dimension} skipped the exact "
                + "scan: a gather over this repository has already stalled while the approximate index builds. "
                + "Serving keyword recall until the plane answers for itself.",
                repoId,
                querySpace.ModelId,
                querySpace.Dimension);

            return Array.Empty<RepoContextVectorMatch>();
        }

        if (!CanAffordExactScan(repoId, querySpace, out var corpus, out var affordable))
        {
            // The gather would range-scan the whole vector-metadata prefix, and the
            // tree's own configuration says a scan that size cannot fill its pages
            // inside the stall ceiling. Starting it would spend that ceiling, fault,
            // and arrive at keyword recall anyway - while loading the tree the build
            // is streaming, so the fallback delays its own resolution. Reporting no
            // matches takes the same destination directly, and the search service
            // resolves it to the keyword.vector_plane_unavailable cause, which is the
            // documented "the plane is still building" answer.
            _logger.LogInformation(
                "Repository-context semantic search for {RepoId} in space {ModelId}/{Dimension} skipped the exact "
                + "scan: the approximate index is still building and the corpus of {Corpus} vectors exceeds the "
                + "{Affordable} a page fill can cover within the configured scan-page budget. Serving keyword "
                + "recall instead of a scan that cannot complete.",
                repoId,
                querySpace.ModelId,
                querySpace.Dimension,
                corpus,
                affordable);

            return Array.Empty<RepoContextVectorMatch>();
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

        try
        {
            return await _exact
                .SearchAsync(repoId, query, querySpace, k, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ScanPageStalledException ex)
        {
            // Only this fault is absorbed. It is the tree telling us a page fill did
            // not return inside its ceiling, which is a statement about contention
            // and not about the corpus: field traces put the abort after between
            // zero and seven leaves, across seventeen shards, so no count of any
            // accuracy predicts it. The fault is therefore the measurement - record
            // it so no later query repeats it, and report the same no-matches answer
            // the predicted skip produces. Letting it propagate would reach keyword
            // recall too, but classified as keyword.index_degraded, which claims a
            // broken index rather than the still-building plane that is true. Every
            // other fault keeps propagating, so a genuinely broken index is still
            // reported as degraded rather than masked as "still building".
            var first = _exactScanBreaker.Trip(repoId);
            _logger.Log(
                first ? LogLevel.Warning : LogLevel.Debug,
                ex,
                "Repository-context semantic search for {RepoId} in space {ModelId}/{Dimension} abandoned the "
                + "exact scan: the gather stalled against the vector-metadata tree while the approximate index "
                + "builds. Serving keyword recall, and skipping the gather for this repository until the plane "
                + "answers for itself.",
                repoId,
                querySpace.ModelId,
                querySpace.Dimension);

            return Array.Empty<RepoContextVectorMatch>();
        }
    }

    /// <summary>
    /// Whether an exact gather over this repository and embedding space can
    /// complete under the vector-metadata tree's configured scan-page bounds.
    /// <para>
    /// <b>Fails open on an unknown corpus, and that is not sufficient on its
    /// own.</b> The size is read from the build progress the plane already holds,
    /// which costs nothing - but it is <c>0</c> until the build has counted the
    /// store of record, so a prediction from it clears every gather for the whole
    /// bootstrap window. Failing open is still the right answer here, because an
    /// uncounted corpus is genuinely no evidence of an unaffordable gather and a
    /// small deployment must keep its exact fallback from first start. What makes
    /// it safe is that it is no longer the only guard:
    /// <see cref="RepoContextExactScanBreaker"/> catches the case this cannot,
    /// from the fault rather than from a count. See issue #2231.
    /// </para>
    /// <para>
    /// <b>The count is repository-wide, not per space.</b> The gather scans the
    /// repository's whole vector prefix and filters by space in memory, so a
    /// single space's progress is a lower bound on the rows it visits - and
    /// under-counting is what lets an unaffordable gather clear the threshold.
    /// <see cref="IRepoContextAnnIndex.KnownVectorCount"/> sums every space the
    /// plane has opened, and the larger of that and this space's own progress is
    /// the best known size.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository being searched.</param>
    /// <param name="space">The embedding space the query was produced in.</param>
    /// <param name="corpus">The best known corpus size, reported for the log line.</param>
    /// <param name="affordable">The budget's affordable vector count, reported for the log line.</param>
    /// <returns><see langword="true"/> when the gather should run.</returns>
    private bool CanAffordExactScan(
        string repoId, EmbeddingSpaceTag space, out int corpus, out int affordable)
    {
        corpus = 0;
        affordable = _exactScanBudget.AffordableVectorCount;
        if (affordable == RepoContextExactScanBudget.Unbounded)
        {
            return true;
        }

        // VectorsExpected is what the build counted in the store of record;
        // VectorsIndexed is what it has taken in so far. Either can lead the other
        // depending on how far the build got and whether the corpus grew under it,
        // so the larger is the best-known size.
        if (_plane.TryGetProgress(repoId, space, out var progress))
        {
            corpus = Math.Max(progress.VectorsExpected, progress.VectorsIndexed);
        }

        corpus = Math.Max(corpus, _plane.KnownVectorCount(repoId));
        return corpus <= 0 || corpus <= affordable;
    }
}
