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
/// <para>
/// <b>Both guards report what they did, at information level.</b> A guard whose
/// steady state is invisible cannot be verified, because its silence is
/// indistinguishable from its absence - and before issue #2253 both of these had
/// that shape. Every distinct decision either guard reaches is announced once per
/// repository at information level, and its steady state is carried by the
/// periodic summary <see cref="RepoContextRetrievalGuardReporter"/> paces, so a
/// repeat-skip or a repeated budget skip is counted rather than logged per query.
/// The two zeros this ladder can produce stay distinguishable: no budget
/// evaluations at all means the budget was never reached, whereas evaluations
/// with no skips means it was reached and declined.
/// </para>
/// </summary>
internal sealed class AnnRepoContextSemanticIndex : IRepoContextSemanticIndex
{
    private readonly IRepoContextAnnIndex _plane;
    private readonly IRepoContextSemanticIndex _exact;
    private readonly RepoContextExactScanBudget _exactScanBudget;
    private readonly RepoContextExactScanBreaker _exactScanBreaker;
    private readonly RepoContextRetrievalGuardReporter _guards;
    private readonly ILogger<AnnRepoContextSemanticIndex> _logger;

    /// <summary>Creates the approximate-first semantic index.</summary>
    /// <param name="plane">The approximate retrieval plane. Must not be <see langword="null"/>.</param>
    /// <param name="exact">The exact scan used while the plane is building, and kept as the correctness oracle. Must not be <see langword="null"/>.</param>
    /// <param name="exactScanBudget">The budget deciding whether an exact gather can finish under the tree's configured scan-page bounds. Must not be <see langword="null"/>.</param>
    /// <param name="exactScanBreaker">The record of gathers that have already proved they cannot finish. Must not be <see langword="null"/>.</param>
    /// <param name="guards">The counters that make both guards' operating state readable from an information-level container. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the fallback report is written to. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public AnnRepoContextSemanticIndex(
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        RepoContextExactScanBudget exactScanBudget,
        RepoContextExactScanBreaker exactScanBreaker,
        RepoContextRetrievalGuardReporter guards,
        ILogger<AnnRepoContextSemanticIndex> logger)
    {
        ArgumentNullException.ThrowIfNull(plane);
        ArgumentNullException.ThrowIfNull(exact);
        ArgumentNullException.ThrowIfNull(exactScanBudget);
        ArgumentNullException.ThrowIfNull(exactScanBreaker);
        ArgumentNullException.ThrowIfNull(guards);
        ArgumentNullException.ThrowIfNull(logger);
        _plane = plane;
        _exact = exact;
        _exactScanBudget = exactScanBudget;
        _exactScanBreaker = exactScanBreaker;
        _guards = guards;
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

        _guards.RecordSearch(repoId);
        try
        {
            return await SearchCoreAsync(repoId, query, querySpace, k, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            // In a finally so the summary is paced by queries rather than by
            // outcomes: a ladder that only ever faults is exactly the state an
            // operator most needs a periodic reading of.
            ReportGuardsIfDue(repoId);
        }
    }

    private async Task<IReadOnlyList<RepoContextVectorMatch>> SearchCoreAsync(
        string repoId,
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag querySpace,
        int k,
        CancellationToken cancellationToken)
    {
        var outcome = await _plane
            .SearchAsync(repoId, query, querySpace, k, cancellationToken)
            .ConfigureAwait(false);

        // Recorded for every outcome, Bootstrapping included, so the instrument
        // partitions the whole query population rather than only its serving half.
        // A zero on one state is then denominated by a total that rises with
        // traffic, which is what makes it a measured absence.
        if (_guards.RecordPlaneOutcome(repoId, outcome.State)
            && outcome.State != RepoContextAnnServingState.Bootstrapping)
        {
            // The first time this repository is served by the plane at all, and
            // again the first time it is served from a trained partitioning. The
            // second of those is the transition issue #2252 says has never been
            // observed in any deployment, so it is stated rather than left to be
            // inferred from a search response the container never sees.
            _logger.LogInformation(
                "Repository-context approximate plane served {RepoId} in space {ModelId}/{Dimension} from state "
                + "{State} for the first time in this process: {Meaning} Further outcomes of the same kind are "
                + "counted into the periodic retrieval-ladder guard summary and onto the "
                + "repocontext.retrieval.ann.search instrument rather than logged per query.",
                repoId,
                querySpace.ModelId,
                querySpace.Dimension,
                outcome.State,
                DescribeServingState(outcome.State));
        }

        if (outcome.State != RepoContextAnnServingState.Bootstrapping)
        {
            // The plane answered for itself, so the build that a stalled gather was
            // competing with is no longer holding the tree. Restoring the fallback
            // here is what keeps a trip from outliving its cause: no cooldown to
            // wait out, and a later rebuild re-arms the breaker on its own evidence.
            if (_exactScanBreaker.Reset(repoId))
            {
                // Rare by construction - once per trip - and the transition issue
                // #2253 singles out, because it is the first execution of a path
                // that has never run in production. Logged every time, not once.
                _guards.RecordBreakerReset(repoId);
                _logger.LogInformation(
                    "Repository-context exact-scan breaker for {RepoId} closed: the approximate plane answered for "
                    + "itself in space {ModelId}/{Dimension}, so the contention that stalled the gather is gone and "
                    + "the exact fallback is armed again.",
                    repoId,
                    querySpace.ModelId,
                    querySpace.Dimension);
            }

            return outcome.Matches;
        }

        if (_exactScanBreaker.IsTripped(repoId))
        {
            // A gather over this repository has already spent a full page-fill
            // ceiling and faulted. Nothing about the plane still building makes the
            // next one cheaper, so it is not started. This is the branch that holds
            // when the corpus is uncounted and the budget below therefore fails
            // open - the state the whole bootstrap window is in.
            //
            // It runs on every subsequent query, so it is announced once at
            // information level (which is the proof the path executed at all) and
            // counted thereafter. Logging it per query at an operator-visible level
            // would trade an unreadable state for a flood.
            if (_guards.RecordBreakerRepeatSkip(repoId))
            {
                _logger.LogInformation(
                    "Repository-context exact-scan breaker for {RepoId} is open and suppressed its first gather in "
                    + "space {ModelId}/{Dimension}: a gather over this repository has already stalled while the "
                    + "approximate index builds. Serving keyword recall until the plane answers for itself. Further "
                    + "suppressed gathers are counted into the periodic retrieval-ladder guard summary rather than "
                    + "logged per query.",
                    repoId,
                    querySpace.ModelId,
                    querySpace.Dimension);
            }
            else
            {
                _logger.LogDebug(
                    "Repository-context semantic search for {RepoId} in space {ModelId}/{Dimension} skipped the exact "
                    + "scan: a gather over this repository has already stalled while the approximate index builds. "
                    + "Serving keyword recall until the plane answers for itself.",
                    repoId,
                    querySpace.ModelId,
                    querySpace.Dimension);
            }

            return Array.Empty<RepoContextVectorMatch>();
        }

        var decision = EvaluateExactScanBudget(repoId, querySpace, out var corpus, out var affordable);
        if (_guards.RecordBudgetDecision(repoId, decision, corpus, affordable))
        {
            // The budget reaches the same decision on every query once the ladder
            // settles, so each distinct decision is announced once and counted
            // thereafter. This line is what resolves the ambiguity issue #2253 was
            // filed over: a budget that never appears here was never reached, and a
            // budget that appears with CorpusUnknown was reached and declined.
            _logger.LogInformation(
                "Repository-context exact-scan budget for {RepoId} in space {ModelId}/{Dimension} reached decision "
                + "{Decision} for the first time: {Reason} Corpus {Corpus} vector(s) against an affordable "
                + "{Affordable}. Further outcomes of the same kind are counted into the periodic "
                + "retrieval-ladder guard summary rather than logged per query.",
                repoId,
                querySpace.ModelId,
                querySpace.Dimension,
                decision,
                DescribeBudgetDecision(decision),
                corpus,
                DescribeAffordable(affordable));
        }

        if (decision == RepoContextExactScanBudgetDecision.Exceeded)
        {
            // The gather would range-scan the whole vector-metadata prefix, and the
            // tree's own configuration says a scan that size cannot fill its pages
            // inside the stall ceiling. Starting it would spend that ceiling, fault,
            // and arrive at keyword recall anyway - while loading the tree the build
            // is streaming, so the fallback delays its own resolution. Reporting no
            // matches takes the same destination directly, and the search service
            // resolves it to the keyword.vector_plane_unavailable cause, which is the
            // documented "the plane is still building" answer.
            _logger.LogDebug(
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
            _guards.RecordBreakerTrip(repoId);
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
    /// Emits the periodic reading of both guards' counters when one is due. This is
    /// the line an operator reads to answer "did this guard act, and if not, why
    /// not" from a container running at information level.
    /// </summary>
    /// <param name="repoId">The repository the search was served for.</param>
    private void ReportGuardsIfDue(string repoId)
    {
        if (!_guards.TryTakeSummary(repoId, out var guards))
        {
            return;
        }

        _logger.LogInformation(
            "Repository-context retrieval-ladder guards for {RepoId}, cumulative since process start: {Searches} "
            + "search(es), of which the approximate plane answered {PlaneServed} so neither guard was consulted and "
            + "{Bootstrapping} reached the fallback. Of the plane's answers, {PlaneApproximate} came from a trained "
            + "partitioning and {PlaneExhaustive} from an exhaustive scan of the vectors it holds, so a zero in the "
            + "first against a non-zero search count is a measured absence of approximate retrieval rather than an "
            + "absent measurement. Exact-scan budget: {BudgetEvaluations} evaluation(s) - "
            + "{BudgetUnbounded} with no bound configured, {BudgetCorpusUnknown} declined for an uncounted corpus, "
            + "{BudgetWithinBudget} cleared as affordable, {BudgetExceeded} skipped as unaffordable; last read "
            + "corpus {Corpus} against an affordable {Affordable}. Exact-scan breaker: currently {BreakerState}, "
            + "{BreakerTrips} trip(s), {BreakerRepeatSkips} gather(s) suppressed while open, {BreakerResets} "
            + "reset(s). Zero evaluations means the guard was never reached; evaluations with zero skips means it "
            + "was reached and declined.",
            repoId,
            guards.Searches,
            guards.PlaneServed,
            guards.Bootstrapping,
            guards.PlaneApproximate,
            guards.PlaneExhaustive,
            guards.BudgetEvaluations,
            guards.BudgetUnbounded,
            guards.BudgetCorpusUnknown,
            guards.BudgetWithinBudget,
            guards.BudgetExceeded,
            guards.LastCorpus,
            DescribeAffordable(guards.LastAffordable),
            _exactScanBreaker.IsTripped(repoId) ? "open" : "closed",
            guards.BreakerTrips,
            guards.BreakerRepeatSkips,
            guards.BreakerResets);
    }

    /// <summary>
    /// What a serving state means in the operator's terms. Kept beside the state
    /// rather than in the log template so every emission carries the same
    /// explanation, and so the distinction issue #2252 turns on - a plane that
    /// answers at all against a plane that answers from a trained partitioning - is
    /// stated in words rather than left to an enumeration name.
    /// </summary>
    /// <param name="state">The state to describe.</param>
    /// <returns>A sentence ending in a full stop.</returns>
    internal static string DescribeServingState(RepoContextAnnServingState state) => state switch
    {
        RepoContextAnnServingState.Approximate =>
            "the index answered from its trained partitioning, so recall is bounded by the published target and "
            + "query cost is sub-linear in the corpus. This is the steady state the plane exists to reach.",
        RepoContextAnnServingState.Exhaustive =>
            "the index answered by exhaustive scan of the vectors it holds, because its corpus is below the "
            + "training threshold or training has not run yet. Recall over the indexed corpus is complete; the "
            + "index is warming up, not degraded.",
        _ =>
            "no usable index exists for this repository and embedding space yet, so the fallback ladder ran.",
    };

    /// <summary>
    /// Why a budget decision came out the way it did, in the operator's terms. Kept
    /// beside the decision rather than in the log template so every emission of a
    /// decision carries the same explanation.
    /// </summary>
    /// <param name="decision">The decision to describe.</param>
    /// <returns>A sentence ending in a full stop.</returns>
    internal static string DescribeBudgetDecision(RepoContextExactScanBudgetDecision decision) => decision switch
    {
        RepoContextExactScanBudgetDecision.Unbounded =>
            "the vector-metadata tree's scan-page configuration disables the bound, so the budget cannot skip a "
            + "gather however large the corpus is.",
        RepoContextExactScanBudgetDecision.CorpusUnknown =>
            "the bound applies but no corpus count exists yet, so the budget failed open and the gather ran. The "
            + "breaker is what covers this window.",
        RepoContextExactScanBudgetDecision.WithinBudget =>
            "the corpus is counted and fits inside the budget, so the gather ran.",
        _ =>
            "the corpus is counted and exceeds the budget, so the gather was skipped and keyword recall served "
            + "instead of a scan that cannot complete.",
    };

    /// <summary>
    /// Renders an affordable vector count for a log line, naming the fail-open
    /// sentinel rather than printing <see cref="int.MaxValue"/>, which reads as a
    /// very large threshold and is not one.
    /// </summary>
    /// <param name="affordable">The budget's reported affordable vector count.</param>
    /// <returns>The rendered value.</returns>
    internal static string DescribeAffordable(int affordable)
        => affordable == RepoContextExactScanBudget.Unbounded
            ? "unbounded"
            : affordable.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// What the exact-scan budget concludes about a gather over this repository and
    /// embedding space, under the vector-metadata tree's configured scan-page
    /// bounds.
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
    /// <b>It reports which fail-open branch it took, not merely that it took
    /// one.</b> <see cref="RepoContextExactScanBudgetDecision.Unbounded"/> and
    /// <see cref="RepoContextExactScanBudgetDecision.CorpusUnknown"/> both run the
    /// gather, but for unrelated reasons: the first says the configuration
    /// disables the guard, the second says the guard is armed and has nothing to
    /// judge against yet. Returning a single boolean collapsed them, and that
    /// collapse is what made the budget's field behaviour unattributable in issue
    /// #2253.
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
    /// <returns>The decision. Only <see cref="RepoContextExactScanBudgetDecision.Exceeded"/> skips the gather.</returns>
    private RepoContextExactScanBudgetDecision EvaluateExactScanBudget(
        string repoId, EmbeddingSpaceTag space, out int corpus, out int affordable)
    {
        corpus = 0;
        affordable = _exactScanBudget.AffordableVectorCount;
        if (affordable == RepoContextExactScanBudget.Unbounded)
        {
            return RepoContextExactScanBudgetDecision.Unbounded;
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
        if (corpus <= 0)
        {
            return RepoContextExactScanBudgetDecision.CorpusUnknown;
        }

        return corpus <= affordable
            ? RepoContextExactScanBudgetDecision.WithinBudget
            : RepoContextExactScanBudgetDecision.Exceeded;
    }
}
