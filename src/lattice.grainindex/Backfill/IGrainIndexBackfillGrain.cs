namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The background backfill of one grain index, keyed by the index's name. One
/// activation per index, cluster-wide, driven by a durable reminder.
/// </summary>
/// <remarks>
/// <para>
/// The activation path only ever indexes a grain that something addresses. A
/// freshly declared index over a large, mostly dormant population would
/// therefore stay empty for those grains indefinitely. This grain closes that
/// gap: it walks the population an <see cref="IGrainKeySource"/> describes, in
/// order and in bounded batches, activating each grain that the index does not
/// already record so the grain enrols itself through the ordinary path.
/// </para>
/// <para>
/// It is reminder-driven rather than timer-driven because a crawl must survive
/// silo restarts and run on a durable cadence independent of any one activation,
/// which is the same reason the core library drives tombstone compaction from a
/// reminder. The reminder is a heartbeat that re-establishes the crawl on
/// whichever silo hosts the activation; within an activation the pacing knob is
/// <see cref="GrainIndexOptions.BackfillInterval"/>.
/// </para>
/// <para>
/// Every method is safe to call repeatedly. Starting an already-running crawl,
/// pausing a paused one, or resuming a completed one all report the current
/// state without disturbing it.
/// </para>
/// </remarks>
[Alias(TypeAliases.IGrainIndexBackfillGrain)]
internal interface IGrainIndexBackfillGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts the crawl if the index owes one, resumes an interrupted one, and
    /// restarts one whose declaration has since been replaced by an accepted
    /// rebuild. Idempotent, so every silo may call it at start.
    /// </summary>
    /// <returns>The crawl's state after the call.</returns>
    Task<GrainIndexBackfillStatus> EnsureStartedAsync();

    /// <summary>Reports the crawl's durable state.</summary>
    /// <returns>The crawl's state.</returns>
    Task<GrainIndexBackfillStatus> GetStatusAsync();

    /// <summary>
    /// Holds the crawl at its checkpoint. A no-op on a crawl that has completed
    /// or has never started.
    /// </summary>
    /// <returns>The crawl's state after the call.</returns>
    Task<GrainIndexBackfillStatus> PauseAsync();

    /// <summary>
    /// Returns a held or failed crawl to running, from its checkpoint rather
    /// than from the beginning. A no-op on a crawl that has completed or has
    /// never started.
    /// </summary>
    /// <returns>The crawl's state after the call.</returns>
    Task<GrainIndexBackfillStatus> ResumeAsync();

    /// <summary>
    /// Discards the checkpoint and crawls the whole range again, re-visiting
    /// grains the index already records so their entries are rewritten under the
    /// current declaration.
    /// </summary>
    /// <returns>The crawl's state after the call.</returns>
    Task<GrainIndexBackfillStatus> RestartAsync();

    /// <summary>
    /// Runs exactly one pass - at most
    /// <see cref="GrainIndexOptions.BackfillBatchSize"/> grains - and advances
    /// the checkpoint.
    /// </summary>
    /// <remarks>
    /// This is the same work a reminder-driven tick does, exposed so a host, an
    /// administrative surface, or a test can drive the crawl at an exact moment
    /// instead of waiting for a schedule. A pass on a crawl that is not running
    /// does nothing and reports the current state.
    /// </remarks>
    /// <returns>What the pass did.</returns>
    Task<GrainIndexBackfillBatchResult> RunBatchAsync();
}
