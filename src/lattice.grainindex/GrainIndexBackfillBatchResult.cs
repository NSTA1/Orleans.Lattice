using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// What one backfill pass did: how many grains it visited, how many it onboarded,
/// how many were already indexed, how many failed, and the state the crawl was
/// left in.
/// </summary>
/// <remarks>
/// This is the per-pass counterpart to <see cref="GrainIndexBackfillStatus"/>,
/// which reports the crawl's running totals. A pass is capped at
/// <see cref="GrainIndexOptions.BackfillBatchSize"/>, so
/// <see cref="Visited"/> reaching that cap is exactly the observation that the
/// crawl paced itself rather than draining the whole population at once.
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexBackfillBatchResult)]
public readonly record struct GrainIndexBackfillBatchResult
{
    /// <summary>Initialises a result.</summary>
    /// <param name="visited">The number of keys the pass took from the key source.</param>
    /// <param name="enrolled">The number of grains the pass activated for indexing.</param>
    /// <param name="skipped">The number of keys already recorded as indexed.</param>
    /// <param name="failed">The number of keys whose activation threw.</param>
    /// <param name="state">The crawl's state once the pass finished.</param>
    /// <param name="exhausted">Whether the key source had no more keys to give.</param>
    public GrainIndexBackfillBatchResult(
        int visited,
        int enrolled,
        int skipped,
        int failed,
        GrainIndexBackfillState state,
        bool exhausted)
    {
        Visited = visited;
        Enrolled = enrolled;
        Skipped = skipped;
        Failed = failed;
        State = state;
        Exhausted = exhausted;
    }

    /// <summary>The number of keys the pass took from the key source.</summary>
    [Id(0)]
    public int Visited { get; init; }

    /// <summary>The number of grains the pass activated so they would index themselves.</summary>
    [Id(1)]
    public int Enrolled { get; init; }

    /// <summary>
    /// The number of keys the pass skipped because the index already records the
    /// grain, which is what stops a crawl re-projecting a population the
    /// activation path has already onboarded.
    /// </summary>
    [Id(2)]
    public int Skipped { get; init; }

    /// <summary>
    /// The number of keys whose activation threw. A failed key is left for the
    /// next full crawl rather than retried inside the pass, so one unreachable
    /// grain never stalls the ones behind it.
    /// </summary>
    [Id(3)]
    public int Failed { get; init; }

    /// <summary>The crawl's state once the pass finished.</summary>
    [Id(4)]
    public GrainIndexBackfillState State { get; init; }

    /// <summary>
    /// Whether the key source had no more keys to give, which is the condition
    /// that moves the crawl to
    /// <see cref="GrainIndexBackfillState.Completed"/>.
    /// </summary>
    [Id(5)]
    public bool Exhausted { get; init; }

    /// <summary>A pass that did no work, as a paused or completed crawl reports.</summary>
    /// <param name="state">The crawl's state.</param>
    /// <returns>An empty result carrying <paramref name="state"/>.</returns>
    public static GrainIndexBackfillBatchResult None(GrainIndexBackfillState state) =>
        new(visited: 0, enrolled: 0, skipped: 0, failed: 0, state, exhausted: false);
}
