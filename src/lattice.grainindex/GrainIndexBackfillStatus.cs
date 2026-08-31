using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The queryable state of one index's background backfill: where the crawl has
/// reached, what it has done so far, and why it stopped if it did.
/// </summary>
/// <remarks>
/// <para>
/// This is the projection of the durable checkpoint the crawl keeps in the
/// index-registry system tree, so it is answered from persisted state rather
/// than from whatever a live activation happens to remember. A silo that has
/// never run a pass reports the same status as the silo that stopped
/// mid-crawl.
/// </para>
/// <para>
/// It is the read half of the control surface: an administrative or
/// observability layer reports these figures and drives the crawl with the pause,
/// resume, and restart primitives.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexBackfillStatus)]
public sealed class GrainIndexBackfillStatus
{
    /// <summary>Initialises a status.</summary>
    /// <param name="indexName">The index the crawl belongs to. Must not be <c>null</c>.</param>
    /// <param name="state">The crawl's lifecycle state.</param>
    /// <param name="resumeAfterKey">The last key the crawl visited, or <c>null</c> when it has visited none.</param>
    /// <param name="visited">The running total of keys taken from the key source.</param>
    /// <param name="enrolled">The running total of grains activated for indexing.</param>
    /// <param name="skipped">The running total of keys already recorded as indexed.</param>
    /// <param name="failed">The running total of keys whose activation threw.</param>
    /// <param name="passes">The number of passes the crawl has run.</param>
    /// <param name="revisitsEnrolled">Whether this run re-visits grains the index already records.</param>
    /// <param name="startedUtc">When the current run began, or <c>null</c>.</param>
    /// <param name="updatedUtc">When the crawl last advanced, or <c>null</c>.</param>
    /// <param name="completedUtc">When the crawl exhausted its key source, or <c>null</c>.</param>
    /// <param name="failureMessage">Why the last pass failed, or <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public GrainIndexBackfillStatus(
        string indexName,
        GrainIndexBackfillState state,
        string? resumeAfterKey,
        long visited,
        long enrolled,
        long skipped,
        long failed,
        long passes,
        bool revisitsEnrolled,
        DateTimeOffset? startedUtc,
        DateTimeOffset? updatedUtc,
        DateTimeOffset? completedUtc,
        string? failureMessage)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        IndexName = indexName;
        State = state;
        ResumeAfterKey = resumeAfterKey;
        Visited = visited;
        Enrolled = enrolled;
        Skipped = skipped;
        Failed = failed;
        Passes = passes;
        RevisitsEnrolled = revisitsEnrolled;
        StartedUtc = startedUtc;
        UpdatedUtc = updatedUtc;
        CompletedUtc = completedUtc;
        FailureMessage = failureMessage;
    }

    /// <summary>The index the crawl belongs to.</summary>
    [Id(0)]
    public string IndexName { get; }

    /// <summary>The crawl's lifecycle state.</summary>
    [Id(1)]
    public GrainIndexBackfillState State { get; }

    /// <summary>
    /// The last key the crawl visited, or <c>null</c> when it has visited none.
    /// The next pass asks the key source for the keys strictly after it, which
    /// is what makes a resumed crawl neither repeat nor skip.
    /// </summary>
    [Id(2)]
    public string? ResumeAfterKey { get; }

    /// <summary>The running total of keys taken from the key source.</summary>
    [Id(3)]
    public long Visited { get; }

    /// <summary>The running total of grains the crawl activated so they would index themselves.</summary>
    [Id(4)]
    public long Enrolled { get; }

    /// <summary>The running total of keys skipped because the index already records the grain.</summary>
    [Id(5)]
    public long Skipped { get; }

    /// <summary>The running total of keys whose activation threw.</summary>
    [Id(6)]
    public long Failed { get; }

    /// <summary>The number of passes the crawl has run, which is how a caller sees it pacing.</summary>
    [Id(7)]
    public long Passes { get; }

    /// <summary>
    /// Whether this run re-visits grains the index already records. A first
    /// backfill skips them; a rebuild scheduled by the drift gate, or an operator
    /// restart, re-visits them so their entries are rewritten under the new
    /// declaration.
    /// </summary>
    [Id(8)]
    public bool RevisitsEnrolled { get; }

    /// <summary>When the current run began, or <c>null</c> when none has.</summary>
    [Id(9)]
    public DateTimeOffset? StartedUtc { get; }

    /// <summary>When the crawl last advanced, or <c>null</c> when it never has.</summary>
    [Id(10)]
    public DateTimeOffset? UpdatedUtc { get; }

    /// <summary>When the crawl exhausted its key source, or <c>null</c> when it has not.</summary>
    [Id(11)]
    public DateTimeOffset? CompletedUtc { get; }

    /// <summary>
    /// Why the last pass failed, or <c>null</c>. Retained across a resume so the
    /// reason a crawl stalled is not lost the moment somebody restarts it.
    /// </summary>
    [Id(12)]
    public string? FailureMessage { get; }

    /// <summary>
    /// The status of an index whose crawl has never been started, which is what
    /// a read reports when no checkpoint has been written.
    /// </summary>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <returns>A <see cref="GrainIndexBackfillState.NotStarted"/> status.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public static GrainIndexBackfillStatus NotStarted(string indexName) =>
        new(
            indexName,
            GrainIndexBackfillState.NotStarted,
            resumeAfterKey: null,
            visited: 0,
            enrolled: 0,
            skipped: 0,
            failed: 0,
            passes: 0,
            revisitsEnrolled: false,
            startedUtc: null,
            updatedUtc: null,
            completedUtc: null,
            failureMessage: null);
}
