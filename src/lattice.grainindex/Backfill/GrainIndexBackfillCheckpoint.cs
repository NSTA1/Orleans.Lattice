using Orleans.Concurrency;
using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The durable resume point of one index's background backfill, persisted in the
/// index-registry system tree under
/// <see cref="GrainIndexRegistryKeys.Checkpoint(string)"/>.
/// </summary>
/// <remarks>
/// <para>
/// One value per index, rewritten once per pass rather than once per grain: a
/// crawl that checkpointed per grain would double its registry traffic to buy
/// nothing, because re-visiting the handful of grains in an interrupted pass is
/// already idempotent. Re-projecting an unchanged grain produces an empty plan
/// and writes nothing, so replaying a pass costs a read and no write.
/// </para>
/// <para>
/// The checkpoint carries the fingerprint of the declaration the crawl is
/// running under. That single field is what couples the crawl to the drift gate:
/// when the registry reconciler adopts a breaking change and schedules a
/// rebuild, the stored fingerprint moves, the crawl notices its own no longer
/// matches, and it restarts over the whole range instead of resuming a crawl
/// that was describing a different index.
/// </para>
/// <para>
/// It lives in the registry tree beside the definition and the seen markers
/// rather than in per-grain state, so it is readable without addressing any
/// application grain and is removed with the rest of an index's bookkeeping.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexBackfillCheckpoint)]
internal sealed class GrainIndexBackfillCheckpoint
{
    /// <summary>Initialises a checkpoint.</summary>
    /// <param name="state">The crawl's lifecycle state.</param>
    /// <param name="fingerprint">The declaration fingerprint the crawl is running under.</param>
    /// <param name="resumeAfterKey">The last key visited, or <c>null</c>.</param>
    /// <param name="visited">The running total of keys taken from the key source.</param>
    /// <param name="enrolled">The running total of grains activated for indexing.</param>
    /// <param name="skipped">The running total of keys already recorded as indexed.</param>
    /// <param name="failed">The running total of keys whose activation threw.</param>
    /// <param name="passes">The number of passes run.</param>
    /// <param name="revisitsEnrolled">Whether this run re-visits grains the index already records.</param>
    /// <param name="startedUtc">When the current run began, or <c>null</c>.</param>
    /// <param name="updatedUtc">When the crawl last advanced, or <c>null</c>.</param>
    /// <param name="completedUtc">When the crawl exhausted its key source, or <c>null</c>.</param>
    /// <param name="failureMessage">Why the last pass failed, or <c>null</c>.</param>
    public GrainIndexBackfillCheckpoint(
        GrainIndexBackfillState state,
        GrainIndexFingerprint fingerprint,
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
        State = state;
        Fingerprint = fingerprint;
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

    /// <summary>The crawl's lifecycle state.</summary>
    [Id(0)]
    public GrainIndexBackfillState State { get; }

    /// <summary>
    /// The fingerprint of the declaration this run is crawling under. A run
    /// whose fingerprint no longer matches the registry record's is crawling a
    /// declaration that has since been replaced, and is restarted rather than
    /// resumed.
    /// </summary>
    [Id(1)]
    public GrainIndexFingerprint Fingerprint { get; }

    /// <summary>The last key visited, or <c>null</c> when the run has visited none.</summary>
    [Id(2)]
    public string? ResumeAfterKey { get; }

    /// <summary>The running total of keys taken from the key source.</summary>
    [Id(3)]
    public long Visited { get; }

    /// <summary>The running total of grains activated so they would index themselves.</summary>
    [Id(4)]
    public long Enrolled { get; }

    /// <summary>The running total of keys skipped because the index already records the grain.</summary>
    [Id(5)]
    public long Skipped { get; }

    /// <summary>The running total of keys whose activation threw.</summary>
    [Id(6)]
    public long Failed { get; }

    /// <summary>The number of passes this run has completed.</summary>
    [Id(7)]
    public long Passes { get; }

    /// <summary>
    /// Whether this run re-visits grains the index already records, which a
    /// rebuild does and a first backfill does not.
    /// </summary>
    [Id(8)]
    public bool RevisitsEnrolled { get; }

    /// <summary>When the current run began, or <c>null</c>.</summary>
    [Id(9)]
    public DateTimeOffset? StartedUtc { get; }

    /// <summary>When the crawl last advanced, or <c>null</c>.</summary>
    [Id(10)]
    public DateTimeOffset? UpdatedUtc { get; }

    /// <summary>When the crawl exhausted its key source, or <c>null</c>.</summary>
    [Id(11)]
    public DateTimeOffset? CompletedUtc { get; }

    /// <summary>Why the last pass failed, or <c>null</c>.</summary>
    [Id(12)]
    public string? FailureMessage { get; }

    /// <summary>
    /// A fresh run over the whole range under <paramref name="fingerprint"/>.
    /// </summary>
    /// <param name="fingerprint">The declaration fingerprint to crawl under.</param>
    /// <param name="revisitsEnrolled">Whether the run re-visits already-indexed grains.</param>
    /// <param name="startedUtc">When the run began.</param>
    /// <returns>A <see cref="GrainIndexBackfillState.Running"/> checkpoint at the start of the range.</returns>
    public static GrainIndexBackfillCheckpoint Start(
        GrainIndexFingerprint fingerprint,
        bool revisitsEnrolled,
        DateTimeOffset startedUtc) =>
        new(
            GrainIndexBackfillState.Running,
            fingerprint,
            resumeAfterKey: null,
            visited: 0,
            enrolled: 0,
            skipped: 0,
            failed: 0,
            passes: 0,
            revisitsEnrolled,
            startedUtc,
            updatedUtc: startedUtc,
            completedUtc: null,
            failureMessage: null);

    /// <summary>This checkpoint advanced by one pass.</summary>
    /// <param name="resumeAfterKey">The last key the pass visited, or <c>null</c> to keep the current position.</param>
    /// <param name="visited">The keys the pass took from the key source.</param>
    /// <param name="enrolled">The grains the pass activated.</param>
    /// <param name="skipped">The keys the pass skipped as already indexed.</param>
    /// <param name="failed">The keys whose activation threw.</param>
    /// <param name="updatedUtc">When the pass finished.</param>
    /// <returns>The advanced checkpoint.</returns>
    public GrainIndexBackfillCheckpoint Advance(
        string? resumeAfterKey,
        int visited,
        int enrolled,
        int skipped,
        int failed,
        DateTimeOffset updatedUtc) =>
        new(
            State,
            Fingerprint,
            resumeAfterKey ?? ResumeAfterKey,
            Visited + visited,
            Enrolled + enrolled,
            Skipped + skipped,
            Failed + failed,
            Passes + 1,
            RevisitsEnrolled,
            StartedUtc,
            updatedUtc,
            CompletedUtc,
            FailureMessage);

    /// <summary>
    /// This checkpoint moved to <paramref name="state"/>, leaving its position
    /// and totals untouched so a pause and resume lose nothing.
    /// </summary>
    /// <param name="state">The state to move to.</param>
    /// <param name="updatedUtc">When the transition happened.</param>
    /// <param name="failureMessage">
    /// Why the crawl failed, or <c>null</c> to keep whatever reason is recorded.
    /// </param>
    /// <returns>The transitioned checkpoint.</returns>
    public GrainIndexBackfillCheckpoint WithState(
        GrainIndexBackfillState state,
        DateTimeOffset updatedUtc,
        string? failureMessage = null) =>
        new(
            state,
            Fingerprint,
            ResumeAfterKey,
            Visited,
            Enrolled,
            Skipped,
            Failed,
            Passes,
            RevisitsEnrolled,
            StartedUtc,
            updatedUtc,
            state == GrainIndexBackfillState.Completed ? updatedUtc : CompletedUtc,
            failureMessage ?? FailureMessage);

    /// <summary>Projects the checkpoint into the status an administrative caller reads.</summary>
    /// <param name="indexName">The index the crawl belongs to. Must not be <c>null</c>.</param>
    /// <returns>The status.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public GrainIndexBackfillStatus ToStatus(string indexName) =>
        new(
            indexName,
            State,
            ResumeAfterKey,
            Visited,
            Enrolled,
            Skipped,
            Failed,
            Passes,
            RevisitsEnrolled,
            StartedUtc,
            UpdatedUtc,
            CompletedUtc,
            FailureMessage);
}
