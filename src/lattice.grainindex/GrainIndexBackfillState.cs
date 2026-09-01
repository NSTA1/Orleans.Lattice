namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The lifecycle of one index's background backfill crawl: the process that
/// onboards grains which existed before the index was declared and never
/// activate on their own.
/// </summary>
/// <remarks>
/// <para>
/// The states form a small machine. A crawl begins at
/// <see cref="NotStarted"/>, runs as <see cref="Running"/>, may be held at
/// <see cref="Paused"/> and returned to <see cref="Running"/> any number of
/// times, and settles at <see cref="Completed"/> once its key source is
/// exhausted. <see cref="Failed"/> is the one terminal-until-asked state: it
/// records that a pass threw for a reason the crawl could not absorb, and it is
/// left for an operator to resume or restart rather than retried forever.
/// </para>
/// <para>
/// The value is persisted with the crawl's checkpoint, so it survives a silo
/// restart and is what a restarted host resumes from.
/// </para>
/// </remarks>
public enum GrainIndexBackfillState
{
    /// <summary>
    /// No crawl has been started for this index. The zero value, so an index
    /// with no checkpoint at all reports it without any special case.
    /// </summary>
    NotStarted = 0,

    /// <summary>
    /// A crawl is in progress. Each pass visits at most
    /// <see cref="GrainIndexOptions.BackfillBatchSize"/> grains and advances the
    /// checkpoint, so a host that stops mid-crawl resumes rather than restarts.
    /// </summary>
    Running = 1,

    /// <summary>
    /// A crawl is held. Its checkpoint is intact and no pass runs until it is
    /// resumed, which is how an operator takes the crawl's load off a cluster
    /// without losing its progress.
    /// </summary>
    Paused = 2,

    /// <summary>
    /// The crawl exhausted its key source. The reminder driving it is
    /// unregistered at this point, so a completed index costs nothing until its
    /// declaration changes and the drift gate schedules a rebuild.
    /// </summary>
    Completed = 3,

    /// <summary>
    /// A pass failed in a way the crawl could not absorb, and the crawl stopped
    /// at its last checkpoint. Resuming retries from that checkpoint; restarting
    /// crawls the whole range again.
    /// </summary>
    Failed = 4,
}
