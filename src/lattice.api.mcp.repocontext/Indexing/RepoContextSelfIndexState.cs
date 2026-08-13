namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The persisted checkpoint of one repository's self-index grain, stored under the
/// grain's key (the repository id). It records just enough to resume the
/// repository's paged gap scan approximately after a host restart - the resume key
/// within the file scan - plus the earliest time a fresh full scan may begin, so a
/// completed scan spaces itself out rather than spinning. The grain is keyed by
/// repository id, so the repository identity is the grain key and is deliberately
/// not duplicated here.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoContextSelfIndexState)]
internal sealed class RepoContextSelfIndexState
{
    /// <summary>
    /// The inclusive structural file key the scan resumes from, or null when no
    /// scan is in progress (before the first scan, or during the cooldown between
    /// scans). A non-null value means a scan is mid-flight and the next tick
    /// continues it regardless of the cooldown.
    /// </summary>
    [Id(0)]
    public string? ResumeKey { get; set; }

    /// <summary>
    /// The earliest UTC tick count at which a new full scan may begin, set when a
    /// scan completes so scans are spaced by a jittered cooldown. A tick before
    /// this time, when no scan is mid-flight, is a cheap no-op.
    /// </summary>
    [Id(1)]
    public long NextSweepAfterTicks { get; set; }

    /// <summary>
    /// The earliest UTC tick count at which a new periodic content reconcile may
    /// run, set when one completes so reconciles are spaced by a jittered interval
    /// that is longer than the gap-scan cooldown. A reconcile re-drives the full
    /// idempotent index (walk, reconcile, prune, vectorise) so on-disk edits and
    /// deletions are picked up automatically between the cheaper presence-only gap
    /// scans. This is a local scheduling deadline only (an Orleans timer concern),
    /// deliberately distinct from the hybrid-logical-clock anchors that order the
    /// structural records.
    /// </summary>
    [Id(2)]
    public long NextReconcileAfterTicks { get; set; }
}
