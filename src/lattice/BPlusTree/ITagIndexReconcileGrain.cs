namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Per-index background reconciliation coordinator for a tag index. One
/// activation exists per index, keyed by <c>{indexName}</c> (the same name that
/// resolves the sibling index tree <c>tag-{indexName}</c>).
/// <para>
/// The coordinator registers a recurring schedule reminder (default hourly)
/// when the index is first materialised, and on each firing runs a digest-gated
/// sweep: it compares every covered tree's current leaf-projection digest
/// against the baseline captured at the last successful reconcile, deep-scans
/// and repairs only the trees whose digests diverge, then advances the baseline.
/// A clean index incurs only digest-probe cost. The sweep runs to completion via
/// the shared coordinator keepalive + phase-timer machinery, so it is crash- and
/// restart-safe and resumable from its persisted cursor.
/// </para>
/// </summary>
[Alias(TypeAliases.ITagIndexReconcileGrain)]
internal interface ITagIndexReconcileGrain : IGrainWithStringKey
{
    /// <summary>
    /// Registers (or updates) the recurring schedule reminder that drives
    /// background reconciliation, honouring the per-index
    /// <see cref="LatticeTagIndexReconciliationOptions"/>. Idempotent: repeated
    /// calls converge on a single schedule. When reconciliation is disabled for
    /// the index, any existing schedule is unregistered instead. Invoked by the
    /// tag-index write path the first time a tree is covered by the index.
    /// </summary>
    Task EnsureScheduleAsync();

    /// <summary>
    /// Runs a single digest-gated reconciliation sweep synchronously to
    /// completion and returns its summary. Bypasses the schedule reminder and
    /// phase timer; intended for tests and manual operator-driven sweeps.
    /// </summary>
    Task<TagReconcileReport> RunSweepAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator is idle - either no sweep has
    /// been started, or the last one has run to completion. Returns <c>false</c>
    /// while a sweep is in flight.
    /// </summary>
    Task<bool> IsIdleAsync();
}
