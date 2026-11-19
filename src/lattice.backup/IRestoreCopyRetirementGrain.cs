namespace Orleans.Lattice.Backup;

/// <summary>
/// Retires, gradually, the physical copies the shadow-cutover restores of one
/// logical tree left behind that no revert can reach any longer (issue #6103).
/// One activation per logical tree, keyed by <c>{treeId}</c>. A restore commit,
/// a revert, a release and the estate-wide sweep schedule it; it then retires at
/// most one copy per pass, a pass per <c>LatticeBackupOptions.RestoreCopyRetirementPassInterval</c>,
/// backs off exponentially on a failure, and stops - unregistering its reminder -
/// once nothing is left to retire. It never fails the caller that scheduled it.
/// </summary>
[Alias(BackupTypeAliases.IRestoreCopyRetirementGrain)]
internal interface IRestoreCopyRetirementGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records that the tree may hold a copy to retire and arms the pass reminder,
    /// first due after <paramref name="delay"/>, without waiting for it.
    /// Idempotent. Internal-origin callers only.
    /// </summary>
    Task ScheduleAsync(TimeSpan delay);

    /// <summary>
    /// Runs one retirement pass now: ends an expired revert window, retires at most
    /// one unreachable copy, then re-arms or stops its reminder. Internal-origin
    /// callers only; the reminder drives it in production.
    /// </summary>
    Task<RestoreCopyPassOutcome> RunPassAsync();
}
