namespace Orleans.Lattice.Backup;

/// <summary>The outcome of <see cref="ILatticeBackupSchedulerGrain.RetireAsync"/>.</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupRetireOutcome)]
internal enum BackupRetireOutcome
{
    /// <summary>The backup (or the orphan row) was removed.</summary>
    Removed = 0,

    /// <summary>
    /// Nothing was removed: the row is gone, or a scrubbed row became resolvable
    /// again (a capture republished it).
    /// </summary>
    Unchanged = 1,

    /// <summary>The id is held by a publish or another retirement beyond one call's wait; the caller retries.</summary>
    Busy = 2,

    /// <summary>
    /// An operator delete's end found a catalog row listing the id again - a
    /// capture or a rebuild listed it once the delete's lease had run out - and
    /// kept that backup, so the delete did not delete it (issue #5639). Decided
    /// under the id's hold, so a capture the end itself unblocks is never
    /// mistaken for one.
    /// </summary>
    Kept = 3,
}
