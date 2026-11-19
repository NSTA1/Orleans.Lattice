namespace Orleans.Lattice.Backup;

/// <summary>
/// The estate-wide safety net for restore-copy retirement (issue #6103): one
/// activation, keyed by <see cref="SingletonKey"/>, that on a reminder every
/// <see cref="LatticeBackupOptions.RestoreCopySweepInterval"/> finds every logical
/// tree that has a registered restore shadow and schedules its
/// <see cref="IRestoreCopyRetirementGrain"/>, one tree a retirement pass apart.
/// It is how copies left behind by a build that predates retirement, or by a
/// cutover that is not a restore, are cleaned up without an operator.
/// </summary>
[Alias(BackupTypeAliases.IRestoreCopySweepGrain)]
internal interface IRestoreCopySweepGrain : IGrainWithStringKey
{
    /// <summary>The single key the sweep runs under.</summary>
    const string SingletonKey = "restore-copy-sweep";

    /// <summary>
    /// Arms the sweep reminder when the sweep is enabled and it is not armed yet, or
    /// removes it when the sweep is disabled. Idempotent, and safe to call from every
    /// silo at start-up: an armed reminder keeps its schedule.
    /// </summary>
    Task EnsureStartedAsync();

    /// <summary>
    /// Runs one sweep now and returns how many trees it scheduled. Internal-origin
    /// callers only; the reminder drives it in production.
    /// </summary>
    Task<int> SweepAsync();
}
