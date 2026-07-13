namespace Orleans.Lattice.Backup;

/// <summary>
/// The cluster-wide, reminder-driven coordinator that periodically verifies the
/// health of every catalogued backup against the durable sink. A single activation
/// exists per cluster (keyed by <see cref="BackupHealthMonitorKey"/>); it registers
/// one recurring sweep reminder honouring <see cref="LatticeBackupHealthOptions"/>,
/// enumerates the catalog on each firing, and re-verifies each enrolled backup
/// whose per-backup cadence is due, persisting the resulting
/// <see cref="BackupHealthReport"/> so the management UI and the monitor share one
/// verification result.
/// <para>
/// The monitor is gated on a durable sink: when the configured
/// <see cref="ILatticeBackupSink"/> is not durable
/// (<see cref="ILatticeBackupSink.IsDurable"/> is <see langword="false"/>) the sweep
/// is inert - it registers no reminder and verifies nothing - because health
/// verification of payload that lives in the same ephemeral cluster the backup
/// protects proves nothing about disaster recovery.
/// </para>
/// </summary>
[Alias(BackupTypeAliases.ILatticeBackupHealthMonitorGrain)]
internal interface ILatticeBackupHealthMonitorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ensures the recurring sweep reminder is registered when monitoring is
    /// enabled and the sink is durable, or unregistered otherwise. Idempotent:
    /// repeated calls converge on a single reminder (or none). Called on silo start
    /// so the sweep begins without any operator action.
    /// </summary>
    Task EnsureStartedAsync();

    /// <summary>
    /// Runs one sweep synchronously - verifying every enrolled catalogued backup
    /// whose per-backup cadence is due and persisting each report - and returns the
    /// number of backups verified. Returns zero without doing anything when the sink
    /// is not durable or monitoring is disabled. This is the same work the sweep
    /// reminder performs; exposed so it can be driven directly in a test without
    /// waiting for the reminder cadence.
    /// </summary>
    /// <returns>The number of backups verified during the sweep.</returns>
    Task<int> SweepAsync();
}
