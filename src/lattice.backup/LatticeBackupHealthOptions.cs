namespace Orleans.Lattice.Backup;

/// <summary>
/// Cluster-wide configuration for the periodic backup-health monitor. Unlike
/// scheduling and retention, health monitoring is <b>on by default</b>: every
/// catalogued backup is auto-enrolled and re-verified at
/// <see cref="DefaultInterval"/> so a silently corrupted or deleted sink payload is
/// surfaced without any opt-in. A per-backup <see cref="BackupHealthConfig"/> can
/// override the default enrolment and cadence for a single backup.
/// <para>
/// The monitor is only meaningful against a durable, external sink; with the
/// ephemeral in-cluster sink it stays inert regardless of these options, because
/// verifying payload that lives in the same cluster the backup protects proves
/// nothing about disaster recovery.
/// </para>
/// </summary>
public sealed class LatticeBackupHealthOptions
{
    /// <summary>The smallest sweep cadence the monitor reminder honours (the Orleans reminder minimum).</summary>
    public static readonly TimeSpan MinimumInterval = TimeSpan.FromMinutes(1);

    /// <summary>The default value of <see cref="DefaultInterval"/> (six hours).</summary>
    public static readonly TimeSpan DefaultSweepInterval = TimeSpan.FromHours(6);

    /// <summary>
    /// Whether the periodic monitor runs at all. Default <see langword="true"/>:
    /// health monitoring is auto-enrolled. Set to <see langword="false"/> to
    /// disable the sweep cluster-wide. Independent of the durable-sink gate - a
    /// non-durable sink keeps the monitor inert even when this is
    /// <see langword="true"/>.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// The default cadence at which the monitor sweeps the catalog and re-verifies
    /// each enrolled backup, and the default per-backup re-verification interval. A
    /// value smaller than <see cref="MinimumInterval"/> is clamped up when the sweep
    /// reminder is registered. Default <see cref="DefaultSweepInterval"/>.
    /// </summary>
    public TimeSpan DefaultInterval { get; set; } = DefaultSweepInterval;
}
