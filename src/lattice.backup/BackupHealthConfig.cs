namespace Orleans.Lattice.Backup;

/// <summary>
/// The per-backup health-monitoring configuration: whether the periodic monitor
/// verifies this backup, and how often it should be re-verified. Every backup is
/// auto-enrolled with the configured defaults, so a backup with no explicit
/// configuration is monitored at the default cadence; this record lets an operator
/// override that per backup - for example disabling monitoring for a backup that is
/// intentionally being retired, or tightening the cadence for a critical one.
/// <para>
/// Health monitoring is only meaningful when backup payload lives in a durable,
/// external sink; with the ephemeral in-cluster sink the monitor is inert
/// regardless of this configuration.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupHealthConfig)]
[Immutable]
public sealed record BackupHealthConfig
{
    /// <summary>Initializes a new <see cref="BackupHealthConfig"/>.</summary>
    /// <param name="monitoringEnabled">Whether the periodic monitor verifies this backup.</param>
    /// <param name="interval">
    /// The minimum time between successive health verifications of this backup.
    /// Must be strictly positive.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is not strictly positive.</exception>
    public BackupHealthConfig(bool monitoringEnabled, TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);
        MonitoringEnabled = monitoringEnabled;
        Interval = interval;
    }

    /// <summary>Whether the periodic monitor verifies this backup.</summary>
    [Id(0)]
    public bool MonitoringEnabled { get; init; }

    /// <summary>The minimum time between successive health verifications of this backup.</summary>
    [Id(1)]
    public TimeSpan Interval { get; init; }
}
