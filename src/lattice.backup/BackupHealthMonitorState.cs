namespace Orleans.Lattice.Backup;

/// <summary>
/// Persistent state for the cluster-wide <c>BackupHealthMonitorGrain</c>. Records
/// the wall-clock time of the most recent completed sweep and how many backups it
/// verified, purely for observability and to make a re-registration idempotent; the
/// Orleans reminder is the durable source of truth for whether the sweep fires.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupHealthMonitorState)]
internal sealed class BackupHealthMonitorState
{
    /// <summary>The wall-clock time the most recent sweep completed, or <c>null</c> when none has run.</summary>
    [Id(0)]
    public DateTimeOffset? LastSweepUtc { get; set; }

    /// <summary>The number of backups verified by the most recent sweep.</summary>
    [Id(1)]
    public int LastSweepVerifiedCount { get; set; }
}
