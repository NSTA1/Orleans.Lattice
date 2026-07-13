namespace Orleans.Lattice.Backup;

/// <summary>
/// The well-known string grain key of the cluster-wide
/// <see cref="ILatticeBackupHealthMonitorGrain"/>. A single activation coordinates
/// the health sweep for the whole cluster, so every caller resolves it by this one
/// stable key.
/// </summary>
internal static class BackupHealthMonitorKey
{
    /// <summary>The singleton grain key of the backup-health monitor.</summary>
    internal const string Value = "sys-backup-health-monitor";
}
