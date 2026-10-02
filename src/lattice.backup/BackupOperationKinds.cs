namespace Orleans.Lattice.Backup;

/// <summary>
/// The operation kinds the backup engine runs as tracked long-running operations.
/// Every kind starts with <see cref="Prefix"/>, which is how a facade scopes the
/// shared status, list and cancel verbs to backup operations.
/// </summary>
public static class BackupOperationKinds
{
    /// <summary>The prefix every backup operation kind starts with.</summary>
    public const string Prefix = "backup.";

    /// <summary>A full capture of one scope. Result reference: the backup id.</summary>
    public const string Capture = "backup.capture";

    /// <summary>An incremental capture layered on a base backup. Result reference: the backup id.</summary>
    public const string IncrementalCapture = "backup.incremental-capture";

    /// <summary>A backup-set capture. Result reference: the set id; see <see cref="BackupOperationResultKeys.MemberBackupIds"/>.</summary>
    public const string SetCapture = "backup.set-capture";

    /// <summary>A restore of a catalogued backup. Result: see <see cref="BackupOperationResults.TryReadRestoreResult"/>.</summary>
    public const string Restore = "backup.restore";

    /// <summary>A catalog-free disaster restore from the sink alone. Result: see <see cref="BackupOperationResults.TryReadRestoreResult"/>.</summary>
    public const string ColdRestore = "backup.cold-restore";

    /// <summary>
    /// A health verification of one backup against the durable sink. Result
    /// reference: the backup id; see <see cref="BackupOperationResultKeys.HealthStatus"/>.
    /// </summary>
    public const string HealthCheck = "backup.health-check";

    /// <summary>A rebuild of the backup catalog from the durable sink. Result: see <see cref="BackupOperationResults.TryReadCatalogRebuildReport"/>.</summary>
    public const string CatalogRebuild = "backup.catalog-rebuild";

    /// <summary>A scrub of the backup catalog against the durable sink. Result: see <see cref="BackupOperationResults.TryReadCatalogScrubReport"/>.</summary>
    public const string CatalogScrub = "backup.catalog-scrub";
}
