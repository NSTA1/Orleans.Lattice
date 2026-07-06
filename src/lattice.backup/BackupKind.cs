namespace Orleans.Lattice.Backup;

/// <summary>
/// Discriminates whether a backup captures the full contents of its scope or only
/// the changes since a base backup.
/// </summary>
public enum BackupKind
{
    /// <summary>A self-contained capture of every key in scope.</summary>
    Full = 0,

    /// <summary>
    /// A capture of only the mutations since a base backup, identified by
    /// <see cref="BackupManifest.BaseBackupId"/>. Restoring an incremental
    /// requires its base chain.
    /// </summary>
    Incremental = 1,
}
