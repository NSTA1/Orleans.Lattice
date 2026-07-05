namespace Orleans.Lattice.Backup;

/// <summary>
/// Configuration for the <c>Orleans.Lattice.Backup</c> catalog store: the durable
/// per-key history retention applied to the reserved <c>sys-backup-catalog</c>
/// tree so the record of backups catalogued and removed stays auditable. History
/// is captured by default; it is never disabled by default.
/// </summary>
public sealed class LatticeBackupOptions
{
    /// <summary>
    /// The retention mode for the durable per-key history captured on the
    /// <c>sys-backup-catalog</c> tree. Defaults to
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>; history is never disabled
    /// by default.
    /// </summary>
    public HistoryRetentionMode HistoryRetentionMode { get; set; } = HistoryRetentionMode.MetadataOnly;

    /// <summary>
    /// The age after which a catalog history revision row expires, or <c>null</c>
    /// for no age bound (the default). Must be strictly positive when supplied.
    /// </summary>
    public TimeSpan? HistoryRetentionWindow { get; set; }

    /// <summary>
    /// Whether to create the durable per-key history materialised view over the
    /// <c>sys-backup-catalog</c> tree so catalog changes remain auditable beyond
    /// the source write-ahead-log window. Defaults to <c>true</c>.
    /// </summary>
    public bool EnableDurableHistoryView { get; set; } = true;
}
