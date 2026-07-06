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

    /// <summary>
    /// The maximum total wall-clock time a cross-tree-consistent backup-set fence
    /// waits for in-flight cross-tree atomic sagas touching the set to drain to a
    /// terminal decision before it gives up and fails the capture. Must be
    /// strictly positive. Defaults to 30 seconds. Single-tree and non-flagged
    /// backups never consult this value.
    /// </summary>
    public TimeSpan CrossTreeFenceDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The poll interval between successive in-flight observations while a
    /// cross-tree-consistent backup-set fence waits for sagas to drain. Must be
    /// strictly positive. Defaults to 25 milliseconds.
    /// </summary>
    public TimeSpan CrossTreeFencePollInterval { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// The maximum number of fence attempts a cross-tree-consistent backup-set
    /// capture makes before failing. Each attempt drains, captures, and
    /// re-observes; an attempt is retried when a cross-tree saga registered on
    /// the set during the capture window. Must be at least 1. Defaults to 5.
    /// </summary>
    public int MaxCrossTreeFenceAttempts { get; set; } = 5;
}
