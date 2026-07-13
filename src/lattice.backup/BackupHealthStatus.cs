namespace Orleans.Lattice.Backup;

/// <summary>
/// The overall health of a backup's durable sink payload, as verified by the
/// periodic backup-health monitor. Health verification checks both that every
/// part of the backup is present in the sink (the manifest and each referenced
/// artifact, present and committed) and that each artifact's content still hashes
/// to the digest the manifest recorded when it was captured, so silent bit-rot or
/// an out-of-band blob edit is caught in addition to a deletion.
/// </summary>
public enum BackupHealthStatus
{
    /// <summary>
    /// The backup has not yet been verified (no health check has run for it), so
    /// its sink payload is of unknown condition.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// The manifest and every referenced artifact are present and committed in the
    /// sink and every artifact's content still matches the manifest's recorded
    /// hash: the backup is fully resolvable and integrity-checked.
    /// </summary>
    Healthy = 1,

    /// <summary>
    /// The manifest is present but the backup is not fully resolvable: at least one
    /// referenced artifact is missing or uncommitted, or at least one artifact's
    /// content no longer matches its recorded hash. The backup should not be relied
    /// on as a restore point until the fault is investigated.
    /// </summary>
    Warning = 2,

    /// <summary>
    /// The backup's manifest itself is absent from the sink, so nothing about the
    /// backup can be resolved or restored. The catalog row is an orphan.
    /// </summary>
    Missing = 3,
}
