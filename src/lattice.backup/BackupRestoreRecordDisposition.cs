namespace Orleans.Lattice.Backup;

/// <summary>
/// The per-record decision a restore stream applies to a single captured entry
/// before it is written into the target tree. Returned by
/// <see cref="IBackupRestoreAdmission.Admit"/> on the hot per-record restore
/// path. This is a purely in-process control value that never crosses a grain
/// boundary, so it carries no Orleans serialization attributes.
/// </summary>
public enum BackupRestoreRecordDisposition
{
    /// <summary>
    /// The record is inside the active tenant's namespace and within quota; the
    /// restore stream writes it into the target tree.
    /// </summary>
    Admit = 0,

    /// <summary>
    /// The record is addressed outside the active tenant's namespace. The restore
    /// stream refuses it (dead-letters it) rather than writing it, so a restore can
    /// never install another tenant's or a platform tree's data.
    /// </summary>
    CrossTenant = 1,

    /// <summary>
    /// The record would take the active tenant past its configured key quota. The
    /// restore stream refuses it (dead-letters it) rather than writing it.
    /// </summary>
    OverQuota = 2,
}
