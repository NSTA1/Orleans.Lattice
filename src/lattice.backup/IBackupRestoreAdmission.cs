namespace Orleans.Lattice.Backup;

/// <summary>
/// A per-restore admission controller that decides, record by record, whether a
/// captured entry may be written into the restore target. One instance is created
/// per restore operation (via
/// <see cref="ILatticeBackupTenantScope.BeginRestoreAsync"/>) and then consulted
/// once per streamed record, so the tenant-namespace and quota checks stay on the
/// hot path without a per-record allocation. A refused record is dead-lettered
/// (skipped), never silently written; the running counters expose how many
/// records were refused and why.
/// </summary>
/// <remarks>
/// Implementations are used from a single restore stream at a time and are not
/// required to be thread-safe. When no tenancy add-on is registered the restore
/// stream holds no admission at all (the tenant scope is inactive), so the
/// tenancy-off path pays only a single null check per record.
/// </remarks>
public interface IBackupRestoreAdmission
{
    /// <summary>
    /// The number of records admitted (written) so far by this restore.
    /// </summary>
    long AdmittedCount { get; }

    /// <summary>
    /// The number of records dead-lettered so far because they were addressed
    /// outside the active tenant's namespace.
    /// </summary>
    long DeadLetteredCrossTenant { get; }

    /// <summary>
    /// The number of records dead-lettered so far because admitting them would
    /// exceed the active tenant's key quota.
    /// </summary>
    long DeadLetteredOverQuota { get; }

    /// <summary>
    /// Decides whether the record identified by <paramref name="key"/> may be
    /// written into the restore target, updating the running counters. A returned
    /// <see cref="BackupRestoreRecordDisposition.Admit"/> means the caller should
    /// write the record; any other disposition means the caller must skip
    /// (dead-letter) it.
    /// </summary>
    /// <param name="key">The record key being restored. Must not be <c>null</c>.</param>
    /// <returns>The disposition the restore stream must apply to the record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <c>null</c>.</exception>
    BackupRestoreRecordDisposition Admit(string key);
}
