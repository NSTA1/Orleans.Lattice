using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="IBackupRestoreAdmission"/> a restore stream consults per
/// record. It is created once per restore by
/// <see cref="TenantBackupScope.BeginRestoreAsync"/> with the cross-tenant verdict
/// and the tenant's key quota already resolved, so each per-record decision is a
/// couple of field reads and an increment - no allocation, no lookup, no string
/// comparison on the hot path.
/// </summary>
/// <remarks>
/// <para>
/// When <paramref name="crossTenant"/> is <c>true</c> every record is refused as
/// <see cref="BackupRestoreRecordDisposition.CrossTenant"/> (a defensive backstop;
/// the target tree is normally rejected up front by
/// <see cref="TenantBackupScope.AuthorizeRestoreTarget"/>). Otherwise records are
/// admitted until the admitted count reaches <paramref name="maxKeys"/>, after
/// which each further record is refused as
/// <see cref="BackupRestoreRecordDisposition.OverQuota"/>.
/// </para>
/// <para>
/// The quota check counts admitted records this restore, not the tenant's live
/// key total across every tree: it is a per-restore bound that a single restore
/// cannot exceed the tenant's whole key budget. Precise usage-aware accounting
/// (subtracting keys the tenant already stores, and de-duplicating a key that
/// recurs across a base + incremental manifest chain) is deferred to the aggregate
/// usage integration; for a single full backup the admitted-record count equals
/// the restored key count exactly. Not thread-safe: a restore stream consults one
/// instance from a single async flow.
/// </para>
/// </remarks>
internal sealed class TenantBackupRestoreAdmission(bool crossTenant, long? maxKeys) : IBackupRestoreAdmission
{
    private long _admitted;
    private long _crossTenantRejected;
    private long _overQuotaRejected;

    /// <inheritdoc />
    public long AdmittedCount => _admitted;

    /// <inheritdoc />
    public long DeadLetteredCrossTenant => _crossTenantRejected;

    /// <inheritdoc />
    public long DeadLetteredOverQuota => _overQuotaRejected;

    /// <inheritdoc />
    public BackupRestoreRecordDisposition Admit(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (crossTenant)
        {
            _crossTenantRejected++;
            return BackupRestoreRecordDisposition.CrossTenant;
        }

        if (maxKeys is long max && _admitted >= max)
        {
            _overQuotaRejected++;
            return BackupRestoreRecordDisposition.OverQuota;
        }

        _admitted++;
        return BackupRestoreRecordDisposition.Admit;
    }
}
