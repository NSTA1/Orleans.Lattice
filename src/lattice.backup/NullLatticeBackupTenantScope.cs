namespace Orleans.Lattice.Backup;

/// <summary>
/// The inert default <see cref="ILatticeBackupTenantScope"/> the core backup
/// package registers when no tenancy add-on is present. <see cref="IsActive"/> is
/// <c>false</c>, both authorization methods are no-ops, and
/// <see cref="BeginRestoreAsync"/> returns a permissive admission that admits
/// every record, so capture and restore behave byte-for-byte as they did before
/// the tenancy seam existed. The tenancy add-on replaces this registration with
/// its active implementation.
/// </summary>
internal sealed class NullLatticeBackupTenantScope : ILatticeBackupTenantScope
{
    /// <summary>The shared stateless singleton instance.</summary>
    internal static readonly NullLatticeBackupTenantScope Instance = new();

    private static readonly ValueTask<IBackupRestoreAdmission> PermissiveAdmission =
        new(PermissiveBackupRestoreAdmission.Instance);

    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public void AuthorizeCapture(string treeId)
    {
    }

    /// <inheritdoc />
    public void AuthorizeRestoreTarget(string treeId)
    {
    }

    /// <inheritdoc />
    public ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(
        string targetTreeId,
        CancellationToken cancellationToken = default) => PermissiveAdmission;
}

/// <summary>
/// An <see cref="IBackupRestoreAdmission"/> that admits every record and tracks
/// only the admitted count. Used by <see cref="NullLatticeBackupTenantScope"/> so
/// the tenancy-off restore path never dead-letters a record. A single shared
/// instance is safe because the counter is only read for reporting and the
/// tenancy-off restore path does not consult it per record (it holds no
/// admission), so no meaningful shared mutation occurs.
/// </summary>
internal sealed class PermissiveBackupRestoreAdmission : IBackupRestoreAdmission
{
    /// <summary>The shared stateless singleton instance.</summary>
    internal static readonly PermissiveBackupRestoreAdmission Instance = new();

    /// <inheritdoc />
    public long AdmittedCount => 0;

    /// <inheritdoc />
    public long DeadLetteredCrossTenant => 0;

    /// <inheritdoc />
    public long DeadLetteredOverQuota => 0;

    /// <inheritdoc />
    public BackupRestoreRecordDisposition Admit(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return BackupRestoreRecordDisposition.Admit;
    }
}
