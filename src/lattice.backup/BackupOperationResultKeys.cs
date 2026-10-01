namespace Orleans.Lattice.Backup;

/// <summary>
/// The keys of the string result map a succeeded backup or restore operation
/// carries.
/// </summary>
public static class BackupOperationResultKeys
{
    /// <summary>The captured backup id (captures), or the restored backup id (restores).</summary>
    public const string BackupId = "backupId";

    /// <summary>The set id of a set capture.</summary>
    public const string SetId = "setId";

    /// <summary>The member backup ids of a set capture, comma-separated in scope order.</summary>
    public const string MemberBackupIds = "memberBackupIds";

    /// <summary>The tree a restore installed into.</summary>
    public const string TargetTreeId = "targetTreeId";

    /// <summary>The restore mode applied (<see cref="LatticeRestoreMode"/> name).</summary>
    public const string Mode = "mode";

    /// <summary>The restore's own resolved idempotency key (<see cref="LatticeRestoreResult.OperationId"/>).</summary>
    public const string RestoreOperationId = "restoreOperationId";

    /// <summary>The replayed chain, base first, comma-separated.</summary>
    public const string ManifestChain = "manifestChain";

    /// <summary>The number of entries a restore installed.</summary>
    public const string EntriesApplied = "entriesApplied";

    /// <summary>The physical tree a shadow-cutover restore now resolves to.</summary>
    public const string ShadowPhysicalTreeId = "shadowPhysicalTreeId";

    /// <summary>The physical tree a shadow-cutover restore retained for revert.</summary>
    public const string PreviousPhysicalTreeId = "previousPhysicalTreeId";

    /// <summary>Records dead-lettered for being outside the active tenant.</summary>
    public const string DeadLetteredCrossTenant = "deadLetteredCrossTenant";

    /// <summary>Records dead-lettered for exceeding the active tenant's quota.</summary>
    public const string DeadLetteredOverQuota = "deadLetteredOverQuota";
}
