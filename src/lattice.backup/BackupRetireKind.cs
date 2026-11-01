namespace Orleans.Lattice.Backup;

/// <summary>How <see cref="ILatticeBackupSchedulerGrain.RetireAsync"/> removes a backup.</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupRetireKind)]
internal enum BackupRetireKind
{
    /// <summary>An operator delete: the catalog row, the sink manifest and the unshared artifacts.</summary>
    Delete = 0,

    /// <summary>A catalog scrub: the catalog row alone, and only while the sink still cannot resolve it.</summary>
    OrphanRow = 1,
}
