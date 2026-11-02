namespace Orleans.Lattice.Backup;

/// <summary>The result of <see cref="ILatticeBackupSchedulerGrain.BeginDeleteAsync"/>.</summary>
/// <param name="Outcome">
/// <see cref="BackupRetireOutcome.Removed"/> when the id is now reserved for the
/// delete and its catalog row is gone; <see cref="BackupRetireOutcome.Unchanged"/>
/// when nothing of the backup remains; <see cref="BackupRetireOutcome.Busy"/> when
/// the id stayed held beyond one call's wait.
/// </param>
/// <param name="Manifest">
/// The manifest the rest of the delete works from - the catalog row, or the sink
/// manifest of a delete interrupted after its row was removed - when
/// <paramref name="Outcome"/> is <see cref="BackupRetireOutcome.Removed"/>;
/// otherwise <see langword="null"/>.
/// </param>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupDeleteStart)]
[Immutable]
internal sealed record BackupDeleteStart(
    [property: Id(0)] BackupRetireOutcome Outcome,
    [property: Id(1)] BackupManifest? Manifest);
