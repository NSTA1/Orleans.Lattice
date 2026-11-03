namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to publish a backup manifest through its scope's
/// <see cref="ILatticeBackupSchedulerGrain"/>, which serialises it per id against
/// retention passes, deletes and other publishes of the same id (issue #5418).
/// </summary>
/// <param name="Manifest">The manifest to publish.</param>
/// <param name="Mode">How to publish it.</param>
/// <param name="Register">
/// For <see cref="BackupPublishMode.Full"/>: whether to register the catalog row
/// now, or defer it to a set's membership stamp.
/// </param>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupPublishRequest)]
[Immutable]
internal sealed record BackupPublishRequest(
    [property: Id(0)] BackupManifest Manifest,
    [property: Id(1)] BackupPublishMode Mode,
    [property: Id(2)] bool Register = true);
