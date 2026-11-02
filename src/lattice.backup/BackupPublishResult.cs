namespace Orleans.Lattice.Backup;

/// <summary>The result of a <see cref="BackupPublishRequest"/>.</summary>
/// <param name="Outcome">What happened.</param>
/// <param name="Manifest">
/// The manifest as published - its final id and reconciled capture times - when
/// <paramref name="Outcome"/> is <see cref="BackupPublishOutcome.Published"/>;
/// otherwise <see langword="null"/>.
/// </param>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupPublishResult)]
[Immutable]
internal sealed record BackupPublishResult(
    [property: Id(0)] BackupPublishOutcome Outcome,
    [property: Id(1)] BackupManifest? Manifest)
{
    /// <summary>A held id: nothing was written and the caller retries.</summary>
    public static BackupPublishResult Busy { get; } = new(BackupPublishOutcome.Busy, null);
}
