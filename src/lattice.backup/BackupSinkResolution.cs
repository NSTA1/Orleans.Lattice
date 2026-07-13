namespace Orleans.Lattice.Backup;

/// <summary>
/// The read-only outcome of probing whether a backup is resolvable in the durable
/// sink: whether the self-describing manifest is present, and the ids of every
/// artifact the manifest references that is missing from the sink (absent, or
/// present but not yet marked committed). A backup is
/// <see cref="IsResolvable"/> only when its manifest is present and no referenced
/// artifact is missing, so a catalog row that probes as not resolvable is a
/// prunable orphan the sink can no longer restore.
/// <para>
/// The probe is deliberately cheap - it checks blob / row existence and the
/// committed-metadata flag, never downloading or hashing artifact payload - and it
/// carries enough detail (<see cref="MissingArtifactIds"/>) to explain which part
/// of a backup is unresolvable. Periodic health verification layers hash
/// consistency on top of this presence signal.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSinkResolution)]
[Immutable]
public sealed record BackupSinkResolution
{
    /// <summary>Initializes a new <see cref="BackupSinkResolution"/>.</summary>
    /// <param name="backupId">The probed backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="manifestPresent">Whether the backup's manifest is present in the sink.</param>
    /// <param name="missingArtifactIds">
    /// The ids of the manifest's referenced artifacts that are missing from the
    /// sink (absent, or present but not committed). Empty when the manifest is
    /// absent (no artifacts can be enumerated) or when every artifact is present.
    /// Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="missingArtifactIds"/> is <c>null</c>.</exception>
    public BackupSinkResolution(string backupId, bool manifestPresent, IReadOnlyList<string> missingArtifactIds)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(missingArtifactIds);
        BackupId = backupId;
        ManifestPresent = manifestPresent;
        MissingArtifactIds = missingArtifactIds;
    }

    /// <summary>The probed backup id.</summary>
    [Id(0)]
    public string BackupId { get; init; }

    /// <summary>Whether the backup's manifest is present in the sink.</summary>
    [Id(1)]
    public bool ManifestPresent { get; init; }

    /// <summary>
    /// The ids of the manifest's referenced artifacts that are missing from the
    /// sink (absent, or present but not committed). Empty when the manifest is
    /// absent or when every referenced artifact is present and committed.
    /// </summary>
    [Id(2)]
    public IReadOnlyList<string> MissingArtifactIds { get; init; }

    /// <summary>
    /// <see langword="true"/> when the manifest is present and no referenced
    /// artifact is missing, so the backup can be resolved and restored from the
    /// sink alone; otherwise <see langword="false"/>.
    /// </summary>
    public bool IsResolvable => ManifestPresent && MissingArtifactIds.Count == 0;
}
