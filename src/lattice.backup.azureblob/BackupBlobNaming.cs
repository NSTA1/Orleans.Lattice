namespace Orleans.Lattice.Backup.AzureBlob;

/// <summary>
/// Deterministic blob-name layout for the Azure Blob backup sink. Manifests and
/// artifacts live under distinct, lexicographically ordered prefixes so listing
/// or reading a chain is a single ordered prefix scan:
/// <list type="bullet">
/// <item><description><c>manifests/{backupId}</c> - one block blob per manifest, keyed by backup id.</description></item>
/// <item><description><c>artifacts/{artifactId}</c> - one append blob per content-addressed artifact.</description></item>
/// </list>
/// Because Azure Blob Storage returns listings in lexicographical name order and
/// the ids never contain a <c>/</c>, listing a prefix yields ids in id order,
/// matching the ordering the <see cref="ILatticeBackupSink"/> contract requires.
/// </summary>
internal static class BackupBlobNaming
{
    /// <summary>The blob-name prefix (including trailing slash) under which manifests are stored.</summary>
    internal const string ManifestPrefix = "manifests/";

    /// <summary>The blob-name prefix (including trailing slash) under which artifacts are stored.</summary>
    internal const string ArtifactPrefix = "artifacts/";

    /// <summary>
    /// Blob metadata key set to <c>"true"</c> once every chunk of an artifact has
    /// been appended. A partially written append blob (created but not yet
    /// committed) is therefore distinguishable from a complete one, so a retried
    /// write overwrites it rather than treating it as an idempotent no-op.
    /// </summary>
    internal const string CommittedMetadataKey = "committed";

    /// <summary>The committed-metadata value written once an artifact is complete.</summary>
    internal const string CommittedMetadataValue = "true";

    /// <summary>Returns the block-blob name for a manifest keyed by <paramref name="backupId"/>.</summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <returns>The manifest blob name.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    internal static string ManifestBlobName(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return string.Concat(ManifestPrefix, backupId);
    }

    /// <summary>Returns the append-blob name for an artifact keyed by <paramref name="artifactId"/>.</summary>
    /// <param name="artifactId">The content-addressed artifact id. Must not be <c>null</c> or empty.</param>
    /// <returns>The artifact blob name.</returns>
    /// <exception cref="ArgumentException"><paramref name="artifactId"/> is <c>null</c> or empty.</exception>
    internal static string ArtifactBlobName(string artifactId)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        return string.Concat(ArtifactPrefix, artifactId);
    }

    /// <summary>
    /// Recovers the backup id from a manifest blob name, or <c>null</c> when the
    /// name does not sit under <see cref="ManifestPrefix"/>.
    /// </summary>
    /// <param name="blobName">The full blob name.</param>
    /// <returns>The backup id, or <c>null</c>.</returns>
    internal static string? BackupIdFromManifestBlobName(string blobName) =>
        blobName is not null && blobName.StartsWith(ManifestPrefix, StringComparison.Ordinal)
            ? blobName[ManifestPrefix.Length..]
            : null;

    /// <summary>
    /// Recovers the artifact id from an artifact blob name, or <c>null</c> when the
    /// name does not sit under <see cref="ArtifactPrefix"/>.
    /// </summary>
    /// <param name="blobName">The full blob name.</param>
    /// <returns>The artifact id, or <c>null</c>.</returns>
    internal static string? ArtifactIdFromBlobName(string blobName) =>
        blobName is not null && blobName.StartsWith(ArtifactPrefix, StringComparison.Ordinal)
            ? blobName[ArtifactPrefix.Length..]
            : null;
}
