using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The silo's backup sink behind the <see cref="JourneyBackupWriteHold"/> fault seam: every call
/// passes straight through, except that an artifact write first waits while the hold is on.
/// </summary>
/// <param name="inner">The registered sink.</param>
/// <param name="hold">The hold.</param>
internal sealed class HeldBackupSink(ILatticeBackupSink inner, JourneyBackupWriteHold hold) : ILatticeBackupSink
{
    /// <inheritdoc />
    public bool IsDurable => inner.IsDurable;

    /// <inheritdoc />
    public async Task WriteArtifactAsync(string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default)
    {
        await hold.WaitAsync(cancellationToken).ConfigureAwait(false);
        await inner.WriteArtifactAsync(artifactId, content, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(string artifactId, CancellationToken cancellationToken = default) =>
        inner.ReadArtifactAsync(artifactId, cancellationToken);

    /// <inheritdoc />
    public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) =>
        inner.DeleteArtifactAsync(artifactId, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default) =>
        inner.ListArtifactIdsAsync(cancellationToken);

    /// <inheritdoc />
    public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default) =>
        inner.WriteManifestAsync(manifest, cancellationToken);

    /// <inheritdoc />
    public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
        inner.ReadManifestAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default) =>
        inner.ListManifestsAsync(cancellationToken);

    /// <inheritdoc />
    public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) =>
        inner.ManifestExistsAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default) =>
        inner.ProbeAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
        inner.DeleteManifestAsync(backupId, cancellationToken);
}
