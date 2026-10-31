namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A test <see cref="ILatticeBackupSink"/> that forwards to the real in-cluster
/// sink and runs an optional hook immediately before and after each manifest
/// write, so a test can place a retention pass at an exact point of an
/// in-flight capture.
/// </summary>
internal sealed class InterceptingBackupSink(InClusterLatticeBackupSink inner) : ILatticeBackupSink
{
    /// <summary>Runs before a manifest is written; <see langword="null"/> for none.</summary>
    public static Func<BackupManifest, Task>? BeforeManifestWrite { get; set; }

    /// <summary>Runs after a manifest is written; <see langword="null"/> for none.</summary>
    public static Func<BackupManifest, Task>? AfterManifestWrite { get; set; }

    /// <summary>Clears both hooks.</summary>
    public static void Reset()
    {
        BeforeManifestWrite = null;
        AfterManifestWrite = null;
    }

    /// <inheritdoc />
    public bool IsDurable => inner.IsDurable;

    /// <inheritdoc />
    public Task WriteArtifactAsync(
        string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default) =>
        inner.WriteArtifactAsync(artifactId, content, cancellationToken);

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
    public async Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        if (BeforeManifestWrite is { } before)
        {
            await before(manifest);
        }

        await inner.WriteManifestAsync(manifest, cancellationToken);

        if (AfterManifestWrite is { } after)
        {
            await after(manifest);
        }
    }

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
