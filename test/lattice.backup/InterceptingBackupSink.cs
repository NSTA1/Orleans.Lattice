namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A test <see cref="ILatticeBackupSink"/> that forwards to the real in-cluster
/// sink and runs an optional hook immediately before and after each manifest
/// write, before each sink manifest listing and as each listed manifest is
/// yielded, before each artifact delete and after each probe, so a test can
/// place a retention pass at an exact point of an in-flight capture, or a
/// capture, a delete or a rebuild at an exact point of a pass or a scrub.
/// </summary>
internal sealed class InterceptingBackupSink(InClusterLatticeBackupSink inner) : ILatticeBackupSink
{
    /// <summary>Runs before a manifest is written; <see langword="null"/> for none.</summary>
    public static Func<BackupManifest, Task>? BeforeManifestWrite { get; set; }

    /// <summary>Runs after a manifest is written; <see langword="null"/> for none.</summary>
    public static Func<BackupManifest, Task>? AfterManifestWrite { get; set; }

    /// <summary>Runs before the sink's manifests are listed; <see langword="null"/> for none.</summary>
    public static Func<Task>? BeforeManifestList { get; set; }

    /// <summary>Runs as each listed manifest is yielded; <see langword="null"/> for none.</summary>
    public static Func<BackupManifest, Task>? OnManifestListed { get; set; }

    /// <summary>Runs before an artifact is deleted; <see langword="null"/> for none.</summary>
    public static Func<string, Task>? BeforeArtifactDelete { get; set; }

    /// <summary>Runs after a backup is probed, with the probe's result; <see langword="null"/> for none.</summary>
    public static Func<string, BackupSinkResolution, Task>? AfterProbe { get; set; }

    /// <summary>Clears every hook.</summary>
    public static void Reset()
    {
        BeforeManifestWrite = null;
        AfterManifestWrite = null;
        BeforeManifestList = null;
        OnManifestListed = null;
        BeforeArtifactDelete = null;
        AfterProbe = null;
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
    public async Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)
    {
        if (BeforeArtifactDelete is { } before)
        {
            await before(artifactId);
        }

        return await inner.DeleteArtifactAsync(artifactId, cancellationToken);
    }

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
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (BeforeManifestList is { } before)
        {
            await before();
        }

        await foreach (var manifest in inner.ListManifestsAsync(cancellationToken))
        {
            if (OnManifestListed is { } listed)
            {
                await listed(manifest);
            }

            yield return manifest;
        }
    }

    /// <inheritdoc />
    public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) =>
        inner.ManifestExistsAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public async Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default)
    {
        var resolution = await inner.ProbeAsync(backupId, cancellationToken);
        if (AfterProbe is { } after)
        {
            await after(backupId, resolution);
        }

        return resolution;
    }

    /// <inheritdoc />
    public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
        inner.DeleteManifestAsync(backupId, cancellationToken);
}
