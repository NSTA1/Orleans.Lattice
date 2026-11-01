using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// One in-memory backup store handed to both regions, so a backup captured in
/// one region resolves in the other.
/// </summary>
/// <remarks>
/// <para>
/// Replicating a tree needs a backup sink the whole replication set can read:
/// the default in-cluster sink keeps its artifacts in a per-cluster reserved
/// tree, so the backup add-on refuses it for any replicated tree. Runtime
/// replication control replicates its own configuration tree, so hosting it -
/// and replicating anything at all - needs a shared sink. The repository ships
/// no local or file-system sink, so the sample shares this one instance between
/// its two in-process regions. It is the in-process stand-in for a durable
/// off-cluster store such as the Azure Blob sink: its contents live as long as
/// the process, like everything else in this sample.
/// </para>
/// <para>
/// Artifacts keep the chunk boundaries they were written with, because each
/// chunk is one entry batch the restore engine decodes on its own. Ids are
/// content-addressed, so rewriting identical content is idempotent.
/// </para>
/// </remarks>
internal sealed class SampleSharedBackupSink : ILatticeBackupSink
{
    private readonly ConcurrentDictionary<string, byte[][]> _artifacts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, BackupManifest> _manifests = new(StringComparer.Ordinal);

    /// <inheritdoc />
    /// <remarks>
    /// The store is not owned by either region, so a backup written by one
    /// region survives to be resolved and restored by the other.
    /// </remarks>
    public bool IsDurable => true;

    /// <summary>The number of artifacts held.</summary>
    public int ArtifactCount => _artifacts.Count;

    /// <summary>The number of manifests held.</summary>
    public int ManifestCount => _manifests.Count;

    /// <inheritdoc />
    public async Task WriteArtifactAsync(
        string artifactId,
        IAsyncEnumerable<ReadOnlyMemory<byte>> content,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ArgumentNullException.ThrowIfNull(content);

        var chunks = new List<byte[]>();
        await foreach (var chunk in content.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            chunks.Add(chunk.ToArray());
        }

        _artifacts[artifactId] = [.. chunks];
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        if (!_artifacts.TryGetValue(artifactId, out var chunks))
        {
            yield break;
        }

        foreach (var chunk in chunks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return chunk;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_artifacts.TryRemove(artifactId, out _));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListArtifactIdsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var id in _artifacts.Keys.Order(StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return id;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        cancellationToken.ThrowIfCancellationRequested();
        _manifests[manifest.Id] = manifest;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_manifests.GetValueOrDefault(backupId));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var id in _manifests.Keys.Order(StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_manifests.TryGetValue(id, out var manifest))
            {
                yield return manifest;
            }
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_manifests.TryRemove(backupId, out _));
    }

    /// <inheritdoc />
    public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_manifests.ContainsKey(backupId));
    }

    /// <inheritdoc />
    public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_manifests.TryGetValue(backupId, out var manifest))
        {
            return Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: false, []));
        }

        var missing = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            if (seen.Add(descriptor.ArtifactId) && !_artifacts.ContainsKey(descriptor.ArtifactId))
            {
                missing.Add(descriptor.ArtifactId);
            }
        }

        return Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: true, missing));
    }
}
