using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupSink"/>. Dogfoods the reserved
/// <c>sys-backup-store</c> <c>ILattice</c> tree: manifests are stored as JSON
/// values under <c>m\u001f{backupId}</c> and artifact bytes are stored as a
/// contiguous run of chunk rows under <c>a\u001f{artifactId}\u001f{index}</c>, so
/// a manifest is a single point read and an artifact is a single bounded prefix
/// scan. Suitable for tests and single-node use; a durable off-cluster provider
/// (for example Azure append-blob) implements the same interface for production.
/// <para>
/// Artifact ids are expected to be content-addressed, so a retried write of
/// identical content is idempotent. To stay correct even when a retry re-chunks
/// the same bytes differently, a write first clears any existing chunk rows for
/// the id, then streams the new run.
/// </para>
/// </summary>
internal sealed class InClusterLatticeBackupSink(IGrainFactory grainFactory) : ILatticeBackupSink
{
    private const string ChunkIndexFormat = "D10";

    private ILattice Store => grainFactory.GetGrain<ILattice>(BackupConstants.StoreTree);

    /// <inheritdoc />
    public async Task WriteArtifactAsync(
        string artifactId,
        IAsyncEnumerable<ReadOnlyMemory<byte>> content,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ArgumentNullException.ThrowIfNull(content);
        ThrowIfSeparator(artifactId, nameof(artifactId));

        // The reserved sys-backup-store tree is created lazily by its first
        // write and has no bootstrap initializer, so the creating write must
        // run on the system-origin path or the registry would reject the
        // self-registration of a sys- tree.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // Clear any prior run so a re-chunked retry cannot leave stale trailing
            // chunks behind the new (possibly shorter) run.
            await DeleteArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);

            var prefix = ArtifactChunkPrefix(artifactId);
            var index = 0;
            await foreach (var chunk in content.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var key = string.Concat(prefix, index.ToString(ChunkIndexFormat));
                await Store.SetAsync(key, chunk.ToArray(), cancellationToken).ConfigureAwait(false);
                index++;
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        var prefix = ArtifactChunkPrefix(artifactId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Store
                .ScanEntriesAsync(prefix, BackupConstants.PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                yield return entry.Value;
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        var prefix = ArtifactChunkPrefix(artifactId);
        var removedAny = false;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var keys = new List<string>();
            await foreach (var key in Store
                .KeysAsync(prefix, BackupConstants.PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                keys.Add(key);
            }

            foreach (var key in keys)
            {
                removedAny |= await Store.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }

        return removedAny;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListArtifactIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var prefix = string.Concat(BackupConstants.ArtifactKeyPrefix.ToString(), BackupConstants.KeySeparator.ToString());
        string? previous = null;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var key in Store
                .KeysAsync(prefix, BackupConstants.PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                var id = ArtifactIdFromChunkKey(key);
                if (id is null || id == previous)
                {
                    continue;
                }

                previous = id;
                yield return id;
            }
        }
    }

    /// <inheritdoc />
    public async Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Store.SetAsync(ManifestKey(manifest.Id), manifest, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Store.GetAsync<BackupManifest>(ManifestKey(backupId), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var prefix = string.Concat(BackupConstants.ManifestKeyPrefix.ToString(), BackupConstants.KeySeparator.ToString());
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Store
                .ScanEntriesAsync<BackupManifest>(prefix, BackupConstants.PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } manifest)
                {
                    yield return manifest;
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Store.DeleteAsync(ManifestKey(backupId), cancellationToken).ConfigureAwait(false);
        }
    }

    private static string ManifestKey(string backupId) =>
        string.Concat(BackupConstants.ManifestKeyPrefix.ToString(), BackupConstants.KeySeparator.ToString(), backupId);

    private static string ArtifactChunkPrefix(string artifactId) =>
        string.Concat(
            BackupConstants.ArtifactKeyPrefix.ToString(),
            BackupConstants.KeySeparator.ToString(),
            artifactId,
            BackupConstants.KeySeparator.ToString());

    private static string? ArtifactIdFromChunkKey(string key)
    {
        // Key shape: a \u001f {artifactId} \u001f {chunkIndex}
        var firstSep = key.IndexOf(BackupConstants.KeySeparator);
        if (firstSep < 0)
        {
            return null;
        }

        var secondSep = key.IndexOf(BackupConstants.KeySeparator, firstSep + 1);
        if (secondSep < 0)
        {
            return null;
        }

        return key[(firstSep + 1)..secondSep];
    }

    private static void ThrowIfSeparator(string value, string paramName)
    {
        if (value.IndexOf(BackupConstants.KeySeparator) >= 0)
        {
            throw new ArgumentException(
                "An artifact id must not contain the reserved unit-separator character (U+001F).",
                paramName);
        }
    }
}
