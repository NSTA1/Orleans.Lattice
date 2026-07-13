using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The process-local, thread-safe backing store shared by two independent test
/// clusters. It holds the raw backup payload - content-addressed artifact bytes
/// and serialized manifest bytes - and nothing else. Because a
/// <see cref="SharedBackupStore"/> is the <b>only</b> object handed to both
/// clusters, it is the single point of shared state a cross-cluster disaster
/// restore is allowed to depend on: neither cluster shares grain storage, a
/// catalog, or a serializer instance with the other.
/// <para>
/// Manifests are stored as bytes (not live object graphs) so a backup written by
/// one cluster is only recoverable by the other after a real
/// serialize/deserialize round-trip, mirroring how a durable off-cluster sink
/// (for example the Azure Blob backend) behaves.
/// </para>
/// </summary>
public sealed class SharedBackupStore
{
    /// <summary>Content-addressed artifact id to its ordered run of chunk buffers.</summary>
    public ConcurrentDictionary<string, byte[][]> Artifacts { get; } = new(StringComparer.Ordinal);

    /// <summary>Backup id to its serialized <see cref="BackupManifest"/> bytes.</summary>
    public ConcurrentDictionary<string, byte[]> Manifests { get; } = new(StringComparer.Ordinal);
}

/// <summary>
/// A shared, in-memory <see cref="ILatticeBackupSink"/> for cross-cluster restore
/// tests. Artifacts and manifests are read from and written to a
/// <see cref="SharedBackupStore"/> that every cluster in the test is handed, so a
/// backup captured on one cluster is resolvable and restorable from an entirely
/// separate cluster whose only shared state is that store. This is the in-memory
/// analogue of the sample <c>FileSystemBackupSink</c>: the built-in in-cluster
/// sink dogfoods a per-cluster reserved tree, so a backup written on one cluster
/// is invisible to any other cluster, which is exactly what a disaster restore
/// into a fresh cluster must not require.
/// <para>
/// Manifests round-trip through the supplied <see cref="Serializer"/>, so a
/// manifest authored on the capturing cluster is decoded from bytes on the
/// restoring cluster rather than shared as a live object. Artifact and backup ids
/// are content-addressed, so re-writing identical content is idempotent.
/// </para>
/// </summary>
public sealed class SharedInMemoryBackupSink : ILatticeBackupSink
{
    private readonly SharedBackupStore _store;
    private readonly Serializer _serializer;

    /// <summary>
    /// Initializes a new <see cref="SharedInMemoryBackupSink"/> over
    /// <paramref name="store"/>, using <paramref name="serializer"/> to persist and
    /// rehydrate manifests.
    /// </summary>
    /// <param name="store">The shared backing store. Must not be <c>null</c>.</param>
    /// <param name="serializer">The Orleans serializer used to persist manifests. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="store"/> or <paramref name="serializer"/> is <c>null</c>.</exception>
    public SharedInMemoryBackupSink(SharedBackupStore store, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(serializer);
        _store = store;
        _serializer = serializer;
    }

    /// <inheritdoc />
    public async Task WriteArtifactAsync(
        string artifactId,
        IAsyncEnumerable<ReadOnlyMemory<byte>> content,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ArgumentNullException.ThrowIfNull(content);

        // Preserve the exact write-time chunk boundaries: each chunk is one
        // serialized entry batch the restore engine deserializes independently, so
        // the run must round-trip chunk-for-chunk (mirroring the in-cluster sink's
        // one-row-per-chunk layout) rather than be concatenated and re-chunked.
        var chunks = new List<byte[]>();
        await foreach (var chunk in content.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            chunks.Add(chunk.ToArray());
        }

        // Content-addressed id: an identical retry overwrites with identical bytes.
        _store.Artifacts[artifactId] = chunks.ToArray();
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        if (!_store.Artifacts.TryGetValue(artifactId, out var chunks))
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
        return Task.FromResult(_store.Artifacts.TryRemove(artifactId, out _));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListArtifactIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var id in _store.Artifacts.Keys.OrderBy(k => k, StringComparer.Ordinal))
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
        _store.Manifests[manifest.Id] = _serializer.SerializeToArray(manifest);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(
            _store.Manifests.TryGetValue(backupId, out var bytes)
                ? _serializer.Deserialize<BackupManifest>(bytes)
                : null);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var id in _store.Manifests.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_store.Manifests.TryGetValue(id, out var bytes))
            {
                yield return _serializer.Deserialize<BackupManifest>(bytes);
            }
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_store.Manifests.TryRemove(backupId, out _));
    }
}
