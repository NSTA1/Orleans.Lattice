using System.Collections.Concurrent;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Fakes;

/// <summary>
/// An <see cref="ILatticeBackupSink"/> whose storage lives in a process-wide
/// static map keyed by <see cref="SinkId"/>, so it <b>outlives the cluster that
/// wrote to it</b>.
/// <para>
/// That single property is the whole point. Issue #2602's criterion 5 requires
/// proving that agent memory survives the destruction of the store, which means
/// the test has to destroy a cluster and stand a new one up against the same
/// backup storage. Every sink fixture in <c>test/lattice.backup</c> keeps its
/// payload inside the test cluster, so none of them can express that: they
/// "destroy" by restoring into a different tree id, which never removes the
/// original data and therefore never tests recovery from an actual loss.
/// </para>
/// <para>
/// This models the deployed arrangement exactly - a blob endpoint on a host bind
/// mount, reachable across the lifetime of any one container - which is why it
/// reports <see cref="IsDurable"/> as <see langword="true"/>. It is an in-memory
/// double, not a durability implementation: durability here means "not owned by
/// the cluster being captured", which is the property under test.
/// </para>
/// </summary>
internal sealed class SharedBackupSink : ILatticeBackupSink
{
    private sealed record Storage(
        ConcurrentDictionary<string, byte[]> Artifacts,
        ConcurrentDictionary<string, BackupManifest> Manifests);

    private static readonly ConcurrentDictionary<string, Storage> Stores = new(StringComparer.Ordinal);

    private readonly Storage _storage;

    /// <summary>Initializes a sink over the shared storage identified by <paramref name="sinkId"/>.</summary>
    /// <param name="sinkId">The storage identity. Two sinks sharing an id share storage.</param>
    /// <exception cref="ArgumentException"><paramref name="sinkId"/> is null or empty.</exception>
    public SharedBackupSink(string sinkId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sinkId);
        SinkId = sinkId;
        _storage = Stores.GetOrAdd(
            sinkId,
            static _ => new Storage(
                new ConcurrentDictionary<string, byte[]>(StringComparer.Ordinal),
                new ConcurrentDictionary<string, BackupManifest>(StringComparer.Ordinal)));
    }

    /// <summary>The shared-storage identity this sink reads and writes.</summary>
    public string SinkId { get; }

    /// <summary>Discards the shared storage for <paramref name="sinkId"/>.</summary>
    /// <param name="sinkId">The storage identity to drop.</param>
    public static void Discard(string sinkId) => Stores.TryRemove(sinkId, out _);

    /// <inheritdoc />
    public bool IsDurable => true;

    /// <inheritdoc />
    public async Task WriteArtifactAsync(
        string artifactId,
        IAsyncEnumerable<ReadOnlyMemory<byte>> content,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ArgumentNullException.ThrowIfNull(content);

        using var buffer = new MemoryStream();
        await foreach (var chunk in content.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await buffer.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
        }

        _storage.Artifacts[artifactId] = buffer.ToArray();
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        if (!_storage.Artifacts.TryGetValue(artifactId, out var bytes))
        {
            yield break;
        }

        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();
        yield return bytes;
    }

    /// <inheritdoc />
    public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        return Task.FromResult(_storage.Artifacts.TryRemove(artifactId, out _));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListArtifactIdsAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        foreach (var id in _storage.Artifacts.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return id;
        }
    }

    /// <inheritdoc />
    public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        _storage.Manifests[manifest.Id] = manifest;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return Task.FromResult(_storage.Manifests.GetValueOrDefault(backupId));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        foreach (var manifest in _storage.Manifests.Values.OrderBy(m => m.Id, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return manifest;
        }
    }

    /// <inheritdoc />
    public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return Task.FromResult(_storage.Manifests.ContainsKey(backupId));
    }

    /// <inheritdoc />
    public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        if (!_storage.Manifests.TryGetValue(backupId, out var manifest))
        {
            return Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: false, []));
        }

        var missing = manifest.ContentDescriptors
            .Select(d => d.ArtifactId)
            .Where(id => !_storage.Artifacts.ContainsKey(id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        return Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: true, missing));
    }

    /// <inheritdoc />
    public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return Task.FromResult(_storage.Manifests.TryRemove(backupId, out _));
    }
}
