using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Backup;
using Orleans.Serialization;

namespace MultiSiteManufacturing.Host.Backup;

/// <summary>
/// A shared, filesystem-backed <see cref="ILatticeBackupSink"/> for the sample.
/// Artifacts and manifests are written under a single root directory that every
/// cluster in the sample topology can read, so a backup captured on one cluster
/// is resolvable and restorable from any peer. This is what makes a coordinated
/// multi-cluster restore of a replicated tree possible: the built-in in-cluster
/// sink dogfoods a per-cluster reserved tree, so a backup written on one cluster
/// is invisible to the others, and the backup package's startup guard rejects a
/// replicated tree backed by that in-cluster sink. Registering this shared sink
/// before <c>AddLatticeBackup</c> satisfies the guard.
/// <para>
/// Artifact ids and backup ids are content-addressed, so a re-write of identical
/// content is idempotent. Ids are hex-encoded into filesystem-safe file names
/// that round-trip exactly, so the list operations can recover the original ids.
/// A write lands through a temp file plus atomic move so a concurrent reader on a
/// peer cluster never observes a half-written artifact. Suitable for the sample's
/// single-machine, shared-directory topology; a production deployment uses a
/// durable off-cluster provider (for example the Azure Blob sink) instead.
/// </para>
/// </summary>
public sealed class FileSystemBackupSink : ILatticeBackupSink
{
    private const string ManifestSuffix = ".manifest";

    private readonly string _artifactsRoot;
    private readonly string _manifestsRoot;
    private readonly Serializer _serializer;
    private readonly ILogger<FileSystemBackupSink> _logger;

    /// <summary>
    /// Initializes a new <see cref="FileSystemBackupSink"/> rooted at
    /// <paramref name="rootPath"/>. The directory is created if it does not exist.
    /// </summary>
    /// <param name="rootPath">The shared root directory. Must not be <c>null</c> or empty.</param>
    /// <param name="serializer">The Orleans serializer used to persist manifests. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentException"><paramref name="rootPath"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> or <paramref name="logger"/> is <c>null</c>.</exception>
    public FileSystemBackupSink(string rootPath, Serializer serializer, ILogger<FileSystemBackupSink> logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(rootPath);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        _serializer = serializer;
        _logger = logger;

        var fullRoot = Path.GetFullPath(rootPath);
        _artifactsRoot = Path.Combine(fullRoot, "artifacts");
        _manifestsRoot = Path.Combine(fullRoot, "manifests");
        Directory.CreateDirectory(_artifactsRoot);
        Directory.CreateDirectory(_manifestsRoot);

        _logger.LogInformation("Shared filesystem backup sink rooted at {Root}.", fullRoot);
    }

    /// <inheritdoc />
    public async Task WriteArtifactAsync(
        string artifactId,
        IAsyncEnumerable<ReadOnlyMemory<byte>> content,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ArgumentNullException.ThrowIfNull(content);

        var path = ArtifactPath(artifactId);
        var temp = path + ".tmp-" + Guid.NewGuid().ToString("N");
        try
        {
            await using (var stream = new FileStream(
                temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 1, useAsync: true))
            {
                await foreach (var chunk in content.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    await stream.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
                }
            }

            // Atomic publish: a peer cluster reading the shared directory never sees
            // a partially written artifact. Overwrite is a no-op-equivalent for a
            // content-addressed id (identical bytes), so a retried write converges.
            File.Move(temp, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp))
            {
                File.Delete(temp);
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        var path = ArtifactPath(artifactId);
        if (!File.Exists(path))
        {
            yield break;
        }

        await using var stream = new FileStream(
            path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1, useAsync: true);

        var buffer = new byte[64 * 1024];
        int read;
        while ((read = await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
        {
            yield return new ReadOnlyMemory<byte>(buffer, 0, read).ToArray();
        }
    }

    /// <inheritdoc />
    public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        cancellationToken.ThrowIfCancellationRequested();

        var path = ArtifactPath(artifactId);
        if (!File.Exists(path))
        {
            return Task.FromResult(false);
        }

        File.Delete(path);
        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListArtifactIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var id in EnumerateIds(_artifactsRoot, suffix: null))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return id;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);

        var bytes = _serializer.SerializeToArray(manifest);
        var path = ManifestPath(manifest.Id);
        var temp = path + ".tmp-" + Guid.NewGuid().ToString("N");
        try
        {
            await File.WriteAllBytesAsync(temp, bytes, cancellationToken).ConfigureAwait(false);
            File.Move(temp, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp))
            {
                File.Delete(temp);
            }
        }
    }

    /// <inheritdoc />
    public async Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var path = ManifestPath(backupId);
        if (!File.Exists(path))
        {
            return null;
        }

        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        return _serializer.Deserialize<BackupManifest>(bytes);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var id in EnumerateIds(_manifestsRoot, ManifestSuffix))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var manifest = await ReadManifestAsync(id, cancellationToken).ConfigureAwait(false);
            if (manifest is not null)
            {
                yield return manifest;
            }
        }
    }

    /// <inheritdoc />
    public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        cancellationToken.ThrowIfCancellationRequested();

        var path = ManifestPath(backupId);
        if (!File.Exists(path))
        {
            return Task.FromResult(false);
        }

        File.Delete(path);
        return Task.FromResult(true);
    }

    private string ArtifactPath(string artifactId) =>
        Path.Combine(_artifactsRoot, Encode(artifactId));

    private string ManifestPath(string backupId) =>
        Path.Combine(_manifestsRoot, Encode(backupId) + ManifestSuffix);

    private static IEnumerable<string> EnumerateIds(string root, string? suffix)
    {
        var ids = new List<string>();
        foreach (var file in Directory.EnumerateFiles(root))
        {
            var name = Path.GetFileName(file);
            if (name.Contains(".tmp-", StringComparison.Ordinal))
            {
                continue;
            }

            if (suffix is not null)
            {
                if (!name.EndsWith(suffix, StringComparison.Ordinal))
                {
                    continue;
                }

                name = name[..^suffix.Length];
            }

            if (TryDecode(name, out var id))
            {
                ids.Add(id);
            }
        }

        ids.Sort(StringComparer.Ordinal);
        return ids;
    }

    // Hex-encode the UTF-8 bytes of an id into a filesystem-safe, reversible name.
    private static string Encode(string id) =>
        Convert.ToHexString(Encoding.UTF8.GetBytes(id));

    private static bool TryDecode(string encoded, out string id)
    {
        try
        {
            id = Encoding.UTF8.GetString(Convert.FromHexString(encoded));
            return true;
        }
        catch (FormatException)
        {
            id = string.Empty;
            return false;
        }
    }
}
