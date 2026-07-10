using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob;

/// <summary>
/// An <see cref="ILatticeBackupSink"/> backed by an Azure Storage account.
/// Content-addressed artifacts are stored as <b>append blobs</b> under
/// <c>artifacts/{artifactId}</c> - the streamed chunks append in natural order
/// and read back in order - and self-describing manifests are stored as
/// <b>block blobs</b> under <c>manifests/{backupId}</c>, so a manifest is a
/// single atomic overwrite and listing a chain is one ordered prefix scan.
/// <para>
/// The artifact surface preserves chunk framing: each written chunk is stored
/// with a 4-byte little-endian length prefix, and <see cref="ReadArtifactAsync"/>
/// reconstructs the <b>same</b> chunk boundaries on read rather than re-chunking
/// the raw byte stream. This matches the framing contract the in-cluster sink
/// provides and the restore path depends on - each chunk is one Orleans-serialized
/// entry array that the restore engine deserializes whole - so a chunk is never
/// split across a read boundary and deserialization never sees a truncated buffer.
/// </para>
/// <para>
/// Writes are idempotent: artifact ids are content-addressed, so a completed
/// (committed) artifact blob already holds identical bytes and a retried write is
/// a no-op. A partially written append blob (created but not yet marked committed
/// via blob metadata) is overwritten on retry, so a crash mid-append never leaves
/// a duplicated or truncated chain. Manifests round-trip through the Orleans
/// serializer so the wire format matches the in-cluster sink.
/// </para>
/// </summary>
internal sealed class AzureBlobLatticeBackupSink : ILatticeBackupSink
{
    // Azure append-block hard limit is 4 MiB per AppendBlock call.
    private const int MaxAppendBlockBytes = 4 * 1024 * 1024;

    // Per-chunk length prefix width: a 4-byte little-endian int32 frames each
    // stored chunk so the read path can restore exact chunk boundaries.
    private const int ChunkLengthPrefixBytes = 4;

    private readonly BlobContainerClient _container;
    private readonly Serializer<BackupManifest> _serializer;
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private bool _containerReady;

    /// <summary>Initializes a new <see cref="AzureBlobLatticeBackupSink"/>.</summary>
    /// <param name="container">The blob container the sink reads from and writes to. Must not be <c>null</c>.</param>
    /// <param name="serializer">The Orleans serializer used to round-trip manifests. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public AzureBlobLatticeBackupSink(BlobContainerClient container, Serializer<BackupManifest> serializer)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(serializer);
        _container = container;
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
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        var blob = _container.GetAppendBlobClient(BackupBlobNaming.ArtifactBlobName(artifactId));

        // Idempotent fast path: a committed blob already holds identical
        // (content-addressed) bytes, so a retried write is a no-op.
        if (await IsCommittedAsync(blob, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        // (Re)create to discard any partial prior attempt, then append the stream
        // in order. CreateAsync overwrites an existing, uncommitted blob.
        await blob.CreateAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        await foreach (var chunk in content.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // Frame the chunk with a 4-byte little-endian length prefix so the read
            // path can restore this exact chunk boundary. Prefix and payload are
            // appended as one contiguous run; the 4 MiB AppendBlock split below is
            // purely physical and does not affect the logical frame the reader sees.
            var framed = new byte[ChunkLengthPrefixBytes + chunk.Length];
            BinaryPrimitives.WriteInt32LittleEndian(framed, chunk.Length);
            chunk.Span.CopyTo(framed.AsSpan(ChunkLengthPrefixBytes));

            var remaining = (ReadOnlyMemory<byte>)framed;
            while (!remaining.IsEmpty)
            {
                var take = Math.Min(remaining.Length, MaxAppendBlockBytes);
                using var slice = new MemoryStream(remaining[..take].ToArray(), writable: false);
                await blob.AppendBlockAsync(slice, cancellationToken: cancellationToken).ConfigureAwait(false);
                remaining = remaining[take..];
            }
        }

        await blob.SetMetadataAsync(
            new Dictionary<string, string>
            {
                [BackupBlobNaming.CommittedMetadataKey] = BackupBlobNaming.CommittedMetadataValue,
            },
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BackupBlobNaming.ArtifactBlobName(artifactId));

        BlobDownloadStreamingResult? result = null;
        try
        {
            result = (await blob.DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false)).Value;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // No artifact with this id: yield nothing.
        }

        if (result is null)
        {
            yield break;
        }

        using (result)
        {
            var content = result.Content;
            var header = new byte[ChunkLengthPrefixBytes];
            while (true)
            {
                // Read the next frame's length prefix. A clean end-of-stream at a
                // frame boundary (zero bytes) terminates the enumeration; a partial
                // prefix means a truncated/corrupt blob and ReadExactlyAsync throws.
                var first = await content
                    .ReadAsync(header.AsMemory(0, ChunkLengthPrefixBytes), cancellationToken)
                    .ConfigureAwait(false);
                if (first == 0)
                {
                    yield break;
                }

                if (first < ChunkLengthPrefixBytes)
                {
                    await content
                        .ReadExactlyAsync(header.AsMemory(first, ChunkLengthPrefixBytes - first), cancellationToken)
                        .ConfigureAwait(false);
                }

                var length = BinaryPrimitives.ReadInt32LittleEndian(header);
                if (length == 0)
                {
                    yield return ReadOnlyMemory<byte>.Empty;
                    continue;
                }

                var payload = new byte[length];
                await content.ReadExactlyAsync(payload.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                yield return payload;
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BackupBlobNaming.ArtifactBlobName(artifactId));
        var response = await blob.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        return response.Value;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListArtifactIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var item in _container
            .GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, BackupBlobNaming.ArtifactPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            // Skip a partially written (uncommitted) artifact so a listing only
            // surfaces complete chains.
            if (!IsCommitted(item.Metadata))
            {
                continue;
            }

            var id = BackupBlobNaming.ArtifactIdFromBlobName(item.Name);
            if (id is not null)
            {
                yield return id;
            }
        }
    }

    /// <inheritdoc />
    public async Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        var bytes = _serializer.SerializeToArray(manifest);
        var blob = _container.GetBlobClient(BackupBlobNaming.ManifestBlobName(manifest.Id));
        using var stream = new MemoryStream(bytes, writable: false);
        await blob.UploadAsync(stream, overwrite: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BackupBlobNaming.ManifestBlobName(backupId));
        try
        {
            var response = await blob.DownloadContentAsync(cancellationToken).ConfigureAwait(false);
            return _serializer.Deserialize(response.Value.Content.ToMemory());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var item in _container
            .GetBlobsAsync(BlobTraits.None, BlobStates.None, BackupBlobNaming.ManifestPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            var blob = _container.GetBlobClient(item.Name);
            BackupManifest manifest;
            try
            {
                var response = await blob.DownloadContentAsync(cancellationToken).ConfigureAwait(false);
                manifest = _serializer.Deserialize(response.Value.Content.ToMemory());
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Concurrently deleted between listing and read: skip it.
                continue;
            }

            yield return manifest;
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        await EnsureContainerAsync(cancellationToken).ConfigureAwait(false);

        var blob = _container.GetBlobClient(BackupBlobNaming.ManifestBlobName(backupId));
        var response = await blob.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        return response.Value;
    }

    private static bool IsCommitted(IDictionary<string, string> metadata) =>
        metadata.TryGetValue(BackupBlobNaming.CommittedMetadataKey, out var value)
        && value == BackupBlobNaming.CommittedMetadataValue;

    private static async Task<bool> IsCommittedAsync(AppendBlobClient blob, CancellationToken cancellationToken)
    {
        try
        {
            var properties = await blob.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            return IsCommitted(properties.Value.Metadata);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
    }

    private async Task EnsureContainerAsync(CancellationToken cancellationToken)
    {
        if (_containerReady)
        {
            return;
        }

        await _initGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_containerReady)
            {
                return;
            }

            await _container.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            _containerReady = true;
        }
        finally
        {
            _initGate.Release();
        }
    }
}
