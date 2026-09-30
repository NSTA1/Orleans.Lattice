using System.Diagnostics.CodeAnalysis;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using NSubstitute;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// An in-memory stand-in for the blob container the Azure Blob backup sink reads
/// and writes, exposed as a real <see cref="BlobContainerClient"/> whose virtual
/// members are substituted onto a dictionary of blobs. It models the three things
/// the sink actually depends on: append-blob semantics (create truncates, each
/// append concatenates), per-blob metadata (which is how an artifact is marked
/// committed), and prefix listing in lexicographical name order.
/// <para>
/// It exists because every behavioural fixture for
/// <see cref="AzureBlobLatticeBackupSink"/> is
/// <c>[Category("AzureStorageEmulator")]</c>, so the whole sink - chunk framing,
/// the committed-metadata idempotency gate, the 404 arms, listing, and probing -
/// never executes in the default test run. Modelling the container in memory moves
/// all of it into the default suite while keeping the assertions behavioural: an
/// artifact round-trips with its chunk boundaries intact, an uncommitted artifact
/// is invisible to a listing, a probe reports exactly the missing ids.
/// </para>
/// </summary>
internal sealed class FakeBackupBlobStore
{
    private readonly Dictionary<string, Entry> _blobs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, BlobClient> _blobClients = new(StringComparer.Ordinal);
    private readonly Dictionary<string, AppendBlobClient> _appendClients = new(StringComparer.Ordinal);

    private sealed class Entry
    {
        public List<byte> Content { get; } = [];

        public Dictionary<string, string> Metadata { get; set; } = new(StringComparer.Ordinal);
    }

    /// <summary>How many times the container's create-if-not-exists call was made.</summary>
    public int ContainerCreateCalls { get; private set; }

    /// <summary>How many append-block calls were issued, across every blob.</summary>
    public int AppendBlockCalls { get; private set; }

    /// <summary>How many times an append blob was created (or truncated).</summary>
    public int AppendBlobCreateCalls { get; private set; }

    /// <summary>
    /// When set, the number of bytes each artifact read may return at once. A
    /// network stream is free to satisfy a read short, and the artifact reader
    /// tolerates a length prefix split across two reads; leaving this null returns
    /// a plain stream, which never exercises that completion path.
    /// </summary>
    public int? ArtifactReadDribbleBytes { get; set; }

    /// <summary>
    /// Blob names that are listed but raise 404 when their content is read,
    /// reproducing a blob concurrently deleted between a listing and the read that
    /// follows it.
    /// </summary>
    public HashSet<string> VanishOnRead { get; } = new(StringComparer.Ordinal);

    /// <summary>The blob names currently present, in lexicographical order.</summary>
    public IReadOnlyList<string> Names => [.. _blobs.Keys.Order(StringComparer.Ordinal)];

    /// <summary>True when a blob with <paramref name="name"/> is present.</summary>
    public bool Exists(string name) => _blobs.ContainsKey(name);

    /// <summary>The raw stored bytes of <paramref name="name"/>.</summary>
    public byte[] ContentOf(string name) => [.. _blobs[name].Content];

    /// <summary>Seeds a blob directly, bypassing the sink's own write path.</summary>
    public void Seed(string name, byte[] content, IDictionary<string, string>? metadata = null)
    {
        var entry = new Entry();
        entry.Content.AddRange(content);
        if (metadata is not null)
        {
            entry.Metadata = new Dictionary<string, string>(metadata, StringComparer.Ordinal);
        }

        _blobs[name] = entry;
    }

    /// <summary>Marks a seeded artifact blob committed, as a completed write would.</summary>
    public void MarkCommitted(string name) =>
        _blobs[name].Metadata[BackupBlobNaming.CommittedMetadataKey] = BackupBlobNaming.CommittedMetadataValue;

    private static RequestFailedException NotFound() =>
        new(404, "The specified blob does not exist.", BlobErrorCode.BlobNotFound.ToString(), innerException: null);

    private static Response<T> Ok<T>(T value) => Response.FromValue(value, Substitute.For<Response>());

    private static readonly ETag Tag = new("\"etag\"");

    /// <summary>Builds the container client the sink is constructed over.</summary>
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Substitutes are owned by the test fixture and have no unmanaged state.")]
    public BlobContainerClient CreateContainerClient()
    {
        var container = Substitute.For<BlobContainerClient>();

        container.CreateIfNotExistsAsync(
                Arg.Any<PublicAccessType>(),
                Arg.Any<IDictionary<string, string>>(),
                Arg.Any<BlobContainerEncryptionScopeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                ContainerCreateCalls++;
                return Task.FromResult(Ok(BlobsModelFactory.BlobContainerInfo(Tag, DateTimeOffset.UnixEpoch)));
            });

        container.GetBlobClient(Arg.Any<string>()).Returns(call => GetOrCreateBlobClient(call.Arg<string>()));
        container.GetAppendBlobClient(Arg.Any<string>()).Returns(call => GetOrCreateAppendClient(call.Arg<string>()));

        container.GetBlobsAsync(
                Arg.Any<BlobTraits>(),
                Arg.Any<BlobStates>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(call => ListBlobs(call.ArgAt<string>(2)));

        return container;
    }

    private AsyncPageable<BlobItem> ListBlobs(string? prefix)
    {
        // Azure returns a listing in lexicographical name order, and the sink's
        // ordering contract depends on that, so the fake sorts rather than
        // enumerating dictionary order.
        var items = _blobs
            .Where(kv => prefix is null || kv.Key.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => BlobsModelFactory.BlobItem(
                name: kv.Key,
                metadata: new Dictionary<string, string>(kv.Value.Metadata, StringComparer.Ordinal)))
            .ToList();

        return AsyncPageable<BlobItem>.FromPages([Page<BlobItem>.FromValues(items, null, Substitute.For<Response>())]);
    }

    private BlobClient GetOrCreateBlobClient(string name)
    {
        if (_blobClients.TryGetValue(name, out var existing))
        {
            return existing;
        }

        var client = BuildBlobClient(name);
        _blobClients[name] = client;
        return client;
    }

    private AppendBlobClient GetOrCreateAppendClient(string name)
    {
        if (_appendClients.TryGetValue(name, out var existing))
        {
            return existing;
        }

        var client = BuildAppendClient(name);
        _appendClients[name] = client;
        return client;
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Substitutes are owned by the test fixture and have no unmanaged state.")]
    private BlobClient BuildBlobClient(string name)
    {
        var blob = Substitute.For<BlobClient>();

        blob.DownloadContentAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            _blobs.TryGetValue(name, out var entry) && !VanishOnRead.Contains(name)
                ? Task.FromResult(Ok(BlobsModelFactory.BlobDownloadResult(
                    BinaryData.FromBytes([.. entry.Content]),
                    BlobsModelFactory.BlobDownloadDetails(metadata: entry.Metadata, eTag: Tag))))
                : Task.FromException<Response<BlobDownloadResult>>(NotFound()));

        ConfigureDownloadStreaming(blob, name);

        blob.ExistsAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Ok(_blobs.ContainsKey(name))));

        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            using var buffer = new MemoryStream();
            call.Arg<Stream>().CopyTo(buffer);
            Seed(name, buffer.ToArray());
            return Task.FromResult(Ok(BlobsModelFactory.BlobContentInfo(Tag, DateTimeOffset.UnixEpoch, [], null, null, null, 0)));
        });

        blob.DeleteIfExistsAsync(
                Arg.Any<DeleteSnapshotsOption>(),
                Arg.Any<BlobRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Ok(_blobs.Remove(name))));

        return blob;
    }

    /// <summary>
    /// Wires every <c>DownloadStreamingAsync</c> overload to the same body. Which
    /// one the sink's <c>cancellationToken:</c>-only call binds to is an overload
    /// -resolution detail of the SDK, so configuring all of them keeps the fake
    /// correct across an SDK revision that adds or reorders one.
    /// </summary>
    private void ConfigureDownloadStreaming(BlobClient blob, string name)
    {
        blob.DownloadStreamingAsync(
                Arg.Any<HttpRange>(),
                Arg.Any<BlobRequestConditions>(),
                Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => DownloadStreaming(name));

        blob.DownloadStreamingAsync(Arg.Any<BlobDownloadOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => DownloadStreaming(name));
    }

    private Task<Response<BlobDownloadStreamingResult>> DownloadStreaming(string name)
    {
        if (!_blobs.TryGetValue(name, out var entry))
        {
            return Task.FromException<Response<BlobDownloadStreamingResult>>(NotFound());
        }

        var bytes = entry.Content.ToArray();
        var details = BlobsModelFactory.BlobDownloadDetails(
            contentLength: bytes.Length,
            metadata: entry.Metadata,
            eTag: Tag);

        Stream content = new MemoryStream(bytes, writable: false);
        if (ArtifactReadDribbleBytes is { } dribble)
        {
            content = new DribbleStream(content, dribble);
        }

        return Task.FromResult(Ok(BlobsModelFactory.BlobDownloadStreamingResult(
            content: content,
            details: details)));
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Substitutes are owned by the test fixture and have no unmanaged state.")]
    private AppendBlobClient BuildAppendClient(string name)
    {
        var blob = Substitute.For<AppendBlobClient>();

        // Create truncates: it is how the sink discards a partial prior attempt.
        blob.CreateAsync(Arg.Any<AppendBlobCreateOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => CreateAppendBlob(name));
        blob.CreateAsync(
                Arg.Any<BlobHttpHeaders>(),
                Arg.Any<IDictionary<string, string>>(),
                Arg.Any<AppendBlobRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => CreateAppendBlob(name));

        blob.AppendBlockAsync(Arg.Any<Stream>(), Arg.Any<AppendBlobAppendBlockOptions>(), Arg.Any<CancellationToken>())
            .Returns(call => AppendBlock(name, call.Arg<Stream>()));
        blob.AppendBlockAsync(
                Arg.Any<Stream>(),
                Arg.Any<byte[]>(),
                Arg.Any<AppendBlobRequestConditions>(),
                Arg.Any<IProgress<long>>(),
                Arg.Any<CancellationToken>())
            .Returns(call => AppendBlock(name, call.Arg<Stream>()));

        blob.GetPropertiesAsync(Arg.Any<BlobRequestConditions>(), Arg.Any<CancellationToken>()).Returns(_ =>
            _blobs.TryGetValue(name, out var entry)
                ? Task.FromResult(Ok(BlobsModelFactory.BlobProperties(
                    metadata: new Dictionary<string, string>(entry.Metadata, StringComparer.Ordinal),
                    eTag: Tag,
                    smartAccessTier: null)))
                : Task.FromException<Response<BlobProperties>>(NotFound()));

        blob.SetMetadataAsync(
                Arg.Any<IDictionary<string, string>>(),
                Arg.Any<BlobRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (!_blobs.TryGetValue(name, out var entry))
                {
                    return Task.FromException<Response<BlobInfo>>(NotFound());
                }

                entry.Metadata = new Dictionary<string, string>(
                    call.Arg<IDictionary<string, string>>(),
                    StringComparer.Ordinal);
                return Task.FromResult(Ok(BlobsModelFactory.BlobInfo(Tag, DateTimeOffset.UnixEpoch)));
            });

        return blob;
    }

    private Task<Response<BlobContentInfo>> CreateAppendBlob(string name)
    {
        AppendBlobCreateCalls++;
        _blobs[name] = new Entry();
        return Task.FromResult(Ok(BlobsModelFactory.BlobContentInfo(Tag, DateTimeOffset.UnixEpoch, [], null, null, null, 0)));
    }

    private Task<Response<BlobAppendInfo>> AppendBlock(string name, Stream content)
    {
        AppendBlockCalls++;
        using var buffer = new MemoryStream();
        content.CopyTo(buffer);
        _blobs[name].Content.AddRange(buffer.ToArray());
        return Task.FromResult(Ok(BlobsModelFactory.BlobAppendInfo(
            Tag, DateTimeOffset.UnixEpoch, [], [], "0", 1, false, null, null)));
    }
}
