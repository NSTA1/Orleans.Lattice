using System.Net;
using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Covers the tolerance paths of <see cref="AzureBlobLatticeBackupSink"/> that a
/// healthy, local emulator never produces: a chunk length prefix that arrives split
/// across two network reads, a manifest deleted between listing and read, and the
/// double-checked container initialisation only a second concurrent caller can reach.
/// <para>
/// Each case drives a real Azure Blob endpoint through
/// <see cref="InterceptingHttpHandler"/>, so only the one call under test behaves
/// unusually and every surrounding operation is genuine blob I/O. That keeps the
/// assertions behavioural - the artifact still reads back with its original chunk
/// boundaries, the surviving manifests are still returned - rather than merely
/// asserting that a branch was entered.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureBlobLatticeBackupSinkFaultInjectionTests
{
    private ServiceProvider _services = null!;
    private Serializer<BackupManifest> _serializer = null!;
    private BlobServiceClient _adminClient = null!;
    private InterceptingHttpHandler _handler = null!;
    private HttpClient _httpClient = null!;
    private string _containerName = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly))
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<BackupManifest>>();
        _adminClient = AzuriteEmulator.CreateServiceClient();

        try
        {
            await foreach (var _ in _adminClient.GetBlobContainersAsync())
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azure Blob endpoint is not reachable on the default development endpoint ({AzuriteEmulator.ConnectionString}). "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _containerName = "backup-azblob-fault-" + Guid.NewGuid().ToString("n");
        _handler = new InterceptingHttpHandler();
        _httpClient = new HttpClient(_handler);
    }

    [TearDown]
    public async Task TearDown()
    {
        _httpClient.Dispose();
        await _adminClient.DeleteBlobContainerAsync(_containerName);
    }

    /// <summary>
    /// Builds a sink whose transport routes through <see cref="_handler"/>. The
    /// service client is constructed here so the blob API version can be pinned to
    /// one the emulator accepts; retries are disabled so an injected failure reaches
    /// the code under test once, immediately, rather than being retried into a
    /// timeout.
    /// </summary>
    private AzureBlobLatticeBackupSink CreateSink()
    {
        var clientOptions = new BlobClientOptions(AzuriteEmulator.ApiVersion)
        {
            Transport = new Azure.Core.Pipeline.HttpClientTransport(_httpClient),
        };
        clientOptions.Retry.MaxRetries = 0;

        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceClient = new BlobServiceClient(AzuriteEmulator.ConnectionString, clientOptions),
            ContainerName = _containerName,
        };
        return new AzureBlobLatticeBackupSink(options.BuildContainerClient(), _serializer);
    }

    // ---- Chunk framing across a split network read -----------------------

    [Test]
    public async Task ReadArtifactAsync_reassembles_a_length_prefix_split_across_reads()
    {
        var sink = CreateSink();
        var chunks = new[]
        {
            Encoding.UTF8.GetBytes("first-chunk"),
            Encoding.UTF8.GetBytes("second-chunk-is-longer"),
            Array.Empty<byte>(),
            Encoding.UTF8.GetBytes("tail"),
        };
        await sink.WriteArtifactAsync("split-prefix", ToAsync(chunks));

        // From here the artifact body is delivered one byte per read, so every
        // 4-byte length prefix is necessarily split and must be completed before it
        // is trusted.
        _handler.DribbleResponseBody = request =>
            InterceptingHttpHandler.IsBlobDownload(request, "artifacts/split-prefix");

        var read = new List<byte[]>();
        await foreach (var chunk in sink.ReadArtifactAsync("split-prefix"))
        {
            read.Add(chunk.ToArray());
        }

        Assert.That(
            read,
            Is.EqualTo(chunks),
            "A prefix split across reads must be completed, not truncated: the reader has to "
            + "restore the exact chunk boundaries the writer framed, including the empty chunk.");
    }

    [Test]
    public async Task ReadArtifactAsync_reassembles_a_chunk_larger_than_one_read()
    {
        var sink = CreateSink();
        var payload = new byte[8 * 1024];
        Random.Shared.NextBytes(payload);
        await sink.WriteArtifactAsync("split-payload", ToAsync([payload]));

        _handler.DribbleResponseBody = request =>
            InterceptingHttpHandler.IsBlobDownload(request, "artifacts/split-payload");

        var read = new List<byte[]>();
        await foreach (var chunk in sink.ReadArtifactAsync("split-payload"))
        {
            read.Add(chunk.ToArray());
        }

        Assert.Multiple(() =>
        {
            Assert.That(read, Has.Count.EqualTo(1));
            Assert.That(read[0], Is.EqualTo(payload));
        });
    }

    // ---- Manifest deleted between listing and read -----------------------

    [Test]
    public async Task ListManifestsAsync_skips_a_manifest_deleted_between_listing_and_read()
    {
        var sink = CreateSink();
        await sink.WriteManifestAsync(SampleManifest("backup-a"));
        await sink.WriteManifestAsync(SampleManifest("backup-b"));
        await sink.WriteManifestAsync(SampleManifest("backup-c"));

        // The listing still reports backup-b, but its download reports the blob as
        // gone - exactly what a concurrent retention prune produces.
        _handler.Interceptor = (request, _) => Task.FromResult(
            InterceptingHttpHandler.IsBlobDownload(request, "manifests/backup-b")
                ? InterceptingHttpHandler.StorageError(HttpStatusCode.NotFound, "BlobNotFound")
                : null);

        var ids = new List<string>();
        await foreach (var manifest in sink.ListManifestsAsync())
        {
            ids.Add(manifest.Id);
        }

        Assert.That(
            ids,
            Is.EqualTo(new[] { "backup-a", "backup-c" }),
            "A manifest that disappears mid-enumeration must be skipped and the enumeration "
            + "continued, not abandoned and not surfaced as an error.");
    }

    [Test]
    public async Task ListManifestsAsync_is_empty_when_every_listed_manifest_has_been_deleted()
    {
        var sink = CreateSink();
        await sink.WriteManifestAsync(SampleManifest("backup-only"));

        _handler.Interceptor = (request, _) => Task.FromResult(
            InterceptingHttpHandler.IsBlobDownload(request, "manifests/backup-only")
                ? InterceptingHttpHandler.StorageError(HttpStatusCode.NotFound, "BlobNotFound")
                : null);

        var manifests = new List<BackupManifest>();
        await foreach (var manifest in sink.ListManifestsAsync())
        {
            manifests.Add(manifest);
        }

        Assert.That(manifests, Is.Empty);
    }

    // ---- Double-checked container initialisation -------------------------

    [Test]
    public async Task A_second_caller_arriving_during_initialisation_does_not_recreate_the_container()
    {
        var sink = CreateSink();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCreateStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var createAttempts = 0;

        _handler.Interceptor = async (request, _) =>
        {
            if (InterceptingHttpHandler.IsContainerCreate(request))
            {
                Interlocked.Increment(ref createAttempts);
                firstCreateStarted.TrySetResult();
                await release.Task.ConfigureAwait(false);
            }

            return null;
        };

        // Caller one enters the init gate and parks inside CreateIfNotExists.
        var first = sink.WriteManifestAsync(SampleManifest("backup-first"));
        await firstCreateStarted.Task;

        // Caller two runs synchronously as far as the gate's WaitAsync, which cannot
        // complete while caller one holds it - so by the time this call returns its
        // task, caller two is provably queued behind the initialisation.
        var second = sink.WriteManifestAsync(SampleManifest("backup-second"));

        release.SetResult();
        await Task.WhenAll(first, second);

        var ids = new List<string>();
        await foreach (var manifest in sink.ListManifestsAsync())
        {
            ids.Add(manifest.Id);
        }

        Assert.Multiple(() =>
        {
            Assert.That(createAttempts, Is.EqualTo(1),
                "The second caller must observe the completed initialisation under the gate "
                + "and skip the create, not issue a redundant one.");
            Assert.That(ids, Is.EqualTo(new[] { "backup-first", "backup-second" }));
        });
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ToAsync(IEnumerable<byte[]> chunks)
    {
        foreach (var chunk in chunks)
        {
            await Task.Yield();
            yield return chunk;
        }
    }

    private static BackupManifest SampleManifest(string id)
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        return new BackupManifest(
            id: id,
            name: "nightly",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, ["d0", "d1"]),
            structuralDigest: "digest-root",
            keyDescriptors: [new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a")],
            contentDescriptors: [new BackupContentDescriptor("artifact-1", "abc123", 12, 1, scope)],
            provenance: [new BackupOriginProvenance("replica-a", 42)]);
    }
}
