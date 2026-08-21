using System.Buffers.Binary;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Behavioural coverage for <see cref="AzureBlobLatticeBackupSink"/> driven against
/// a real Azure Blob Storage endpoint (canonically Azurite on the development
/// connection string). Unlike the sibling
/// <see cref="AzureBlobLatticeBackupSinkEmulatorTests"/>, this fixture pins the
/// Azure SDK <see cref="BlobClientOptions.ServiceVersion"/> to a value the shared
/// emulator understands, so it exercises the sink even when the SDK's newest
/// default API version outruns the emulator build. It supplies the pre-built
/// <see cref="BlobServiceClient"/> via the options' <c>ServiceClient</c> mode.
/// <para>
/// Every test uses a fresh, uniquely prefixed container so concurrent runners do
/// not collide, and tears it down afterward.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureTableEmulator")]
public sealed class AzureBlobLatticeBackupSinkVersionedEmulatorTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";

    // Pinned to the newest blob-service API version the CI Azurite build (3.36.0)
    // accepts (2025-11-05); the SDK default advertises a newer API version than the
    // emulator supports. Kept identical to the sibling caching.azureblob versioned
    // fixture so both blob suites make the same assumption.
    private const BlobClientOptions.ServiceVersion PinnedVersion =
        BlobClientOptions.ServiceVersion.V2025_11_05;

    private ServiceProvider _services = null!;
    private Serializer<BackupManifest> _serializer = null!;
    private BlobServiceClient _serviceClient = null!;
    private BlobContainerClient _adminContainer = null!;
    private string _containerName = null!;
    private AzureBlobLatticeBackupSink _sut = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly))
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<BackupManifest>>();
        _serviceClient = new BlobServiceClient(AzuriteConnectionString, new BlobClientOptions(PinnedVersion));

        try
        {
            await foreach (var _ in _serviceClient.GetBlobContainersAsync())
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azure Blob endpoint is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public async Task SetUp()
    {
        _containerName = "backup-azblob-cov-" + Guid.NewGuid().ToString("n");
        _adminContainer = _serviceClient.GetBlobContainerClient(_containerName);
        await _adminContainer.CreateIfNotExistsAsync();

        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceClient = _serviceClient,
            ContainerName = _containerName,
        };

        _sut = new AzureBlobLatticeBackupSink(options.BuildContainerClient(), _serializer);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _serviceClient.DeleteBlobContainerAsync(_containerName);
    }

    // ---- Durability contract --------------------------------------------

    [Test]
    public void IsDurable_is_true()
    {
        Assert.That(_sut.IsDurable, Is.True);
    }

    // ---- Manifests -------------------------------------------------------

    [Test]
    public async Task WriteManifestAsync_then_ReadManifestAsync_round_trips()
    {
        var manifest = SampleManifest("backup-1");

        await _sut.WriteManifestAsync(manifest);
        var read = await _sut.ReadManifestAsync("backup-1");

        Assert.That(read, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(read!.Id, Is.EqualTo("backup-1"));
            Assert.That(read.Name, Is.EqualTo("nightly"));
            Assert.That(read.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(read.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(read.ContentDescriptors[0].ArtifactId, Is.EqualTo("artifact-1"));
        });
    }

    [Test]
    public async Task ReadManifestAsync_returns_null_when_absent()
    {
        var read = await _sut.ReadManifestAsync("missing");
        Assert.That(read, Is.Null);
    }

    [Test]
    public async Task WriteManifestAsync_overwrites_existing_manifest()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-1", name: "first"));
        await _sut.WriteManifestAsync(SampleManifest("backup-1", name: "second"));

        var manifests = await CollectAsync(_sut.ListManifestsAsync());
        Assert.Multiple(() =>
        {
            Assert.That(manifests, Has.Count.EqualTo(1));
            Assert.That(manifests[0].Name, Is.EqualTo("second"));
        });
    }

    [Test]
    public async Task ListManifestsAsync_returns_manifests_in_id_order()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-c"));
        await _sut.WriteManifestAsync(SampleManifest("backup-a"));
        await _sut.WriteManifestAsync(SampleManifest("backup-b"));

        var ids = (await CollectAsync(_sut.ListManifestsAsync())).Select(m => m.Id).ToArray();

        Assert.That(ids, Is.EqualTo(new[] { "backup-a", "backup-b", "backup-c" }));
    }

    [Test]
    public async Task ListManifestsAsync_is_empty_when_none_written()
    {
        var manifests = await CollectAsync(_sut.ListManifestsAsync());
        Assert.That(manifests, Is.Empty);
    }

    [Test]
    public async Task DeleteManifestAsync_removes_then_reports_absence()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-1"));

        var first = await _sut.DeleteManifestAsync("backup-1");
        var second = await _sut.DeleteManifestAsync("backup-1");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(second, Is.False);
        });
    }

    [Test]
    public async Task ManifestExistsAsync_is_true_after_write_and_false_after_delete()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-exists"));
        var present = await _sut.ManifestExistsAsync("backup-exists");

        await _sut.DeleteManifestAsync("backup-exists");
        var absent = await _sut.ManifestExistsAsync("backup-exists");

        Assert.Multiple(() =>
        {
            Assert.That(present, Is.True);
            Assert.That(absent, Is.False);
        });
    }

    [Test]
    public async Task ManifestExistsAsync_is_false_for_an_unknown_backup()
    {
        Assert.That(await _sut.ManifestExistsAsync("never-written"), Is.False);
    }

    // ---- Artifacts -------------------------------------------------------

    [Test]
    public async Task WriteArtifactAsync_appends_chunks_and_reads_them_back_in_order()
    {
        var chunks = new[]
        {
            new byte[] { 1, 2, 3 },
            new byte[] { 4, 5 },
            new byte[] { 6 },
        };

        await _sut.WriteArtifactAsync("artifact-1", ToAsync(chunks));
        var read = await ReadAllBytesAsync(_sut.ReadArtifactAsync("artifact-1"));

        Assert.That(read, Is.EqualTo(new byte[] { 1, 2, 3, 4, 5, 6 }));
    }

    [Test]
    public async Task ReadArtifactAsync_preserves_written_chunk_boundaries()
    {
        var chunks = new[]
        {
            new byte[] { 1, 2, 3 },
            new byte[] { 4, 5 },
            new byte[] { 6 },
        };

        await _sut.WriteArtifactAsync("artifact-1", ToAsync(chunks));
        var read = await CollectAsync(_sut.ReadArtifactAsync("artifact-1"));

        Assert.That(read.Select(c => c.ToArray()), Is.EqualTo(chunks));
    }

    [Test]
    public async Task WriteArtifactAsync_splits_a_chunk_larger_than_the_append_block_limit()
    {
        // A single chunk exceeding the 4 MiB AppendBlock limit forces the sink's
        // physical block-splitting loop, and must still read back as one whole
        // logical frame on the same boundary it was written.
        var big = new byte[(5 * 1024 * 1024) + 17];
        for (var i = 0; i < big.Length; i++)
        {
            big[i] = (byte)(i % 251);
        }

        var chunks = new[] { big, new byte[] { 42 } };

        await _sut.WriteArtifactAsync("artifact-big", ToAsync(chunks));
        var read = await CollectAsync(_sut.ReadArtifactAsync("artifact-big"));

        Assert.That(read.Select(c => c.ToArray()), Is.EqualTo(chunks));
    }

    [Test]
    public async Task ReadArtifactAsync_yields_an_empty_chunk_for_a_zero_length_frame()
    {
        var chunks = new[]
        {
            new byte[] { 1 },
            Array.Empty<byte>(),
            new byte[] { 2 },
        };

        await _sut.WriteArtifactAsync("artifact-empty", ToAsync(chunks));
        var read = await CollectAsync(_sut.ReadArtifactAsync("artifact-empty"));

        Assert.That(read.Select(c => c.ToArray()), Is.EqualTo(chunks));
    }

    [Test]
    public async Task ReadArtifactAsync_yields_nothing_when_absent()
    {
        var read = await CollectAsync(_sut.ReadArtifactAsync("missing"));
        Assert.That(read, Is.Empty);
    }

    [Test]
    public async Task WriteArtifactAsync_is_idempotent_for_a_committed_artifact()
    {
        var chunks = new[] { new byte[] { 9, 8, 7 } };

        await _sut.WriteArtifactAsync("artifact-1", ToAsync(chunks));
        await _sut.WriteArtifactAsync("artifact-1", ToAsync(chunks));

        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());
        var read = await ReadAllBytesAsync(_sut.ReadArtifactAsync("artifact-1"));

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.EqualTo(new[] { "artifact-1" }));
            Assert.That(read, Is.EqualTo(new byte[] { 9, 8, 7 }));
        });
    }

    [Test]
    public async Task WriteArtifactAsync_overwrites_a_partial_uncommitted_blob()
    {
        // A crash mid-append leaves an uncommitted append blob. A retried write
        // must discard it (CreateAsync overwrite) and produce a clean, committed
        // chain rather than treating the torn blob as an idempotent no-op.
        var blobName = BackupBlobNaming.ArtifactBlobName("artifact-partial");
        var partial = _adminContainer.GetAppendBlobClient(blobName);
        await partial.CreateAsync();
        using (var junk = new MemoryStream(new byte[] { 0xDE, 0xAD }))
        {
            await partial.AppendBlockAsync(junk);
        }

        await _sut.WriteArtifactAsync("artifact-partial", ToAsync(new[] { new byte[] { 1, 2, 3 } }));

        var read = await ReadAllBytesAsync(_sut.ReadArtifactAsync("artifact-partial"));
        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(ids, Is.EqualTo(new[] { "artifact-partial" }));
        });
    }

    [Test]
    public async Task ListArtifactIdsAsync_returns_committed_ids_in_order()
    {
        await _sut.WriteArtifactAsync("c", ToAsync(new[] { new byte[] { 1 } }));
        await _sut.WriteArtifactAsync("a", ToAsync(new[] { new byte[] { 2 } }));
        await _sut.WriteArtifactAsync("b", ToAsync(new[] { new byte[] { 3 } }));

        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());

        Assert.That(ids, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task ListArtifactIdsAsync_skips_an_uncommitted_artifact()
    {
        await _sut.WriteArtifactAsync("committed", ToAsync(new[] { new byte[] { 1 } }));

        // A raw, uncommitted append blob (no committed metadata) must not surface
        // in a listing of complete chains.
        var uncommitted = _adminContainer.GetAppendBlobClient(BackupBlobNaming.ArtifactBlobName("uncommitted"));
        await uncommitted.CreateAsync();

        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());

        Assert.That(ids, Is.EqualTo(new[] { "committed" }));
    }

    [Test]
    public async Task ListArtifactIdsAsync_is_empty_when_none_written()
    {
        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());
        Assert.That(ids, Is.Empty);
    }

    [Test]
    public async Task DeleteArtifactAsync_removes_then_reports_absence()
    {
        await _sut.WriteArtifactAsync("artifact-1", ToAsync(new[] { new byte[] { 1 } }));

        var first = await _sut.DeleteArtifactAsync("artifact-1");
        var second = await _sut.DeleteArtifactAsync("artifact-1");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(second, Is.False);
        });
    }

    [Test]
    public void ReadArtifactAsync_throws_on_a_corrupt_over_large_frame()
    {
        // A frame length prefix larger than the blob that carries it means the
        // artifact is truncated or hostile; the reader must refuse it rather than
        // size an arbitrary buffer from an untrusted prefix.
        Assert.That(
            async () =>
            {
                var blob = _adminContainer.GetAppendBlobClient(BackupBlobNaming.ArtifactBlobName("artifact-corrupt"));
                await blob.CreateAsync();
                var header = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(header, int.MaxValue);
                using (var stream = new MemoryStream(header))
                {
                    await blob.AppendBlockAsync(stream);
                }

                await CollectAsync(_sut.ReadArtifactAsync("artifact-corrupt"));
            },
            Throws.InstanceOf<InvalidDataException>());
    }

    // ---- Probe -----------------------------------------------------------

    [Test]
    public async Task ProbeAsync_reports_resolvable_when_manifest_and_committed_artifact_are_present()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-resolvable"));
        await _sut.WriteArtifactAsync("artifact-1", ToAsync(new[] { new byte[] { 1, 2, 3 } }));

        var resolution = await _sut.ProbeAsync("backup-resolvable");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.BackupId, Is.EqualTo("backup-resolvable"));
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.Empty);
            Assert.That(resolution.IsResolvable, Is.True);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_the_missing_artifact_when_only_the_manifest_is_present()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-torn"));

        var resolution = await _sut.ProbeAsync("backup-torn");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "artifact-1" }));
            Assert.That(resolution.IsResolvable, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_treats_an_uncommitted_artifact_as_missing()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-uncommitted"));

        // Present-but-uncommitted append blob: a torn write that must count as
        // missing (a HEAD on the committed metadata, never a download).
        var uncommitted = _adminContainer.GetAppendBlobClient(BackupBlobNaming.ArtifactBlobName("artifact-1"));
        await uncommitted.CreateAsync();

        var resolution = await _sut.ProbeAsync("backup-uncommitted");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "artifact-1" }));
            Assert.That(resolution.IsResolvable, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_absent_manifest_for_an_unknown_backup()
    {
        var resolution = await _sut.ProbeAsync("never-written");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.False);
            Assert.That(resolution.MissingArtifactIds, Is.Empty);
            Assert.That(resolution.IsResolvable, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_deduplicates_repeated_artifact_ids()
    {
        // A manifest can reference the same artifact from several descriptors; the
        // probe must check each distinct artifact once and not double-report it.
        var scope = BackupScopeSelector.WholeTree("orders");
        var manifest = new BackupManifest(
            id: "backup-dup",
            name: "nightly",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: new[]
            {
                new BackupContentDescriptor("artifact-dup", "abc123", 12, 1, scope),
                new BackupContentDescriptor("artifact-dup", "abc123", 12, 1, scope),
            },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) });

        await _sut.WriteManifestAsync(manifest);

        var resolution = await _sut.ProbeAsync("backup-dup");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "artifact-dup" }));
        });
    }

    // ---- Helpers ---------------------------------------------------------

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ToAsync(IEnumerable<byte[]> chunks)
    {
        foreach (var chunk in chunks)
        {
            await Task.Yield();
            yield return chunk;
        }
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
        {
            list.Add(item);
        }

        return list;
    }

    private static async Task<byte[]> ReadAllBytesAsync(IAsyncEnumerable<ReadOnlyMemory<byte>> source)
    {
        using var buffer = new MemoryStream();
        await foreach (var chunk in source)
        {
            buffer.Write(chunk.Span);
        }

        return buffer.ToArray();
    }

    private static BackupManifest SampleManifest(string id, string name = "nightly")
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        return new BackupManifest(
            id: id,
            name: name,
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: new[] { new BackupContentDescriptor("artifact-1", "abc123", 12, 1, scope) },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) });
    }
}
