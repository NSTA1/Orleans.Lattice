using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// End-to-end tests for <see cref="AzureBlobLatticeBackupSink"/> driven against an
/// Azure Blob Storage endpoint - canonically
/// <see href="https://learn.microsoft.com/azure/storage/common/storage-use-azurite">Azurite</see>
/// listening on the default development connection string. Gated under the
/// <c>AzureBlobEmulator</c> NUnit category so the default dev loop skips them when
/// no emulator is running; <see cref="OneTimeSetUp"/> probes reachability and
/// falls through to <see cref="Assert.Inconclusive(string)"/> if the probe fails.
/// <para>
/// Each test uses a fresh, GUID-named container created on first sink use and torn
/// down in <see cref="TearDown"/>, so tests exercise the sink's own
/// create-on-first-use codepath rather than relying on an out-of-band setup step.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureBlobEmulator")]
public class AzureBlobLatticeBackupSinkEmulatorTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";

    private ServiceProvider _services = null!;
    private Serializer<BackupManifest> _serializer = null!;
    private BlobServiceClient _adminClient = null!;
    private string _containerName = null!;
    private AzureBlobLatticeBackupSink _sut = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly))
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<BackupManifest>>();
        _adminClient = new BlobServiceClient(AzuriteConnectionString);

        try
        {
            // Cheap round-trip to prove Azurite (or a real account wired via
            // `UseDevelopmentStorage=true`) is reachable.
            await foreach (var _ in _adminClient.GetBlobContainersAsync())
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + "Start it via 'azurite --silent --location <dir>' or skip the AzureBlobEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _containerName = "backup-test-" + Guid.NewGuid().ToString("n");
        var options = new LatticeBackupAzureBlobOptions
        {
            ConnectionString = AzuriteConnectionString,
            ContainerName = _containerName,
        };

        _sut = new AzureBlobLatticeBackupSink(options.BuildContainerClient(), _serializer);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _adminClient.DeleteBlobContainerAsync(_containerName);
    }

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
            Assert.That(read.Provenance[0].OriginId, Is.EqualTo("replica-a"));
        });
    }

    [Test]
    public async Task ReadManifestAsync_returns_null_when_absent()
    {
        var read = await _sut.ReadManifestAsync("missing");
        Assert.That(read, Is.Null);
    }

    [Test]
    public async Task WriteManifestAsync_is_idempotent()
    {
        var manifest = SampleManifest("backup-1");

        await _sut.WriteManifestAsync(manifest);
        await _sut.WriteManifestAsync(manifest);

        var manifests = await CollectAsync(_sut.ListManifestsAsync());
        Assert.That(manifests, Has.Count.EqualTo(1));
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
    public async Task DeleteManifestAsync_removes_then_reports_absence()
    {
        await _sut.WriteManifestAsync(SampleManifest("backup-1"));

        Assert.Multiple(async () =>
        {
            Assert.That(await _sut.DeleteManifestAsync("backup-1"), Is.True);
            Assert.That(await _sut.DeleteManifestAsync("backup-1"), Is.False);
        });
    }

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
    public async Task ReadArtifactAsync_yields_nothing_when_absent()
    {
        var read = await CollectAsync(_sut.ReadArtifactAsync("missing"));
        Assert.That(read, Is.Empty);
    }

    [Test]
    public async Task WriteArtifactAsync_is_idempotent_for_identical_content()
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
    public async Task ListArtifactIdsAsync_returns_committed_ids_in_order()
    {
        await _sut.WriteArtifactAsync("c", ToAsync(new[] { new byte[] { 1 } }));
        await _sut.WriteArtifactAsync("a", ToAsync(new[] { new byte[] { 2 } }));
        await _sut.WriteArtifactAsync("b", ToAsync(new[] { new byte[] { 3 } }));

        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());

        Assert.That(ids, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task DeleteArtifactAsync_removes_then_reports_absence()
    {
        await _sut.WriteArtifactAsync("artifact-1", ToAsync(new[] { new byte[] { 1 } }));

        Assert.Multiple(async () =>
        {
            Assert.That(await _sut.DeleteArtifactAsync("artifact-1"), Is.True);
            Assert.That(await _sut.DeleteArtifactAsync("artifact-1"), Is.False);
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
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: new[] { new BackupContentDescriptor("artifact-1", "abc123", 12, 1, scope) },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) });
    }
}
