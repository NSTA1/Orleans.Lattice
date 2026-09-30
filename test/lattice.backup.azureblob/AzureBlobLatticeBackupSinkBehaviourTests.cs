using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Behavioural coverage for <see cref="AzureBlobLatticeBackupSink"/> driven against
/// an in-memory <see cref="FakeBackupBlobStore"/>, so the sink's own orchestration
/// runs in the default test suite rather than only when Azurite happens to be up.
/// <para>
/// Every other behavioural fixture for this type is
/// <c>[Category("AzureStorageEmulator")]</c> and so is excluded by the repository's
/// standard filter, which left the artifact framing, the committed-metadata
/// idempotency gate, the 404 arms, listing, and probing unexecuted in an ordinary
/// run. It is also the shape the testing conventions warn about: an emulator-gated
/// fixture reports <c>Inconclusive</c> when the emulator is absent, which NUnit
/// counts as neither pass nor skip, so the run still prints <c>Passed!</c> while
/// the coverage silently vanishes.
/// </para>
/// </summary>
[TestFixture]
public sealed class AzureBlobLatticeBackupSinkBehaviourTests
{
    private ServiceProvider _services = null!;
    private Serializer<BackupManifest> _serializer = null!;
    private FakeBackupBlobStore _store = null!;
    private AzureBlobLatticeBackupSink _sut = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly))
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<BackupManifest>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _store = new FakeBackupBlobStore();
        _sut = new AzureBlobLatticeBackupSink(_store.CreateContainerClient(), _serializer);
    }

    // ----- Helpers -----

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ToAsync(params byte[][] chunks)
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

    private static byte[][] ChunksOf(IEnumerable<ReadOnlyMemory<byte>> read) => [.. read.Select(c => c.ToArray())];

    private static BackupManifest SampleManifest(string id, params string[] artifactIds)
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        var descriptors = artifactIds.Length == 0 ? ["artifact-1"] : artifactIds;
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
            contentDescriptors: [.. descriptors.Select(a => new BackupContentDescriptor(a, "abc123", 12, 1, scope))],
            provenance: [new BackupOriginProvenance("replica-a", 42)]);
    }

    private Task WriteArtifactAsync(string id, params byte[][] chunks) =>
        _sut.WriteArtifactAsync(id, ToAsync(chunks));

    // ----- Artifact round-trip and chunk framing -----

    [Test]
    public async Task IsDurable_reports_true_because_payload_outlives_the_capturing_cluster()
    {
        await Task.CompletedTask;
        Assert.That(_sut.IsDurable, Is.True);
    }

    [Test]
    public async Task An_artifact_round_trips_with_its_chunk_boundaries_preserved()
    {
        // The framing contract: the reader must see the SAME chunks the writer
        // supplied, not a re-chunking of the concatenated bytes.
        await WriteArtifactAsync("a1", [1, 2, 3], [4, 5], [6]);

        var read = ChunksOf(await CollectAsync(_sut.ReadArtifactAsync("a1")));

        Assert.That(read, Is.EqualTo(new[] { new byte[] { 1, 2, 3 }, [4, 5], [6] }));
    }

    [Test]
    public async Task An_empty_chunk_survives_the_round_trip_as_an_empty_chunk()
    {
        // A zero-length frame is a real chunk, not an end-of-stream marker.
        await WriteArtifactAsync("a1", [1], [], [2]);

        var read = ChunksOf(await CollectAsync(_sut.ReadArtifactAsync("a1")));

        Assert.That(read, Is.EqualTo(new[] { new byte[] { 1 }, [], [2] }));
    }

    [Test]
    public async Task An_artifact_with_no_chunks_reads_back_as_an_empty_sequence()
    {
        await WriteArtifactAsync("a1");

        Assert.That(await CollectAsync(_sut.ReadArtifactAsync("a1")), Is.Empty);
    }

    [Test]
    public async Task ReadArtifactAsync_yields_nothing_for_an_artifact_that_was_never_written()
    {
        Assert.That(await CollectAsync(_sut.ReadArtifactAsync("absent")), Is.Empty);
    }

    [Test]
    public async Task A_chunk_larger_than_the_append_block_limit_is_split_across_appends_but_read_back_whole()
    {
        // The 4 MiB AppendBlock split is physical; the logical frame must survive it.
        var big = new byte[(4 * 1024 * 1024) + 1024];
        Random.Shared.NextBytes(big);

        await WriteArtifactAsync("a1", big);
        var read = ChunksOf(await CollectAsync(_sut.ReadArtifactAsync("a1")));

        Assert.Multiple(() =>
        {
            Assert.That(_store.AppendBlockCalls, Is.EqualTo(2), "the payload must have needed two physical appends");
            Assert.That(read, Has.Length.EqualTo(1), "and still read back as one logical chunk");
            Assert.That(read[0], Is.EqualTo(big));
        });
    }

    [Test]
    public void ReadArtifactAsync_rejects_a_frame_whose_declared_length_exceeds_the_blob()
    {
        // A hostile or truncated blob must not be allowed to size an arbitrary
        // buffer from an untrusted length prefix.
        _store.Seed(BackupBlobNaming.ArtifactBlobName("a1"), [0xFF, 0xFF, 0xFF, 0x7F]);
        _store.MarkCommitted(BackupBlobNaming.ArtifactBlobName("a1"));

        Assert.That(
            async () => await CollectAsync(_sut.ReadArtifactAsync("a1")),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void ReadArtifactAsync_rejects_a_frame_declaring_a_negative_length()
    {
        _store.Seed(BackupBlobNaming.ArtifactBlobName("a1"), [0xFF, 0xFF, 0xFF, 0xFF]);

        Assert.That(
            async () => await CollectAsync(_sut.ReadArtifactAsync("a1")),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void ReadArtifactAsync_throws_on_a_truncated_length_prefix()
    {
        // A partial prefix at a frame boundary is a corrupt blob, not a clean end.
        _store.Seed(BackupBlobNaming.ArtifactBlobName("a1"), [0x01, 0x00]);

        Assert.That(
            async () => await CollectAsync(_sut.ReadArtifactAsync("a1")),
            Throws.InstanceOf<EndOfStreamException>());
    }

    [Test]
    public async Task An_artifact_read_tolerates_a_length_prefix_split_across_reads()
    {
        // A network stream may satisfy a read short, so the 4-byte frame prefix can
        // arrive in pieces. The reader completes the prefix before trusting it; a
        // regression that read a half-filled prefix would decode a garbage length
        // and corrupt a restore only under real network conditions.
        await WriteArtifactAsync("a1", [1, 2, 3], [4, 5]);
        _store.ArtifactReadDribbleBytes = 1;

        var read = ChunksOf(await CollectAsync(_sut.ReadArtifactAsync("a1")));

        Assert.That(read, Is.EqualTo(new[] { new byte[] { 1, 2, 3 }, [4, 5] }));
    }

    // ----- Write idempotency -----

    [Test]
    public async Task Rewriting_a_committed_artifact_is_a_no_op()
    {
        await WriteArtifactAsync("a1", [1, 2, 3]);
        var appendsAfterFirst = _store.AppendBlockCalls;

        await WriteArtifactAsync("a1", [9, 9, 9]);

        Assert.Multiple(() =>
        {
            Assert.That(_store.AppendBlockCalls, Is.EqualTo(appendsAfterFirst), "a committed artifact must not be rewritten");
            Assert.That(
                ChunksOf(CollectAsync(_sut.ReadArtifactAsync("a1")).GetAwaiter().GetResult()),
                Is.EqualTo(new[] { new byte[] { 1, 2, 3 } }),
                "and the original bytes must survive");
        });
    }

    [Test]
    public async Task A_partially_written_artifact_is_overwritten_on_retry()
    {
        // Anti-vacuity for the idempotent no-op above: an uncommitted blob is a
        // torn write, so the retry must discard it rather than skip.
        var name = BackupBlobNaming.ArtifactBlobName("a1");
        _store.Seed(name, [0xDE, 0xAD]);

        await WriteArtifactAsync("a1", [1, 2, 3]);

        Assert.That(
            ChunksOf(await CollectAsync(_sut.ReadArtifactAsync("a1"))),
            Is.EqualTo(new[] { new byte[] { 1, 2, 3 } }));
    }

    [Test]
    public async Task A_completed_write_marks_the_artifact_committed()
    {
        await WriteArtifactAsync("a1", [1]);

        Assert.That(await CollectAsync(_sut.ListArtifactIdsAsync()), Is.EqualTo(new[] { "a1" }));
    }

    // ----- Artifact deletion and listing -----

    [Test]
    public async Task DeleteArtifactAsync_reports_whether_it_removed_anything()
    {
        await WriteArtifactAsync("a1", [1]);

        Assert.Multiple(() =>
        {
            Assert.That(_sut.DeleteArtifactAsync("a1").GetAwaiter().GetResult(), Is.True);
            Assert.That(_sut.DeleteArtifactAsync("a1").GetAwaiter().GetResult(), Is.False, "a second delete removes nothing");
        });
    }

    [Test]
    public async Task ListArtifactIdsAsync_omits_a_partially_written_artifact()
    {
        await WriteArtifactAsync("committed", [1]);
        _store.Seed(BackupBlobNaming.ArtifactBlobName("torn"), [0xDE]);

        var ids = await CollectAsync(_sut.ListArtifactIdsAsync());

        Assert.That(ids, Is.EqualTo(new[] { "committed" }), "only complete chains are listed");
    }

    [Test]
    public async Task ListArtifactIdsAsync_returns_ids_in_order_and_ignores_manifests()
    {
        await WriteArtifactAsync("a2", [1]);
        await WriteArtifactAsync("a1", [1]);
        await _sut.WriteManifestAsync(SampleManifest("b1"));

        Assert.That(await CollectAsync(_sut.ListArtifactIdsAsync()), Is.EqualTo(new[] { "a1", "a2" }));
    }

    [Test]
    public async Task ListArtifactIdsAsync_is_empty_when_nothing_has_been_written()
    {
        Assert.That(await CollectAsync(_sut.ListArtifactIdsAsync()), Is.Empty);
    }

    // ----- Manifests -----

    [Test]
    public async Task A_manifest_round_trips_through_the_sink()
    {
        await _sut.WriteManifestAsync(SampleManifest("b1"));

        var read = await _sut.ReadManifestAsync("b1");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.Not.Null);
            Assert.That(read!.Id, Is.EqualTo("b1"));
            Assert.That(read.Name, Is.EqualTo("nightly"));
            Assert.That(read.StructuralDigest, Is.EqualTo("digest-root"));
        });
    }

    [Test]
    public async Task ReadManifestAsync_returns_null_for_a_backup_that_was_never_written()
    {
        Assert.That(await _sut.ReadManifestAsync("absent"), Is.Null);
    }

    [Test]
    public async Task WriteManifestAsync_overwrites_an_existing_manifest()
    {
        await _sut.WriteManifestAsync(SampleManifest("b1", "artifact-old"));
        await _sut.WriteManifestAsync(SampleManifest("b1", "artifact-new"));

        var read = await _sut.ReadManifestAsync("b1");

        Assert.That(read!.ContentDescriptors.Select(d => d.ArtifactId), Is.EqualTo(new[] { "artifact-new" }));
    }

    [Test]
    public async Task ListManifestsAsync_returns_every_manifest_in_id_order()
    {
        await _sut.WriteManifestAsync(SampleManifest("b2"));
        await _sut.WriteManifestAsync(SampleManifest("b1"));
        await WriteArtifactAsync("a1", [1]);

        var ids = (await CollectAsync(_sut.ListManifestsAsync())).Select(m => m.Id);

        Assert.That(ids, Is.EqualTo(new[] { "b1", "b2" }), "artifacts must not appear in a manifest listing");
    }

    [Test]
    public async Task ListManifestsAsync_is_empty_when_nothing_has_been_written()
    {
        Assert.That(await CollectAsync(_sut.ListManifestsAsync()), Is.Empty);
    }

    [Test]
    public async Task ListManifestsAsync_skips_a_manifest_deleted_between_the_listing_and_the_read()
    {
        // The listing and the per-manifest read are two round trips, so a manifest
        // can vanish in between. That must skip the entry, not fail the enumeration.
        await _sut.WriteManifestAsync(SampleManifest("b1"));
        await _sut.WriteManifestAsync(SampleManifest("b2"));
        _store.VanishOnRead.Add(BackupBlobNaming.ManifestBlobName("b1"));

        var ids = (await CollectAsync(_sut.ListManifestsAsync())).Select(m => m.Id);

        Assert.That(ids, Is.EqualTo(new[] { "b2" }), "the vanished manifest is skipped and the rest still enumerate");
    }

    [Test]
    public async Task DeleteManifestAsync_reports_whether_it_removed_anything()
    {
        await _sut.WriteManifestAsync(SampleManifest("b1"));

        Assert.Multiple(() =>
        {
            Assert.That(_sut.DeleteManifestAsync("b1").GetAwaiter().GetResult(), Is.True);
            Assert.That(_sut.DeleteManifestAsync("b1").GetAwaiter().GetResult(), Is.False);
        });
    }

    [Test]
    public async Task ManifestExistsAsync_distinguishes_a_written_manifest_from_an_absent_one()
    {
        await _sut.WriteManifestAsync(SampleManifest("b1"));

        Assert.Multiple(() =>
        {
            Assert.That(_sut.ManifestExistsAsync("b1").GetAwaiter().GetResult(), Is.True);
            Assert.That(_sut.ManifestExistsAsync("absent").GetAwaiter().GetResult(), Is.False);
        });
    }

    // ----- Probe -----

    [Test]
    public async Task ProbeAsync_reports_an_absent_manifest()
    {
        var resolution = await _sut.ProbeAsync("absent");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.False);
            Assert.That(resolution.MissingArtifactIds, Is.Empty);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_a_fully_resolvable_backup()
    {
        await WriteArtifactAsync("artifact-1", [1]);
        await _sut.WriteManifestAsync(SampleManifest("b1"));

        var resolution = await _sut.ProbeAsync("b1");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.Empty);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_an_artifact_that_was_never_written_as_missing()
    {
        await _sut.WriteManifestAsync(SampleManifest("b1", "artifact-1", "artifact-2"));
        await WriteArtifactAsync("artifact-1", [1]);

        var resolution = await _sut.ProbeAsync("b1");

        Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "artifact-2" }));
    }

    [Test]
    public async Task ProbeAsync_counts_a_torn_artifact_as_missing()
    {
        // Present-but-uncommitted is the interesting case: the blob exists, so a
        // mere existence check would wrongly call the backup resolvable.
        await _sut.WriteManifestAsync(SampleManifest("b1"));
        _store.Seed(BackupBlobNaming.ArtifactBlobName("artifact-1"), [0xDE]);

        var resolution = await _sut.ProbeAsync("b1");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "artifact-1" }));
        });
    }

    [Test]
    public async Task ProbeAsync_reports_a_duplicated_artifact_id_once()
    {
        await _sut.WriteManifestAsync(SampleManifest("b1", "artifact-dup", "artifact-dup"));

        var resolution = await _sut.ProbeAsync("b1");

        Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "artifact-dup" }));
    }

    // ----- Container initialisation -----

    [Test]
    public async Task The_container_is_created_once_however_many_operations_run()
    {
        await WriteArtifactAsync("a1", [1]);
        await CollectAsync(_sut.ReadArtifactAsync("a1"));
        await _sut.WriteManifestAsync(SampleManifest("b1"));
        await _sut.ReadManifestAsync("b1");
        await _sut.ManifestExistsAsync("b1");
        await CollectAsync(_sut.ListArtifactIdsAsync());

        Assert.That(_store.ContainerCreateCalls, Is.EqualTo(1));
    }
}
