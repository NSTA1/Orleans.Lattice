using System.Runtime.CompilerServices;
using System.Text;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Integration coverage for the default in-cluster <see cref="ILatticeBackupSink"/>:
/// artifact and manifest round-trips over the streaming surface, and idempotent
/// re-writes of content-addressed artifacts and manifests.
/// </summary>
[Category("Integration")]
public sealed class InClusterBackupSinkIntegrationTests
{
    private BackupClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new BackupClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task WriteArtifactAsync_then_ReadArtifactAsync_round_trips_the_bytes()
    {
        var payload = Encoding.UTF8.GetBytes("the quick brown fox jumps over the lazy dog");
        var artifactId = BackupContentHash.Compute(payload);

        await _fixture.Sink.WriteArtifactAsync(artifactId, Chunks(payload, chunkSize: 8));

        var readBack = await ReadAllAsync(_fixture.Sink.ReadArtifactAsync(artifactId));
        Assert.That(readBack, Is.EqualTo(payload));
    }

    [Test]
    public async Task ReadArtifactAsync_yields_nothing_for_a_missing_artifact()
    {
        var readBack = await ReadAllAsync(_fixture.Sink.ReadArtifactAsync("does-not-exist"));
        Assert.That(readBack, Is.Empty);
    }

    [Test]
    public async Task WriteArtifactAsync_is_idempotent_for_identical_content()
    {
        var payload = Encoding.UTF8.GetBytes("idempotent-payload");
        var artifactId = BackupContentHash.Compute(payload);

        await _fixture.Sink.WriteArtifactAsync(artifactId, Chunks(payload, chunkSize: 4));
        await _fixture.Sink.WriteArtifactAsync(artifactId, Chunks(payload, chunkSize: 7));

        var ids = await ToListAsync(_fixture.Sink.ListArtifactIdsAsync());
        Assert.That(ids, Is.EqualTo(new[] { artifactId }));

        var readBack = await ReadAllAsync(_fixture.Sink.ReadArtifactAsync(artifactId));
        Assert.That(readBack, Is.EqualTo(payload));
    }

    [Test]
    public async Task DeleteArtifactAsync_removes_the_artifact()
    {
        var payload = Encoding.UTF8.GetBytes("to-be-deleted");
        var artifactId = BackupContentHash.Compute(payload);
        await _fixture.Sink.WriteArtifactAsync(artifactId, Chunks(payload, chunkSize: 5));

        Assert.That(await _fixture.Sink.DeleteArtifactAsync(artifactId), Is.True);
        Assert.That(await _fixture.Sink.DeleteArtifactAsync(artifactId), Is.False);
        Assert.That(await ToListAsync(_fixture.Sink.ListArtifactIdsAsync()), Is.Empty);
    }

    [Test]
    public async Task WriteManifestAsync_then_ReadManifestAsync_round_trips_the_manifest()
    {
        var manifest = BackupManifestModelTests.Sample(id: "backup-round-trip");

        await _fixture.Sink.WriteManifestAsync(manifest);

        var readBack = await _fixture.Sink.ReadManifestAsync("backup-round-trip");
        Assert.That(readBack, Is.Not.Null);
        Assert.That(readBack!.Id, Is.EqualTo("backup-round-trip"));
        Assert.That(readBack.Scope.TreeId, Is.EqualTo("orders"));
        Assert.That(readBack.ContentDescriptors[0].ArtifactId, Is.EqualTo("artifact-1"));
        Assert.That(readBack.Provenance[0].OriginId, Is.EqualTo("replica-a"));
    }

    [Test]
    public async Task WriteManifestAsync_is_idempotent_for_the_same_backup_id()
    {
        var manifest = BackupManifestModelTests.Sample(id: "backup-once");

        await _fixture.Sink.WriteManifestAsync(manifest);
        await _fixture.Sink.WriteManifestAsync(manifest);

        var all = await ToListAsync(_fixture.Sink.ListManifestsAsync());
        Assert.That(all.Select(m => m.Id), Is.EqualTo(new[] { "backup-once" }));
    }

    [Test]
    public async Task DeleteManifestAsync_removes_the_manifest()
    {
        var manifest = BackupManifestModelTests.Sample(id: "backup-del");
        await _fixture.Sink.WriteManifestAsync(manifest);

        Assert.That(await _fixture.Sink.DeleteManifestAsync("backup-del"), Is.True);
        Assert.That(await _fixture.Sink.DeleteManifestAsync("backup-del"), Is.False);
        Assert.That(await _fixture.Sink.ReadManifestAsync("backup-del"), Is.Null);
    }

    // ---- Sink-existence probe -------------------------------------------

    [Test]
    public async Task ManifestExistsAsync_is_true_after_write_and_false_after_delete()
    {
        var manifest = BackupManifestModelTests.Sample(id: "backup-probe-exists");
        await _fixture.Sink.WriteManifestAsync(manifest);

        Assert.That(await _fixture.Sink.ManifestExistsAsync("backup-probe-exists"), Is.True);

        await _fixture.Sink.DeleteManifestAsync("backup-probe-exists");
        Assert.That(await _fixture.Sink.ManifestExistsAsync("backup-probe-exists"), Is.False);
    }

    [Test]
    public async Task ManifestExistsAsync_is_false_for_an_unknown_backup()
    {
        Assert.That(await _fixture.Sink.ManifestExistsAsync("never-written"), Is.False);
    }

    [Test]
    public async Task ProbeAsync_reports_resolvable_when_manifest_and_artifacts_are_present()
    {
        var manifest = BackupManifestModelTests.Sample(id: "backup-probe-full");
        await _fixture.Sink.WriteManifestAsync(manifest);
        // The Sample manifest references a single artifact, "artifact-1".
        await _fixture.Sink.WriteArtifactAsync("artifact-1", Chunks(Encoding.UTF8.GetBytes("payload"), chunkSize: 4));

        var resolution = await _fixture.Sink.ProbeAsync("backup-probe-full");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.BackupId, Is.EqualTo("backup-probe-full"));
            Assert.That(resolution.ManifestPresent, Is.True);
            Assert.That(resolution.MissingArtifactIds, Is.Empty);
            Assert.That(resolution.IsResolvable, Is.True);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_the_missing_artifact_when_only_the_manifest_is_present()
    {
        var manifest = BackupManifestModelTests.Sample(id: "backup-probe-torn");
        await _fixture.Sink.WriteManifestAsync(manifest);

        var resolution = await _fixture.Sink.ProbeAsync("backup-probe-torn");

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
        var resolution = await _fixture.Sink.ProbeAsync("never-written");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.ManifestPresent, Is.False);
            Assert.That(resolution.MissingArtifactIds, Is.Empty);
            Assert.That(resolution.IsResolvable, Is.False);
        });
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(
        byte[] payload,
        int chunkSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var offset = 0; offset < payload.Length; offset += chunkSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var length = Math.Min(chunkSize, payload.Length - offset);
            yield return new ReadOnlyMemory<byte>(payload, offset, length);
            await Task.Yield();
        }
    }

    private static async Task<byte[]> ReadAllAsync(IAsyncEnumerable<ReadOnlyMemory<byte>> chunks)
    {
        using var buffer = new MemoryStream();
        await foreach (var chunk in chunks)
        {
            buffer.Write(chunk.Span);
        }

        return buffer.ToArray();
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
        {
            list.Add(item);
        }

        return list;
    }
}
