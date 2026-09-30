using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class SampleSharedBackupSinkTests
{
    [Test]
    public void It_reports_as_durable() =>
        Assert.That(new SampleSharedBackupSink().IsDurable, Is.True);

    [Test]
    public async Task An_artifact_reads_back_with_its_chunk_boundaries()
    {
        var sink = new SampleSharedBackupSink();
        await sink.WriteArtifactAsync("a1", Chunks([1, 2], [3], [4, 5, 6]));

        var read = new List<byte[]>();
        await foreach (var chunk in sink.ReadArtifactAsync("a1"))
        {
            read.Add(chunk.ToArray());
        }

        Assert.That(read, Is.EqualTo(new[] { new byte[] { 1, 2 }, new byte[] { 3 }, new byte[] { 4, 5, 6 } }));
        Assert.That(sink.ArtifactCount, Is.EqualTo(1));
    }

    [Test]
    public async Task An_unknown_artifact_reads_as_empty()
    {
        var count = 0;
        await foreach (var _ in new SampleSharedBackupSink().ReadArtifactAsync("missing"))
        {
            count++;
        }

        Assert.That(count, Is.Zero);
    }

    [Test]
    public async Task Artifacts_list_in_ordinal_order_and_delete_once()
    {
        var sink = new SampleSharedBackupSink();
        await sink.WriteArtifactAsync("b", Chunks([1]));
        await sink.WriteArtifactAsync("a", Chunks([2]));

        Assert.That(await ToListAsync(sink.ListArtifactIdsAsync()), Is.EqualTo(new[] { "a", "b" }));
        Assert.That(await sink.DeleteArtifactAsync("a"), Is.True);
        Assert.That(await sink.DeleteArtifactAsync("a"), Is.False);
        Assert.That(await ToListAsync(sink.ListArtifactIdsAsync()), Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public async Task A_manifest_round_trips_and_lists_and_deletes()
    {
        var sink = new SampleSharedBackupSink();
        await sink.WriteManifestAsync(Manifest("m2"));
        await sink.WriteManifestAsync(Manifest("m1"));

        Assert.That((await sink.ReadManifestAsync("m1"))?.Id, Is.EqualTo("m1"));
        Assert.That(await sink.ReadManifestAsync("missing"), Is.Null);
        Assert.That(await sink.ManifestExistsAsync("m2"), Is.True);
        Assert.That((await ToListAsync(sink.ListManifestsAsync())).Select(manifest => manifest.Id), Is.EqualTo(new[] { "m1", "m2" }));
        Assert.That(sink.ManifestCount, Is.EqualTo(2));

        Assert.That(await sink.DeleteManifestAsync("m1"), Is.True);
        Assert.That(await sink.DeleteManifestAsync("m1"), Is.False);
        Assert.That(await sink.ManifestExistsAsync("m1"), Is.False);
    }

    [Test]
    public async Task A_probe_reports_a_missing_manifest()
    {
        var resolution = await new SampleSharedBackupSink().ProbeAsync("missing");

        Assert.That(resolution.ManifestPresent, Is.False);
    }

    [Test]
    public async Task A_probe_reports_the_artifacts_a_manifest_names_but_the_sink_lacks()
    {
        var sink = new SampleSharedBackupSink();
        await sink.WriteManifestAsync(Manifest("m1", "present", "absent", "present"));
        await sink.WriteArtifactAsync("present", Chunks([1]));

        var resolution = await sink.ProbeAsync("m1");

        Assert.That(resolution.ManifestPresent, Is.True);
        Assert.That(resolution.MissingArtifactIds, Is.EqualTo(new[] { "absent" }));
    }

    [Test]
    public void Invalid_arguments_throw()
    {
        var sink = new SampleSharedBackupSink();

        Assert.That(() => sink.WriteArtifactAsync("", Chunks()), Throws.ArgumentException);
        Assert.That(() => sink.WriteArtifactAsync("a", null!), Throws.ArgumentNullException);
        Assert.That(() => sink.WriteManifestAsync(null!), Throws.ArgumentNullException);
        Assert.That(() => sink.ReadManifestAsync(""), Throws.ArgumentException);
        Assert.That(() => sink.DeleteArtifactAsync(""), Throws.ArgumentException);
        Assert.That(() => sink.DeleteManifestAsync(""), Throws.ArgumentException);
        Assert.That(() => sink.ManifestExistsAsync(""), Throws.ArgumentException);
        Assert.That(() => sink.ProbeAsync(""), Throws.ArgumentException);
    }

    private static BackupManifest Manifest(string id, params string[] artifacts)
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        return new BackupManifest(
            id: id,
            name: "sample",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(1, 1),
            topology: new BackupTopologySnapshot(1, 4096, ["d0"]),
            structuralDigest: "digest",
            keyDescriptors: [],
            contentDescriptors: [.. artifacts.Select(artifact => new BackupContentDescriptor(artifact, "hash", 1, 1, scope))],
            provenance: []);
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(params byte[][] chunks)
    {
        foreach (var chunk in chunks)
        {
            yield return chunk;
        }

        await Task.CompletedTask;
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> items)
    {
        var list = new List<T>();
        await foreach (var item in items)
        {
            list.Add(item);
        }

        return list;
    }
}
