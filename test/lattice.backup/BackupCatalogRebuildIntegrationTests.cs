using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Integration coverage for <see cref="ILatticeBackupCatalogRebuildService"/> over
/// a live single-silo cluster with the real in-cluster sink and the real catalog
/// store. Proves the rebuild-from-sink pass repopulates an empty catalog from the
/// sink's self-describing manifests, is idempotent on re-run through the real
/// reconcile path, and heals a catalog that has drifted behind the sink. Each
/// sink manifest's artifact is written too: a rebuild lists only whole backups.
/// </summary>
[Category("Integration")]
public sealed class BackupCatalogRebuildIntegrationTests
{
    private BackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new BackupClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static BackupManifest Manifest(string id, DateTimeOffset createdAtUtc) =>
        BackupManifestModelTests.Sample(id: id) with { CreatedAtUtc = createdAtUtc };

    // A rebuild lists only a backup whose every artifact is in the sink (issue
    // #5637), so each test's manifests stand for whole backups: their one
    // artifact is written first.
    private async Task InitializeWithArtifactAsync()
    {
        await _fixture.InitializeAsync();
        var artifactId = BackupManifestModelTests.Sample().ContentDescriptors.Single().ArtifactId;
        await _fixture.Sink.WriteArtifactAsync(artifactId, OneChunk());
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> OneChunk()
    {
        await Task.CompletedTask;
        yield return new byte[] { 1, 2, 3 };
    }

    private ILatticeBackupCatalogRebuildService Rebuild =>
        (ILatticeBackupCatalogRebuildService)_fixture.SiloServices
            .GetService(typeof(ILatticeBackupCatalogRebuildService))!;

    private async Task<List<string>> CatalogIdsAsync()
    {
        var ids = new List<string>();
        await foreach (var manifest in _fixture.Catalog.ListAsync())
        {
            ids.Add(manifest.Id);
        }

        return ids;
    }

    [Test]
    public async Task RebuildFromSinkAsync_repopulates_an_empty_catalog_from_the_sink()
    {
        await InitializeWithArtifactAsync();
        await _fixture.Sink.WriteManifestAsync(Manifest("aaaa", DateTimeOffset.UnixEpoch));
        await _fixture.Sink.WriteManifestAsync(Manifest("bbbb", DateTimeOffset.UnixEpoch.AddHours(1)));

        Assert.That(await CatalogIdsAsync(), Is.Empty, "catalog starts empty before the rebuild");

        var report = await Rebuild.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(2));
            Assert.That(report.RegisteredCount, Is.EqualTo(2));
            Assert.That(report.ReconciledCount, Is.Zero);
        });
        Assert.That(await CatalogIdsAsync(), Is.EquivalentTo(new[] { "aaaa", "bbbb" }));
    }

    [Test]
    public async Task RebuildFromSinkAsync_is_idempotent_and_preserves_capture_timestamp()
    {
        await InitializeWithArtifactAsync();
        var firstSeen = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Catalogue the manifest first, then let the sink hold a later-timestamped
        // re-capture of the same content-addressed id. The rebuild must reconcile
        // in place and keep the first capture time.
        await _fixture.Catalog.RegisterAsync(Manifest("cafef00d", firstSeen));
        await _fixture.Sink.WriteManifestAsync(Manifest("cafef00d", firstSeen.AddDays(3)));

        var report = await Rebuild.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(1));
            Assert.That(report.RegisteredCount, Is.Zero);
            Assert.That(report.ReconciledCount, Is.EqualTo(1));
        });

        var stored = await _fixture.Catalog.GetAsync("cafef00d");
        Assert.That(stored, Is.Not.Null);
        Assert.That(stored!.CreatedAtUtc, Is.EqualTo(firstSeen));
    }

    [Test]
    public async Task RebuildFromSinkAsync_heals_a_catalog_drifted_behind_the_sink()
    {
        await InitializeWithArtifactAsync();
        await _fixture.Sink.WriteManifestAsync(Manifest("aaaa", DateTimeOffset.UnixEpoch));
        await _fixture.Sink.WriteManifestAsync(Manifest("bbbb", DateTimeOffset.UnixEpoch.AddHours(1)));
        await _fixture.Sink.WriteManifestAsync(Manifest("cccc", DateTimeOffset.UnixEpoch.AddHours(2)));

        // The catalog only knows one of the three the sink holds.
        await _fixture.Catalog.RegisterAsync(Manifest("bbbb", DateTimeOffset.UnixEpoch.AddHours(1)));

        var report = await Rebuild.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(3));
            Assert.That(report.RegisteredCount, Is.EqualTo(2));
            Assert.That(report.ReconciledCount, Is.EqualTo(1));
        });
        Assert.That(await CatalogIdsAsync(), Is.EquivalentTo(new[] { "aaaa", "bbbb", "cccc" }));
    }
}
