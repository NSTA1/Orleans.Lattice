using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupCatalogRebuildService"/>: the
/// rebuild-from-sink pass makes the sink the single source of truth and the
/// catalog a rebuildable projection. It drains every manifest the sink holds and
/// re-registers each into the catalog, is idempotent on re-run, and - because
/// registration reconciles - preserves the immutable capture timestamp of a
/// manifest that was already catalogued. Uses in-memory fakes for the sink and
/// catalog so the rebuild logic is exercised without a cluster.
/// </summary>
[TestFixture]
public sealed class LatticeBackupCatalogRebuildServiceTests
{
    private static BackupManifest Manifest(string id, DateTimeOffset createdAtUtc) =>
        BackupManifestModelTests.Sample(id: id) with { CreatedAtUtc = createdAtUtc };

    private static ILatticeBackupCatalogRebuildService CreateService(
        FakeSink sink,
        FakeCatalog catalog) =>
        new LatticeBackupCatalogRebuildService(sink, catalog);

    [Test]
    public void Constructor_null_sink_throws()
    {
        Assert.That(
            () => new LatticeBackupCatalogRebuildService(null!, new FakeCatalog()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_catalog_throws()
    {
        Assert.That(
            () => new LatticeBackupCatalogRebuildService(new FakeSink(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task RebuildFromSinkAsync_empty_sink_registers_nothing()
    {
        var service = CreateService(new FakeSink(), new FakeCatalog());

        var report = await service.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.Zero);
            Assert.That(report.RegisteredCount, Is.Zero);
            Assert.That(report.ReconciledCount, Is.Zero);
        });
    }

    [Test]
    public async Task RebuildFromSinkAsync_populates_catalog_from_sink_manifests()
    {
        var sink = new FakeSink();
        await sink.WriteManifestAsync(Manifest("a", DateTimeOffset.UnixEpoch));
        await sink.WriteManifestAsync(Manifest("b", DateTimeOffset.UnixEpoch.AddHours(1)));
        await sink.WriteManifestAsync(Manifest("c", DateTimeOffset.UnixEpoch.AddHours(2)));
        var catalog = new FakeCatalog();
        var service = CreateService(sink, catalog);

        var report = await service.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(3));
            Assert.That(report.RegisteredCount, Is.EqualTo(3));
            Assert.That(report.ReconciledCount, Is.Zero);
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a", "b", "c" }));
        });
    }

    [Test]
    public async Task RebuildFromSinkAsync_is_idempotent_on_re_run()
    {
        var sink = new FakeSink();
        await sink.WriteManifestAsync(Manifest("a", DateTimeOffset.UnixEpoch));
        await sink.WriteManifestAsync(Manifest("b", DateTimeOffset.UnixEpoch.AddHours(1)));
        var catalog = new FakeCatalog();
        var service = CreateService(sink, catalog);

        await service.RebuildFromSinkAsync();
        var second = await service.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            // The second pass finds the same manifests already catalogued: it
            // scans and reconciles them in place, adding nothing new, and the
            // catalog holds exactly one row per backup (no duplicates).
            Assert.That(second.ScannedCount, Is.EqualTo(2));
            Assert.That(second.RegisteredCount, Is.Zero);
            Assert.That(second.ReconciledCount, Is.EqualTo(2));
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task RebuildFromSinkAsync_reconcile_preserves_first_capture_timestamp()
    {
        var firstSeen = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("cafef00d", firstSeen));

        // The sink holds the same content-addressed backup id but a later capture
        // time (a non-clean restart re-captured it). Rebuild must keep the first
        // timestamp, matching the reconcile contract the catalog store enforces.
        var sink = new FakeSink();
        await sink.WriteManifestAsync(Manifest("cafef00d", firstSeen.AddDays(3)));
        var service = CreateService(sink, catalog);

        var report = await service.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(1));
            Assert.That(report.RegisteredCount, Is.Zero);
            Assert.That(report.ReconciledCount, Is.EqualTo(1));
            Assert.That(catalog.Get("cafef00d")!.CreatedAtUtc, Is.EqualTo(firstSeen));
        });
    }

    [Test]
    public async Task RebuildFromSinkAsync_repopulates_a_catalog_missing_rows_the_sink_has()
    {
        // Drift: the sink has three manifests but the catalog only knows one.
        var sink = new FakeSink();
        await sink.WriteManifestAsync(Manifest("a", DateTimeOffset.UnixEpoch));
        await sink.WriteManifestAsync(Manifest("b", DateTimeOffset.UnixEpoch.AddHours(1)));
        await sink.WriteManifestAsync(Manifest("c", DateTimeOffset.UnixEpoch.AddHours(2)));
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("b", DateTimeOffset.UnixEpoch.AddHours(1)));
        var service = CreateService(sink, catalog);

        var report = await service.RebuildFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(3));
            Assert.That(report.RegisteredCount, Is.EqualTo(2));
            Assert.That(report.ReconciledCount, Is.EqualTo(1));
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a", "b", "c" }));
        });
    }

    /// <summary>An in-memory <see cref="ILatticeBackupSink"/> exercising only the manifest surface.</summary>
    private sealed class FakeSink : ILatticeBackupSink
    {
        private readonly SortedDictionary<string, BackupManifest> _manifests = new(StringComparer.Ordinal);

        public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
        {
            _manifests[manifest.Id] = manifest;
            return Task.CompletedTask;
        }

        public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_manifests.TryGetValue(backupId, out var manifest) ? manifest : null);

        public async IAsyncEnumerable<BackupManifest> ListManifestsAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var manifest in _manifests.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return manifest;
                await Task.Yield();
            }
        }

        public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_manifests.Remove(backupId));

        public Task WriteArtifactAsync(
            string artifactId,
            IAsyncEnumerable<ReadOnlyMemory<byte>> content,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
            string artifactId,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }

    /// <summary>An in-memory <see cref="ILatticeBackupCatalogStore"/> that reconciles like the real store.</summary>
    private sealed class FakeCatalog : ILatticeBackupCatalogStore
    {
        private readonly Dictionary<string, BackupManifest> _rows = new(StringComparer.Ordinal);

        public IEnumerable<string> Ids => _rows.Keys;

        public BackupManifest? Get(string backupId) => _rows.GetValueOrDefault(backupId);

        public Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
        {
            _rows.TryGetValue(manifest.Id, out var existing);
            _rows[manifest.Id] = BackupManifestRegistration.Reconcile(existing, manifest);
            return Task.CompletedTask;
        }

        public Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.GetValueOrDefault(backupId));

        public Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.Remove(backupId));

        public async IAsyncEnumerable<BackupManifest> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var manifest in _rows.Values)
            {
                yield return manifest;
                await Task.Yield();
            }
        }
    }
}
