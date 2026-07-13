using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupCatalogScrubService"/>: the
/// reconcile / scrub pass cross-checks every catalog row against the sink and
/// reports orphans - catalog rows whose sink payload is no longer resolvable. It
/// is non-destructive by default (flags only), removes orphans only on explicit
/// opt-in, and is idempotent on re-run. Uses in-memory fakes for the sink and
/// catalog so the scrub logic is exercised without a cluster.
/// </summary>
[TestFixture]
public sealed class LatticeBackupCatalogScrubServiceTests
{
    private static BackupManifest Manifest(string id) => BackupManifestModelTests.Sample(id: id);

    private static ILatticeBackupCatalogScrubService CreateService(FakeCatalog catalog, FakeSink sink) =>
        new LatticeBackupCatalogScrubService(catalog, sink);

    [Test]
    public void Constructor_null_catalog_throws()
    {
        Assert.That(
            () => new LatticeBackupCatalogScrubService(null!, new FakeSink()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_sink_throws()
    {
        Assert.That(
            () => new LatticeBackupCatalogScrubService(new FakeCatalog(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ScrubAsync_empty_catalog_reports_nothing()
    {
        var service = CreateService(new FakeCatalog(), new FakeSink());

        var report = await service.ScrubAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.Zero);
            Assert.That(report.OrphanCount, Is.Zero);
            Assert.That(report.RemovedCount, Is.Zero);
            Assert.That(report.Pruned, Is.False);
            Assert.That(report.OrphanBackupIds, Is.Empty);
        });
    }

    [Test]
    public async Task ScrubAsync_all_resolvable_reports_no_orphans()
    {
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("a"));
        await catalog.RegisterAsync(Manifest("b"));
        var sink = new FakeSink().WithResolvable("a", "b");

        var report = await CreateService(catalog, sink).ScrubAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(2));
            Assert.That(report.OrphanCount, Is.Zero);
            Assert.That(report.OrphanBackupIds, Is.Empty);
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task ScrubAsync_flags_orphans_non_destructively_by_default()
    {
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("a"));
        await catalog.RegisterAsync(Manifest("orphan"));
        await catalog.RegisterAsync(Manifest("c"));

        // The sink can resolve a and c but not "orphan" (manifest gone).
        var sink = new FakeSink().WithResolvable("a", "c");

        var report = await CreateService(catalog, sink).ScrubAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(3));
            Assert.That(report.OrphanCount, Is.EqualTo(1));
            Assert.That(report.RemovedCount, Is.Zero);
            Assert.That(report.Pruned, Is.False);
            Assert.That(report.OrphanBackupIds, Is.EqualTo(new[] { "orphan" }));
            // Non-destructive default leaves the catalog untouched.
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a", "orphan", "c" }));
        });
    }

    [Test]
    public async Task ScrubAsync_treats_a_missing_artifact_as_an_orphan()
    {
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("torn"));

        // Manifest present but an artifact is missing: not resolvable, so an orphan.
        var sink = new FakeSink().WithManifestPresentMissingArtifacts("torn", "artifact-1");

        var report = await CreateService(catalog, sink).ScrubAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanCount, Is.EqualTo(1));
            Assert.That(report.OrphanBackupIds, Is.EqualTo(new[] { "torn" }));
        });
    }

    [Test]
    public async Task ScrubAsync_with_prune_removes_orphans()
    {
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("a"));
        await catalog.RegisterAsync(Manifest("orphan"));
        var sink = new FakeSink().WithResolvable("a");

        var report = await CreateService(catalog, sink).ScrubAsync(pruneOrphans: true);

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanCount, Is.EqualTo(1));
            Assert.That(report.RemovedCount, Is.EqualTo(1));
            Assert.That(report.Pruned, Is.True);
            Assert.That(report.OrphanBackupIds, Is.EqualTo(new[] { "orphan" }));
            // The orphan row is gone; the resolvable row remains.
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a" }));
        });
    }

    [Test]
    public async Task ScrubAsync_with_prune_is_idempotent_on_re_run()
    {
        var catalog = new FakeCatalog();
        await catalog.RegisterAsync(Manifest("a"));
        await catalog.RegisterAsync(Manifest("orphan"));
        var sink = new FakeSink().WithResolvable("a");
        var service = CreateService(catalog, sink);

        await service.ScrubAsync(pruneOrphans: true);
        var second = await service.ScrubAsync(pruneOrphans: true);

        Assert.Multiple(() =>
        {
            // The orphan was removed on the first pass, so the second finds none.
            Assert.That(second.ScannedCount, Is.EqualTo(1));
            Assert.That(second.OrphanCount, Is.Zero);
            Assert.That(second.RemovedCount, Is.Zero);
            Assert.That(second.OrphanBackupIds, Is.Empty);
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "a" }));
        });
    }

    /// <summary>An in-memory sink whose probe verdict per backup id is scripted.</summary>
    private sealed class FakeSink : ILatticeBackupSink
    {
        private readonly Dictionary<string, BackupSinkResolution> _resolutions = new(StringComparer.Ordinal);

        public FakeSink WithResolvable(params string[] backupIds)
        {
            foreach (var id in backupIds)
            {
                _resolutions[id] = new BackupSinkResolution(id, manifestPresent: true, Array.Empty<string>());
            }

            return this;
        }

        public FakeSink WithManifestPresentMissingArtifacts(string backupId, params string[] missingArtifactIds)
        {
            _resolutions[backupId] = new BackupSinkResolution(backupId, manifestPresent: true, missingArtifactIds);
            return this;
        }

        public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_resolutions.TryGetValue(backupId, out var resolution)
                ? resolution
                : new BackupSinkResolution(backupId, manifestPresent: false, Array.Empty<string>()));

        public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_resolutions.TryGetValue(backupId, out var resolution) && resolution.ManifestPresent);

        public Task WriteArtifactAsync(string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(string artifactId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    /// <summary>An in-memory <see cref="ILatticeBackupCatalogStore"/> mirroring the real reconcile behavior.</summary>
    private sealed class FakeCatalog : ILatticeBackupCatalogStore
    {
        private readonly Dictionary<string, BackupManifest> _rows = new(StringComparer.Ordinal);

        public IEnumerable<string> Ids => _rows.Keys;

        public Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
        {
            _rows[manifest.Id] = manifest;
            return Task.CompletedTask;
        }

        public Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.GetValueOrDefault(backupId));

        public Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.Remove(backupId));

        public async IAsyncEnumerable<BackupManifest> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var manifest in _rows.Values.OrderBy(m => m.Id, StringComparer.Ordinal))
            {
                yield return manifest;
                await Task.Yield();
            }
        }
    }
}
