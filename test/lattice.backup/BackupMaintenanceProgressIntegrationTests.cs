using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Progress reporting of the backup maintenance engines through the ambient
/// <see cref="LatticeOperationProgress"/> sink (#4125): a health check counts the
/// backup's artifacts checked against its artifact total, a catalog rebuild counts
/// the sink's manifests re-registered, and a catalog scrub counts the catalog rows
/// probed and, when pruning, the orphans removed. Real engines, real cluster; the
/// recorded sink completes synchronously, so the sequence is deterministic.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BackupMaintenanceProgressIntegrationTests
{
    private RestoreClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>("maintenance-a");
        for (var i = 0; i < 12; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private T Service<T>()
        where T : notnull => _fixture.SiloServices.GetRequiredService<T>();

    private static async Task<T> WithProgressAsync<T>(RecordingProgress recorder, Func<Task<T>> work)
    {
        using (LatticeOperationProgress.Enter(recorder))
        {
            return await work();
        }
    }

    [Test]
    public async Task A_health_check_counts_artifacts_checked_against_the_backups_artifact_total()
    {
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("health", BackupScopeSelector.WholeTree("maintenance-a"), pageSize: 4));
        var artifacts = backup.Manifest.ContentDescriptors.Select(d => d.ArtifactId).Distinct(StringComparer.Ordinal).Count();
        var recorder = new RecordingProgress();

        var report = await WithProgressAsync(recorder, () => Service<ILatticeBackupHealthService>().VerifyAsync(backup.BackupId));

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(artifacts, Is.GreaterThan(0));
            Assert.That(recorder.Reports, Is.Not.Empty);
            Assert.That(recorder.Reports.All(r => r.Phase == BackupOperationPhases.Verifying), Is.True);
            Assert.That(recorder.Reports.All(r => r.Unit == BackupOperationUnits.Artifacts && r.Total == artifacts), Is.True);
            Assert.That(recorder.Reports.Select(r => r.Completed), Is.EqualTo(Enumerable.Range(0, artifacts + 1).Select(i => (long)i)));
        });
    }

    [Test]
    public async Task A_health_check_of_a_backup_missing_from_the_sink_reports_no_units()
    {
        var recorder = new RecordingProgress();

        var report = await WithProgressAsync(recorder, () => Service<ILatticeBackupHealthService>().VerifyAsync("no-such-backup"));

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Missing));
            Assert.That(recorder.Reports, Is.Empty, "No total is invented for a manifest that is not there.");
        });
    }

    [Test]
    public async Task A_catalog_rebuild_counts_the_sinks_manifests_with_no_invented_total()
    {
        await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("rebuild", BackupScopeSelector.WholeTree("maintenance-a")));
        var recorder = new RecordingProgress();

        var report = await WithProgressAsync(recorder, () => Service<ILatticeBackupCatalogRebuildService>().RebuildFromSinkAsync());

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.GreaterThan(0));
            Assert.That(recorder.Reports[0], Is.EqualTo(new Report(BackupOperationPhases.RebuildingCatalog, 0, null, BackupOperationUnits.Manifests)));
            Assert.That(recorder.Reports.All(r => r.Phase == BackupOperationPhases.RebuildingCatalog && r.Total is null), Is.True);
            Assert.That(recorder.Reports.Select(r => r.Completed), Is.Ordered.Ascending);
            Assert.That(recorder.Reports[^1].Completed, Is.EqualTo(report.ScannedCount));
        });
    }

    [Test]
    public async Task A_pruning_catalog_scrub_counts_rows_probed_then_orphans_removed()
    {
        await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("scrub", BackupScopeSelector.WholeTree("maintenance-a")));
        await _fixture.Catalog.RegisterAsync(BackupManifestModelTests.Sample(id: "maintenance-orphan-" + Guid.NewGuid().ToString("N")));
        var recorder = new RecordingProgress();

        var report = await WithProgressAsync(
            recorder, () => Service<ILatticeBackupCatalogScrubService>().ScrubAsync(pruneOrphans: true));

        var scrubbing = recorder.Reports.Where(r => r.Phase == BackupOperationPhases.ScrubbingCatalog).ToList();
        var pruning = recorder.Reports.Where(r => r.Phase == BackupOperationPhases.PruningOrphans).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(scrubbing[0], Is.EqualTo(new Report(BackupOperationPhases.ScrubbingCatalog, 0, null, BackupOperationUnits.Manifests)));
            Assert.That(scrubbing[^1].Completed, Is.EqualTo(report.ScannedCount));
            Assert.That(pruning[0], Is.EqualTo(new Report(BackupOperationPhases.PruningOrphans, 0, report.OrphanCount, BackupOperationUnits.Manifests)));
            Assert.That(pruning[^1].Completed, Is.EqualTo(report.OrphanCount));
            Assert.That(report.RemovedCount, Is.EqualTo(report.OrphanCount));
        });
    }

    [Test]
    public async Task A_non_pruning_catalog_scrub_never_enters_the_pruning_phase()
    {
        var recorder = new RecordingProgress();

        await WithProgressAsync(recorder, () => Service<ILatticeBackupCatalogScrubService>().ScrubAsync());

        Assert.That(recorder.Reports.Any(r => r.Phase == BackupOperationPhases.PruningOrphans), Is.False);
    }

    [Test]
    public async Task A_cold_restore_does_not_surface_its_nested_catalog_rebuild_as_a_phase()
    {
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cold", BackupScopeSelector.WholeTree("maintenance-a")));
        var recorder = new RecordingProgress();

        await WithProgressAsync(recorder, () => _fixture.ColdRestore.ColdRestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, "maintenance-cold-" + Guid.NewGuid().ToString("N"))));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Reports.Any(r => r.Phase == BackupOperationPhases.RebuildingCatalog), Is.False);
            Assert.That(recorder.Reports[^1].Phase, Is.EqualTo(BackupOperationPhases.Cataloguing));
        });
    }

    private readonly record struct Report(string Phase, long Completed, long? Total, string? Unit);

    /// <summary>Records every report; completes synchronously.</summary>
    private sealed class RecordingProgress : ILatticeOperationProgress
    {
        private readonly List<Report> _reports = [];

        public IReadOnlyList<Report> Reports
        {
            get
            {
                lock (_reports)
                {
                    return _reports.ToList();
                }
            }
        }

        public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            lock (_reports)
            {
                _reports.Add(new Report(phase, completedUnits, totalUnits, unitName));
            }

            return ValueTask.CompletedTask;
        }
    }
}
