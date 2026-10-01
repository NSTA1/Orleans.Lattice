using System.Text;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Progress reporting of the backup engine through the ambient
/// <see cref="LatticeOperationProgress"/> sink (#4122): a capture counts entries
/// against the in-scope total, a set capture counts members (and suppresses each
/// member's own entry counts), a restore counts manifests validated, entries
/// applied and, on the bulk-load path, shards replayed, and a cold restore opens
/// with bootstrapping and closes with cataloguing. Real engine, real cluster; the
/// recorded sink completes synchronously, so the sequence is deterministic.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BackupOperationProgressIntegrationTests
{
    private const int KeyCount = 25;

    private RestoreClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync();
        await SeedAsync("progress-a", KeyCount);
        await SeedAsync("progress-b", 3);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task SeedAsync(string treeId, int count)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        for (var i = 0; i < count; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }
    }

    private static async Task<T> WithProgressAsync<T>(RecordingProgress recorder, Func<Task<T>> work)
    {
        using (LatticeOperationProgress.Enter(recorder))
        {
            return await work();
        }
    }

    [Test]
    public async Task A_capture_counts_entries_against_the_in_scope_total_then_catalogues()
    {
        var recorder = new RecordingProgress();

        await WithProgressAsync(recorder, () => _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("progress", BackupScopeSelector.WholeTree("progress-a"), pageSize: 10)));

        var capturing = recorder.Reports.Where(r => r.Phase == BackupOperationPhases.Capturing).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(capturing[0], Is.EqualTo(new Report(BackupOperationPhases.Capturing, 0, KeyCount, BackupOperationUnits.Entries)));
            Assert.That(capturing.Select(r => r.Completed), Is.Ordered.Ascending);
            Assert.That(capturing[^1].Completed, Is.EqualTo(KeyCount));
            Assert.That(capturing, Has.Count.GreaterThan(2), "Pages of ten report as they stream.");
            Assert.That(recorder.Reports[^1].Phase, Is.EqualTo(BackupOperationPhases.Cataloguing));
        });
    }

    [Test]
    public async Task A_set_capture_counts_members_and_suppresses_member_entry_counts()
    {
        var recorder = new RecordingProgress();

        await WithProgressAsync(recorder, () => _fixture.Capture.CaptureSetAsync(
            new LatticeBackupSetCaptureRequest(
                "set",
                [BackupScopeSelector.WholeTree("progress-a"), BackupScopeSelector.WholeTree("progress-b")])));

        Assert.That(recorder.Reports, Is.EqualTo(new[]
        {
            new Report(BackupOperationPhases.CapturingMembers, 0, 2, BackupOperationUnits.Members),
            new Report(BackupOperationPhases.CapturingMembers, 1, 2, BackupOperationUnits.Members),
            new Report(BackupOperationPhases.CapturingMembers, 2, 2, BackupOperationUnits.Members),
        }));
    }

    [Test]
    public async Task A_restore_into_an_empty_tree_validates_applies_and_replays_shards()
    {
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("for-restore", BackupScopeSelector.WholeTree("progress-a")));
        var recorder = new RecordingProgress();

        var result = await WithProgressAsync(recorder, () => _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, "progress-restored-" + Guid.NewGuid().ToString("N"))));

        var validating = recorder.Reports.Where(r => r.Phase == BackupOperationPhases.Validating).ToList();
        var applying = recorder.Reports.Where(r => r.Phase == BackupOperationPhases.Applying).ToList();
        var replaying = recorder.Reports.Where(r => r.Phase == BackupOperationPhases.Replaying).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(KeyCount));
            Assert.That(validating.Select(r => r.Completed), Is.EqualTo(new long[] { 0, 1 }));
            Assert.That(validating.All(r => r.Total == 1 && r.Unit == BackupOperationUnits.Manifests), Is.True);
            Assert.That(applying[0], Is.EqualTo(new Report(BackupOperationPhases.Applying, 0, KeyCount, BackupOperationUnits.Entries)));
            Assert.That(applying[^1].Completed, Is.EqualTo(KeyCount), "Every streamed entry counts once.");
            Assert.That(replaying, Is.Not.Empty, "The bulk-load path replays shards.");
            Assert.That(replaying[0].Completed, Is.Zero);
            Assert.That(replaying.Max(r => r.Completed), Is.EqualTo(replaying[0].Total), "Every shard is counted.");
            Assert.That(replaying.All(r => r.Unit == BackupOperationUnits.Shards), Is.True);
        });
    }

    [Test]
    public async Task A_restore_into_a_live_tree_merges_without_a_replay_phase()
    {
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("for-merge", BackupScopeSelector.WholeTree("progress-b")));
        var recorder = new RecordingProgress();

        await WithProgressAsync(recorder, () => _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId)));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Reports.Where(r => r.Phase == BackupOperationPhases.Applying).Last().Completed, Is.EqualTo(3));
            Assert.That(recorder.Reports.Any(r => r.Phase == BackupOperationPhases.Replaying), Is.False);
        });
    }

    [Test]
    public async Task A_cold_restore_opens_with_bootstrapping_and_closes_with_cataloguing()
    {
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("for-cold", BackupScopeSelector.WholeTree("progress-b")));
        var recorder = new RecordingProgress();

        await WithProgressAsync(recorder, () => _fixture.ColdRestore.ColdRestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, "progress-cold-" + Guid.NewGuid().ToString("N"))));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Reports[0].Phase, Is.EqualTo(BackupOperationPhases.Bootstrapping));
            Assert.That(recorder.Reports.Any(r => r.Phase == BackupOperationPhases.Validating), Is.True);
            Assert.That(recorder.Reports[^1].Phase, Is.EqualTo(BackupOperationPhases.Cataloguing));
        });
    }

    [Test]
    public async Task Outside_a_tracked_operation_the_engine_reports_nothing()
    {
        Assert.That(LatticeOperationProgress.Current, Is.Null);

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("untracked", BackupScopeSelector.WholeTree("progress-b")));

        Assert.That(backup.BackupId, Is.Not.Empty);
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
