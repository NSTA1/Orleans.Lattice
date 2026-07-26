using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression coverage for issue #1386: after a <see cref="LatticeRestoreMode.ShadowCutover"/>
/// restore the logical tree aliases to a fresh physical tree
/// (<c>{tree}-bkprestore-restore-shadow-N</c>). The snapshot capture/seed path
/// keys its per-shard baseline by the physical shard-root tree id, so the
/// cursor's open/read path must resolve the same physical id - otherwise a fresh
/// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/> reaches an unseeded
/// activation, falls into the durable-reload branch, and throws
/// <see cref="LatticeSnapshotExpiredException"/> permanently. These tests drive a
/// real full and incremental shadow-cutover restore and then a fresh snapshot
/// scan (the same path backup capture drains through), asserting the restored
/// data is returned rather than the open faulting.
/// </summary>
[Category("Integration")]
public sealed class LatticeSnapshotAfterShadowCutoverTests
{
    private const string Source = "orders";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Snapshot_scan_after_full_shadow_cutover_restore_returns_restored_data()
    {
        await _fixture.InitializeAsync();

        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("backup-v1"));
        await source.SetAsync("k2", Bytes("backup-v2"));
        await source.SetAsync("k3", Bytes("backup-v3"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cutover-snap", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-snap-live";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await live.SetAsync("live-key", Bytes("live-value"));

        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
        Assert.That(result.ShadowPhysicalTreeId, Is.Not.EqualTo(target),
            "the restore must have aliased the logical tree to a distinct physical shadow tree");

        // The regression: a fresh Snapshot cursor on the cutover-restored logical
        // tree must serve the restored baseline, not fault permanently.
        var snapshot = await DrainSnapshotEntriesAsync(live);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot["k1"], Is.EqualTo("backup-v1"));
            Assert.That(snapshot["k2"], Is.EqualTo("backup-v2"));
            Assert.That(snapshot["k3"], Is.EqualTo("backup-v3"));
            Assert.That(snapshot.ContainsKey("live-key"), Is.False,
                "the pre-cutover live key is not part of the restored shadow");
        });
    }

    [Test]
    public async Task Snapshot_scan_after_incremental_shadow_cutover_restore_returns_folded_data()
    {
        await _fixture.InitializeAsync();

        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));
        var baseBackup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("inc-base", BackupScopeSelector.WholeTree(Source)));

        await source.SetAsync("k1", Bytes("v1-updated"));
        await source.SetAsync("k4", Bytes("v4"));
        await source.DeleteAsync("k2");
        var increment = await _fixture.Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "inc-tip", BackupScopeSelector.WholeTree(Source), baseBackup.BackupId));

        const string target = "orders-snap-inc";
        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(increment.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));

        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        var snapshot = await DrainSnapshotEntriesAsync(live);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot["k1"], Is.EqualTo("v1-updated"), "overwrite folded");
            Assert.That(snapshot.ContainsKey("k2"), Is.False, "delete folded");
            Assert.That(snapshot["k3"], Is.EqualTo("v3"), "untouched base entry survives");
            Assert.That(snapshot["k4"], Is.EqualTo("v4"), "new key folded");
        });
    }

    [Test]
    public async Task Backup_capture_after_shadow_cutover_restore_succeeds()
    {
        // Escalation on issue #1386: backup capture also drains a Snapshot cursor
        // (RawEntryCollector), so the same identity bug made a cutover-restored
        // tree impossible to re-capture. Capturing here must succeed.
        await _fixture.InitializeAsync();

        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("backup-v1"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("recapture-src", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-recapture";
        await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));

        var recapture = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("recapture", BackupScopeSelector.WholeTree(target)));

        Assert.That(recapture.Manifest, Is.Not.Null);
        Assert.That(recapture.Manifest.ContentDescriptors, Is.Not.Empty);
    }

    private static async Task<Dictionary<string, string>> DrainSnapshotEntriesAsync(ILattice tree)
    {
        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var entries = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            while (true)
            {
                var page = await tree.NextEntriesAsync(cursorId, pageSize: 128);
                foreach (var entry in page.Entries)
                    entries[entry.Key] = Encoding.UTF8.GetString(entry.Value);
                if (!page.HasMore)
                    break;
            }
        }
        finally
        {
            await tree.CloseCursorAsync(cursorId);
        }

        return entries;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}
