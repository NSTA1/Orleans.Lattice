using System.Text;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="BackupToolInvocations"/>, the pure adapter layer
/// between the backup MCP tools and the <see cref="Orleans.Lattice.Api.Backup.ILatticeBackupControl"/>
/// facade. Proves the capture-then-inspect round trip, the fail-closed denial an
/// unauthorized caller gets from the facade gate (the MCP layer adds none), the
/// bounded paged artifact export with its resume cursor, and the DTO projections.
/// All deterministic against a stateful fake - no cluster, no ordering-by-timing.
/// </summary>
[TestFixture]
public sealed class BackupToolInvocationsTests
{
    [Test]
    public async Task Create_then_list_and_describe_round_trips_a_backup()
    {
        var control = new FakeLatticeBackupControl();

        var created = await BackupToolInvocations.CreateBackupAsync(
            control, "nightly", "orders", scopeKind: null, keyOrPrefix: null, pageSize: 0, CancellationToken.None);

        var listed = await BackupToolInvocations.ListBackupsAsync(
            control, pageSize: 0, pageToken: null, orderByCreatedDescending: false, CancellationToken.None);
        var described = await BackupToolInvocations.DescribeBackupAsync(
            control, created.BackupId, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(created.Manifest.Name, Is.EqualTo("nightly"));
            Assert.That(created.Manifest.TreeId, Is.EqualTo("orders"));
            Assert.That(listed.Entries.Select(e => e.Id), Does.Contain(created.BackupId));
            Assert.That(described.Found, Is.True);
            Assert.That(described.Manifest!.Id, Is.EqualTo(created.BackupId));
            Assert.That(described.ChainBackupIds, Is.EqualTo(new[] { created.BackupId }));
        });
    }

    [Test]
    public async Task Create_incremental_records_the_base_and_chain()
    {
        var control = new FakeLatticeBackupControl();
        var full = await BackupToolInvocations.CreateBackupAsync(
            control, "full", "orders", null, null, 0, CancellationToken.None);

        var incremental = await BackupToolInvocations.CreateIncrementalBackupAsync(
            control, "delta", "orders", null, null, full.BackupId, 0, CancellationToken.None);
        var described = await BackupToolInvocations.DescribeBackupAsync(
            control, incremental.BackupId, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(incremental.Manifest.BaseBackupId, Is.EqualTo(full.BackupId));
            Assert.That(incremental.Manifest.Kind, Is.EqualTo(nameof(BackupKind.Incremental)));
            Assert.That(described.ChainBackupIds, Is.EqualTo(new[] { full.BackupId, incremental.BackupId }));
        });
    }

    [Test]
    public async Task Describe_missing_backup_reports_not_found()
    {
        var control = new FakeLatticeBackupControl();

        var described = await BackupToolInvocations.DescribeBackupAsync(control, "absent", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(described.Found, Is.False);
            Assert.That(described.Manifest, Is.Null);
        });
    }

    [Test]
    public void Unauthorized_caller_is_denied_fail_closed_on_every_operation()
    {
        var control = new FakeLatticeBackupControl { Authorized = false };

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await BackupToolInvocations.ListBackupsAsync(
                    control, 0, null, false, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await BackupToolInvocations.CreateBackupAsync(
                    control, "n", "orders", null, null, 0, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await BackupToolInvocations.DeleteBackupAsync(control, "bk-0", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public async Task Delete_reports_whether_a_backup_existed()
    {
        var control = new FakeLatticeBackupControl();
        var created = await BackupToolInvocations.CreateBackupAsync(
            control, "nightly", "orders", null, null, 0, CancellationToken.None);

        var first = await BackupToolInvocations.DeleteBackupAsync(control, created.BackupId, CancellationToken.None);
        var second = await BackupToolInvocations.DeleteBackupAsync(control, created.BackupId, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Deleted, Is.True);
            Assert.That(second.Deleted, Is.False);
        });
    }

    [Test]
    public async Task Export_artifact_pages_bytes_and_surfaces_a_resume_cursor()
    {
        var control = new FakeLatticeBackupControl();
        var payload = Encoding.ASCII.GetBytes(new string('x', 40));
        control.SeedArtifact("bk-0", "artifact-0", payload);

        // The fake chunks at 16 bytes; a 20-byte budget takes two chunks (32 bytes).
        var first = await BackupToolInvocations.ExportArtifactAsync(
            control, "bk-0", "artifact-0", chunkOffset: 0, maxBytes: 20, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.ByteCount, Is.EqualTo(32));
            Assert.That(first.EndOfStream, Is.False);
            Assert.That(first.NextChunkOffset, Is.EqualTo(2));
            Assert.That(Convert.FromBase64String(first.Base64Chunk), Has.Length.EqualTo(32));
        });

        var second = await BackupToolInvocations.ExportArtifactAsync(
            control, "bk-0", "artifact-0", chunkOffset: first.NextChunkOffset!.Value, maxBytes: 20, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.ByteCount, Is.EqualTo(8), "The final page carries the remaining 8 bytes.");
            Assert.That(second.EndOfStream, Is.True);
            Assert.That(second.NextChunkOffset, Is.Null);
        });
    }

    [Test]
    public async Task Export_artifact_of_a_small_artifact_is_a_single_terminal_page()
    {
        var control = new FakeLatticeBackupControl();
        control.SeedArtifact("bk-0", "artifact-0", Encoding.ASCII.GetBytes("hello"));

        var page = await BackupToolInvocations.ExportArtifactAsync(
            control, "bk-0", "artifact-0", chunkOffset: 0, maxBytes: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Encoding.ASCII.GetString(Convert.FromBase64String(page.Base64Chunk)), Is.EqualTo("hello"));
            Assert.That(page.EndOfStream, Is.True);
            Assert.That(page.NextChunkOffset, Is.Null);
        });
    }

    [Test]
    public async Task Restore_then_revert_round_trips_the_shadow_cutover_fields()
    {
        var control = new FakeLatticeBackupControl();

        var restore = await BackupToolInvocations.RestoreBackupAsync(
            control, "bk-0", "orders", "ShadowCutover", "op-7", CancellationToken.None);

        await BackupToolInvocations.RevertRestoreAsync(
            control,
            restore.BackupId,
            restore.TargetTreeId,
            restore.Mode,
            restore.OperationId,
            restore.ManifestChain,
            restore.EntriesApplied,
            restore.ShadowPhysicalTreeId,
            restore.PreviousPhysicalTreeId,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(restore.Mode, Is.EqualTo(nameof(LatticeRestoreMode.ShadowCutover)));
            Assert.That(restore.ShadowPhysicalTreeId, Is.EqualTo("phys-new"));
            Assert.That(control.LastReverted, Is.Not.Null);
            Assert.That(control.LastReverted!.OperationId, Is.EqualTo("op-7"));
            Assert.That(control.LastReverted.PreviousPhysicalTreeId, Is.EqualTo("phys-old"));
        });
    }

    [Test]
    public async Task Inventory_projects_the_report_fields()
    {
        var control = new FakeLatticeBackupControl
        {
            Inventory = new Orleans.Lattice.Api.Backup.BackupInventoryReport(
                totalBackupCount: 5,
                totalCatalogBytes: 4096,
                fullBackupCount: 3,
                incrementalBackupCount: 2,
                oldestBackupUtc: DateTimeOffset.UnixEpoch,
                newestBackupUtc: DateTimeOffset.UnixEpoch.AddDays(1),
                captureFailureCount: 1,
                restoreFailureCount: 0,
                bytesReclaimed: 512),
        };

        var inventory = await BackupToolInvocations.GetInventoryAsync(control, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(inventory.TotalBackupCount, Is.EqualTo(5));
            Assert.That(inventory.FullBackupCount, Is.EqualTo(3));
            Assert.That(inventory.IncrementalBackupCount, Is.EqualTo(2));
            Assert.That(inventory.BytesReclaimed, Is.EqualTo(512));
        });
    }

    [Test]
    public async Task Scope_status_projects_a_present_status()
    {
        var control = new FakeLatticeBackupControl
        {
            ScopeStatus = new Orleans.Lattice.Api.Backup.BackupScopeStatus(
                scope: BackupScopeSelector.Prefix("orders", "eu/"),
                fullScheduleRegistered: true,
                incrementalScheduleRegistered: false,
                lastFullRunUtc: DateTimeOffset.UnixEpoch,
                lastFullSuccessUtc: DateTimeOffset.UnixEpoch,
                lastIncrementalRunUtc: null,
                lastIncrementalSuccessUtc: null,
                lastRunOutcome: BackupScopeRunOutcome.Success,
                chainDepth: 2),
        };

        var status = await BackupToolInvocations.GetScopeStatusAsync(
            control, "orders", "Prefix", "eu/", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Found, Is.True);
            Assert.That(status.TreeId, Is.EqualTo("orders"));
            Assert.That(status.ScopeKind, Is.EqualTo(nameof(BackupScopeKind.Prefix)));
            Assert.That(status.KeyOrPrefix, Is.EqualTo("eu/"));
            Assert.That(status.FullScheduleRegistered, Is.True);
            Assert.That(status.LastRunOutcome, Is.EqualTo(nameof(BackupScopeRunOutcome.Success)));
            Assert.That(status.ChainDepth, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Scope_status_of_an_unknown_scope_reports_not_found()
    {
        var control = new FakeLatticeBackupControl { ScopeStatus = null };

        var status = await BackupToolInvocations.GetScopeStatusAsync(
            control, "orders", null, null, CancellationToken.None);

        Assert.That(status.Found, Is.False);
    }

    [Test]
    public async Task Manifest_projection_carries_scope_and_artifact_count()
    {
        var control = new FakeLatticeBackupControl();
        var created = await BackupToolInvocations.CreateBackupAsync(
            control, "nightly", "orders", "Prefix", "eu/", 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(created.Manifest.ScopeKind, Is.EqualTo(nameof(BackupScopeKind.Prefix)));
            Assert.That(created.Manifest.KeyOrPrefix, Is.EqualTo("eu/"));
            Assert.That(created.Manifest.Kind, Is.EqualTo(nameof(BackupKind.Full)));
            Assert.That(created.Manifest.ArtifactCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void Unrecognised_scope_kind_is_rejected()
    {
        var control = new FakeLatticeBackupControl();

        Assert.That(
            async () => await BackupToolInvocations.CreateBackupAsync(
                control, "nightly", "orders", "Nonsense", null, 0, CancellationToken.None),
            Throws.ArgumentException);
    }
}
