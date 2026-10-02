using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// How the Backups area reads a cluster-tracked operation (#4122): its title by
/// kind, the links and figures a succeeded operation produced, and the restore
/// result a revert needs - only from a succeeded point-in-time restore.
/// </summary>
[TestFixture]
public sealed class BackupClusterOperationTests
{
    [Test]
    [TestCase(BackupOperationKinds.Capture, "Capture a full backup of orders")]
    [TestCase(BackupOperationKinds.IncrementalCapture, "Capture an incremental backup of orders")]
    [TestCase(BackupOperationKinds.Restore, "Restore orders")]
    [TestCase(BackupOperationKinds.ColdRestore, "Cold-restore orders")]
    [TestCase(BackupOperationKinds.HealthCheck, "Check the health of a backup of orders")]
    [TestCase(BackupOperationKinds.CatalogRebuild, "Rebuild the catalogue from the backup store")]
    [TestCase(BackupOperationKinds.CatalogScrub, "Check the catalogue against the backup store")]
    [TestCase("backup.future", "Backup operation")]
    public void The_title_reads_by_kind(string kind, string expected)
    {
        Assert.That(BackupClusterOperation.Title(FakeBackupControl.Running("op-1", kind, "orders")), Is.EqualTo(expected));
    }

    [Test]
    [TestCase(BackupOperationPhases.Verifying, "Verifying artifacts")]
    [TestCase(BackupOperationPhases.RebuildingCatalog, "Rebuilding the catalogue")]
    [TestCase(BackupOperationPhases.ScrubbingCatalog, "Checking the catalogue")]
    [TestCase(BackupOperationPhases.PruningOrphans, "Removing orphan rows")]
    [TestCase(BackupOperationPhases.CapturingMembers, "Capturing members")]
    public void Maintenance_phases_read_in_the_areas_words(string phase, string expected)
    {
        Assert.That(BackupClusterOperation.PhaseName(phase), Is.EqualTo(expected));
    }

    [Test]
    public void A_succeeded_health_check_links_the_backups_health_and_reports_its_verdict()
    {
        var control = new FakeBackupControl();
        control.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.HealthCheck, "orders");
        var running = control.Statuses["op-1"];
        control.Succeed("op-1", "b1", FakeBackupControl.HealthResult("b1", BackupHealthStatus.Warning, missing: 1, mismatched: 2));
        var status = control.Statuses["op-1"];

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.Links(running), Is.Empty);
            Assert.That(BackupClusterOperation.Links(status).Select(static link => link.Target), Is.EqualTo(new[] { BackupsAddresses.HealthOf("b1"), BackupsAddresses.Backup("b1") }));
            Assert.That(BackupClusterOperation.Facts(status), Is.EqualTo(new KeyValuePair<string, string>[]
            {
                new("Verdict", "Warning"),
                new("Missing or uncommitted artifacts", "1"),
                new("Hash mismatches", "2"),
            }));
            Assert.That(BackupClusterOperation.Summary(status), Is.EqualTo("The backup needs attention: Warning."));
        });
    }

    [Test]
    public void A_succeeded_rebuild_reports_its_counts()
    {
        var status = FakeBackupControl.Running("op-1", BackupOperationKinds.CatalogRebuild, "sys-backup-catalog") with
        {
            State = LatticeOperationState.Succeeded,
            Result = FakeBackupControl.RebuildResult(10, 3, 7),
        };

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.Facts(status), Is.EqualTo(new KeyValuePair<string, string>[]
            {
                new("Manifests scanned", "10"),
                new("Added to the catalogue", "3"),
                new("Reconciled in place", "7"),
            }));
            Assert.That(BackupClusterOperation.Summary(status), Is.EqualTo("Rebuilt the catalogue from the backup store."));
            Assert.That(BackupClusterOperation.Items(status), Is.Empty);
        });
    }

    [Test]
    [TestCase(false, new string[0], "Every catalogue row has its backup in the store.", false)]
    [TestCase(false, new[] { "o1", "o2" }, "Found 2 orphan rows. They are never offered as restore points; remove them from the maintenance page.", true)]
    [TestCase(true, new[] { "o1", "o2" }, "Removed 2 orphan rows from the catalogue.", false)]
    public void A_succeeded_scrub_lists_its_orphans_and_says_whether_any_are_left(bool pruned, string[] orphans, string summary, bool removable)
    {
        var status = FakeBackupControl.Running("op-1", BackupOperationKinds.CatalogScrub, "sys-backup-catalog") with
        {
            State = LatticeOperationState.Succeeded,
            Result = FakeBackupControl.ScrubResult(5, pruned, orphans),
        };

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.Items(status), Is.EqualTo(orphans));
            Assert.That(BackupClusterOperation.Summary(status), Is.EqualTo(summary));
            Assert.That(BackupClusterOperation.HasOrphansToRemove(status), Is.EqualTo(removable));
            Assert.That(BackupClusterOperation.Facts(status).Select(static fact => fact.Key), Is.EqualTo(new[] { "Rows scanned", "Orphan rows", "Rows removed" }));
        });
    }

    [Test]
    public void An_unfinished_maintenance_operation_summarises_as_its_state_and_offers_nothing()
    {
        var running = FakeBackupControl.Running("op-1", BackupOperationKinds.CatalogScrub, "sys-backup-catalog") with { State = LatticeOperationState.Running };
        var failed = running with { State = LatticeOperationState.Failed, FailureReason = "boom" };

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.Summary(running), Is.EqualTo("Running"));
            Assert.That(BackupClusterOperation.Summary(failed), Is.EqualTo("Failed"));
            Assert.That(BackupClusterOperation.Items(running), Is.Empty);
            Assert.That(BackupClusterOperation.HasOrphansToRemove(running), Is.False);
        });
    }

    [Test]
    public void A_health_check_started_here_is_named_for_its_backup()
    {
        var at = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero);
        var id = BackupClusterOperation.HealthCheckId("b1", at);
        var mine = FakeBackupControl.Running(id!, BackupOperationKinds.HealthCheck, "orders");
        var other = FakeBackupControl.Running(BackupClusterOperation.HealthCheckId("b10", at)!, BackupOperationKinds.HealthCheck, "orders");
        var finishedElsewhere = FakeBackupControl.Running("op-9", BackupOperationKinds.HealthCheck, "orders") with
        {
            State = LatticeOperationState.Succeeded,
            ResultReference = "b1",
        };

        Assert.Multiple(() =>
        {
            Assert.That(id, Does.StartWith("health.b1."));
            Assert.That(BackupClusterOperation.IsHealthCheckOf(mine, "b1"), Is.True);
            Assert.That(BackupClusterOperation.IsHealthCheckOf(other, "b1"), Is.False, "a longer id that shares the prefix is another backup");
            Assert.That(BackupClusterOperation.IsHealthCheckOf(finishedElsewhere, "b1"), Is.True, "a finished check names its backup");
            Assert.That(BackupClusterOperation.IsHealthCheckOf(FakeBackupControl.Running(id!, BackupOperationKinds.Capture, "orders"), "b1"), Is.False);
            Assert.That(BackupClusterOperation.HealthCheckId(new string('a', 200), at), Is.Null, "an id the cluster would refuse is left to the cluster to generate");
            Assert.That(BackupClusterOperation.HealthCheckId("has space", at), Is.Null);
        });
    }

    [Test]
    public void A_set_capture_title_counts_its_trees_and_an_empty_scope_reads_as_a_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.Title(FakeBackupControl.Running("op-1", BackupOperationKinds.SetCapture, "orders", "users")), Is.EqualTo("Capture a backup set of 2 trees"));
            Assert.That(BackupClusterOperation.Title(FakeBackupControl.Running("op-1", BackupOperationKinds.SetCapture, "orders")), Is.EqualTo("Capture a backup set of 1 tree"));
            Assert.That(BackupClusterOperation.Title(FakeBackupControl.Running("op-1", BackupOperationKinds.Restore)), Is.EqualTo("Restore a tree"));
        });
    }

    [Test]
    public void A_succeeded_capture_links_its_backup_and_a_running_one_links_nothing()
    {
        var control = new FakeBackupControl();
        control.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Capture, "orders");
        var running = control.Statuses["op-1"];
        control.SucceedCapture("op-1", "b1");

        var links = BackupClusterOperation.Links(control.Statuses["op-1"]);

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.Links(running), Is.Empty);
            Assert.That(BackupClusterOperation.Facts(running), Is.Empty);
            Assert.That(links, Has.Count.EqualTo(1));
            Assert.That(links[0].Text, Is.EqualTo("The captured backup"));
            Assert.That(links[0].Target, Is.EqualTo(BackupsAddresses.Backup("b1")));
            Assert.That(BackupClusterOperation.Facts(control.Statuses["op-1"]), Is.Empty);
            Assert.That(BackupClusterOperation.RestoreResult(control.Statuses["op-1"]), Is.Null);
        });
    }

    [Test]
    public void A_succeeded_set_links_each_member_by_its_tree_and_counts_them()
    {
        var status = FakeBackupControl.Running("op-1", BackupOperationKinds.SetCapture, "orders", "users") with
        {
            State = LatticeOperationState.Succeeded,
            Result = new Dictionary<string, string> { [BackupOperationResultKeys.MemberBackupIds] = "b1,b2" },
        };

        var links = BackupClusterOperation.Links(status);

        Assert.Multiple(() =>
        {
            Assert.That(links.Select(static link => link.Text), Is.EqualTo(new[] { "The backup of orders", "The backup of users" }));
            Assert.That(links.Select(static link => link.Target), Is.EqualTo(new[] { BackupsAddresses.Backup("b1"), BackupsAddresses.Backup("b2") }));
            Assert.That(BackupClusterOperation.Facts(status), Is.EqualTo(new[] { new KeyValuePair<string, string>("Backups captured", "2") }));
        });
    }

    [Test]
    public void A_succeeded_point_in_time_restore_carries_its_result_its_figures_and_can_be_reverted()
    {
        var control = new FakeBackupControl();
        control.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Restore, "orders");
        var restore = new LatticeRestoreResult("b1", "orders", LatticeRestoreMode.ShadowCutover, "r-1", ["b0", "b1"], 42)
        {
            ShadowPhysicalTreeId = "orders-shadow",
            PreviousPhysicalTreeId = "orders-previous",
        };
        control.SucceedRestore("op-1", restore);
        var status = control.Statuses["op-1"];

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.RestoreResult(status)!.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(BackupClusterOperation.CanRevert(status), Is.True);
            Assert.That(BackupClusterOperation.Links(status).Single().Target, Is.EqualTo(BackupsAddresses.Backup("b1")));
            Assert.That(BackupClusterOperation.Facts(status), Does.Contain(new KeyValuePair<string, string>("Entries applied", "42")));
        });
    }

    [Test]
    public void An_in_place_or_unfinished_restore_cannot_be_reverted()
    {
        var control = new FakeBackupControl();
        control.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Restore, "orders");
        var running = control.Statuses["op-1"];
        control.SucceedRestore("op-1", new LatticeRestoreResult("b1", "orders", LatticeRestoreMode.InPlace, "r-1", ["b1"], 3));

        Assert.Multiple(() =>
        {
            Assert.That(BackupClusterOperation.CanRevert(running), Is.False);
            Assert.That(BackupClusterOperation.CanRevert(control.Statuses["op-1"]), Is.False);
            Assert.That(BackupClusterOperation.RestoreResult(control.Statuses["op-1"]), Is.Not.Null);
        });
    }

    [Test]
    public void The_readers_need_a_status()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupClusterOperation.Title(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.Links(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.Facts(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.RestoreResult(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.Items(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.Summary(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.HasOrphansToRemove(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.IsHealthCheckOf(null!, "b1"), Throws.ArgumentNullException);
            Assert.That(() => BackupClusterOperation.HealthCheckId(string.Empty, DateTimeOffset.UnixEpoch), Throws.ArgumentException);
        });
    }
}
