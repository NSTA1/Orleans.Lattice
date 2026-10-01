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
    [TestCase("backup.future", "Backup operation")]
    public void The_title_reads_by_kind(string kind, string expected)
    {
        Assert.That(BackupClusterOperation.Title(FakeBackupControl.Running("op-1", kind, "orders")), Is.EqualTo(expected));
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
        });
    }
}
