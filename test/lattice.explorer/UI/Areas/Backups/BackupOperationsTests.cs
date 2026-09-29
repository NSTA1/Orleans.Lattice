using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Staged operations (epic E15): each action checks access first, advances
/// through its stages as the facade answers, reports what it produced, and
/// fails with one plain sentence. Every call is held open on a
/// <see cref="TaskCompletionSource"/>, so no test depends on timing.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupOperationsTests : BackupsTestContext
{
    [Test]
    public async Task A_full_capture_checks_access_captures_and_records_in_stages()
    {
        var capture = new TaskCompletionSource<LatticeBackupCaptureResult>();
        Backups.Capture = _ => capture.Task;

        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("a/crm/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(operation.Kind, Is.EqualTo(BackupOperationKind.FullCapture));
            Assert.That(operation.Title, Is.EqualTo("Capture a full backup of orders"));
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Running));
            Assert.That(operation.CurrentStage, Is.EqualTo(1), "the probe answered, so the capture is under way");
            Assert.That(operation.Message, Is.Null);
        });

        var manifest = FakeBackupControl.Manifest("b9", "nightly", "a/crm/orders", artifacts: ["a1", "a2"]);
        capture.SetResult(new LatticeBackupCaptureResult("b9", manifest));
        await operation.Completion;

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.CurrentStage, Is.EqualTo(2));
            Assert.That(operation.Message, Is.EqualTo("Captured backup nightly."));
            Assert.That(operation.Links.Single().Target, Is.EqualTo(BackupsAddresses.Backup("b9")));
            Assert.That(operation.Facts.Select(fact => fact.Key), Is.EqualTo(new[] { "Backups captured", "Artifacts", "Size" }));
            Assert.That(operation.Facts[2].Value, Is.EqualTo("4 KiB"));
            Assert.That(operation.CompletedAt, Is.EqualTo(Time.GetUtcNow()));
        });
    }

    [Test]
    public void A_capture_the_probe_denies_stops_at_the_access_stage_without_capturing()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));

        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(operation.CurrentStage, Is.Zero);
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotPermitted));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CreateBackupAsync)), Is.Zero);
        });
    }

    [Test]
    public void An_incremental_capture_needs_the_incremental_grant_and_names_its_base()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope) with { CanCaptureIncremental = false });
        var denied = Actions.CaptureIncremental("delta", BackupScopeSelector.WholeTree("orders"), "base1");

        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope));
        var captured = Actions.CaptureIncremental("delta", BackupScopeSelector.WholeTree("orders"), "base1");

        Assert.Multiple(() =>
        {
            Assert.That(denied.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(captured.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(captured.Message, Does.StartWith("Captured incremental backup"));
            Assert.That(Backups.LastOf<LatticeBackupIncrementalCaptureRequest>(nameof(ILatticeBackupControl.CreateIncrementalBackupAsync)).BaseBackupId, Is.EqualTo("base1"));
        });
    }

    [Test]
    public void A_set_capture_checks_every_member_and_links_every_member_backup()
    {
        var operation = Actions.CaptureSet("quarter", [BackupScopeSelector.WholeTree("orders"), BackupScopeSelector.WholeTree("a/crm/customers")], crossTreeConsistent: true);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.Title, Is.EqualTo("Capture a backup set of 2 trees"));
            Assert.That(operation.Stages[1], Is.EqualTo("Capture every tree at one fence"));
            Assert.That(operation.Links, Has.Count.EqualTo(2));
            Assert.That(operation.Message, Is.EqualTo("Captured backup set quarter with 2 members."));
            Assert.That(Backups.LastOf<LatticeBackupSetCaptureRequest>(nameof(ILatticeBackupControl.CreateBackupSetAsync)).CrossTreeConsistent, Is.True);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ProbeCapabilitiesAsync)), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_restore_validates_the_chain_restores_and_keeps_its_result_for_revert()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        var restore = new TaskCompletionSource<LatticeRestoreResult>();
        Backups.Restore = _ => restore.Task;

        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.ShadowCutover, cold: false);
        Assert.That(operation.CurrentStage, Is.EqualTo(2));

        var request = Backups.LastOf<LatticeRestoreRequest>(nameof(ILatticeBackupControl.RestoreBackupAsync));
        restore.SetResult(FakeBackupControl.RestoreResult(request));
        await operation.Completion;

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.CanRevert, Is.True);
            Assert.That(operation.RestoreResult!.ShadowPhysicalTreeId, Is.Not.Null);
            Assert.That(operation.Facts.Select(fact => fact.Value), Has.None.Contains("physical"), "physical tree ids are never shown");
            Assert.That(operation.Facts.Single(fact => fact.Key == "Entries applied").Value, Is.EqualTo("42"));
            Assert.That(request.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(request.TargetTreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void An_in_place_restore_cannot_be_reverted()
    {
        Seed(FakeBackupControl.Manifest("b1"));

        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.InPlace, cold: false);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.CanRevert, Is.False);
            Assert.That(() => Actions.Revert(operation), Throws.InvalidOperationException);
        });
    }

    [Test]
    public void A_restore_of_a_backup_that_has_gone_fails_as_not_found()
    {
        var operation = Actions.Restore("missing", "orders", LatticeRestoreMode.InPlace, cold: false);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotFound));
            Assert.That(operation.CurrentStage, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_restore_the_probe_denies_never_reaches_the_facade()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope) with { CanRestore = false });

        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.InPlace, cold: false);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotPermitted));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RestoreBackupAsync)), Is.Zero);
        });
    }

    [Test]
    public void A_cold_restore_skips_the_catalogue_and_a_refusal_as_not_served_withdraws_the_extensions()
    {
        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.InPlace, cold: true);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Kind, Is.EqualTo(BackupOperationKind.ColdRestore));
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.DescribeBackupAsync)), Is.Zero);
            Assert.That(Services.GetRequiredService<BackupsAccess>().ExtensionsServed, Is.False);
        });
    }

    [Test]
    public void A_served_cold_restore_succeeds()
    {
        Backups.ColdRestore = request => Task.FromResult(FakeBackupControl.RestoreResult(request) with { DeadLetteredOverQuota = 2, DeadLetteredCrossTenant = 1 });

        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.InPlace, cold: true);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.Facts.Select(fact => fact.Key), Does.Contain("Set aside: over quota").And.Contain("Set aside: another tenant's keys"));
        });
    }

    [Test]
    public void Reverting_a_point_in_time_restore_marks_it_reverted()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        var restore = Actions.Restore("b1", "orders", LatticeRestoreMode.ShadowCutover, cold: false);

        var revert = Actions.Revert(restore);

        Assert.Multiple(() =>
        {
            Assert.That(revert.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(restore.RevertedBy, Is.EqualTo(revert.Id));
            Assert.That(restore.CanRevert, Is.False);
            Assert.That(Backups.LastOf<LatticeRestoreResult>(nameof(ILatticeBackupControl.RevertRestoreAsync)), Is.SameAs(restore.RestoreResult));
            Assert.That(() => Actions.Revert(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Catalogue_rebuild_and_scrub_report_their_figures()
    {
        Backups.Rebuild = () => Task.FromResult(new BackupCatalogRebuildReport(10, 3, 7));
        Backups.Scrub = prune => Task.FromResult(new BackupCatalogScrubReport(10, 2, prune ? 2 : 0, prune, ["o1", "o2"]));

        var rebuild = Actions.RebuildCatalogue();
        var check = Actions.ScrubCatalogue(pruneOrphans: false);
        var prune = Actions.ScrubCatalogue(pruneOrphans: true);

        Assert.Multiple(() =>
        {
            Assert.That(rebuild.Facts.Select(fact => fact.Value), Is.EqualTo(new[] { "10", "3", "7" }));
            Assert.That(check.Items, Is.EqualTo(new[] { "o1", "o2" }));
            Assert.That(check.Message, Does.StartWith("Found 2 orphan rows"));
            Assert.That(prune.Message, Is.EqualTo("Removed 2 orphan rows from the catalogue."));
            Assert.That(Operations.Latest(BackupOperationKind.ScrubCatalogue), Is.SameAs(prune));
            Assert.That(Operations.Recent.First(), Is.SameAs(prune));
        });
    }

    [Test]
    public void A_clean_scrub_says_so_and_unserved_maintenance_withdraws_the_extensions()
    {
        Backups.Scrub = _ => Task.FromResult(new BackupCatalogScrubReport(4, 0, 0, false, []));
        Assert.That(Actions.ScrubCatalogue(false).Message, Is.EqualTo("Every catalogue row has its backup in the store."));

        Backups.Rebuild = () => Task.FromException<BackupCatalogRebuildReport>(new NotSupportedException());
        var rebuild = Actions.RebuildCatalogue();
        Backups.Scrub = _ => Task.FromException<BackupCatalogScrubReport>(new NotSupportedException());
        var scrub = Actions.ScrubCatalogue(true);

        Assert.Multiple(() =>
        {
            Assert.That(rebuild.Message, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(scrub.Message, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(Services.GetRequiredService<BackupsAccess>().ExtensionsServed, Is.False);
        });
    }

    [Test]
    public async Task Ending_the_circuit_stops_a_running_operation()
    {
        var operations = new BackupOperations(Time);
        var changes = 0;
        var operation = operations.Start(BackupOperationKind.FullCapture, "t", ["one", "two"], (_, token) => Task.Delay(Timeout.Infinite, token));
        operation.Changed += () => changes++;

        operations.Dispose();
        await operation.Completion;

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Cancelled));
            Assert.That(operation.Message, Does.Contain("session ended"));
            Assert.That(changes, Is.EqualTo(1));
            Assert.That(() => operations.Start(BackupOperationKind.FullCapture, "t", ["one"], (_, _) => Task.CompletedTask), Throws.InstanceOf<ObjectDisposedException>());
        });
        operations.Dispose();
    }

    [Test]
    public void Work_that_returns_without_finishing_succeeds_and_a_finished_operation_does_not_move()
    {
        var operations = new BackupOperations(Time);
        var operation = operations.Start(BackupOperationKind.FullCapture, "t", ["one", "two"], (current, _) =>
        {
            current.Advance(1);
            return Task.CompletedTask;
        });

        operation.Advance(0);
        operation.Fail("late");

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.Message, Is.EqualTo("Done."));
            Assert.That(operation.CurrentStage, Is.EqualTo(1));
            Assert.That(operations.Find(operation.Id), Is.SameAs(operation));
            Assert.That(operations.Find("nope"), Is.Null);
            Assert.That(operations.Find(null), Is.Null);
            Assert.That(operations.Latest(BackupOperationKind.Restore), Is.Null);
            Assert.That(() => operation.Advance(2), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => operation.Advance(-1), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new BackupOperation("1", BackupOperationKind.Restore, "t", [], Time), Throws.ArgumentException);
            Assert.That(() => operations.Start(BackupOperationKind.Restore, "t", ["x"], null!), Throws.ArgumentNullException);
            Assert.That(() => new BackupOperations(null!), Throws.ArgumentNullException);
        });
        operations.Dispose();
    }

    [Test]
    public void A_synchronous_throw_fails_the_operation_with_a_sentence()
    {
        using var operations = new BackupOperations(Time);

        var operation = operations.Start(BackupOperationKind.Restore, "t", ["one"], (_, _) => throw new LatticeAuthorizationDeniedException());

        Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotPermitted));
    }

    [Test]
    public void The_actions_refuse_missing_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => Actions.Restore("", "t", LatticeRestoreMode.InPlace, false), Throws.ArgumentException);
            Assert.That(() => Actions.Restore("b", "", LatticeRestoreMode.InPlace, false), Throws.ArgumentException);
            Assert.That(() => new BackupActions(null!, null!, null!), Throws.ArgumentNullException);
        });
    }

    private BackupActions Actions => Services.GetRequiredService<BackupActions>();
}
