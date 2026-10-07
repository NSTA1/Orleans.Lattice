using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Staged operations (epic E15): each action checks access first, advances
/// through its stages as the facade answers, and fails with one plain sentence; a
/// capture or restore hands off to the cluster's tracked operation (#4122). Every call is held open on a
/// <see cref="TaskCompletionSource"/>, so no test depends on timing.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupOperationsTests : BackupsTestContext
{
    [Test]
    public async Task A_full_capture_checks_access_starts_on_the_cluster_and_hands_off()
    {
        var start = new TaskCompletionSource();
        Backups.StartGate = start.Task;

        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("a/crm/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(operation.Kind, Is.EqualTo(BackupOperationKind.FullCapture));
            Assert.That(operation.Title, Is.EqualTo("Capture a full backup of orders"));
            Assert.That(operation.Stages, Is.EqualTo(new[] { BackupActions.CheckAccessStage, BackupActions.StartStage }));
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Running));
            Assert.That(operation.CurrentStage, Is.EqualTo(1), "the probe answered, so the start is under way");
            Assert.That(operation.ClusterOperationId, Is.Null);
        });

        start.SetResult();
        await operation.Completion;

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.ClusterOperationId, Is.EqualTo("op-1"));
            Assert.That(operation.Message, Does.Contain("keeps running if you close this page"));
            Assert.That(Backups.LastOf<LatticeBackupCaptureRequest>(nameof(ILatticeBackupOperations.StartBackupAsync)).Name, Is.EqualTo("nightly"));
            Assert.That(Backups.CountOf("CreateBackupAsync"), Is.Zero, "the deprecated blocking verb is never called");
        });
    }

    [Test]
    public void A_capture_the_probe_denies_stops_at_the_access_stage_without_starting()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));

        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(operation.CurrentStage, Is.Zero);
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotPermitted));
            Assert.That(operation.ClusterOperationId, Is.Null);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupAsync)), Is.Zero);
        });
    }

    [Test]
    public void A_start_the_cluster_refuses_fails_with_a_sentence_and_hands_off_nothing()
    {
        Backups.StartFault = new LatticeAuthorizationDeniedException("no");

        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(operation.CurrentStage, Is.EqualTo(1));
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotPermitted));
            Assert.That(operation.ClusterOperationId, Is.Null);
        });
    }

    [Test]
    public void An_incremental_capture_needs_the_incremental_grant_and_names_its_base()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope) with { CanCaptureIncremental = false });
        var denied = Actions.CaptureIncremental("delta", BackupScopeSelector.WholeTree("orders"), "base1");

        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope));
        var started = Actions.CaptureIncremental("delta", BackupScopeSelector.WholeTree("orders"), "base1");

        Assert.Multiple(() =>
        {
            Assert.That(denied.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(started.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(started.ClusterOperationId, Is.Not.Null);
            Assert.That(Backups.LastOf<LatticeBackupIncrementalCaptureRequest>(nameof(ILatticeBackupOperations.StartIncrementalBackupAsync)).BaseBackupId, Is.EqualTo("base1"));
        });
    }

    [Test]
    public void A_set_capture_checks_every_member_and_starts_one_set()
    {
        var operation = Actions.CaptureSet("quarter", [BackupScopeSelector.WholeTree("orders"), BackupScopeSelector.WholeTree("a/crm/customers")], crossTreeConsistent: true);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.Title, Is.EqualTo("Capture a backup set of 2 trees"));
            Assert.That(operation.ClusterOperationId, Is.Not.Null);
            Assert.That(Backups.LastOf<LatticeBackupSetCaptureRequest>(nameof(ILatticeBackupOperations.StartBackupSetAsync)).CrossTreeConsistent, Is.True);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ProbeCapabilitiesAsync)), Is.EqualTo(2));
        });
    }

    [Test]
    public void A_restore_checks_the_backup_exists_and_starts_with_its_mode_and_target()
    {
        Seed(FakeBackupControl.Manifest("b1"));

        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.ShadowCutover, cold: false);

        var request = Backups.LastOf<LatticeRestoreRequest>(nameof(ILatticeBackupOperations.StartRestoreAsync));
        Assert.Multiple(() =>
        {
            Assert.That(operation.Kind, Is.EqualTo(BackupOperationKind.Restore));
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(operation.ClusterOperationId, Is.Not.Null);
            Assert.That(request.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(request.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(Backups.CountOf("RestoreBackupAsync"), Is.Zero);
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
            Assert.That(operation.CurrentStage, Is.Zero);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartRestoreAsync)), Is.Zero);
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
            Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartRestoreAsync)), Is.Zero);
        });
    }

    [Test]
    public void A_cold_restore_skips_the_catalogue_and_starts_a_cold_restore()
    {
        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.InPlace, cold: true);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Kind, Is.EqualTo(BackupOperationKind.ColdRestore));
            Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.DescribeBackupAsync)), Is.Zero);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartColdRestoreAsync)), Is.EqualTo(1));
        });
    }

    [Test]
    public void A_cold_restore_refused_as_not_served_fails_its_start_but_leaves_the_extensions_alone()
    {
        Backups.StartFault = new NotSupportedException("not served");

        var operation = Actions.Restore("b1", "orders", LatticeRestoreMode.InPlace, cold: true);

        Assert.Multiple(() =>
        {
            Assert.That(operation.Message, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(Services.GetRequiredService<BackupsAccess>().ExtensionsServed, Is.Null, "cold restore is a cluster operation, not an extension (#4218)");
        });
    }

    [Test]
    public void Reverting_a_point_in_time_restore_swaps_back_and_is_found_by_the_restore()
    {
        var result = FakeBackupControl.RestoreResult(new LatticeRestoreRequest("b1", "orders", mode: LatticeRestoreMode.ShadowCutover));

        var revert = Actions.Revert("op-restore", result);

        Assert.Multiple(() =>
        {
            Assert.That(revert.Status, Is.EqualTo(BackupOperationStatus.Succeeded));
            Assert.That(revert.Reverts, Is.EqualTo("op-restore"));
            Assert.That(Operations.RevertOf("op-restore"), Is.SameAs(revert));
            Assert.That(revert.Links.Single().Target, Is.EqualTo(BackupsAddresses.Operation("op-restore")));
            Assert.That(Backups.LastOf<LatticeRestoreResult>(nameof(ILatticeBackupControl.RevertRestoreAsync)), Is.SameAs(result));
        });
    }

    [Test]
    public void A_failed_revert_leaves_the_restore_revertible_and_only_a_point_in_time_restore_reverts()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope) with { CanRestore = false });
        var shadow = FakeBackupControl.RestoreResult(new LatticeRestoreRequest("b1", "orders", mode: LatticeRestoreMode.ShadowCutover));
        var inPlace = FakeBackupControl.RestoreResult(new LatticeRestoreRequest("b1", "orders", mode: LatticeRestoreMode.InPlace));

        var revert = Actions.Revert("op-restore", shadow);

        Assert.Multiple(() =>
        {
            Assert.That(revert.Status, Is.EqualTo(BackupOperationStatus.Failed));
            Assert.That(Operations.RevertOf("op-restore"), Is.Null);
            Assert.That(() => Actions.Revert("op-restore", inPlace), Throws.InvalidOperationException);
            Assert.That(() => Actions.Revert("op-restore", null!), Throws.ArgumentNullException);
            Assert.That(() => Actions.Revert(string.Empty, shadow), Throws.ArgumentException);
        });
    }
    [Test]
    public void Catalogue_rebuild_and_scrub_start_on_the_cluster_and_hand_off()
    {
        var rebuild = Actions.RebuildCatalogue();
        var check = Actions.ScrubCatalogue(pruneOrphans: false);
        var prune = Actions.ScrubCatalogue(pruneOrphans: true);

        Assert.Multiple(() =>
        {
            Assert.That(rebuild.ClusterOperationId, Is.EqualTo("op-1"));
            Assert.That(check.ClusterOperationId, Is.EqualTo("op-2"));
            Assert.That(prune.ClusterOperationId, Is.EqualTo("op-3"));
            Assert.That(Backups.Statuses["op-1"].Kind, Is.EqualTo(BackupOperationKinds.CatalogRebuild));
            Assert.That(Backups.Calls.Where(call => call.Verb == nameof(ILatticeBackupOperations.StartCatalogScrubAsync)).Select(call => call.Argument), Is.EqualTo(new object[] { false, true }));
            Assert.That(Backups.CountOf("RebuildCatalogFromSinkAsync") + Backups.CountOf("ScrubCatalogAgainstSinkAsync"), Is.Zero);
            Assert.That(Operations.Latest(BackupOperationKind.ScrubCatalogue), Is.SameAs(prune));
        });
    }

    [Test]
    public void Unserved_maintenance_fails_its_start_but_leaves_the_extensions_alone()
    {
        Backups.StartFault = new NotSupportedException();

        var rebuild = Actions.RebuildCatalogue();
        var scrub = Actions.ScrubCatalogue(true);

        Assert.Multiple(() =>
        {
            Assert.That(rebuild.Message, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(scrub.Message, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(Services.GetRequiredService<BackupsAccess>().ExtensionsServed, Is.Null, "the inventory's answer is its own");
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
            Assert.That(() => new BackupActions(null!, null!, null!, null!), Throws.ArgumentNullException);
        });
    }

    private BackupActions Actions => Services.GetRequiredService<BackupActions>();
}
