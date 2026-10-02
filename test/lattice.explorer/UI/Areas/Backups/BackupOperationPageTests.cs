using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The backup operation status page. A session-staged operation (epic E15) redraws
/// as it moves and reports a failure in one sentence; a capture or restore hands
/// the page to the cluster's tracked operation (#4122), whose phase and real units
/// are polled on the circuit's manual clock until it is terminal, which can be
/// cancelled, which survives a fresh page, and whose finished point-in-time restore
/// offers its revert behind a type-the-name confirmation without ever showing a
/// physical tree id. No test depends on wall time.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupOperationPageTests : BackupsTestContext
{
    [Test]
    public void A_staged_operation_redraws_each_stage_as_it_moves()
    {
        var revert = new TaskCompletionSource();
        Backups.Revert = _ => revert.Task;
        var restore = FakeBackupControl.RestoreResult(new LatticeRestoreRequest("b1", "orders", mode: LatticeRestoreMode.ShadowCutover));
        var operation = Actions.Revert("op-r", restore);

        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        cut.WaitUntil(() =>
        {
            var stages = cut.FindAll(".lt-backups-stages__stage");
            Assert.That(stages.Select(stage => stage.QuerySelector(".lt-backups-stages__state")!.TextContent), Is.EqualTo(new[] { "Done", "Under way", "Waiting" }));
            Assert.That(stages[1].GetAttribute("aria-current"), Is.EqualTo("step"));
            Assert.That(cut.Find("[role=status] .lt-pill").TextContent.Trim(), Is.EqualTo("Running"));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Revert the restore of orders"));
        });

        revert.SetResult();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-backups-stages__state").Select(state => state.TextContent), Has.All.EqualTo("Done"));
            Assert.That(cut.Find("[role=status]").TextContent, Does.Contain("Succeeded"));
            Assert.That(cut.FindAll("a").Select(a => a.TextContent), Does.Contain("The reverted restore"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("finished"));
        });
    }

    [Test]
    public void A_started_rebuild_hands_the_page_to_the_cluster_operation_with_its_counts()
    {
        var operation = Actions.RebuildCatalogue();
        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/op-1")));
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RebuildCatalogFromSinkAsync)), Is.Zero, "the blocking verb is not called");

        Backups.Succeed("op-1", null, FakeBackupControl.RebuildResult(10, 3, 7));
        var followed = RenderAt<BackupOperationPage>("backups/operations/op-1");
        followed.WaitUntil(() =>
        {
            Assert.That(followed.Find("h1").TextContent, Is.EqualTo("Rebuild the catalogue from the backup store"));
            Assert.That(followed.FindAll(".lt-dl__term").Select(term => term.TextContent), Is.EqualTo(new[] { "Manifests scanned", "Added to the catalogue", "Reconciled in place" }));
        });
    }

    [Test]
    public void A_failed_start_marks_the_stage_it_stopped_at_and_says_why()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));
        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));

        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-backups-stages__state").Select(state => state.TextContent), Is.EqualTo(new[] { "Failed", "Waiting" }));
            Assert.That(cut.Find("[role=status]").TextContent, Does.Contain("Failed").And.Contain(BackupsFaults.NotPermitted));
        });
    }

    [Test]
    public void A_started_capture_hands_the_page_to_the_cluster_operation()
    {
        var start = new TaskCompletionSource();
        Backups.StartGate = start.Task;
        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));
        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-backups-stages__state").Select(state => state.TextContent), Is.EqualTo(new[] { "Done", "Under way" })));

        start.SetResult();

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/op-1")));

        // bUnit has no router, so render the address the page moved to, as the router would.
        var followed = RenderAt<BackupOperationPage>("backups/operations/op-1");
        followed.WaitUntil(() =>
        {
            Assert.That(followed.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Queued"));
            Assert.That(followed.Find("h1").TextContent, Is.EqualTo("Capture a full backup of orders"));
        });

        // A staged start that was handed off sends its own address on to the cluster's.
        Navigation.NavigateTo("backups");
        RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);
        Assert.That(CurrentPath, Is.EqualTo("/backups/operations/op-1"));
    }

    [Test]
    public void A_cluster_operation_shows_each_phase_with_real_units_until_it_succeeds()
    {
        Backups.Statuses["op-7"] = Phase(FakeBackupControl.Running("op-7", BackupOperationKinds.Capture, "a/crm/orders"), BackupOperationPhases.Capturing, 0, 3, 10, BackupOperationUnits.Entries);

        var cut = RenderAt<BackupOperationPage>("backups/operations/op-7");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Capture a full backup of orders"));
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-valuenow"), Is.EqualTo("30"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("3 of 10 entries"));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Capturing"));
            Assert.That(cut.Find(".lt-operation-progress__step").TextContent, Is.EqualTo("Step 1 of 2"));
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Running"));
        });

        Backups.Move("op-7", status => Phase(status, BackupOperationPhases.Cataloguing, 1, 0, null, null));
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Cataloguing"));
            Assert.That(cut.Find(".lt-operation-progress__step").TextContent, Is.EqualTo("Step 2 of 2"));
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.EqualTo("indeterminate"));
        });

        Backups.SucceedCapture("op-7", "b9");
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Succeeded"));
            Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty, "a finished operation has no bar");
            Assert.That(cut.FindAll("a").Single(a => a.TextContent == "The captured backup").GetAttribute("href"), Is.EqualTo("backups/b9"));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Cancel operation"), Is.Empty);
        });

        var reads = Backups.StatusReads;
        Time.Advance(ClusterStatusPoller.Interval * 3);
        Assert.That(Backups.StatusReads, Is.EqualTo(reads), "a terminal operation is no longer polled");
    }

    [Test]
    public void An_unknown_total_draws_a_hatched_bar_with_the_count_alone()
    {
        Backups.Statuses["op-7"] = Phase(FakeBackupControl.Running("op-7", BackupOperationKinds.IncrementalCapture, "orders"), BackupOperationPhases.Capturing, 0, 1200, null, BackupOperationUnits.Entries);

        var cut = RenderAt<BackupOperationPage>("backups/operations/op-7");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.EqualTo("indeterminate"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("1,200 entries so far"));
            Assert.That(cut.Markup, Does.Not.Contain("%"), "no invented percentage");
        });
    }

    [TestCase(LatticeOperationState.Failed, "Failed")]
    [TestCase(LatticeOperationState.Cancelled, "Cancelled")]
    public void A_stopped_cluster_operation_says_where_it_stopped_and_why(LatticeOperationState state, string pill)
    {
        Backups.Statuses["op-7"] = Phase(FakeBackupControl.Running("op-7", BackupOperationKinds.Restore, "orders"), BackupOperationPhases.Applying, 1, 3, 9, BackupOperationUnits.Entries) with
        {
            State = state,
            FailureReason = "The silo running this operation was lost.",
            FinishedAtUtc = new DateTimeOffset(2026, 10, 1, 9, 5, 0, TimeSpan.Zero),
        };

        var cut = RenderAt<BackupOperationPage>("backups/operations/op-7");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo(pill));
            Assert.That(cut.Find(".lt-operation-progress__stopped").TextContent, Is.EqualTo("Stopped during Applying, after 3 of 9 entries."));
            Assert.That(cut.Find(".lt-operation-progress__reason").TextContent, Is.EqualTo("The silo running this operation was lost."));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("finished"));
        });
    }

    [Test]
    public void A_running_cluster_operation_can_be_cancelled()
    {
        Backups.Statuses["op-7"] = Phase(FakeBackupControl.Running("op-7", BackupOperationKinds.Capture, "orders"), BackupOperationPhases.Capturing, 0, 1, 10, BackupOperationUnits.Entries);
        var cut = RenderAt<BackupOperationPage>("backups/operations/op-7");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Cancel operation"), Has.Exactly(1).Items));

        cut.FindAll("button").Single(button => button.TextContent == "Cancel operation").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.CancelOperationAsync)), Is.EqualTo(1));
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Cancelling"));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Cancel operation"), Is.Empty);
        });

        Backups.Move("op-7", status => status with { State = LatticeOperationState.Cancelled, FailureReason = "Cancellation was requested." });
        Tick();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Cancelled")));
    }

    [Test]
    public void A_cluster_operation_survives_a_fresh_page_because_its_status_is_the_clusters()
    {
        Backups.Statuses["op-7"] = Phase(FakeBackupControl.Running("op-7", BackupOperationKinds.Capture, "orders"), BackupOperationPhases.Capturing, 0, 4, 10, BackupOperationUnits.Entries);
        var first = RenderAt<BackupOperationPage>("backups/operations/op-7");
        first.WaitUntil(() => Assert.That(first.Find(".lt-progress__detail").TextContent, Is.EqualTo("4 of 10 entries")));
        first.Instance.Dispose();

        Backups.Move("op-7", status => status with { CompletedUnits = 9 });
        var resumed = RenderAt<BackupOperationPage>("backups/operations/op-7");

        resumed.WaitUntil(() => Assert.That(resumed.Find(".lt-progress__detail").TextContent, Is.EqualTo("9 of 10 entries")));
        Assert.That(Operations.Recent, Is.Empty, "nothing about it was kept in the session");
    }

    [Test]
    public void A_status_read_that_fails_says_why_and_can_be_retried()
    {
        Backups.Statuses["op-7"] = Phase(FakeBackupControl.Running("op-7", BackupOperationKinds.Capture, "orders"), BackupOperationPhases.Capturing, 0, 1, 2, BackupOperationUnits.Entries);
        Backups.StatusFault = new TimeoutException("slow");
        var cut = RenderAt<BackupOperationPage>("backups/operations/op-7");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("The operation could not be read")));

        Backups.StatusFault = null;
        cut.FindAll("button").Single(button => button.TextContent == "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("1 of 2 entries")));
    }

    [Test]
    public void An_unknown_operation_is_not_found()
    {
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;

        RenderAt<BackupOperationPage>("backups/operations/op-404");

        Assert.That(notFound, Is.True);
    }

    [Test]
    public void A_point_in_time_restore_offers_its_revert_behind_the_tree_name_and_never_shows_a_physical_id()
    {
        Backups.Statuses["op-r"] = FakeBackupControl.Running("op-r", BackupOperationKinds.Restore, "a/crm/orders");
        Backups.SucceedRestore("op-r", FakeBackupControl.RestoreResult(new LatticeRestoreRequest("b1", "a/crm/orders", mode: LatticeRestoreMode.ShadowCutover)));
        var cut = RenderAt<BackupOperationPage>("backups/operations/op-r");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Revert restore..."), Has.Exactly(1).Items));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Markup, Does.Not.Contain("physical"));
            Assert.That(cut.FindAll(".lt-dl__value").Select(value => value.TextContent), Does.Contain("orders").And.Contain("42"));
            Assert.That(cut.FindAll("a").Single(a => a.TextContent == "The restored backup").GetAttribute("href"), Is.EqualTo("backups/b1"));
        });

        cut.FindAll("button").Single(button => button.TextContent == "Revert restore...").Click();
        var dialog = cut.Find("[role=alertdialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector("code")!.TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(dialog.TextContent, Does.Contain("Every write made to it since the restore is dropped"));
            Assert.That(cut.Markup, Does.Not.Contain("physical"));
        });

        cut.Find("[role=alertdialog] input").Input("a/crm/orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/1")));
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RevertRestoreAsync)), Is.EqualTo(1));

        var reverted = RenderAt<BackupOperationPage>("backups/operations/op-r");
        reverted.WaitUntil(() =>
        {
            Assert.That(reverted.Markup, Does.Contain("This restore was reverted."));
            Assert.That(reverted.FindAll("button").Where(button => button.TextContent == "Revert restore..."), Is.Empty);
        });
    }

    [Test]
    public void An_in_place_restore_offers_no_revert()
    {
        Backups.Statuses["op-r"] = FakeBackupControl.Running("op-r", BackupOperationKinds.Restore, "orders");
        Backups.SucceedRestore("op-r", FakeBackupControl.RestoreResult(new LatticeRestoreRequest("b1", "orders", mode: LatticeRestoreMode.InPlace)));

        var cut = RenderAt<BackupOperationPage>("backups/operations/op-r");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Succeeded"));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Revert restore..."), Is.Empty);
        });
    }

    [Test]
    public void A_stopped_staged_operation_says_the_session_ended()
    {
        var operations = Operations;
        var operation = operations.Start(BackupOperationKind.RebuildCatalogue, "Rebuild", ["one", "two"], (_, token) => Task.Delay(Timeout.Infinite, token));
        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        operations.Dispose();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=status] .lt-pill").TextContent.Trim(), Is.EqualTo("Stopped"));
            Assert.That(cut.FindAll(".lt-backups-stages__state").Select(state => state.TextContent), Is.EqualTo(new[] { "Stopped", "Waiting" }));
        });
    }

    [Test]
    public void A_scrub_lists_its_orphan_rows()
    {
        Backups.Statuses["op-5"] = FakeBackupControl.Running("op-5", BackupOperationKinds.CatalogScrub, "sys-backup-catalog");
        Backups.Succeed("op-5", null, FakeBackupControl.ScrubResult(5, false, "orphan-1"));

        var cut = RenderAt<BackupOperationPage>("backups/operations/op-5");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-backups-list__item").TextContent, Is.EqualTo("orphan-1"));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Check the catalogue against the backup store"));
        });
    }

    [Test]
    public void A_running_maintenance_operation_names_its_phase_in_the_areas_words()
    {
        Backups.Statuses["op-6"] = Phase(FakeBackupControl.Running("op-6", BackupOperationKinds.CatalogScrub, "sys-backup-catalog"), BackupOperationPhases.PruningOrphans, 1, 1, 2, BackupOperationUnits.Manifests);

        var cut = RenderAt<BackupOperationPage>("backups/operations/op-6");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Removing orphan rows"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("1 of 2 manifests"));
        });
    }

    private BackupActions Actions => Services.GetRequiredService<BackupActions>();

    private static LatticeOperationStatus Phase(
        LatticeOperationStatus status, string phase, int index, long completed, long? total, string? unit) =>
        status with
        {
            State = LatticeOperationState.Running,
            Phase = phase,
            PhaseIndex = index,
            PhaseCount = 2,
            CompletedUnits = completed,
            TotalUnits = total,
            UnitName = unit,
        };

    /// <summary>Moves the circuit's clock one polling interval, once the follower has armed its wait.</summary>
    private void Tick()
    {
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        Time.Advance(ClusterStatusPoller.Interval);
    }
}
