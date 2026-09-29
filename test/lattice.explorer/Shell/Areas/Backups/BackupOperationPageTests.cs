using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Shell.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Backups;

/// <summary>
/// A staged operation's status page (epic E15): it redraws as the operation
/// moves, can be left and resumed, reports a failure in one sentence, and
/// offers a point-in-time restore's revert behind a type-the-name confirmation
/// without ever showing a physical tree id.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupOperationPageTests : BackupsTestContext
{
    [Test]
    public void The_page_redraws_each_stage_as_the_operation_moves()
    {
        var capture = new TaskCompletionSource<LatticeBackupCaptureResult>();
        Backups.Capture = _ => capture.Task;
        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));

        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        cut.WaitUntil(() =>
        {
            var stages = cut.FindAll(".lt-backups-stages__stage");
            Assert.That(stages.Select(stage => stage.QuerySelector(".lt-backups-stages__state")!.TextContent), Is.EqualTo(new[] { "Done", "Under way", "Waiting" }));
            Assert.That(stages[1].GetAttribute("aria-current"), Is.EqualTo("step"));
            Assert.That(stages[1].QuerySelector(".lt-node--join"), Is.Not.Null, "the stage under way is the marker node");
            Assert.That(cut.Find("[role=status] .lt-pill").TextContent.Trim(), Is.EqualTo("Running"));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Capture a full backup of orders"));
        });

        capture.SetResult(new LatticeBackupCaptureResult("b9", FakeBackupControl.Manifest("b9", "nightly", "orders", artifacts: ["a1"])));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-backups-stages__state").Select(state => state.TextContent), Has.All.EqualTo("Done"));
            Assert.That(cut.FindAll("[aria-current]"), Is.Empty);
            Assert.That(cut.Find("[role=status]").TextContent, Does.Contain("Succeeded").And.Contain("Captured backup nightly."));
            Assert.That(cut.FindAll("a").Single(a => a.TextContent == "nightly (orders)").GetAttribute("href"), Is.EqualTo("backups/b9"));
            Assert.That(cut.FindAll(".lt-dl__term").Select(term => term.TextContent), Does.Contain("Backups captured"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("finished"));
        });
    }

    [Test]
    public void A_status_page_can_be_left_and_resumed()
    {
        var capture = new TaskCompletionSource<LatticeBackupCaptureResult>();
        Backups.Capture = _ => capture.Task;
        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));
        var first = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);
        first.Instance.Dispose();

        capture.SetResult(new LatticeBackupCaptureResult("b9", FakeBackupControl.Manifest("b9")));
        var resumed = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        resumed.WaitUntil(() => Assert.That(resumed.Find("[role=status] .lt-pill").TextContent.Trim(), Is.EqualTo("Succeeded")));
    }

    [Test]
    public void A_failed_operation_marks_the_stage_it_stopped_at_and_says_why()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));
        var operation = Actions.CaptureFull("nightly", BackupScopeSelector.WholeTree("orders"));

        var cut = RenderAt<BackupOperationPage>("backups/operations/" + operation.Id);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-backups-stages__state").Select(state => state.TextContent), Is.EqualTo(new[] { "Failed", "Waiting", "Waiting" }));
            Assert.That(cut.Find("[role=status]").TextContent, Does.Contain("Failed").And.Contain(BackupsFaults.NotPermitted));
        });
    }

    [Test]
    public void A_stopped_operation_says_the_session_ended()
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
    public void An_unknown_operation_is_not_found()
    {
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;

        RenderAt<BackupOperationPage>("backups/operations/99");

        Assert.That(notFound, Is.True);
    }

    [Test]
    public void A_point_in_time_restore_offers_its_revert_behind_the_tree_name_and_never_shows_a_physical_id()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "a/crm/orders"));
        var restore = Actions.Restore("b1", "a/crm/orders", LatticeRestoreMode.ShadowCutover, cold: false);
        var cut = RenderAt<BackupOperationPage>("backups/operations/" + restore.Id);
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Revert restore..."), Has.Exactly(1).Items));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Markup, Does.Not.Contain("physical"));
            Assert.That(cut.FindAll(".lt-dl__value").Select(value => value.TextContent), Does.Contain("orders"));
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

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/2")));
        Assert.Multiple(() =>
        {
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RevertRestoreAsync)), Is.EqualTo(1));
            Assert.That(restore.RevertedBy, Is.EqualTo("2"));
        });

        var reverted = RenderAt<BackupOperationPage>("backups/operations/" + restore.Id);
        reverted.WaitUntil(() =>
        {
            Assert.That(reverted.Markup, Does.Contain("This restore was reverted."));
            Assert.That(reverted.FindAll("button").Where(button => button.TextContent == "Revert restore..."), Is.Empty);
        });
    }

    [Test]
    public void A_scrub_lists_its_orphan_rows()
    {
        Backups.Scrub = _ => Task.FromResult(new BackupCatalogScrubReport(5, 1, 0, false, ["orphan-1"]));
        var scrub = Actions.ScrubCatalogue(pruneOrphans: false);

        var cut = RenderAt<BackupOperationPage>("backups/operations/" + scrub.Id);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-backups-list__item").TextContent, Is.EqualTo("orphan-1")));
    }

    private BackupActions Actions => Services.GetRequiredService<BackupActions>();
}
