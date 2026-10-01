using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// A tree's schedules at <c>/backups/schedules</c>: status, schedule, change and
/// cancel, each gated by the capability probe.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupSchedulesPageTests : BackupsTestContext
{
    [Test]
    public void Without_a_tree_the_page_asks_for_one_and_showing_one_navigates()
    {
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules");

        Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("Choose a tree"));
        cut.Find("form.lt-toolbar").Submit();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name a tree."));

        cut.Find("form.lt-toolbar input").Input("a/crm/orders");
        cut.Find("form.lt-toolbar").Submit();
        Assert.That(CurrentPath, Is.EqualTo("/backups/schedules?tree=a%2Fcrm%2Forders").Or.EqualTo("/backups/schedules?tree=a/crm/orders"));
    }

    [Test]
    public void A_trees_schedules_show_their_status()
    {
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(
            BackupScopeSelector.WholeTree("orders"), true, false,
            new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), null, null,
            BackupScopeRunOutcome.Failure, 2, TimeSpan.FromHours(12));

        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].TextContent, Does.Contain("Full").And.Contain("Registered").And.Contain("12 h").And.Contain("2026-09-01 00:00:00 UTC"));
            Assert.That(rows[1].TextContent, Does.Contain("Incremental").And.Contain("None").And.Contain("Never"));
            Assert.That(cut.Markup, Does.Contain("Last scheduled run: Failed. Chain depth: 2."));
            Assert.That(cut.Find(".lt-backups-nav__link[aria-current]").TextContent, Is.EqualTo("Schedules"));
        });
    }

    [Test]
    public void Saving_a_schedule_registers_it_at_the_typed_interval()
    {
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-backups-form"), Has.Count.EqualTo(1)));

        cut.Find("form.lt-backups-form select").Change("incremental");
        var inputs = cut.FindAll("form.lt-backups-form input");
        // #4148: the interval is one duration field with a box per unit.
        Assert.That(inputs.Select(input => input.GetAttribute("aria-label")), Is.EqualTo(new[] { "Every, hours", "Every, minutes" }));
        Assert.That(inputs.Select(input => input.ClassList.Contains("lt-duration__input")), Is.All.True);
        inputs[0].Input("1");
        cut.FindAll("form.lt-backups-form input")[1].Input("30");
        cut.Find("form.lt-backups-form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("form.lt-backups-form [role=status]").TextContent, Is.EqualTo("Scheduled an incremental backup every 1 h 30 min.")));
        var request = Backups.LastOf<LatticeBackupScheduleRequest>(nameof(ILatticeBackupControl.ScheduleBackupAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Incremental, Is.True);
            Assert.That(request.Interval, Is.EqualTo(TimeSpan.FromMinutes(90)));
            Assert.That(request.Scope.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void A_bad_interval_is_refused_before_any_call()
    {
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-backups-form"), Has.Count.EqualTo(1)));

        cut.FindAll("form.lt-backups-form input")[0].Input("0");
        cut.FindAll("form.lt-backups-form input")[1].Input("x");
        cut.Find("form.lt-backups-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("whole number of hours and minutes"));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupAsync)), Is.Zero);
        });
    }

    [Test]
    public void A_refused_schedule_says_why()
    {
        Backups.Schedule = _ => Task.FromException(new LatticeAuthorizationDeniedException());
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-backups-form"), Has.Count.EqualTo(1)));

        cut.Find("form.lt-backups-form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("form.lt-backups-form [role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void Cancelling_a_schedule_asks_first_and_then_cancels_that_kind()
    {
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(BackupScopeSelector.WholeTree("orders"), true, true, null, null, null, null, BackupScopeRunOutcome.None, 0);
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody button"), Has.Count.EqualTo(2)));

        cut.Find("tbody button[aria-label='Cancel the incremental schedule']").Click();
        var dialog = cut.Find("[role=alertdialog]");
        Assert.That(dialog.TextContent, Does.Contain("The incremental schedule of orders stops."));
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Cancel schedule").Click();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CancelScheduleAsync)), Is.EqualTo(1)));
        var (scope, incremental) = Backups.LastOf<(BackupScopeSelector, bool)>(nameof(ILatticeBackupControl.CancelScheduleAsync));
        Assert.Multiple(() =>
        {
            Assert.That(scope.TreeId, Is.EqualTo("orders"));
            Assert.That(incremental, Is.True);
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
        });
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Cancelled the incremental schedule.")));
    }

    [Test]
    public void Keeping_a_schedule_closes_the_question_without_cancelling()
    {
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(BackupScopeSelector.WholeTree("orders"), true, false, null, null, null, null, BackupScopeRunOutcome.None, 0);
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody button"), Has.Count.EqualTo(1)));

        cut.Find("tbody button").Click();
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Keep it").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CancelScheduleAsync)), Is.Zero);
        });
    }

    [Test]
    public void A_restricted_identity_may_neither_read_nor_change_the_schedules()
    {
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));

        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("You may not read this tree's backup status."));
            Assert.That(cut.Markup, Does.Contain("You may not schedule backups of this tree."));
            Assert.That(cut.FindAll("form.lt-backups-form"), Is.Empty);
        });
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetScopeStatusAsync)), Is.Zero);
    }

    [Test]
    public void A_status_that_cannot_be_read_says_why()
    {
        Backups.ScopeStatus = _ => Task.FromException<BackupScopeStatus?>(new InvalidOperationException("busy"));

        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Is.EqualTo("The operation could not be completed: busy")));
    }

    [Test]
    public void The_schedules_table_is_compact_below_the_small_breakpoint()
    {
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(BackupScopeSelector.WholeTree("orders"), true, false, null, null, null, null, BackupScopeRunOutcome.None, 0, TimeSpan.FromHours(1));

        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(2)));
        Assert.That(cut.FindAll(".lt-compact-row__secondary")[0].TextContent, Does.Contain("Every 1 h"));
        cut.FindAll(".lt-table-list__row button")[0].Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain("Cancel schedule")));
    }
}
