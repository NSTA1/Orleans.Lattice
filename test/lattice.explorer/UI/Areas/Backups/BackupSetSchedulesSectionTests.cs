using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The Schedules page's backup-set section (#6067): the listing, the details,
/// registering named-tree and all-tree schedules, the retention rule, run now,
/// pause, resume and remove, restoring a retained set as a whole and reverting it,
/// and withdrawal when the connection does not serve backup-set schedules.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class BackupSetSchedulesSectionTests : BackupsTestContext
{
    private const string Page = "backups/schedules";

    private static readonly LatticeBackupSetScheduleRequest Nightly = LatticeBackupSetScheduleRequest.ForTrees(
        "pics-nightly",
        [BackupScopeSelector.WholeTree("photos"), BackupScopeSelector.WholeTree("albums")],
        TimeSpan.FromHours(24),
        7,
        TimeSpan.FromDays(30));

    private static readonly LatticeBackupSetScheduleRequest Everything = LatticeBackupSetScheduleRequest.ForAllTrees(
        "everything", TimeSpan.FromHours(6), includeNewTrees: true, retentionKeepLast: null, TimeSpan.FromDays(2), ["tmp/"]);

    [Test]
    public void The_listing_shows_each_schedules_trees_cadence_retention_state_and_last_run()
    {
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly) with
        {
            IsRunning = true,
            LastOutcome = BackupScopeRunOutcome.Failure,
            LastFailureReason = "sink offline",
            ConsecutiveFailures = 2,
        };
        Backups.SetSchedules[Everything.Name] = FakeBackupControl.SetStatus(Everything, scheduled: false);

        var cut = RenderAt<BackupSchedulesPage>(Page);

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("section[aria-labelledby=lt-backups-sets-title] tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].TextContent, Does.Contain("everything")
                .And.Contain("All trees, including trees created later, except under tmp/")
                .And.Contain("6 h")
                .And.Contain("Keep sets younger than 2 d")
                .And.Contain("Paused")
                .And.Contain("No run yet"));
            Assert.That(rows[1].TextContent, Does.Contain("pics-nightly")
                .And.Contain("2 named trees: photos, albums")
                .And.Contain("1 d")
                .And.Contain("Keep the last 7 sets, keep sets younger than 30 d")
                .And.Contain("Scheduled")
                .And.Contain("Running")
                .And.Contain("2026-10-01 02:00:00 UTC")
                .And.Contain("Failed: sink offline")
                .And.Contain("2"));
        });
    }

    [Test]
    public void Details_list_runs_newest_first_and_the_retained_sets_members_link_to_their_backups()
    {
        var older = new BackupSetScheduleRun(
            new DateTimeOffset(2026, 9, 1, 2, 0, 0, TimeSpan.Zero), new DateTimeOffset(2026, 9, 1, 2, 5, 0, TimeSpan.Zero),
            BackupSetScheduleTrigger.Schedule, BackupScopeRunOutcome.Failure, null, 2, 3, "sink offline");
        var newer = new BackupSetScheduleRun(
            new DateTimeOffset(2026, 9, 2, 2, 0, 0, TimeSpan.Zero), new DateTimeOffset(2026, 9, 2, 2, 5, 0, TimeSpan.Zero),
            BackupSetScheduleTrigger.Manual, BackupScopeRunOutcome.Success, "set-42", 2, 1, null);
        var set = new ScheduledBackupSet(
            "set-42", "pics-nightly", newer.StartedAtUtc, newer.FinishedAtUtc, true,
            [new ScheduledBackupSetMember(BackupScopeSelector.WholeTree("photos"), "backup-photos-1")]);
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly, runs: [older, newer], sets: [set]);

        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.FindAll("button[aria-label='Details, pics-nightly']"), Has.Count.EqualTo(1)));
        cut.Find("button[aria-label='Details, pics-nightly']").Click();

        var detail = cut.Find("[data-lt-backup-set='pics-nightly']");
        var runs = detail.QuerySelectorAll("tbody tr");
        Assert.Multiple(() =>
        {
            Assert.That(detail.TextContent, Does.Contain("Cross-tree consistent").And.Contain("Yes"));
            Assert.That(runs[0].TextContent, Does.Contain("2026-09-02 02:00:00 UTC").And.Contain("Run now").And.Contain("Succeeded").And.Contain("set-42"));
            Assert.That(runs[1].TextContent, Does.Contain("2026-09-01 02:00:00 UTC").And.Contain("Schedule").And.Contain("Failed").And.Contain("sink offline"));
            Assert.That(detail.QuerySelector("a.lt-backups-link")!.GetAttribute("href"), Does.EndWith("backups/backup-photos-1"));
            Assert.That(detail.TextContent, Does.Contain("set-42").And.Contain("photos"));
        });
    }

    [Test]
    public void Saving_named_trees_sends_an_explicit_request()
    {
        var cut = OpenForm();

        Input(cut, "Name", "pics-nightly");
        Input(cut, "Tree to add", "photos");
        Button(cut, "Add tree").Click();
        Input(cut, "Tree to add", "albums");
        Button(cut, "Add tree").Click();
        cut.Find("[data-lt-backup-set-form] input[aria-label='Keep sets younger than, days']").Input("30");
        cut.Find("[data-lt-backup-set-form]").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupSetAsync)), Is.EqualTo(1)));
        var request = Backups.LastOf<LatticeBackupSetScheduleRequest>(nameof(ILatticeBackupControl.ScheduleBackupSetAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Name, Is.EqualTo("pics-nightly"));
            Assert.That(request.Membership, Is.EqualTo(BackupSetMembershipMode.Explicit));
            Assert.That(request.Scopes.Select(scope => scope.TreeId), Is.EqualTo(new[] { "photos", "albums" }));
            Assert.That(request.Interval, Is.EqualTo(TimeSpan.FromHours(24)));
            Assert.That(request.RetentionKeepLast, Is.EqualTo(7));
            Assert.That(request.RetentionMaxAge, Is.EqualTo(TimeSpan.FromDays(30)));
            Assert.That(request.CrossTreeConsistent, Is.True);
        });
        cut.WaitUntil(() =>
        {
            Assert.That(Status(cut), Does.Contain("Saved backup-set schedule pics-nightly."));
            Assert.That(cut.FindAll("[data-lt-backup-set-form]"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-backup-set='pics-nightly']"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Saving_all_trees_sends_the_new_trees_choice_the_exclusions_and_the_fence_choice()
    {
        var cut = OpenForm();

        Input(cut, "Name", "everything");
        cut.Find("[data-lt-backup-set-form] select").Change("all");
        Check(cut, "Include trees created later", false);
        Input(cut, "Leave out trees starting with", "tmp/, scratch/ ,tmp/");
        Input(cut, "Keep the last", "3");
        Check(cut, "Capture every tree at one consistency fence", false);
        cut.Find("[data-lt-backup-set-form]").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupSetAsync)), Is.EqualTo(1)));
        var request = Backups.LastOf<LatticeBackupSetScheduleRequest>(nameof(ILatticeBackupControl.ScheduleBackupSetAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Membership, Is.EqualTo(BackupSetMembershipMode.AllTrees));
            Assert.That(request.Scopes, Is.Empty);
            Assert.That(request.IncludeNewTrees, Is.False);
            Assert.That(request.ExcludedTreePrefixes, Is.EqualTo(new[] { "tmp/", "scratch/" }));
            Assert.That(request.RetentionKeepLast, Is.EqualTo(3));
            Assert.That(request.RetentionMaxAge, Is.Null);
            Assert.That(request.CrossTreeConsistent, Is.False);
        });
    }

    [Test]
    public void A_schedule_with_no_retention_bound_is_refused_before_any_call()
    {
        var cut = OpenForm();

        Input(cut, "Name", "everything");
        cut.Find("[data-lt-backup-set-form] select").Change("all");
        Input(cut, "Keep the last", string.Empty);
        cut.Find("[data-lt-backup-set-form]").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-backup-set-form]").TextContent, Does.Contain("Set how many sets to keep, how old a set may grow, or both."));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupSetAsync)), Is.Zero);
        });
    }

    [Test]
    public void The_form_checks_the_name_the_trees_and_the_count()
    {
        var errors = Describe(name: "bad name!", trees: [], keepLast: "0");

        Assert.Multiple(() =>
        {
            Assert.That(errors.Name, Does.Contain("letters, digits"));
            Assert.That(errors.Trees, Is.EqualTo("Add at least one tree."));
            Assert.That(errors.KeepLast, Is.EqualTo("Give a whole number of sets, at least 1."));
            Assert.That(errors.Retention, Is.Null, "a bad count is reported as itself, not as a missing bound");
            Assert.That(Describe(name: " ", trees: ["photos"], keepLast: "2").Name, Is.EqualTo("Give the schedule a name."));
            Assert.That(Describe(name: "ok", trees: ["photos"], keepLast: "2").Any, Is.False);
        });
    }

    [Test]
    public void Run_now_reports_a_started_run_and_a_run_already_in_flight()
    {
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly);
        var cut = OpenDetails();

        Button(cut, "Run now").Click();
        cut.WaitUntil(() => Assert.That(Status(cut), Is.EqualTo("Started a run of pics-nightly.")));

        Backups.RunSetNow = _ => Task.FromResult(false);
        Button(cut, "Run now").Click();
        cut.WaitUntil(() => Assert.That(Status(cut), Is.EqualTo("A run of pics-nightly is already in progress.")));
        Assert.That(Backups.LastOf<string>(nameof(ILatticeBackupControl.RunBackupSetScheduleNowAsync)), Is.EqualTo("pics-nightly"));
    }

    [Test]
    public void Pause_asks_first_then_pauses_and_resume_registers_the_same_request_again()
    {
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly);
        var cut = OpenDetails();

        Button(cut, "Pause").Click();
        Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("No further run of pics-nightly starts until it is resumed."));
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Pause schedule").Click();

        cut.WaitUntil(() => Assert.That(Status(cut), Is.EqualTo("Paused pics-nightly. Its retained sets are kept.")));
        Assert.That(Backups.LastOf<string>(nameof(ILatticeBackupControl.CancelBackupSetScheduleAsync)), Is.EqualTo("pics-nightly"));

        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Resume"), Has.Exactly(1).Items));
        Button(cut, "Resume").Click();

        cut.WaitUntil(() => Assert.That(Status(cut), Does.StartWith("Resumed pics-nightly.")));
        Assert.That(Backups.LastOf<LatticeBackupSetScheduleRequest>(nameof(ILatticeBackupControl.ScheduleBackupSetAsync)), Is.EqualTo(Nightly));
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public void Remove_asks_for_the_name_and_sends_the_delete_sets_choice(bool deleteSets)
    {
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly);
        var cut = OpenDetails();

        Button(cut, "Remove...").Click();
        var dialog = cut.Find("[role=alertdialog]");
        Assert.That(dialog.TextContent, Does.Contain("Also delete the retained sets' backups"));
        if (deleteSets)
        {
            cut.Find("[role=alertdialog] input[type=checkbox]").Change(true);
        }

        cut.Find("[role=alertdialog] input[type=text]").Input("pics-nightly");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RemoveBackupSetScheduleAsync)), Is.EqualTo(1)));
        Assert.That(
            Backups.LastOf<(string, bool)>(nameof(ILatticeBackupControl.RemoveBackupSetScheduleAsync)),
            Is.EqualTo(("pics-nightly", deleteSets)));
        cut.WaitUntil(() =>
        {
            Assert.That(Status(cut), Does.StartWith("Removed pics-nightly"));
            Assert.That(cut.FindAll("[data-lt-backup-set]"), Is.Empty);
            Assert.That(cut.Find("[data-lt-backup-sets=empty]").TextContent, Is.EqualTo("No backup-set schedule is registered."));
        });
    }

    [Test]
    public void A_refused_action_says_why()
    {
        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly);
        Backups.RunSetNow = _ => Task.FromException<bool>(new LatticeAuthorizationDeniedException());
        var cut = OpenDetails();

        Button(cut, "Run now").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("section[aria-labelledby=lt-backups-sets-title] [role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void A_connection_that_does_not_serve_backup_set_schedules_hides_the_section_and_remembers_it()
    {
        Backups.ListSetSchedules = () => Task.FromException<IReadOnlyList<BackupSetScheduleStatus>>(new NotSupportedException("not served"));

        var cut = RenderAt<BackupSchedulesPage>(Page);

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(1)));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Not.Contain("Backup-set schedules")));
        Assert.That(Services.GetRequiredService<BackupsAccess>().BackupSetSchedulesServed, Is.False);

        RenderAt<BackupSchedulesPage>(Page);
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(1), "a known answer is not asked again");
    }

    [Test]
    public void A_listing_that_is_refused_says_so()
    {
        Backups.ListSetSchedules = () => Task.FromException<IReadOnlyList<BackupSetScheduleStatus>>(new LatticeAuthorizationDeniedException());

        var cut = RenderAt<BackupSchedulesPage>(Page);

        cut.WaitUntil(() => Assert.That(
            cut.Find("section[aria-labelledby=lt-backups-sets-title] [role=alert]").TextContent,
            Is.EqualTo("You may not read the backup-set schedules.")));
    }

    [Test]
    public void The_text_forms_describe_membership_retention_and_triggers()
    {
        var many = LatticeBackupSetScheduleRequest.ForTrees(
            "many",
            [.. new[] { "a", "b", "c", "d", "e" }.Select(BackupScopeSelector.WholeTree)],
            TimeSpan.FromHours(1),
            1,
            null);
        var pinned = LatticeBackupSetScheduleRequest.ForAllTrees("pinned", TimeSpan.FromHours(1), includeNewTrees: false, 2, null);

        Assert.Multiple(() =>
        {
            Assert.That(BackupSetSchedulesText.Membership(FakeBackupControl.SetStatus(many)), Is.EqualTo("5 named trees: a, b, c and 2 more"));
            Assert.That(BackupSetSchedulesText.Membership(FakeBackupControl.SetStatus(pinned)), Is.EqualTo("All trees, pinned to 1 tree"));
            Assert.That(BackupSetSchedulesText.Retention(many), Is.EqualTo("Keep the last 1 set"));
            Assert.That(BackupSetSchedulesText.NextRun(FakeBackupControl.SetStatus(many, scheduled: false)), Is.EqualTo("Paused"));
            Assert.That(BackupSetSchedulesText.Trigger(BackupSetScheduleTrigger.Manual), Is.EqualTo("Run now"));
            Assert.That(BackupSetSchedulesText.Prefixes(" a/ , ,b/,a/"), Is.EqualTo(new[] { "a/", "b/" }));
            Assert.That(BackupSetSchedulesText.Prefixes(null), Is.Empty);
        });
    }

    private static BackupSetSchedulesSection.FormErrors Describe(string? name, IReadOnlyList<string> trees, string? keepLast)
    {
        BackupSetSchedulesSection.Describe(
            name, "named", trees, true, null, TimeSpan.FromHours(1), keepLast, null, true, out var errors);
        return errors;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<BackupSchedulesPage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent == text);

    private static string Status(IRenderedComponent<BackupSchedulesPage> cut) =>
        cut.Find("section[aria-labelledby=lt-backups-sets-title] > p[role=status]").TextContent;

    private static void Input(IRenderedComponent<BackupSchedulesPage> cut, string label, string value)
    {
        var id = cut.FindAll("[data-lt-backup-set-form] label").Single(candidate => candidate.TextContent == label).GetAttribute("for");
        cut.Find("#" + id).Input(value);
    }

    private static void Check(IRenderedComponent<BackupSchedulesPage> cut, string label, bool value)
    {
        var id = cut.FindAll("[data-lt-backup-set-form] label").Single(candidate => candidate.TextContent == label).GetAttribute("for");
        cut.Find("#" + id).Change(value);
    }

    private IRenderedComponent<BackupSchedulesPage> OpenForm()
    {
        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "New backup-set schedule"), Has.Exactly(1).Items));
        Button(cut, "New backup-set schedule").Click();
        return cut;
    }

    private IRenderedComponent<BackupSchedulesPage> OpenDetails()
    {
        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.FindAll("button[aria-label='Details, pics-nightly']"), Has.Count.EqualTo(1)));
        cut.Find("button[aria-label='Details, pics-nightly']").Click();
        return cut;
    }
}
