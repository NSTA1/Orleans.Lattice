using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// A running backup-set schedule's live progress, and the section re-reading the
/// schedules while any run is in flight and only then.
/// </summary>
public sealed partial class BackupSetSchedulesSectionTests
{
    [Test]
    public void A_run_with_a_known_total_draws_a_determinate_bar_with_its_stage_count_tree_attempt_trigger_and_elapsed_time()
    {
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 1, 3, "t/acme/albums", attempt: 2, elapsed: TimeSpan.FromSeconds(65), trigger: BackupSetScheduleTrigger.Manual));

        var cut = OpenDetails();

        Assert.Multiple(() =>
        {
            var row = cut.Find("section[aria-labelledby=lt-backups-sets-title] tbody tr");
            Assert.That(row.QuerySelector("[data-lt-set-run-progress='pics-nightly']")!.TextContent, Does.Contain("Writing trees")
                .And.Contain("1 of 3 trees, now t/acme/albums, attempt 2, started by Run now, running for 1 m 05 s"));
            var detail = cut.Find("[data-lt-backup-set='pics-nightly'] [data-lt-set-run-progress='pics-nightly']");
            Assert.That(detail.TextContent, Does.Contain("Writing trees").And.Contain("1 of 3 trees"));
            Assert.That(detail.QuerySelector("[data-lt-progress]")!.GetAttribute("data-lt-progress"), Is.EqualTo("determinate"));
            Assert.That(detail.QuerySelector("[role=progressbar]")!.GetAttribute("aria-valuenow"), Is.EqualTo("33"));
            Assert.That(detail.QuerySelector(".lt-progress__fill")!.GetAttribute("style"), Is.EqualTo("inline-size: 33%"));
        });
    }

    [Test]
    public void A_run_with_no_known_total_draws_an_indeterminate_bar_with_no_count_and_no_first_attempt()
    {
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.ResolvingMembers, 0, null, null));

        var cut = RenderAt<BackupSchedulesPage>(Page);

        cut.WaitUntil(() =>
        {
            var progress = cut.Find("[data-lt-set-run-progress='pics-nightly']");
            Assert.That(progress.TextContent, Does.Contain("Resolving trees").And.Contain("scheduled run, running for 30 s").And.Not.Contain(" of ").And.Not.Contain("attempt"));
            Assert.That(progress.QuerySelector("[data-lt-progress]")!.GetAttribute("data-lt-progress"), Is.EqualTo("indeterminate"));
            Assert.That(progress.QuerySelector("[role=progressbar]")!.HasAttribute("aria-valuenow"), Is.False);
            Assert.That(progress.QuerySelectorAll(".lt-progress__fill"), Is.Empty);
        });
    }

    [Test]
    public void The_compact_list_row_carries_the_progress_bar()
    {
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.CapturingMembers, 0, 2, null));

        var cut = RenderAt<BackupSchedulesPage>(Page, LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-set-run-progress='pics-nightly']").TextContent,
            Does.Contain("Capturing (pinning) trees").And.Contain("0 of 2 trees")));
    }

    [Test]
    public void The_phases_and_elapsed_times_read_in_plain_words()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupSetSchedulesText.Phase(BackupOperationPhases.ResolvingMembers), Is.EqualTo("Resolving trees"));
            Assert.That(BackupSetSchedulesText.Phase(BackupOperationPhases.CapturingMembers), Is.EqualTo("Capturing (pinning) trees"));
            Assert.That(BackupSetSchedulesText.Phase(BackupOperationPhases.WritingMembers), Is.EqualTo("Writing trees"));
            Assert.That(BackupSetSchedulesText.Phase(BackupOperationPhases.ApplyingRetention), Is.EqualTo("Applying retention"));
            Assert.That(BackupSetSchedulesText.Elapsed(TimeSpan.FromSeconds(-5)), Is.EqualTo("0 s"));
            Assert.That(BackupSetSchedulesText.Elapsed(TimeSpan.FromMinutes(125)), Is.EqualTo("2 h 05 m"));
        });
    }

    [Test]
    public void The_section_re_reads_while_a_run_is_in_flight_and_stops_once_none_is()
    {
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 1, 2, "albums"));
        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-set-run-progress]").TextContent, Does.Contain("1 of 2 trees")));

        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 2, 2, "photos"));
        Tick();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-set-run-progress]").TextContent, Does.Contain("2 of 2 trees, now photos")));

        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly) with { IsRunning = true };
        Tick();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-set-run-progress]"), Is.Empty));

        var reads = Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync));
        Time.Advance(ClusterStatusPoller.Interval * 3);
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(reads), "nothing running, nothing re-read");
    }

    [Test]
    public void While_a_run_is_in_flight_the_section_reads_only_its_progress_then_the_listing_once_it_ends()
    {
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 1, 2, "albums"));
        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-set-run-progress]").TextContent, Does.Contain("1 of 2 trees")));
        var lists = Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync));

        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 2, 2, "photos"));
        Tick();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-set-run-progress]").TextContent, Does.Contain("2 of 2 trees, now photos")));
        Assert.Multiple(() =>
        {
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupSetScheduleProgressAsync)), Is.EqualTo(1));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(lists), "a running tick reads no listing");
        });

        Backups.SetSchedules[Nightly.Name] = FakeBackupControl.SetStatus(Nightly);
        Tick();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-set-run-progress]"), Is.Empty));
        Time.Advance(ClusterStatusPoller.Interval * 3);

        Assert.Multiple(() =>
        {
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupSetScheduleProgressAsync)), Is.EqualTo(2));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(lists + 1), "one listing once the run ends, then none");
        });
    }

    [Test]
    public void A_connection_without_the_progress_read_falls_back_to_re_reading_the_listing()
    {
        Backups.SetProgress = _ => Task.FromException<BackupSetScheduleRunProgress?>(new NotSupportedException());
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 1, 2, "albums"));
        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-set-run-progress]").TextContent, Does.Contain("1 of 2 trees")));
        var lists = Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync));

        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.WritingMembers, 2, 2, "photos"));
        Tick();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-set-run-progress]").TextContent, Does.Contain("2 of 2 trees, now photos")));
        Tick();
        cut.WaitUntil(() => Assert.That(
            Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(lists + 2)));

        Assert.That(
            Backups.CountOf(nameof(ILatticeBackupControl.GetBackupSetScheduleProgressAsync)),
            Is.EqualTo(1),
            "a refused progress read is not asked again");
    }

    [Test]
    public void Leaving_the_page_stops_re_reading_a_running_schedule()
    {
        Backups.SetSchedules[Nightly.Name] = Running(Progress(BackupOperationPhases.CapturingMembers, 0, 2, null));
        var cut = RenderAt<BackupSchedulesPage>(Page);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-set-run-progress]"), Has.Count.EqualTo(1)));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True);

        cut.FindComponent<BackupSetSchedulesSection>().Instance.Dispose();
        var reads = Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync));
        Time.Advance(ClusterStatusPoller.Interval * 3);

        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupSetSchedulesAsync)), Is.EqualTo(reads));
    }

    private static BackupSetScheduleStatus Running(BackupSetScheduleRunProgress progress) =>
        FakeBackupControl.SetStatus(Nightly) with { IsRunning = true, CurrentRun = progress };

    private BackupSetScheduleRunProgress Progress(
        string phase,
        long completed,
        long? total,
        string? tree,
        int attempt = 1,
        TimeSpan? elapsed = null,
        BackupSetScheduleTrigger trigger = BackupSetScheduleTrigger.Schedule)
    {
        var now = Time.GetUtcNow();
        return new BackupSetScheduleRunProgress(
            now - (elapsed ?? TimeSpan.FromSeconds(30)), trigger, attempt, phase, completed, total, "members", tree, now);
    }

    /// <summary>Moves the circuit's clock one polling interval, once the poller has armed its wait.</summary>
    private void Tick()
    {
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True, "the poller re-arms");
        Time.Advance(ClusterStatusPoller.Interval);
    }
}
