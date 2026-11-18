namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The live progress of a backup-set schedule's run (issue #6067): each report is
/// published as a whole snapshot carrying the attempt, and a member phase names
/// the tree at its completed count, because members are pinned and written in
/// scope order.
/// </summary>
[TestFixture]
public sealed class BackupSetScheduleRunProgressTests
{
    private static readonly DateTimeOffset Started = DateTimeOffset.UnixEpoch;

    [Test]
    public void Snapshot_is_null_before_the_first_report()
    {
        var progress = new BackupSetSchedulerGrain.RunProgress(Started, BackupSetScheduleTrigger.Schedule);

        Assert.That(progress.Snapshot, Is.Null);
    }

    [Test]
    public async Task A_member_phase_names_the_tree_at_its_completed_count()
    {
        var progress = new BackupSetSchedulerGrain.RunProgress(Started, BackupSetScheduleTrigger.Manual);
        progress.BeginAttempt(2, [BackupScopeSelector.WholeTree("a"), BackupScopeSelector.WholeTree("b")]);

        await progress.ReportAsync(BackupOperationPhases.WritingMembers, 1, 2, BackupOperationUnits.Members);
        var writing = progress.Snapshot!;
        await progress.ReportAsync(BackupOperationPhases.WritingMembers, 2, 2, BackupOperationUnits.Members);
        var done = progress.Snapshot!;

        Assert.Multiple(() =>
        {
            Assert.That(writing.Phase, Is.EqualTo(BackupOperationPhases.WritingMembers));
            Assert.That(writing.Attempt, Is.EqualTo(2));
            Assert.That(writing.Trigger, Is.EqualTo(BackupSetScheduleTrigger.Manual));
            Assert.That(writing.StartedAtUtc, Is.EqualTo(Started));
            Assert.That((writing.CompletedUnits, writing.TotalUnits, writing.UnitName), Is.EqualTo((1L, (long?)2, BackupOperationUnits.Members)));
            Assert.That(writing.CurrentTreeId, Is.EqualTo("b"));
            Assert.That(done.CurrentTreeId, Is.Null, "every member is written");
        });
    }

    [Test]
    public async Task A_run_phase_names_no_tree_and_reports_the_first_attempt_before_any_capture()
    {
        var progress = new BackupSetSchedulerGrain.RunProgress(Started, BackupSetScheduleTrigger.Schedule);

        await progress.ReportAsync(BackupOperationPhases.ResolvingMembers);

        Assert.Multiple(() =>
        {
            Assert.That(progress.Snapshot!.Phase, Is.EqualTo(BackupOperationPhases.ResolvingMembers));
            Assert.That(progress.Snapshot.Attempt, Is.EqualTo(1));
            Assert.That(progress.Snapshot.CurrentTreeId, Is.Null);
            Assert.That(progress.Snapshot.TotalUnits, Is.Null);
        });
    }

    [Test]
    public void Constructor_rejects_an_empty_phase()
    {
        Assert.That(
            () => new BackupSetScheduleRunProgress(Started, BackupSetScheduleTrigger.Schedule, 1, "", 0, null, null, null, Started),
            Throws.ArgumentException);
    }
}
