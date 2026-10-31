using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>Backup display labels, invariant figures and boundary values.</summary>
[TestFixture]
[SetCulture("fr-FR")]
public sealed class BackupsFormatTests
{
    [TestCase(0L, "0 B")]
    [TestCase(-512L, "-512 B")]
    [TestCase(1024L, "1 KiB")]
    [TestCase(1_099_511_627_776L, "1 TiB")]
    [TestCase(long.MaxValue, "8388608 TiB")]
    [TestCase(long.MinValue, "-8388608 TiB")]
    public void Bytes_boundaries_and_unit_ceiling_use_invariant_figures(long bytes, string expected) =>
        Assert.That(BackupsFormat.Bytes(bytes), Is.EqualTo(expected));

    [Test]
    public void Time_present_nullable_value_converts_the_offset_to_UTC()
    {
        DateTimeOffset? instant = new DateTimeOffset(2026, 10, 30, 1, 2, 3, TimeSpan.FromHours(2));

        Assert.Multiple(() =>
        {
            Assert.That(BackupsFormat.Time(instant, "Never"), Is.EqualTo("2026-10-29 23:02:03 UTC"));
            Assert.That(BackupsFormat.Time(null, "Never"), Is.EqualTo("Never"));
            Assert.That(BackupsFormat.Count(-1234), Is.EqualTo("-1,234"));
        });
    }

    [TestCase(-1, "none")]
    [TestCase(0, "none")]
    [TestCase(0.001, "1 s")]
    [TestCase(59, "59 s")]
    [TestCase(60, "1 min")]
    [TestCase(90060, "1 d 1 h 1 min")]
    public void Interval_nonpositive_subsecond_and_compound_values_have_explicit_words(double seconds, string expected) =>
        Assert.That(BackupsFormat.Interval(TimeSpan.FromSeconds(seconds)), Is.EqualTo(expected));

    [TestCase(BackupHealthStatus.Healthy, "Healthy")]
    [TestCase(BackupHealthStatus.Warning, "Warning")]
    [TestCase(BackupHealthStatus.Missing, "Missing")]
    [TestCase(BackupHealthStatus.Unknown, "Unknown")]
    [TestCase((BackupHealthStatus)999, "Unknown")]
    public void Health_each_status_has_a_label(BackupHealthStatus status, string expected) =>
        Assert.That(BackupsFormat.Health(status), Is.EqualTo(expected));

    [TestCase(BackupScopeRunOutcome.Success, "Succeeded")]
    [TestCase(BackupScopeRunOutcome.Failure, "Failed")]
    [TestCase(BackupScopeRunOutcome.Denied, "Denied")]
    [TestCase(BackupScopeRunOutcome.None, "No run yet")]
    [TestCase((BackupScopeRunOutcome)999, "No run yet")]
    public void Outcome_each_status_has_a_label(BackupScopeRunOutcome outcome, string expected) =>
        Assert.That(BackupsFormat.Outcome(outcome), Is.EqualTo(expected));

    [TestCase((int)BackupOperationStatus.Succeeded, "Succeeded")]
    [TestCase((int)BackupOperationStatus.Failed, "Failed")]
    [TestCase((int)BackupOperationStatus.Cancelled, "Stopped")]
    [TestCase(999, "Running")]
    public void OperationStatus_terminal_and_fallback_states_have_labels(int status, string expected) =>
        Assert.That(BackupsFormat.OperationStatus((BackupOperationStatus)status), Is.EqualTo(expected));

    [TestCase("", "")]
    [TestCase("short", "short")]
    [TestCase("0123456789ab", "0123456789ab")]
    [TestCase("0123456789abc", "0123456789ab")]
    public void ShortId_at_the_cutoff_preserves_or_truncates_the_id(string id, string expected) =>
        Assert.That(BackupsFormat.ShortId(id), Is.EqualTo(expected));

    [TestCase("", "0123456789ab")]
    [TestCase(" \t", "0123456789ab")]
    [TestCase("nightly", "nightly")]
    public void Name_blank_manifest_name_falls_back_to_short_id(string name, string expected) =>
        Assert.That(BackupsFormat.Name(FakeBackupControl.Manifest("0123456789abcdef", name: name)), Is.EqualTo(expected));

    [Test]
    public void Null_arguments_fail_at_the_formatting_seam()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupsFormat.Scope(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsFormat.ShortId(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsFormat.Name(null!), Throws.ArgumentNullException);
        });
    }
}
