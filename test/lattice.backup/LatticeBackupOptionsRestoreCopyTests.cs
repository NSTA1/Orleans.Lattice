using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Tests that <see cref="LatticeBackupOptionsValidator"/> bounds the restore-copy
/// retirement options (issue #6103): a revert window must be strictly positive and
/// the estate-wide sweep may not run more often than a reminder can fire.
/// </summary>
[TestFixture]
public sealed class LatticeBackupOptionsRestoreCopyTests
{
    private static ValidateOptionsResult Validate(LatticeBackupOptions options) =>
        new LatticeBackupOptionsValidator().Validate(name: null, options);

    [Test]
    public void The_defaults_keep_reverts_open_and_sweep_daily()
    {
        var options = new LatticeBackupOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.RestoreRevertRetention, Is.Null);
            Assert.That(options.RestoreCopySweepInterval, Is.EqualTo(TimeSpan.FromDays(1)));
            Assert.That(Validate(options).Succeeded, Is.True);
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void A_revert_window_that_is_not_positive_is_rejected(int seconds)
    {
        var result = Validate(new LatticeBackupOptions { RestoreRevertRetention = TimeSpan.FromSeconds(seconds) });

        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.RestoreRevertRetention)));
    }

    [Test]
    public void A_sweep_interval_under_a_minute_is_rejected_and_a_disabled_sweep_is_accepted()
    {
        var tooShort = Validate(new LatticeBackupOptions { RestoreCopySweepInterval = TimeSpan.FromSeconds(59) });
        var disabled = Validate(new LatticeBackupOptions { RestoreCopySweepInterval = null });

        Assert.Multiple(() =>
        {
            Assert.That(tooShort.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.RestoreCopySweepInterval)));
            Assert.That(disabled.Succeeded, Is.True);
        });
    }
}
