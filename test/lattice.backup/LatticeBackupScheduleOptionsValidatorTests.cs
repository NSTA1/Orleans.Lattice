namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupScheduleOptionsValidator"/>: the
/// default options are valid, and each out-of-range knob (non-positive interval,
/// keep-last below one, non-positive retention age) is rejected.
/// </summary>
public sealed class LatticeBackupScheduleOptionsValidatorTests
{
    private readonly LatticeBackupScheduleOptionsValidator _validator = new();

    [Test]
    public void Validate_default_options_succeed()
    {
        var result = _validator.Validate(null, new LatticeBackupScheduleOptions());

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_valid_configured_options_succeed()
    {
        var options = new LatticeBackupScheduleOptions
        {
            FullBackupScheduleEnabled = true,
            FullBackupInterval = TimeSpan.FromHours(6),
            IncrementalBackupScheduleEnabled = true,
            IncrementalBackupInterval = TimeSpan.FromMinutes(30),
            RetentionEnabled = true,
            RetentionKeepLast = 5,
            RetentionMaxAge = TimeSpan.FromDays(30),
        };

        var result = _validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_non_positive_full_interval_fails()
    {
        var options = new LatticeBackupScheduleOptions { FullBackupInterval = TimeSpan.Zero };

        var result = _validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_non_positive_incremental_interval_fails()
    {
        var options = new LatticeBackupScheduleOptions
        {
            IncrementalBackupInterval = TimeSpan.FromSeconds(-1),
        };

        var result = _validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_keep_last_below_one_fails()
    {
        var options = new LatticeBackupScheduleOptions { RetentionKeepLast = 0 };

        var result = _validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_non_positive_retention_age_fails()
    {
        var options = new LatticeBackupScheduleOptions { RetentionMaxAge = TimeSpan.Zero };

        var result = _validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }
}
