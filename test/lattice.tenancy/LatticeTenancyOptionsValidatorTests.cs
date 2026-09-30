namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenancyOptionsValidator"/>. The validator is
/// wired with <c>ValidateOnStart()</c>, so these assert the fail-fast contract:
/// a non-positive <see cref="LatticeTenancyOptions.HistoryRetentionWindow"/> is
/// rejected at startup, and an unset or strictly positive window is accepted.
/// </summary>
[TestFixture]
public sealed class LatticeTenancyOptionsValidatorTests
{
    private static readonly LatticeTenancyOptionsValidator Validator = new();

    [Test]
    public void Validate_null_window_succeeds()
    {
        var result = Validator.Validate(null, new LatticeTenancyOptions { HistoryRetentionWindow = null });

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_positive_window_succeeds()
    {
        var result = Validator.Validate(null, new LatticeTenancyOptions { HistoryRetentionWindow = TimeSpan.FromHours(1) });

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_zero_window_fails()
    {
        var result = Validator.Validate(null, new LatticeTenancyOptions { HistoryRetentionWindow = TimeSpan.Zero });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("HistoryRetentionWindow"));
        });
    }

    [Test]
    public void Validate_negative_window_fails()
    {
        var result = Validator.Validate(null, new LatticeTenancyOptions { HistoryRetentionWindow = TimeSpan.FromSeconds(-1) });

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_default_lease_duration_succeeds()
    {
        Assert.That(Validator.Validate(null, new LatticeTenancyOptions()).Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_non_positive_lease_duration_fails(int seconds)
    {
        var result = Validator.Validate(null, new LatticeTenancyOptions { PolicySnapshotLeaseDuration = TimeSpan.FromSeconds(seconds) });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("PolicySnapshotLeaseDuration").And.Contain("strictly positive"));
        });
    }

    [Test]
    public void Validate_lease_duration_at_the_timer_ceiling_succeeds()
    {
        var result = Validator.Validate(
            null,
            new LatticeTenancyOptions { PolicySnapshotLeaseDuration = LatticeTenancyOptionsValidator.MaxTimerDuration });

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_lease_duration_above_the_timer_ceiling_fails()
    {
        var result = Validator.Validate(null, new LatticeTenancyOptions { PolicySnapshotLeaseDuration = TimeSpan.MaxValue });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("PolicySnapshotLeaseDuration").And.Contain("at most"));
        });
    }

    [Test]
    public void MaxTimerDuration_is_the_longest_wait_a_timer_accepts()
    {
        Assert.That(LatticeTenancyOptionsValidator.MaxTimerDuration, Is.EqualTo(TimeSpan.FromMilliseconds(0xFFFFFFFE)));
        Assert.That(
            () => new CancellationTokenSource(LatticeTenancyOptionsValidator.MaxTimerDuration).Dispose(),
            Throws.Nothing);
    }

    [Test]
    public void Validate_null_options_throws()
    {
        Assert.That(() => Validator.Validate(null, null!), Throws.ArgumentNullException);
    }
}
