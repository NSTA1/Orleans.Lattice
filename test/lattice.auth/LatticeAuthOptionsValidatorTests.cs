using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAuthOptionsValidator"/>: the history
/// retention window must be strictly positive when supplied and the retention
/// mode must be a defined enum value.
/// </summary>
[TestFixture]
public class LatticeAuthOptionsValidatorTests
{
    private static readonly LatticeAuthOptionsValidator Validator = new();

    [Test]
    public void Default_options_validate_successfully()
    {
        var result = Validator.Validate(null, new LatticeAuthOptions());

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Positive_retention_window_validates_successfully()
    {
        var options = new LatticeAuthOptions { HistoryRetentionWindow = TimeSpan.FromDays(30) };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Zero_retention_window_fails_validation()
    {
        var options = new LatticeAuthOptions { HistoryRetentionWindow = TimeSpan.Zero };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Negative_retention_window_fails_validation()
    {
        var options = new LatticeAuthOptions { HistoryRetentionWindow = TimeSpan.FromMinutes(-1) };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Undefined_retention_mode_fails_validation()
    {
        var options = new LatticeAuthOptions { HistoryRetentionMode = (HistoryRetentionMode)999 };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void History_view_is_enabled_by_default()
    {
        Assert.That(new LatticeAuthOptions().EnableDurableHistoryView, Is.True);
    }
}
