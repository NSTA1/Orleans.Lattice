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
    public void Undefined_default_effect_fails_validation()
    {
        var options = new LatticeAuthOptions { DefaultEffect = (LatticeEffect)999 };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Allow_default_effect_validates_successfully()
    {
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Default_effect_is_deny_and_user_beats_group_by_default()
    {
        var options = new LatticeAuthOptions();

        Assert.That(options.DefaultEffect, Is.EqualTo(LatticeEffect.Deny));
        Assert.That(options.UserRuleBeatsGroupRuleAtEqualScope, Is.True);
    }

    [Test]
    public void History_view_is_enabled_by_default()
    {
        Assert.That(new LatticeAuthOptions().EnableDurableHistoryView, Is.True);
    }

    [Test]
    public void Access_administration_delegation_is_disabled_by_default()
    {
        Assert.That(new LatticeAuthOptions().AccessAdministrationDelegationEnabled, Is.False);
    }

    [Test]
    public void Access_administration_delegation_enabled_validates_successfully()
    {
        var options = new LatticeAuthOptions { AccessAdministrationDelegationEnabled = true };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Strict_consistency_trees_is_null_by_default()
    {
        Assert.That(new LatticeAuthOptions().StrictConsistencyTrees, Is.Null);
    }

    [Test]
    public void Populated_strict_consistency_trees_validate_successfully()
    {
        var options = new LatticeAuthOptions
        {
            StrictConsistencyTrees = new HashSet<string>(StringComparer.Ordinal) { "sys-auth-policy" },
        };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Empty_strict_consistency_tree_id_fails_validation()
    {
        var options = new LatticeAuthOptions
        {
            StrictConsistencyTrees = new HashSet<string>(StringComparer.Ordinal) { string.Empty },
        };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Audit_options_are_off_and_deny_only_at_full_sampling_by_default()
    {
        var options = new LatticeAuthOptions();

        Assert.That(options.EnableAuditSink, Is.False);
        Assert.That(options.AuditVerbosity, Is.EqualTo(LatticeAuthAuditVerbosity.DenyOnly));
        Assert.That(options.AuditSamplingRatio, Is.EqualTo(1.0));
        Assert.That(options.EnableDurableAuditTrail, Is.False);
        Assert.That(options.AuditTrailTimeToLive, Is.Null);
    }

    [Test]
    public void Undefined_audit_verbosity_fails_validation()
    {
        var options = new LatticeAuthOptions { AuditVerbosity = (LatticeAuthAuditVerbosity)999 };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [TestCase(0.0)]
    [TestCase(0.5)]
    [TestCase(1.0)]
    public void In_range_audit_sampling_ratio_validates_successfully(double ratio)
    {
        var options = new LatticeAuthOptions { AuditSamplingRatio = ratio };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(-0.01)]
    [TestCase(1.01)]
    [TestCase(double.NaN)]
    public void Out_of_range_audit_sampling_ratio_fails_validation(double ratio)
    {
        var options = new LatticeAuthOptions { AuditSamplingRatio = ratio };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Positive_audit_trail_ttl_validates_successfully()
    {
        var options = new LatticeAuthOptions { AuditTrailTimeToLive = TimeSpan.FromHours(1) };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Zero_audit_trail_ttl_fails_validation()
    {
        var options = new LatticeAuthOptions { AuditTrailTimeToLive = TimeSpan.Zero };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Negative_audit_trail_ttl_fails_validation()
    {
        var options = new LatticeAuthOptions { AuditTrailTimeToLive = TimeSpan.FromSeconds(-1) };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }
}
