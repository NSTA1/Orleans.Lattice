using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeMembershipOptionsValidator"/>, the silo-start
/// gate on <see cref="LatticeMembershipOptions"/>. Each rejection rule is asserted
/// individually - a negative resolution-cache lifetime, a non-positive history
/// retention window, and an undefined enum for either the history retention mode
/// or the group merge mode - so a rule that stops firing is caught rather than
/// masked by a sibling failure.
/// </summary>
[TestFixture]
public sealed class LatticeMembershipOptionsValidatorTests
{
    private static ValidateOptionsResult Validate(LatticeMembershipOptions options) =>
        new LatticeMembershipOptionsValidator().Validate(name: null, options);

    [Test]
    public void Default_options_are_valid()
    {
        Assert.That(Validate(new LatticeMembershipOptions()).Succeeded, Is.True);
    }

    [Test]
    public void A_negative_resolution_cache_ttl_is_rejected()
    {
        var result = Validate(new LatticeMembershipOptions
        {
            ResolutionCacheTtl = TimeSpan.FromSeconds(-1),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeMembershipOptions.ResolutionCacheTtl)));
        });
    }

    [Test]
    public void A_zero_resolution_cache_ttl_is_allowed()
    {
        Assert.That(
            Validate(new LatticeMembershipOptions { ResolutionCacheTtl = TimeSpan.Zero }).Succeeded,
            Is.True,
            "zero disables caching; it is not an error");
    }

    [TestCase(0)]
    [TestCase(-5)]
    public void A_non_positive_history_retention_window_is_rejected(int seconds)
    {
        var result = Validate(new LatticeMembershipOptions
        {
            HistoryRetentionWindow = TimeSpan.FromSeconds(seconds),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeMembershipOptions.HistoryRetentionWindow)));
        });
    }

    [Test]
    public void An_absent_history_retention_window_is_allowed()
    {
        Assert.That(
            Validate(new LatticeMembershipOptions { HistoryRetentionWindow = null }).Succeeded,
            Is.True);
    }

    [Test]
    public void An_undefined_history_retention_mode_is_rejected()
    {
        var result = Validate(new LatticeMembershipOptions
        {
            HistoryRetentionMode = (HistoryRetentionMode)9999,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeMembershipOptions.HistoryRetentionMode)));
        });
    }

    [Test]
    public void An_undefined_group_merge_mode_is_rejected()
    {
        var result = Validate(new LatticeMembershipOptions
        {
            GroupMergeMode = (SubjectGroupMergeMode)9999,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeMembershipOptions.GroupMergeMode)));
        });
    }

    [Test]
    public void Every_broken_rule_is_reported_together()
    {
        var result = Validate(new LatticeMembershipOptions
        {
            ResolutionCacheTtl = TimeSpan.FromSeconds(-1),
            HistoryRetentionWindow = TimeSpan.Zero,
            HistoryRetentionMode = (HistoryRetentionMode)9999,
            GroupMergeMode = (SubjectGroupMergeMode)9999,
        });

        Assert.That(result.Failures?.Count(), Is.EqualTo(4),
            "the validator accumulates failures rather than stopping at the first");
    }

    [Test]
    public void Validate_rejects_null_options()
    {
        Assert.That(
            () => new LatticeMembershipOptionsValidator().Validate(name: null, options: null!),
            Throws.ArgumentNullException);
    }
}
