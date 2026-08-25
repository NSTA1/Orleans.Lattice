namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAccessDecision"/>: the allow/deny factories,
/// the cached allow singleton, and the reason-guard on a denial.
/// </summary>
public sealed class TenantAccessDecisionTests
{
    [Test]
    public void Allow_is_allowed_with_no_reason()
    {
        var decision = TenantAccessDecision.Allow();

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.Reason, Is.Null);
        });
    }

    [Test]
    public void Deny_is_not_allowed_and_carries_the_reason()
    {
        var decision = TenantAccessDecision.Deny("nope");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Is.EqualTo("nope"));
        });
    }

    [Test]
    public void Deny_with_a_null_or_empty_reason_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantAccessDecision.Deny(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => TenantAccessDecision.Deny(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }
}
