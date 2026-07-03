namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAccessDecision"/>: the
/// <see cref="LatticeAccessDecision.Allow"/> /
/// <see cref="LatticeAccessDecision.Deny"/> /
/// <see cref="LatticeAccessDecision.Filtered"/> factories, their members, and
/// the <see cref="LatticeAccessDecision.KeyFilter"/> predicate behaviour.
/// </summary>
[TestFixture]
public class LatticeAccessDecisionTests
{
    [Test]
    public void Allow_is_allowed_with_no_reason_or_filter()
    {
        var decision = LatticeAccessDecision.Allow();

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.Reason, Is.Null);
            Assert.That(decision.KeyFilter, Is.Null);
        });
    }

    [Test]
    public void Deny_is_not_allowed_and_carries_the_reason()
    {
        var decision = LatticeAccessDecision.Deny("forbidden");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Is.EqualTo("forbidden"));
            Assert.That(decision.KeyFilter, Is.Null);
        });
    }

    [Test]
    public void Deny_rejects_a_null_reason()
    {
        Assert.That(() => LatticeAccessDecision.Deny(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Deny_rejects_an_empty_reason()
    {
        Assert.That(() => LatticeAccessDecision.Deny(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void Filtered_is_allowed_and_carries_the_predicate()
    {
        Func<string, bool> predicate = key => key.StartsWith("pub", StringComparison.Ordinal);
        var decision = LatticeAccessDecision.Filtered(predicate);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.KeyFilter, Is.SameAs(predicate));
            Assert.That(decision.Reason, Is.Null);
        });
    }

    [Test]
    public void Filtered_predicate_keeps_and_prunes_keys()
    {
        var decision = LatticeAccessDecision.Filtered(key => key.StartsWith("pub", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter!("pub:1"), Is.True);
            Assert.That(decision.KeyFilter!("secret:1"), Is.False);
        });
    }

    [Test]
    public void Filtered_carries_an_optional_reason()
    {
        var decision = LatticeAccessDecision.Filtered(_ => true, "row-level filter");

        Assert.That(decision.Reason, Is.EqualTo("row-level filter"));
    }

    [Test]
    public void Filtered_rejects_a_null_predicate()
    {
        Assert.That(() => LatticeAccessDecision.Filtered(null!), Throws.ArgumentNullException);
    }
}
