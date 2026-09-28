namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class TreeOwnershipDecisionTests
{
    [Test]
    public void Default_denies_without_a_reason()
    {
        var decision = default(TreeOwnershipDecision);
        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Is.Null);
    }

    [Test]
    public void Allow_explicitly_allows_without_a_reason()
    {
        var decision = TreeOwnershipDecision.Allow();
        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.Reason, Is.Null);
    }

    [Test]
    public void Deny_retains_the_reason()
    {
        var decision = TreeOwnershipDecision.Deny("different owner");
        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Is.EqualTo("different owner"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase(" ")]
    public void Deny_rejects_a_missing_reason(string? reason)
        => Assert.That(() => TreeOwnershipDecision.Deny(reason!), Throws.InstanceOf<ArgumentException>());
}
