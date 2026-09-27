namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAppRuleIds"/>: the app-owned rule-id prefix
/// value and the ordinal, case-sensitive ownership predicate.
/// </summary>
[TestFixture]
public sealed class LatticeAppRuleIdsTests
{
    [Test]
    public void Prefix_is_app_colon()
    {
        Assert.That(LatticeAppRuleIds.Prefix, Is.EqualTo("app:"));
    }

    [TestCase("app:")]
    [TestCase("app:orders/reader")]
    [TestCase("app:orders:1.0.0:role:reader")]
    public void IsAppOwned_prefixed_id_returns_true(string ruleId)
    {
        Assert.That(LatticeAppRuleIds.IsAppOwned(ruleId), Is.True);
    }

    [TestCase("")]
    [TestCase("app")]
    [TestCase("APP:orders")]
    [TestCase("App:orders")]
    [TestCase(" app:orders")]
    [TestCase("operator-rule")]
    [TestCase("myapp:orders")]
    public void IsAppOwned_id_outside_the_prefix_returns_false(string ruleId)
    {
        Assert.That(LatticeAppRuleIds.IsAppOwned(ruleId), Is.False);
    }

    [Test]
    public void IsAppOwned_null_id_throws()
    {
        Assert.That(() => LatticeAppRuleIds.IsAppOwned(null!), Throws.ArgumentNullException);
    }
}
