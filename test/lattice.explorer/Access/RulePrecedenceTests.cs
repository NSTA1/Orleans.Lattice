using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

[TestFixture]
public class RulePrecedenceTests
{
    private static LatticeAuthorizationRule Rule(string id, LatticeScope scope, LatticeEffect effect) =>
        new(id, LatticeSubjectSelector.User("alice"), scope, LatticeOperation.Read, effect);

    [Test]
    public void Rank_null_throws()
    {
        Assert.That(() => RulePrecedence.Rank(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Rank_empty_returns_empty()
    {
        Assert.That(RulePrecedence.Rank(Array.Empty<LatticeAuthorizationRule>()), Is.Empty);
    }

    [Test]
    public void SpecificityOf_null_throws()
    {
        Assert.That(() => RulePrecedence.SpecificityOf(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void SpecificityOf_key_beats_prefix_beats_tree()
    {
        var key = RulePrecedence.SpecificityOf(LatticeScope.Key("t", "abc"));
        var prefix = RulePrecedence.SpecificityOf(LatticeScope.Prefix("t", "ab"));
        var tree = RulePrecedence.SpecificityOf(LatticeScope.Tree("t"));

        Assert.Multiple(() =>
        {
            Assert.That(key, Is.GreaterThan(prefix));
            Assert.That(prefix, Is.GreaterThan(tree));
        });
    }

    [Test]
    public void SpecificityOf_longer_prefix_is_more_specific()
    {
        var longer = RulePrecedence.SpecificityOf(LatticeScope.Prefix("t", "abcd"));
        var shorter = RulePrecedence.SpecificityOf(LatticeScope.Prefix("t", "ab"));

        Assert.That(longer, Is.GreaterThan(shorter));
    }

    [Test]
    public void Rank_orders_most_specific_scope_first()
    {
        var rules = new[]
        {
            Rule("tree", LatticeScope.Tree("t"), LatticeEffect.Allow),
            Rule("key", LatticeScope.Key("t", "k"), LatticeEffect.Allow),
            Rule("prefix", LatticeScope.Prefix("t", "p"), LatticeEffect.Allow),
        };

        var ranked = RulePrecedence.Rank(rules);

        Assert.That(ranked.Select(r => r.Rule.RuleId), Is.EqualTo(new[] { "key", "prefix", "tree" }));
    }

    [Test]
    public void Rank_deny_overrides_allow_at_equal_specificity()
    {
        var rules = new[]
        {
            Rule("allow", LatticeScope.Key("t", "k"), LatticeEffect.Allow),
            Rule("deny", LatticeScope.Key("t", "k"), LatticeEffect.Deny),
        };

        var ranked = RulePrecedence.Rank(rules);

        Assert.Multiple(() =>
        {
            Assert.That(ranked[0].Rule.RuleId, Is.EqualTo("deny"));
            Assert.That(ranked[0].DenyOverrides, Is.True);
        });
    }

    [Test]
    public void Rank_ties_break_on_rule_id()
    {
        var rules = new[]
        {
            Rule("b", LatticeScope.Key("t", "k"), LatticeEffect.Allow),
            Rule("a", LatticeScope.Key("t", "k"), LatticeEffect.Allow),
        };

        var ranked = RulePrecedence.Rank(rules);

        Assert.That(ranked.Select(r => r.Rule.RuleId), Is.EqualTo(new[] { "a", "b" }));
    }
}
