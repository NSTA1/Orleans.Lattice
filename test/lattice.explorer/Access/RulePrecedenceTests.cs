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

    // ----- All-trees (Tree:*) tier ordering -----

    [Test]
    public void SpecificityOf_all_trees_scope_ranks_below_a_specific_whole_tree_scope()
    {
        var allTrees = RulePrecedence.SpecificityOf(LatticeScope.ClusterWide());
        var tree = RulePrecedence.SpecificityOf(LatticeScope.Tree("t"));

        Assert.That(allTrees, Is.LessThan(tree));
    }

    [Test]
    public void Rank_all_trees_deny_sorts_to_the_very_top()
    {
        var rules = new[]
        {
            Rule("key-allow", LatticeScope.Key("t", "k"), LatticeEffect.Allow),
            Rule("tree-deny", LatticeScope.Tree("t"), LatticeEffect.Deny),
            Rule("all-deny", LatticeScope.ClusterWide(), LatticeEffect.Deny),
        };

        var ranked = RulePrecedence.Rank(rules);

        Assert.That(ranked[0].Rule.RuleId, Is.EqualTo("all-deny"),
            "a cluster-wide deny wins outright and ranks above every specific rule");
    }

    [Test]
    public void Rank_all_trees_allow_sorts_to_the_bottom()
    {
        var rules = new[]
        {
            Rule("all-allow", LatticeScope.ClusterWide(), LatticeEffect.Allow),
            Rule("tree-allow", LatticeScope.Tree("t"), LatticeEffect.Allow),
            Rule("key-allow", LatticeScope.Key("t", "k"), LatticeEffect.Allow),
        };

        var ranked = RulePrecedence.Rank(rules);

        Assert.That(ranked[^1].Rule.RuleId, Is.EqualTo("all-allow"),
            "a specific-tree rule outranks a cluster-wide allow");
        Assert.That(ranked.Select(r => r.Rule.RuleId), Is.EqualTo(new[] { "key-allow", "tree-allow", "all-allow" }));
    }

    [Test]
    public void Rank_reproduces_the_engine_tier_order()
    {
        var rules = new[]
        {
            Rule("all-allow", LatticeScope.ClusterWide(), LatticeEffect.Allow),
            Rule("tree-allow", LatticeScope.Tree("t"), LatticeEffect.Allow),
            Rule("all-deny", LatticeScope.ClusterWide(), LatticeEffect.Deny),
        };

        var ranked = RulePrecedence.Rank(rules);

        // Engine order: all-trees deny (top) > specific > all-trees allow (bottom).
        Assert.That(ranked.Select(r => r.Rule.RuleId), Is.EqualTo(new[] { "all-deny", "tree-allow", "all-allow" }));
    }
}
