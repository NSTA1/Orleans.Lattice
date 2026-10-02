using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Full-coverage tests for a collection request over a key range: a range every
/// key of which resolves to the same allow is returned as a plain allow, so a
/// prefix-scoped grant authorizes an all-or-nothing operation over exactly its
/// prefix, while any exact key or stored prefix that governs only part of the
/// range keeps the decision filtered (issue #4278).
/// </summary>
public sealed partial class CompiledPolicyEvaluationTests
{
    private static LatticeAccessDecision EvalPrefixRange(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeOperation operation,
        string prefix,
        LatticeAuthOptions? options = null) =>
        Eval(rules, Subject("alice"), operation, key: null,
            rangeStart: prefix, rangeEnd: LatticeKeyRange.PrefixUpperBound(prefix), options: options);

    [Test]
    public void Evaluate_prefix_range_under_a_matching_prefix_allow_is_a_plain_allow()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "tenant-a/"), LatticeOperation.Backup, LatticeEffect.Allow),
            User("other", "alice", LatticeScope.Key(Tree, "tenant-b/x"), LatticeOperation.Backup, LatticeEffect.Deny),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "tenant-a/");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.KeyFilter, Is.Null,
                "every key in the prefix range resolves to the same prefix allow, so the decision is uniform");
        });
    }

    [Test]
    public void Evaluate_sub_prefix_range_under_a_broader_prefix_allow_is_a_plain_allow()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "tenant-a/"), LatticeOperation.Backup, LatticeEffect.Allow),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "tenant-a/orders/");

        Assert.That(decision.KeyFilter, Is.Null);
        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public void Evaluate_prefix_range_under_a_key_allow_at_the_prefix_string_is_not_a_plain_allow()
    {
        var rules = new[]
        {
            User("k", "alice", LatticeScope.Key(Tree, "tenant-a/config"), LatticeOperation.Backup, LatticeEffect.Allow),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "tenant-a/config");

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed && decision.KeyFilter is null, Is.False,
                "a key grant covers one key, not the subtree the key spells");
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("tenant-a/config"), Is.True);
            Assert.That(decision.KeyFilter!("tenant-a/config/db-password"), Is.False);
        });
    }

    [Test]
    public void Evaluate_prefix_range_with_a_key_deny_carve_out_inside_it_stays_filtered()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.Backup, LatticeEffect.Allow),
            User("ssn", "alice", LatticeScope.Key(Tree, "x/ssn"), LatticeOperation.Backup, LatticeEffect.Deny),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "x");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null, "the carve-out inside the range must remain visible");
            Assert.That(decision.KeyFilter!("x/ssn"), Is.False);
            Assert.That(decision.KeyFilter!("x/name"), Is.True);
        });
    }

    [Test]
    public void Evaluate_prefix_range_with_a_prefix_deny_carve_out_inside_it_stays_filtered()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.Backup, LatticeEffect.Allow),
            User("secrets", "alice", LatticeScope.Prefix(Tree, "x/secrets/"), LatticeOperation.Backup, LatticeEffect.Deny),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "x");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("x/secrets/a"), Is.False);
        });
    }

    [Test]
    public void Evaluate_prefix_range_with_an_exact_rule_at_the_range_start_stays_filtered()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "x"), LatticeOperation.Backup, LatticeEffect.Allow),
            User("k", "alice", LatticeScope.Key(Tree, "x"), LatticeOperation.Backup, LatticeEffect.Deny),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "x");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("x"), Is.False);
            Assert.That(decision.KeyFilter!("xy"), Is.True);
        });
    }

    [Test]
    public void Evaluate_range_wider_than_a_prefix_of_its_start_stays_filtered()
    {
        // "ab" is a prefix of the range start but does not cover "ac", which is in
        // the range, so the range is not uniform.
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "ab"), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, key: null, rangeStart: "ab", rangeEnd: "b");

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("ab1"), Is.True);
            Assert.That(decision.KeyFilter!("ac"), Is.False);
        });
    }

    [Test]
    public void Evaluate_longest_containing_prefix_decides_a_uniform_range()
    {
        var rules = new[]
        {
            User("broad", "alice", LatticeScope.Prefix(Tree, "x"), LatticeOperation.Backup, LatticeEffect.Deny),
            User("narrow", "alice", LatticeScope.Prefix(Tree, "x/pub/"), LatticeOperation.Backup, LatticeEffect.Allow),
        };

        Assert.Multiple(() =>
        {
            var inner = EvalPrefixRange(rules, LatticeOperation.Backup, "x/pub/");
            Assert.That(inner.Allowed, Is.True);
            Assert.That(inner.KeyFilter, Is.Null, "the longest prefix containing the range decides it");

            var outer = EvalPrefixRange(rules, LatticeOperation.Backup, "x");
            Assert.That(outer.Allowed && outer.KeyFilter is null, Is.False,
                "the wider range contains the narrower allow, so it is not a uniform allow");
        });
    }

    [Test]
    public void Evaluate_uniform_deny_over_a_prefix_range_is_never_a_plain_allow()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "x"), LatticeOperation.Backup, LatticeEffect.Deny),
            User("other", "alice", LatticeScope.Prefix(Tree, "y"), LatticeOperation.Backup, LatticeEffect.Allow),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, "x");

        Assert.That(decision.Allowed && decision.KeyFilter is null, Is.False);
    }

    [Test]
    public void Evaluate_prefix_range_of_unbounded_prefix_is_a_plain_allow_under_its_own_prefix()
    {
        var prefix = new string(char.MaxValue, 2);
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, prefix), LatticeOperation.Backup, LatticeEffect.Allow),
            User("other", "alice", LatticeScope.Key(Tree, "a"), LatticeOperation.Backup, LatticeEffect.Deny),
        };

        var decision = EvalPrefixRange(rules, LatticeOperation.Backup, prefix);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.KeyFilter, Is.Null);
        });
    }

    [Test]
    public void Evaluate_open_ended_range_under_a_bounded_prefix_stays_filtered()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "x"), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, key: null, rangeStart: "x", rangeEnd: null);

        Assert.That(decision.KeyFilter, Is.Not.Null, "keys above the prefix are in the range but outside the grant");
    }

    [Test]
    public void Evaluate_whole_tree_collection_with_per_key_rules_stays_filtered()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "x"), LatticeOperation.Backup, LatticeEffect.Allow),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.Backup, key: null);

        Assert.That(decision.KeyFilter, Is.Not.Null, "a whole-tree request is unchanged by the range full-coverage test");
    }

    [Test]
    public void Evaluate_uniform_range_allow_reports_the_deciding_rule()
    {
        var rules = new[]
        {
            User("p", "alice", LatticeScope.Prefix(Tree, "x"), LatticeOperation.Backup, LatticeEffect.Allow),
        };
        var policy = CompiledPolicy.Compile(rules);

        var decision = PolicyEvaluator.Evaluate(
            policy, new LatticeAuthOptions(), Subject("alice"), Tree, LatticeOperation.Backup,
            key: null, rangeStart: "x", rangeEnd: "y", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.KeyFilter, Is.Null);
            Assert.That(match.Matched, Is.True);
            Assert.That(match.RuleId, Is.EqualTo("p"));
            Assert.That(match.ScopeKind, Is.EqualTo(LatticeScopeKind.Prefix));
        });
    }
}
