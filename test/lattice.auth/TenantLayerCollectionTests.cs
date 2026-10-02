using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Collection-request and existence-probe tests for the two-layer evaluation: when
/// a range decision stays uniform, when it becomes a per-key filter, and how
/// <see cref="PolicyEvaluator.HasAnyGrant(CompiledPolicy, LatticeAuthOptions, in LatticeSubject, string, LatticeOperation, bool)"/>
/// composes the layers without ever reporting a grant enforcement would deny.
/// </summary>
[TestFixture]
public sealed class TenantLayerCollectionTests
{
    private static readonly LatticeSubjectSelector Alice = LatticeSubjectSelector.User("alice");

    private static bool HasAnyGrant(IEnumerable<LatticeAuthorizationRule> rules, LatticeAuthOptions? options = null, bool active = true) =>
        PolicyEvaluator.HasAnyGrant(
            CompiledPolicy.Compile(rules, includeTenantLayer: active),
            options ?? new LatticeAuthOptions(),
            Subject("alice"),
            TenantTree,
            LatticeOperation.Read,
            active);

    [Test]
    public void Matched_uniform_operator_verdict_decides_the_whole_collection()
    {
        var rules = new[]
        {
            Operator("ops-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.RangeRead, LatticeEffect.Deny),
            Tenant("key-allow", Alice, LatticeScope.Key(TenantTree, "k"), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.RangeRead, key: null, out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.KeyFilter, Is.Null);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Operator));
        });
    }

    [Test]
    public void Unmatched_operator_and_uniform_tenant_layer_yield_a_uniform_tenant_decision()
    {
        var rules = new[]
        {
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.RangeRead, key: null, out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(decision.KeyFilter, Is.Null);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Tenant));
        });
    }

    [Test]
    public void Tenant_per_key_rules_under_an_unmatched_operator_layer_yield_a_filter()
    {
        var rules = new[]
        {
            Tenant("prefix-allow", Alice, LatticeScope.Prefix(TenantTree, "pub/"), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.RangeRead, key: null, out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("pub/1"), Is.True);
            Assert.That(decision.KeyFilter!("priv/1"), Is.False);
            Assert.That(match.Matched, Is.False);
        });
    }

    [Test]
    public void Operator_per_key_deny_carves_the_tenant_allow_in_the_filter()
    {
        var rules = new[]
        {
            Operator("ops-key-deny", Alice, LatticeScope.Key(TenantTree, "pub/blocked"), LatticeOperation.RangeRead, LatticeEffect.Deny),
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.RangeRead, key: null);

        Assert.Multiple(() =>
        {
            Assert.That(decision.KeyFilter, Is.Not.Null);
            Assert.That(decision.KeyFilter!("pub/blocked"), Is.False);
            Assert.That(decision.KeyFilter!("pub/open"), Is.True);
        });
    }

    [Test]
    public void Inactive_layer_collection_decision_ignores_tenant_rules()
    {
        var rules = new[]
        {
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.RangeRead, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.RangeRead, key: null, active: false);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.KeyFilter, Is.Null);
        });
    }

    // ---- Existence probe ---------------------------------------------------

    [Test]
    public void HasAnyGrant_reports_a_tree_reachable_only_through_a_tenant_rule()
    {
        var rules = new[] { Tenant("prefix-allow", Alice, LatticeScope.Prefix(TenantTree, "p/"), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That(HasAnyGrant(rules), Is.True);
            Assert.That(HasAnyGrant(rules, active: false), Is.False);
        });
    }

    [Test]
    public void HasAnyGrant_reports_a_tenant_wide_allow()
    {
        var rules = new[] { Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(HasAnyGrant(rules), Is.True);
    }

    [Test]
    public void HasAnyGrant_hides_a_tree_an_operator_tree_deny_closes_over_a_tenant_allow()
    {
        var rules = new[]
        {
            Operator("ops-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
            Tenant("key-allow", Alice, LatticeScope.Key(TenantTree, "k"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(HasAnyGrant(rules), Is.False);
    }

    [Test]
    public void HasAnyGrant_hides_a_tree_a_tenant_wide_deny_closes_under_default_allow()
    {
        var rules = new[] { Tenant("wide-deny", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Deny) };

        Assert.That(HasAnyGrant(rules, new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }), Is.False);
    }

    [Test]
    public void HasAnyGrant_reports_an_operator_key_allow_beside_a_tenant_wide_deny()
    {
        var rules = new[]
        {
            Operator("ops-key", Alice, LatticeScope.Key(TenantTree, "k"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("wide-deny", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.That(HasAnyGrant(rules), Is.True);
    }

    [Test]
    public void HasAnyGrant_matches_the_operator_only_probe_when_no_tenant_bucket_governs_the_tree()
    {
        var rules = new[]
        {
            Operator("ops-prefix", Alice, LatticeScope.Prefix(TenantTree, "p/"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant(Fabrikam, "other", Alice, LatticeScope.TenantWide(Fabrikam), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.That(HasAnyGrant(rules), Is.EqualTo(HasAnyGrant(rules, active: false)));
    }
}
