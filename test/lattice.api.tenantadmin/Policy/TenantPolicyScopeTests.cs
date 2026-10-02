using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Unit tests for <see cref="TenantPolicyScope"/>: the tenant-relative composition,
/// classification and projection the tenant policy facade is built on.
/// </summary>
[TestFixture]
public sealed class TenantPolicyScopeTests
{
    private static readonly TenantPolicyScope Scope = TenantPolicyScope.For(TenantId.Parse("acme"));

    private static LatticeAuthorizationRule Rule(string ruleId, string treeId) =>
        new(ruleId, LatticeSubjectSelector.User("bob"), LatticeScope.Tree(treeId), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void For_composes_the_tenants_prefixes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Scope.RuleIdPrefix, Is.EqualTo("tenant:acme:"));
            Assert.That(Scope.TreePrefix, Is.EqualTo("t/acme/"));
            Assert.That(Scope.TenantWideTreeId, Is.EqualTo("t/acme/*"));
            Assert.That(Scope.ComposeRuleId("r1"), Is.EqualTo("tenant:acme:r1"));
        });
    }

    [TestCase("tenant:acme:r1", "t/acme/orders", "Tenant")]
    [TestCase("tenant:acme:wide", "t/acme/*", "Tenant")]
    [TestCase("tenant:globex:r1", "t/globex/orders", "Hidden")]
    [TestCase("tenant:acmecorp:r1", "t/acmecorp/orders", "Hidden")]
    [TestCase("guardrail", "t/acme/orders", "PlatformTree")]
    [TestCase("guardrail", "t/acmecorp/orders", "Hidden")]
    [TestCase("guardrail", "t/globex/orders", "Hidden")]
    [TestCase("guardrail", "orders", "Hidden")]
    [TestCase("guardrail", "sys-audit", "Hidden")]
    [TestCase("everywhere", "*", "PlatformWide")]
    [TestCase("app:shop:reader:1", "t/acme/a/shop/items", "AppRole")]
    [TestCase("app:shop:reader:1", "t/globex/a/shop/items", "Hidden")]
    public void Classify_decides_how_the_tenant_sees_a_stored_rule(string ruleId, string treeId, string expected)
    {
        Assert.That(Scope.Classify(Rule(ruleId, treeId)), Is.EqualTo(Enum.Parse<TenantRuleVisibility>(expected)));
    }

    [TestCase("orders", false, "t/acme/orders")]
    [TestCase("nested/name", false, "t/acme/nested/name")]
    [TestCase("a/shop/items", true, "t/acme/a/shop/items")]
    public void ComposeTreeId_composes_a_tree_the_tenant_may_govern(string name, bool admitAppTrees, string expected)
    {
        Assert.That(Scope.ComposeTreeId(name, admitAppTrees, "treeName"), Is.EqualTo(expected));
    }

    [TestCase("a/shop/items")]
    [TestCase("sys-audit")]
    [TestCase("_lattice_x")]
    [TestCase("*")]
    public void ComposeTreeId_refuses_a_tree_outside_the_tenant_layer(string name)
    {
        var ex = Assert.Throws<TenantAccessConfinementException>(() => Scope.ComposeTreeId(name, false, "treeName"));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.RuleTree));
            Assert.That(ex.ParamName, Is.EqualTo("treeName"));
        });
    }

    [Test]
    public void ComposeTreeId_refuses_an_empty_name()
    {
        Assert.That(() => Scope.ComposeTreeId(string.Empty, false, "treeName"), Throws.ArgumentException);
    }

    [Test]
    public void ToView_refuses_a_hidden_rule()
    {
        Assert.That(
            () => Scope.ToView(Rule("guardrail", "orders"), TenantRuleVisibility.Hidden),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ToView_reports_a_foreign_group_subject_as_a_cluster_group_by_full_id()
    {
        var rule = new LatticeAuthorizationRule(
            "guardrail", LatticeSubjectSelector.Group("t/globex/x"), LatticeScope.Tree("t/acme/orders"), LatticeOperation.Read, LatticeEffect.Deny);

        var view = Scope.ToView(rule, TenantRuleVisibility.PlatformTree);

        Assert.Multiple(() =>
        {
            Assert.That(view.SubjectKind, Is.EqualTo(TenantSubjectKind.ClusterGroup));
            Assert.That(view.SubjectId, Is.EqualTo("t/globex/x"));
        });
    }

    [Test]
    public void Withheld_carries_only_id_origin_and_effect()
    {
        var view = TenantPolicyScope.Withheld("everywhere", TenantRuleOrigin.PlatformWide, LatticeEffect.Deny);

        Assert.Multiple(() =>
        {
            Assert.That(view.RuleId, Is.EqualTo("everywhere"));
            Assert.That(view.Layer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(view.Effect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(view.SubjectWithheld, Is.True);
            Assert.That(view.SubjectId, Is.Null);
            Assert.That(view.Operations, Is.EqualTo(LatticeOperation.None));
            Assert.That(view.Editable, Is.False);
        });
    }

    [TestCase("bob", TenantSubjectKind.User, "bob")]
    [TestCase("readers", TenantSubjectKind.TenantGroup, "t/acme/readers")]
    [TestCase("entra-staff", TenantSubjectKind.ClusterGroup, "entra-staff")]
    public void ComposeSubjectId_composes_the_principal_id(string subjectId, TenantSubjectKind kind, string expected)
    {
        Assert.That(Scope.ComposeSubjectId(subjectId, kind, "subjectId"), Is.EqualTo(expected));
    }
}
