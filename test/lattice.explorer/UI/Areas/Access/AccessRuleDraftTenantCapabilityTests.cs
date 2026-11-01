using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The rule editor's tenant capability scope (issue #5582): a rule granting
/// Telemetry for one tenant, at <see cref="LatticeScope.TenantCapability(TenantId)"/>,
/// and nothing else.
/// </summary>
[TestFixture]
public sealed class AccessRuleDraftTenantCapabilityTests
{
    [Test]
    public void A_telemetry_grant_for_one_tenant_converts_to_the_tenant_capability_scope()
    {
        var draft = Draft("acme", LatticeOperation.Telemetry);

        Assert.That(draft.Validate().HasAny, Is.False);
        var rule = draft.ToRule();
        Assert.Multiple(() =>
        {
            Assert.That(rule.Scope, Is.EqualTo(LatticeScope.TenantCapability(TenantId.Parse("acme"))));
            Assert.That(rule.Operations, Is.EqualTo(LatticeOperation.Telemetry));
        });
    }

    [TestCase(LatticeOperation.Read)]
    [TestCase(LatticeOperation.Admin)]
    [TestCase(LatticeOperation.AppInstall)]
    [TestCase(LatticeOperation.Telemetry | LatticeOperation.Admin)]
    public void Anything_but_telemetry_is_refused_on_a_tenant_capability_scope(LatticeOperation operations)
    {
        var draft = Draft("acme", operations);

        Assert.That(draft.Validate().Operations, Is.EqualTo(AccessRuleDraft.TenantCapabilityMessage));
        Assert.Throws<InvalidOperationException>(() => draft.ToRule());
    }

    [TestCase("", "Enter the tenant whose telemetry the rule grants.")]
    [TestCase("Not A Tenant", "Not A Tenant is not a valid tenant id.")]
    [TestCase("default", "The default tenant has no tenant capability scope. Grant its telemetry with the cluster-wide scope.")]
    public void A_missing_invalid_or_default_tenant_is_refused(string tenant, string message)
    {
        var draft = Draft(tenant, LatticeOperation.Telemetry);

        Assert.That(draft.Validate().Tenant, Is.EqualTo(message));
    }

    [Test]
    public void Telemetry_on_a_tree_scope_is_still_refused_as_scopeless()
    {
        var draft = Draft("acme", LatticeOperation.Telemetry);
        draft.ScopeKind = AccessRuleDraft.TreeScope;
        draft.TreeId = "t/acme/orders";

        Assert.That(draft.Validate().Operations, Is.EqualTo(AccessRuleDraft.ScopelessMessage));
    }

    [Test]
    public void An_existing_tenant_capability_rule_round_trips()
    {
        var rule = new LatticeAuthorizationRule(
            "acme-telemetry",
            LatticeSubjectSelector.User("ada"),
            LatticeScope.TenantCapability(TenantId.Parse("acme")),
            LatticeOperation.Telemetry,
            LatticeEffect.Allow);

        var draft = AccessRuleDraft.From(rule);

        Assert.Multiple(() =>
        {
            Assert.That(draft.ScopeKind, Is.EqualTo(AccessRuleDraft.TenantCapabilityScope));
            Assert.That(draft.IsTenantCapability, Is.True);
            Assert.That(draft.Tenant, Is.EqualTo("acme"));
            Assert.That(draft.ToRule(), Is.EqualTo(rule));
        });
    }

    [Test]
    public void The_scope_label_names_the_tenant()
    {
        Assert.That(
            AccessRuleFormat.ScopeLabel(LatticeScope.TenantCapability(TenantId.Parse("acme"))),
            Is.EqualTo("tenant acme capability (telemetry)"));
    }

    private static AccessRuleDraft Draft(string tenant, LatticeOperation operations)
    {
        var draft = AccessRuleDraft.New();
        draft.RuleId = "acme-telemetry";
        draft.SubjectKind = LatticeSubjectSelectorKind.User;
        draft.SubjectId = "ada";
        draft.ScopeKind = AccessRuleDraft.TenantCapabilityScope;
        draft.Tenant = tenant;
        draft.Operations = operations;
        return draft;
    }
}
