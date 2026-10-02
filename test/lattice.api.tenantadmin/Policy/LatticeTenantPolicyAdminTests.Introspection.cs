using Orleans.Lattice.Auth;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Policy.TenantPolicyTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>Layer-aware explain and effective permissions.</summary>
public sealed partial class LatticeTenantPolicyAdminTests
{
    private static TenantPolicyVerdict Decided(
        TenantRuleLayer layer,
        string ruleId,
        LatticeEffect effect = LatticeEffect.Allow,
        bool allTrees = false,
        bool tenantWide = false) =>
        new(effect == LatticeEffect.Allow, false, effect == LatticeEffect.Allow ? null : "denied", layer, ruleId, effect, allTrees, tenantWide);

    [Test]
    public async Task Explain_refuses_with_the_tenant_gate_verdict_when_the_subject_cannot_act_as_the_tenant()
    {
        var harness = new Harness();
        var explanation = await harness.Create().ExplainAsync(Tenant, Stranger, "orders", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.False);
            Assert.That(explanation.Reason, Does.Contain("cannot act as tenant 'acme'"));
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRule, Is.Null);
            Assert.That(harness.Decisions.Evaluations, Is.Empty, "the rules are never consulted past a tenant-gate refusal");
        });
    }

    [Test]
    public async Task Explain_admits_a_subject_through_a_tenant_group_in_the_member_set_and_passes_its_groups_to_the_engine()
    {
        // Only the group is in the member set, so the subject passes the tenant gate
        // only when its resolved groups reach the check.
        var harness = new Harness();
        harness.Directory.WithGroups(Member, "t/acme/readers", "entra-staff");
        harness.AdmitMember("t/acme/readers");

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        var evaluation = harness.Decisions.Evaluations.Single();
        Assert.Multiple(() =>
        {
            Assert.That(explanation.Reason, Does.Not.Contain("cannot act as tenant"));
            Assert.That(evaluation.Subject.GroupIds, Is.EquivalentTo(new[] { "t/acme/readers", "entra-staff" }));
            Assert.That(evaluation.TreeId, Is.EqualTo("t/acme/orders"));
            Assert.That(evaluation.Key, Is.EqualTo("k"));
            Assert.That(evaluation.Operation, Is.EqualTo(LatticeOperation.Read));
        });
    }

    [Test]
    public async Task Explain_decides_the_tenant_gate_from_the_registry_record_it_authorized_against()
    {
        // The compiled tenant snapshot rebuilds asynchronously after a registry
        // write; the explanation reads the record itself, so a member recorded a
        // moment ago is already admitted (the gate's own registry confirmation rule).
        var harness = new Harness();
        var facade = harness.Create();
        var before = await facade.ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        harness.AdmitMember(Member);
        var after = await facade.ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(before.Reason, Does.Contain("cannot act as tenant"));
            Assert.That(after.Reason, Does.Not.Contain("cannot act as tenant"));
            Assert.That(harness.Decisions.Evaluations, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Explain_admits_a_tenant_admin_without_a_member_entry()
    {
        var harness = new Harness();

        await harness.Create().ExplainAsync(Tenant, Admin, "orders", "k", LatticeOperation.Read);

        Assert.That(harness.Decisions.Evaluations, Has.Count.EqualTo(1), "admins are implicitly members");
    }

    [Test]
    public async Task Explain_reports_a_tenant_layer_decision_with_the_local_rule_in_full()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(TenantRule(Tenant, "grant", "orders"));
        harness.Decisions.Verdict = Decided(TenantRuleLayer.Tenant, "tenant:acme:grant");

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.True);
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("grant"));
            Assert.That(explanation.DecidingRule!.Editable, Is.True);
            Assert.That(explanation.DecidingRule.SubjectId, Is.EqualTo(Member));
            Assert.That(explanation.MatchedRules.Select(r => r.RuleId), Is.EqualTo(new[] { "grant" }));
        });
    }

    [Test]
    public async Task Explain_reports_a_tenant_wide_decision_read_from_the_sentinel_bucket()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(new LatticeAuthorizationRule(
            "tenant:acme:wide",
            LatticeSubjectSelector.User(Member),
            LatticeScope.TenantWide(TenantId.Parse(Tenant)),
            LatticeOperation.Read,
            LatticeEffect.Allow));
        harness.Decisions.Verdict = Decided(TenantRuleLayer.Tenant, "tenant:acme:wide", tenantWide: true);

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", null, LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingRule!.RuleId, Is.EqualTo("wide"));
            Assert.That(explanation.DecidingRule.ScopeKind, Is.EqualTo(TenantRuleScopeKind.TenantWide));
            Assert.That(explanation.MatchedRules.Single().RuleId, Is.EqualTo("wide"));
        });
    }

    [Test]
    public async Task Explain_names_the_platform_layer_when_an_operator_deny_on_the_tree_decides()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(TenantRule(Tenant, "grant", "orders"));
        harness.Store.Seed(OperatorRule("guardrail", "t/acme/orders", effect: LatticeEffect.Deny));
        harness.Decisions.Verdict = Decided(TenantRuleLayer.Platform, "guardrail", LatticeEffect.Deny);

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.False);
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("guardrail"));
            Assert.That(explanation.DecidingRule!.Origin, Is.EqualTo(TenantRuleOrigin.PlatformTree));
            Assert.That(explanation.DecidingRule.Editable, Is.False);
            Assert.That(
                explanation.MatchedRules.Select(r => r.RuleId),
                Is.EqualTo(new[] { "guardrail", "grant" }),
                "the platform layer is listed first");
        });
    }

    [Test]
    public async Task Explain_withholds_the_subject_of_a_deciding_platform_wide_rule()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(OperatorRule("everywhere", LatticeScope.ClusterWideTreeId));
        harness.Decisions.Verdict = Decided(TenantRuleLayer.Platform, "everywhere", allTrees: true);

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        var deciding = explanation.DecidingRule!;
        Assert.Multiple(() =>
        {
            Assert.That(deciding.RuleId, Is.EqualTo("everywhere"));
            Assert.That(deciding.Origin, Is.EqualTo(TenantRuleOrigin.PlatformWide));
            Assert.That(deciding.SubjectWithheld, Is.True);
            Assert.That(deciding.SubjectId, Is.Null);
            Assert.That(deciding.TreeName, Is.Null);
            Assert.That(deciding.Operations, Is.EqualTo(LatticeOperation.None));
            Assert.That(deciding.Effect, Is.EqualTo(LatticeEffect.Allow));
            Assert.That(explanation.MatchedRules, Is.Empty, "a platform-wide rule is never listed in full");
        });
    }

    [Test]
    public async Task Explain_withholds_the_subject_of_a_deciding_app_role_rule()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(OperatorRule("app:shop:reader:1", "t/acme/a/shop/items"));
        harness.Decisions.Verdict = Decided(TenantRuleLayer.Platform, "app:shop:reader:1");

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "a/shop/items", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingRule!.Origin, Is.EqualTo(TenantRuleOrigin.AppRole));
            Assert.That(explanation.DecidingRule.SubjectWithheld, Is.True);
            Assert.That(explanation.MatchedRules, Is.Empty);
        });
    }

    [Test]
    public async Task Explain_reports_no_deciding_rule_when_the_default_effect_decides()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Decisions.DefaultEffect = LatticeEffect.Deny;

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRule, Is.Null);
            Assert.That(explanation.DefaultEffect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(explanation.TreeName, Is.EqualTo("orders"));
            Assert.That(explanation.Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public async Task Explain_names_a_deciding_rule_the_store_no_longer_holds_by_id_and_effect()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Decisions.Verdict = Decided(TenantRuleLayer.Tenant, "tenant:acme:gone");

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingRule!.RuleId, Is.EqualTo("gone"));
            Assert.That(explanation.DecidingRule.Layer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRule.SubjectId, Is.Null);
        });
    }

    [Test]
    public async Task Explain_matched_rules_honour_the_operation_the_subject_and_the_key()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(TenantRule(Tenant, "read", "orders"));
        harness.Store.Seed(TenantRule(Tenant, "write", "orders", operations: LatticeOperation.Write));
        harness.Store.Seed(TenantRule(Tenant, "other-user", "orders", LatticeSubjectSelector.User("carol")));
        harness.Store.Seed(new LatticeAuthorizationRule(
            "tenant:acme:key-hit", LatticeSubjectSelector.User(Member), LatticeScope.Key("t/acme/orders", "k"), LatticeOperation.Read, LatticeEffect.Allow));
        harness.Store.Seed(new LatticeAuthorizationRule(
            "tenant:acme:key-miss", LatticeSubjectSelector.User(Member), LatticeScope.Key("t/acme/orders", "z"), LatticeOperation.Read, LatticeEffect.Allow));
        harness.Store.Seed(new LatticeAuthorizationRule(
            "tenant:acme:prefix-hit", LatticeSubjectSelector.User(Member), LatticeScope.Prefix("t/acme/orders", "k"), LatticeOperation.Read, LatticeEffect.Allow));
        harness.Store.Seed(TenantRule(Tenant, "other-tree", "invoices"));

        var explanation = await harness.Create().ExplainAsync(Tenant, Member, "orders", "k", LatticeOperation.Read);

        Assert.That(
            explanation.MatchedRules.Select(r => r.RuleId),
            Is.EqualTo(new[] { "key-hit", "prefix-hit", "read" }));
    }

    [Test]
    public async Task Explain_for_a_tenant_group_evaluates_a_member_of_the_composed_group()
    {
        var harness = new Harness();
        harness.Directory.WithClosure("t/acme/readers", "entra-staff");
        harness.AdmitMember("t/acme/readers");

        await harness.Create().ExplainAsync(
            Tenant, "readers", "orders", null, LatticeOperation.Read, TenantSubjectKind.TenantGroup);

        var subject = harness.Decisions.Evaluations.Single().Subject;
        Assert.Multiple(() =>
        {
            Assert.That(harness.Directory.Expanded, Is.EqualTo(new[] { "t/acme/readers" }));
            Assert.That(subject.SubjectId, Is.EqualTo("t/acme/readers"));
            Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "t/acme/readers", "entra-staff" }));
        });
    }

    [Test]
    public void Explain_for_a_cluster_group_in_the_tenant_group_namespace_is_refused()
    {
        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(() => new Harness().Create().ExplainAsync(
            Tenant, "t/globex/readers", "orders", null, LatticeOperation.Read, TenantSubjectKind.ClusterGroup));
        Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
    }

    [TestCase("sys-audit")]
    [TestCase("_lattice_queue")]
    [TestCase("*")]
    public void Explain_on_a_tree_outside_the_tenant_is_refused_never_answered(string treeName)
    {
        var harness = new Harness();
        harness.AdmitMember(Member);

        Assert.That(
            () => harness.Create().ExplainAsync(Tenant, Member, treeName, null, LatticeOperation.Read),
            Throws.TypeOf<TenantAccessConfinementException>());
        Assert.That(harness.Decisions.Evaluations, Is.Empty);
    }

    [Test]
    public void Explain_refuses_malformed_arguments()
    {
        var facade = new Harness().Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.ExplainAsync(Tenant, "", "orders", null, LatticeOperation.Read), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => facade.ExplainAsync(Tenant, Member, "", null, LatticeOperation.Read), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => facade.ExplainAsync(Tenant, Member, "orders", "", LatticeOperation.Read), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => facade.ExplainAsync(Tenant, Member, "orders", null, LatticeOperation.Read, (TenantSubjectKind)9), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task EffectivePermissions_lists_both_layers_and_withholds_platform_wide_and_app_role_rules()
    {
        var harness = new Harness();
        harness.Directory.WithGroups(Member, "t/acme/readers");
        harness.AdmitMember(Member);
        harness.Store.Seed(TenantRule(Tenant, "via-group", "orders", LatticeSubjectSelector.Group("t/acme/readers")));
        harness.Store.Seed(OperatorRule("guardrail", "t/acme/orders", effect: LatticeEffect.Deny));
        harness.Store.Seed(OperatorRule("everywhere", LatticeScope.ClusterWideTreeId));
        harness.Store.Seed(OperatorRule("app:shop:reader:1", "t/acme/a/shop/items"));
        harness.Store.Seed(TenantRule(Tenant, "not-mine", "orders", LatticeSubjectSelector.User("carol")));
        harness.Store.Seed(OperatorRule("elsewhere", "t/globex/orders"));
        harness.Store.Seed(OperatorRule("legacy", "orders"));

        var result = await harness.Create().EffectivePermissionsAsync(Tenant, Member);

        Assert.That(result.Rules.Select(r => r.RuleId), Is.EquivalentTo(new[] { "via-group", "guardrail", "everywhere", "app:shop:reader:1" }));
        Assert.Multiple(() =>
        {
            Assert.That(result.Rules.Single(r => r.RuleId == "everywhere").SubjectWithheld, Is.True);
            Assert.That(result.Rules.Single(r => r.RuleId == "app:shop:reader:1").SubjectWithheld, Is.True);
            Assert.That(result.Rules.Single(r => r.RuleId == "via-group").Layer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(result.Rules[^1].Layer, Is.EqualTo(TenantRuleLayer.Tenant), "the platform layer comes first");
            Assert.That(result.TreeName, Is.Null);
        });
    }

    [Test]
    public async Task EffectivePermissions_narrowed_to_a_tree_keeps_its_own_rules_tenant_wide_rules_and_platform_wide_rules()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(TenantRule(Tenant, "orders-rule", "orders"));
        harness.Store.Seed(TenantRule(Tenant, "invoices-rule", "invoices"));
        harness.Store.Seed(new LatticeAuthorizationRule(
            "tenant:acme:wide", LatticeSubjectSelector.User(Member), LatticeScope.TenantWide(TenantId.Parse(Tenant)), LatticeOperation.Read, LatticeEffect.Allow));
        harness.Store.Seed(OperatorRule("everywhere", LatticeScope.ClusterWideTreeId));

        var result = await harness.Create().EffectivePermissionsAsync(Tenant, Member, "orders");

        Assert.Multiple(() =>
        {
            Assert.That(result.Rules.Select(r => r.RuleId), Is.EquivalentTo(new[] { "orders-rule", "wide", "everywhere" }));
            Assert.That(result.TreeName, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task EffectivePermissions_on_an_app_tree_omits_tenant_wide_rules_which_never_reach_it()
    {
        var harness = new Harness();
        harness.AdmitMember(Member);
        harness.Store.Seed(new LatticeAuthorizationRule(
            "tenant:acme:wide", LatticeSubjectSelector.User(Member), LatticeScope.TenantWide(TenantId.Parse(Tenant)), LatticeOperation.Read, LatticeEffect.Allow));
        harness.Store.Seed(OperatorRule("app:shop:reader:1", "t/acme/a/shop/items"));

        var result = await harness.Create().EffectivePermissionsAsync(Tenant, Member, "a/shop/items");

        Assert.That(result.Rules.Select(r => r.RuleId), Is.EqualTo(new[] { "app:shop:reader:1" }));
    }

    [Test]
    public async Task EffectivePermissions_is_empty_for_a_subject_that_cannot_act_as_the_tenant()
    {
        var harness = new Harness();
        harness.Store.Seed(TenantRule(Tenant, "r1", "orders", LatticeSubjectSelector.User(Stranger)));

        var result = await harness.Create().EffectivePermissionsAsync(Tenant, Stranger);

        Assert.That(result.Rules, Is.Empty);
    }

    [Test]
    public void EffectivePermissions_refuses_malformed_arguments_and_foreign_trees()
    {
        var facade = new Harness().Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.EffectivePermissionsAsync(Tenant, ""), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => facade.EffectivePermissionsAsync(Tenant, Member, ""), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => facade.EffectivePermissionsAsync(Tenant, Member, "sys-audit"), Throws.TypeOf<TenantAccessConfinementException>());
        });
    }
}
