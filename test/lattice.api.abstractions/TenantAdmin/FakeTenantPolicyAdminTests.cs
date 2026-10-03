using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Fakes;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Proves the shared <see cref="FakeTenantPolicyAdmin"/> honours the
/// <see cref="ILatticeTenantPolicyAdmin"/> contract the Explorer pages and bindings
/// are tested against: confinement, the cap, listing visibility, the layering of
/// an explanation, withheld platform-wide and app rules, and the posture probe.
/// </summary>
[TestFixture]
public sealed class FakeTenantPolicyAdminTests
{
    private const string Tenant = "acme";

    private static TenantRuleDraft Draft(
        string ruleId,
        LatticeEffect effect = LatticeEffect.Allow,
        TenantRuleScopeKind scope = TenantRuleScopeKind.Tree,
        string? tree = "orders",
        string? keyOrPrefix = null) => new()
        {
            RuleId = ruleId,
            SubjectId = "bob",
            ScopeKind = scope,
            TreeName = tree,
            KeyOrPrefix = keyOrPrefix,
            Operations = LatticeOperation.Read,
            Effect = effect,
        };

    private static TenantRuleView Platform(string ruleId, TenantRuleOrigin origin, LatticeEffect effect) => new()
    {
        RuleId = ruleId,
        Origin = origin,
        SubjectId = "bob",
        ScopeKind = TenantRuleScopeKind.Tree,
        TreeName = "orders",
        Operations = LatticeOperation.Read,
        Effect = effect,
    };

    [Test]
    public async Task A_rule_round_trips_as_an_editable_tenant_rule()
    {
        var policy = new FakeTenantPolicyAdmin();

        var stored = await policy.PutRuleAsync(Tenant, Draft("r1"));

        Assert.Multiple(async () =>
        {
            Assert.That(stored.Layer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(stored.Origin, Is.EqualTo(TenantRuleOrigin.Tenant));
            Assert.That(stored.Editable, Is.True);
            Assert.That(await policy.GetRuleAsync(Tenant, "r1"), Is.EqualTo(stored));
            Assert.That(await policy.RemoveRuleAsync(Tenant, "r1"), Is.True);
            Assert.That(await policy.RemoveRuleAsync(Tenant, "r1"), Is.False);
            Assert.That(await policy.GetRuleAsync(Tenant, "r1"), Is.Null);
        });
    }

    [TestCase("tenant:acme:r1", TenantAccessConfinementRule.ReservedRuleId)]
    [TestCase("app:inventory:reader", TenantAccessConfinementRule.ReservedRuleId)]
    public void A_reserved_rule_id_is_a_confinement_failure(string ruleId, TenantAccessConfinementRule rule)
    {
        var exception = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => new FakeTenantPolicyAdmin().PutRuleAsync(Tenant, Draft(ruleId)));

        Assert.That(exception!.Rule, Is.EqualTo(rule));
    }

    [Test]
    public void An_operation_outside_the_data_plane_mask_is_a_confinement_failure()
    {
        var exception = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => new FakeTenantPolicyAdmin().PutRuleAsync(Tenant, Draft("r1") with { Operations = LatticeOperation.Telemetry }));

        Assert.That(exception!.Rule, Is.EqualTo(TenantAccessConfinementRule.RuleOperations));
    }

    [Test]
    public void An_app_owned_tree_is_a_confinement_failure()
    {
        var exception = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => new FakeTenantPolicyAdmin().PutRuleAsync(Tenant, Draft("r1", tree: "a/inventory")));

        Assert.That(exception!.Rule, Is.EqualTo(TenantAccessConfinementRule.RuleTree));
    }

    [TestCase(TenantRuleScopeKind.Tree, "orders", "k")]
    [TestCase(TenantRuleScopeKind.Key, "orders", null)]
    [TestCase(TenantRuleScopeKind.Prefix, null, "p")]
    [TestCase(TenantRuleScopeKind.TenantWide, "orders", null)]
    public void A_scope_shape_its_kind_does_not_allow_is_an_argument_error(TenantRuleScopeKind scope, string? tree, string? keyOrPrefix) =>
        Assert.ThrowsAsync<ArgumentException>(
            () => new FakeTenantPolicyAdmin().PutRuleAsync(Tenant, Draft("r1", scope: scope, tree: tree, keyOrPrefix: keyOrPrefix)));

    [Test]
    public void A_rule_covering_no_operation_is_an_argument_error() =>
        Assert.ThrowsAsync<ArgumentException>(
            () => new FakeTenantPolicyAdmin().PutRuleAsync(Tenant, Draft("r1") with { Operations = LatticeOperation.None }));

    [Test]
    public async Task The_rule_cap_refuses_a_new_rule_but_not_a_replacement()
    {
        var policy = new FakeTenantPolicyAdmin { MaxTenantRules = 1 };
        await policy.PutRuleAsync(Tenant, Draft("r1"));

        Assert.ThrowsAsync<LatticeQuotaExceededException>(() => policy.PutRuleAsync(Tenant, Draft("r2")));
        Assert.DoesNotThrowAsync(() => policy.PutRuleAsync(Tenant, Draft("r1", LatticeEffect.Deny)));
    }

    [Test]
    public async Task The_listing_shows_tenant_rules_and_platform_tree_rules_but_never_withheld_ones()
    {
        var policy = new FakeTenantPolicyAdmin();
        policy.SeedPlatformRule(Tenant, Platform("op-orders", TenantRuleOrigin.PlatformTree, LatticeEffect.Allow));
        policy.SeedPlatformRule(Tenant, Platform("op-wide", TenantRuleOrigin.PlatformWide, LatticeEffect.Deny));
        policy.SeedPlatformRule(Tenant, Platform("app:inv:reader", TenantRuleOrigin.AppRole, LatticeEffect.Allow));
        await policy.PutRuleAsync(Tenant, Draft("wide", scope: TenantRuleScopeKind.TenantWide, tree: null));
        await policy.PutRuleAsync(Tenant, Draft("t1"));

        var first = await policy.ListRulesAsync(Tenant, new TenantAccessPageRequest { PageSize = 2 });
        var second = await policy.ListRulesAsync(Tenant, new TenantAccessPageRequest { PageSize = 2, PageToken = first.NextPageToken });

        var listed = first.Entries.Concat(second.Entries).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(listed.Select(r => r.RuleId), Is.EqualTo(new[] { "wide", "op-orders", "t1" }));
            Assert.That(listed.Single(r => r.RuleId == "op-orders").Editable, Is.False);
            Assert.That(listed.Single(r => r.RuleId == "op-orders").Layer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(second.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void Seeding_a_tenant_origin_platform_rule_is_refused() =>
        Assert.That(
            () => new FakeTenantPolicyAdmin().SeedPlatformRule(Tenant, Platform("x", TenantRuleOrigin.Tenant, LatticeEffect.Allow)),
            Throws.ArgumentException);

    [Test]
    public async Task A_matching_platform_rule_is_final_over_a_tenant_allow()
    {
        var policy = new FakeTenantPolicyAdmin();
        policy.SeedPlatformRule(Tenant, Platform("op-deny", TenantRuleOrigin.PlatformTree, LatticeEffect.Deny));
        await policy.PutRuleAsync(Tenant, Draft("t-allow", scope: TenantRuleScopeKind.Key, keyOrPrefix: "k1"));

        var explanation = await policy.ExplainAsync(Tenant, "bob", "orders", "k1", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.False);
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("op-deny"));
            Assert.That(explanation.MatchedRules.Select(r => r.RuleId), Is.EqualTo(new[] { "op-deny", "t-allow" }));
        });
    }

    [Test]
    public async Task A_platform_wide_deciding_rule_is_reported_by_id_and_effect_only()
    {
        var policy = new FakeTenantPolicyAdmin();
        policy.SeedPlatformRule(Tenant, Platform("op-wide", TenantRuleOrigin.PlatformWide, LatticeEffect.Allow));

        var explanation = await policy.ExplainAsync(Tenant, "bob", "orders", null, LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.True);
            Assert.That(explanation.DecidingRule!.SubjectWithheld, Is.True);
            Assert.That(explanation.DecidingRule.SubjectId, Is.Null);
            Assert.That(explanation.DecidingRule.TreeName, Is.Null);
            Assert.That(explanation.DecidingRule.Effect, Is.EqualTo(LatticeEffect.Allow));
            Assert.That(explanation.MatchedRules, Is.Empty);
        });
    }

    [Test]
    public async Task In_the_tenant_layer_a_tenant_wide_deny_beats_a_specific_allow()
    {
        var policy = new FakeTenantPolicyAdmin();
        await policy.PutRuleAsync(Tenant, Draft("wide-deny", LatticeEffect.Deny, TenantRuleScopeKind.TenantWide, tree: null));
        await policy.PutRuleAsync(Tenant, Draft("key-allow", scope: TenantRuleScopeKind.Key, keyOrPrefix: "k1"));

        var explanation = await policy.ExplainAsync(Tenant, "bob", "orders", "k1", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.False);
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("wide-deny"));
        });
    }

    [Test]
    public async Task In_the_tenant_layer_the_most_specific_tree_rule_beats_a_tenant_wide_allow()
    {
        var policy = new FakeTenantPolicyAdmin();
        await policy.PutRuleAsync(Tenant, Draft("wide-allow", scope: TenantRuleScopeKind.TenantWide, tree: null));
        await policy.PutRuleAsync(Tenant, Draft("tree-allow"));
        await policy.PutRuleAsync(Tenant, Draft("prefix-deny", LatticeEffect.Deny, TenantRuleScopeKind.Prefix, keyOrPrefix: "eu/"));

        var denied = await policy.ExplainAsync(Tenant, "bob", "orders", "eu/1", LatticeOperation.Read);
        var allowed = await policy.ExplainAsync(Tenant, "bob", "orders", "us/1", LatticeOperation.Read);
        var otherTree = await policy.ExplainAsync(Tenant, "bob", "invoices", null, LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(denied.DecidingRuleId, Is.EqualTo("prefix-deny"));
            Assert.That(denied.Allowed, Is.False);
            Assert.That(allowed.DecidingRuleId, Is.EqualTo("tree-allow"));
            Assert.That(otherTree.DecidingRuleId, Is.EqualTo("wide-allow"));
            Assert.That(otherTree.Allowed, Is.True);
        });
    }

    [Test]
    public async Task With_no_matching_rule_the_default_effect_decides()
    {
        var policy = new FakeTenantPolicyAdmin();

        var explanation = await policy.ExplainAsync(Tenant, "bob", "orders", null, LatticeOperation.Write);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.False);
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRule, Is.Null);
            Assert.That(explanation.DefaultEffect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(explanation.Reason, Is.Not.Null);
        });
    }

    [Test]
    public async Task Effective_permissions_list_both_layers_and_withhold_app_rules()
    {
        var policy = new FakeTenantPolicyAdmin();
        policy.SeedPlatformRule(Tenant, Platform("app:inv:reader", TenantRuleOrigin.AppRole, LatticeEffect.Allow));
        await policy.PutRuleAsync(Tenant, Draft("t1"));
        await policy.PutRuleAsync(Tenant, Draft("other-tree", tree: "invoices"));

        var all = await policy.EffectivePermissionsAsync(Tenant, "bob");
        var orders = await policy.EffectivePermissionsAsync(Tenant, "bob", "orders");

        Assert.Multiple(() =>
        {
            Assert.That(all.Rules.Select(r => r.RuleId), Is.EqualTo(new[] { "app:inv:reader", "other-tree", "t1" }));
            Assert.That(all.Rules[0].SubjectWithheld, Is.True);
            Assert.That(all.Rules[0].SubjectId, Is.Null);
            Assert.That(orders.TreeName, Is.EqualTo("orders"));
            Assert.That(orders.Rules.Select(r => r.RuleId), Is.EqualTo(new[] { "app:inv:reader", "t1" }));
        });
    }

    [Test]
    public async Task The_posture_answers_while_disabled_and_reports_rule_usage()
    {
        var policy = new FakeTenantPolicyAdmin { CallerIsPlatformOperator = true, MaxTenantRules = 10 };
        await policy.PutRuleAsync(Tenant, Draft("r1"));
        policy.Gate.Enabled = false;

        var posture = await policy.GetPostureAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(posture.Enabled, Is.False);
            Assert.That(posture.CallerIsTenantAdmin, Is.True);
            Assert.That(posture.CallerIsPlatformOperator, Is.True);
            Assert.That(posture.TenantRules.Usage, Is.EqualTo(1));
            Assert.That(posture.TenantRules.Limit, Is.EqualTo(10));
            Assert.That(posture.Groups.Limit, Is.EqualTo(500));
            Assert.That(posture.MembershipEdges.Limit, Is.EqualTo(10000));
            Assert.That(posture.MemberSubjects.Limit, Is.EqualTo(5000));
        });
        Assert.ThrowsAsync<TenantAccessAdministrationDisabledException>(() => policy.GetRuleAsync(Tenant, "r1"));
    }

    [Test]
    public void The_posture_is_still_denied_to_an_unauthorized_caller()
    {
        var policy = new FakeTenantPolicyAdmin();
        policy.Gate.Denied = true;

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => policy.GetPostureAsync(Tenant));
    }

    [Test]
    public void The_posture_refuses_the_default_tenant() =>
        Assert.ThrowsAsync<ReservedTenantOperationException>(() => new FakeTenantPolicyAdmin().GetPostureAsync("default"));

    [Test]
    public async Task RemoveRulesNaming_removes_only_rules_on_the_named_tenant_group()
    {
        var policy = new FakeTenantPolicyAdmin();
        await policy.PutRuleAsync(Tenant, Draft("user-rule"));
        await policy.PutRuleAsync(Tenant, Draft("group-rule") with { SubjectId = "readers", SubjectKind = TenantSubjectKind.TenantGroup });
        await policy.PutRuleAsync(Tenant, Draft("cluster-rule") with { SubjectId = "readers", SubjectKind = TenantSubjectKind.ClusterGroup });

        var removed = policy.RemoveRulesNaming(Tenant, "readers");

        Assert.Multiple(async () =>
        {
            Assert.That(removed, Is.EqualTo(new[] { "group-rule" }));
            Assert.That(await policy.GetRuleAsync(Tenant, "cluster-rule"), Is.Not.Null);
            Assert.That(policy.RemoveRulesNaming("globex", "readers"), Is.Empty);
        });
    }
}
