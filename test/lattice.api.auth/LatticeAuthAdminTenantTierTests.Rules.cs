using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// The rule administration half of <see cref="LatticeAuthAdminTenantTierTests"/>:
/// operators cannot author tenant-tier rules or tenant-wide scopes (D7, D9), may
/// remove a tenant-tier rule as a break-glass action, and see tenant-tier rules in
/// both rule listings, marked with their tenant.
/// </summary>
public sealed partial class LatticeAuthAdminTenantTierTests
{
    private const string TenantTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Globex = TenantId.Parse("globex");

    private static LatticeAuthorizationRule OperatorRule(string id, LatticeScope scope, LatticeEffect effect = LatticeEffect.Allow) =>
        new(id, LatticeSubjectSelector.User(UserId), scope, LatticeOperation.Read, effect);

    private static LatticeAuthorizationRule TenantRule(TenantId tenant, string localId, LatticeScope scope, LatticeEffect effect = LatticeEffect.Allow) =>
        new(LatticeTenantRuleIds.For(tenant, localId), LatticeSubjectSelector.User(UserId), scope, LatticeOperation.Read, effect);

    // ----- PutRuleAsync: operators never author the tenant tier (D7, D9) -----

    [Test]
    public void PutRuleAsync_refuses_a_tenant_tier_rule_id_before_reaching_the_store()
    {
        var store = new InMemoryPolicyStore();
        var rule = TenantRule(Acme, "read", LatticeScope.Tree(TenantTree));

        var ex = Assert.ThrowsAsync<LatticeTenantOwnedRuleException>(
            () => CreateAdmin(store: store).PutRuleAsync(rule));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.RuleId, Is.EqualTo(rule.RuleId));
            Assert.That(ex.ParamName, Is.EqualTo("rule"));
            Assert.That(store.Puts, Is.Empty);
        });
    }

    [Test]
    public void PutRuleAsync_refuses_a_malformed_tenant_tier_rule_id()
    {
        // The whole tenant: prefix is reserved, not only the strict tenant:{tenant}:{id} shape.
        var store = new InMemoryPolicyStore();
        var rule = OperatorRule("tenant:not a tenant", LatticeScope.Tree(TenantTree));

        Assert.ThrowsAsync<LatticeTenantOwnedRuleException>(() => CreateAdmin(store: store).PutRuleAsync(rule));
        Assert.That(store.Puts, Is.Empty);
    }

    [TestCase("t/acme/*")]
    [TestCase("t/default/*")]
    [TestCase("t/Not A Tenant/*")]
    public void PutRuleAsync_refuses_a_tenant_wide_scope_on_an_operator_rule(string treeId)
    {
        var store = new InMemoryPolicyStore();
        var rule = OperatorRule("op-wide", LatticeScope.Tree(treeId));

        var ex = Assert.ThrowsAsync<ArgumentException>(() => CreateAdmin(store: store).PutRuleAsync(rule));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("rule"));
            Assert.That(ex.Message, Does.Contain("ILatticeTenantPolicyAdmin"),
                "the refusal must point the operator at the tenant policy facade");
            Assert.That(store.Puts, Is.Empty);
        });
    }

    [Test]
    public void PutRuleAsync_refuses_a_tenant_wide_operator_rule_built_with_the_tenant_wide_factory()
    {
        var store = new InMemoryPolicyStore();

        Assert.ThrowsAsync<ArgumentException>(
            () => CreateAdmin(store: store).PutRuleAsync(OperatorRule("op-wide", LatticeScope.TenantWide(Acme))));
        Assert.That(store.Puts, Is.Empty);
    }

    [Test]
    public void PutRuleAsync_reports_the_reserved_rule_id_before_the_tenant_wide_scope()
    {
        var rule = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));

        Assert.ThrowsAsync<LatticeTenantOwnedRuleException>(() => CreateAdmin().PutRuleAsync(rule));
    }

    [TestCase(TenantTree)]
    [TestCase("orders")]
    [TestCase("*")]
    [TestCase("t/acme/a/crm")]
    public async Task PutRuleAsync_writes_an_operator_rule(string treeId)
    {
        var store = new InMemoryPolicyStore();
        var rule = OperatorRule("op-read", LatticeScope.Tree(treeId));

        await CreateAdmin(store: store).PutRuleAsync(rule);

        Assert.That(store.Puts, Is.EqualTo(new[] { rule }));
    }

    // ----- RemoveRuleAsync: break-glass removal of a tenant-tier rule -----

    [Test]
    public async Task RemoveRuleAsync_removes_a_tenant_tier_rule_under_system_origin()
    {
        var rule = TenantRule(Acme, "read", LatticeScope.Tree(TenantTree));
        var store = new InMemoryPolicyStore().Seed(rule);

        var removed = await CreateAdmin(store: store).RemoveRuleAsync(TenantTree, rule.RuleId);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(store.Removes, Is.EqualTo(new[] { (TenantTree, rule.RuleId, true) }));
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the system-origin scope must not leak to the caller");
        });
    }

    [Test]
    public async Task RemoveRuleAsync_removes_a_tenant_wide_rule_under_system_origin()
    {
        var rule = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var store = new InMemoryPolicyStore().Seed(rule);

        var removed = await CreateAdmin(store: store).RemoveRuleAsync(rule.Scope.TreeId, rule.RuleId);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(store.Removes.Single().SystemOrigin, Is.True);
        });
    }

    [Test]
    public async Task RemoveRuleAsync_keeps_an_operator_rule_removal_outside_system_origin()
    {
        var rule = OperatorRule("op-read", LatticeScope.Tree(TenantTree));
        var store = new InMemoryPolicyStore().Seed(rule);

        var removed = await CreateAdmin(store: store).RemoveRuleAsync(TenantTree, rule.RuleId);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(store.Removes.Single().SystemOrigin, Is.False);
        });
    }

    [Test]
    public async Task RemoveRuleAsync_of_an_absent_tenant_tier_rule_reports_false()
    {
        var store = new InMemoryPolicyStore();

        var removed = await CreateAdmin(store: store).RemoveRuleAsync(TenantTree, LatticeTenantRuleIds.For(Acme, "gone"));

        Assert.That(removed, Is.False);
    }

    // ----- ListRulesAsync / ListRulesForTreeAsync: tenant-tier rules are listed -----

    [Test]
    public async Task ListRulesAsync_lists_tenant_tier_rules_marked_with_their_tenant()
    {
        var operatorRule = OperatorRule("op-read", LatticeScope.Tree("orders"));
        var tenantRule = TenantRule(Acme, "read", LatticeScope.Tree(TenantTree));
        var wideRule = TenantRule(Globex, "wide", LatticeScope.TenantWide(Globex));
        var store = new InMemoryPolicyStore().Seed(tenantRule, wideRule, operatorRule);

        var page = await CreateAdmin(store: store).ListRulesAsync(new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { operatorRule, tenantRule, wideRule }));
            Assert.That(page.TenantRuleTenants, Is.EqualTo(new string?[] { null, "acme", "globex" }));
        });
    }

    [Test]
    public async Task ListRulesAsync_leaves_the_tenant_marks_empty_when_no_rule_is_tenant_tier()
    {
        var store = new InMemoryPolicyStore().Seed(
            OperatorRule("op-1", LatticeScope.Tree("orders")),
            OperatorRule("op-2", LatticeScope.Tree(TenantTree)));

        var page = await CreateAdmin(store: store).ListRulesAsync(new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(2));
            Assert.That(page.TenantRuleTenants, Is.Empty);
        });
    }

    [Test]
    public async Task ListRulesForTreeAsync_folds_the_owning_tenants_tenant_wide_rules_into_a_tenant_tree()
    {
        var allTrees = OperatorRule("all", LatticeScope.Tree(LatticeScope.ClusterWideTreeId));
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var operatorRule = OperatorRule("op-read", LatticeScope.Tree(TenantTree));
        var tenantRule = TenantRule(Acme, "read", LatticeScope.Tree(TenantTree));
        var store = new InMemoryPolicyStore().Seed(
            tenantRule,
            wide,
            operatorRule,
            allTrees,
            TenantRule(Globex, "wide", LatticeScope.TenantWide(Globex)),
            TenantRule(Acme, "other", LatticeScope.Tree("t/acme/other")),
            OperatorRule("unrelated", LatticeScope.Tree("orders")));

        var page = await CreateAdmin(store: store).ListRulesForTreeAsync(TenantTree, new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { allTrees, wide, operatorRule, tenantRule }));
            Assert.That(page.TenantRuleTenants, Is.EqualTo(new string?[] { null, "acme", null, "acme" }));
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListRulesForTreeAsync_pages_monotonically_across_the_folded_buckets()
    {
        var allTrees = OperatorRule("all", LatticeScope.Tree(LatticeScope.ClusterWideTreeId));
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var operatorRule = OperatorRule("op-read", LatticeScope.Tree(TenantTree));
        var tenantRule = TenantRule(Acme, "read", LatticeScope.Tree(TenantTree));
        var admin = CreateAdmin(store: new InMemoryPolicyStore().Seed(tenantRule, wide, operatorRule, allTrees));

        var seen = new List<LatticeAuthorizationRule>();
        string? token = null;
        do
        {
            var page = await admin.ListRulesForTreeAsync(TenantTree, new AuthPageRequest { PageSize = 1, PageToken = token });
            seen.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (token is not null);

        Assert.That(seen, Is.EqualTo(new[] { allTrees, wide, operatorRule, tenantRule }));
    }

    [TestCase("orders")]
    [TestCase("t/acme/a/crm")]
    [TestCase("t/acme/sys-audit")]
    [TestCase("t/default/orders")]
    public async Task ListRulesForTreeAsync_folds_no_tenant_wide_rules_into_a_tree_the_tenant_layer_does_not_govern(string treeId)
    {
        var onTree = OperatorRule("op-read", LatticeScope.Tree(treeId));
        var store = new InMemoryPolicyStore().Seed(
            onTree,
            TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme)));

        var page = await CreateAdmin(store: store).ListRulesForTreeAsync(treeId, new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { onTree }));
            Assert.That(page.TenantRuleTenants, Is.Empty);
        });
    }

    [Test]
    public async Task ListRulesForTreeAsync_of_the_tenant_wide_bucket_lists_it_once()
    {
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var allTrees = OperatorRule("all", LatticeScope.Tree(LatticeScope.ClusterWideTreeId));
        var store = new InMemoryPolicyStore().Seed(wide, allTrees);

        var page = await CreateAdmin(store: store).ListRulesForTreeAsync("t/acme/*", new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { allTrees, wide }));
            Assert.That(page.TenantRuleTenants, Is.EqualTo(new string?[] { null, "acme" }));
        });
    }

    // ----- Internal helpers -----

    [TestCase(TenantTree, "t/acme/*")]
    [TestCase("t/acme/deep/tree", "t/acme/*")]
    public void TryGetTenantWideTreeId_resolves_the_owning_tenants_sentinel(string treeId, string expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeAuthAdmin.TryGetTenantWideTreeId(treeId, out var wide), Is.True);
            Assert.That(wide, Is.EqualTo(expected));
        });
    }

    [TestCase("orders")]
    [TestCase("*")]
    [TestCase("t/acme/*")]
    [TestCase("t/acme/a/crm")]
    [TestCase("t/default/orders")]
    [TestCase("t/")]
    public void TryGetTenantWideTreeId_is_false_outside_the_tenant_layer(string treeId)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeAuthAdmin.TryGetTenantWideTreeId(treeId, out var wide), Is.False);
            Assert.That(wide, Is.Null);
        });
    }

    [Test]
    public void TenantRuleTenantsOf_an_empty_page_is_empty() =>
        Assert.That(LatticeAuthAdmin.TenantRuleTenantsOf(Array.Empty<LatticeAuthorizationRule>()), Is.Empty);

    [Test]
    public void IsOwnedBy_attributes_a_tenant_wide_rule_to_its_tenant_only()
    {
        // ActiveTenantOnly narrows the catalogue with IsOwnedBy, so a tenant's own
        // tenant-wide rules appear in its narrowed listing and in no other tenant's.
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));

        Assert.Multiple(() =>
        {
            Assert.That(LatticeAuthAdmin.IsOwnedBy(wide, Acme), Is.True);
            Assert.That(LatticeAuthAdmin.IsOwnedBy(wide, Globex), Is.False);
            Assert.That(LatticeAuthAdmin.IsOwnedBy(wide, TenantId.Default), Is.False);
        });
    }
}
