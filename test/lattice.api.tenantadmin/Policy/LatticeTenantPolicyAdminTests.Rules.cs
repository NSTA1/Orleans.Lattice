using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Policy.TenantPolicyTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>Tenant-tier rule authoring, confinement, the rule cap, and listing.</summary>
public sealed partial class LatticeTenantPolicyAdminTests
{
    [Test]
    public async Task PutRule_composes_the_tenant_rule_id_and_tree_and_writes_under_system_origin()
    {
        var harness = new Harness();
        var facade = harness.Create();

        var view = await facade.PutRuleAsync(Tenant, Draft());

        var stored = harness.Store.All.Single();
        Assert.Multiple(() =>
        {
            Assert.That(stored.RuleId, Is.EqualTo("tenant:acme:r1"));
            Assert.That(stored.Scope, Is.EqualTo(LatticeScope.Tree("t/acme/orders")));
            Assert.That(stored.Subject, Is.EqualTo(LatticeSubjectSelector.User(Member)));
            Assert.That(harness.Store.TenantWriteOrigins, Is.EqualTo(new[] { true }), "the store's tenant-tier guard needs system origin");
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the origin scope is closed after the call");
            Assert.That(view.RuleId, Is.EqualTo("r1"), "the view reports the local id");
            Assert.That(view.Layer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(view.Origin, Is.EqualTo(TenantRuleOrigin.Tenant));
            Assert.That(view.Editable, Is.True);
            Assert.That(view.TreeName, Is.EqualTo("orders"));
            Assert.That(view.SubjectId, Is.EqualTo(Member));
        });
    }

    [Test]
    public async Task PutRule_composes_a_tenant_group_subject_and_reports_its_local_name()
    {
        var harness = new Harness();
        var view = await harness.Create().PutRuleAsync(
            Tenant, Draft(subjectId: "readers", subjectKind: TenantSubjectKind.TenantGroup));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Store.All.Single().Subject, Is.EqualTo(LatticeSubjectSelector.Group("t/acme/readers")));
            Assert.That(view.SubjectId, Is.EqualTo("readers"));
            Assert.That(view.SubjectKind, Is.EqualTo(TenantSubjectKind.TenantGroup));
        });
    }

    [Test]
    public async Task PutRule_keeps_a_cluster_group_subject_as_given()
    {
        var harness = new Harness();
        var view = await harness.Create().PutRuleAsync(
            Tenant, Draft(subjectId: "entra-staff", subjectKind: TenantSubjectKind.ClusterGroup));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Store.All.Single().Subject, Is.EqualTo(LatticeSubjectSelector.Group("entra-staff")));
            Assert.That(view.SubjectKind, Is.EqualTo(TenantSubjectKind.ClusterGroup));
        });
    }

    [Test]
    public async Task PutRule_maps_a_tenant_wide_draft_to_the_tenant_wide_scope()
    {
        var harness = new Harness();
        var view = await harness.Create().PutRuleAsync(
            Tenant, Draft(treeName: null, scopeKind: TenantRuleScopeKind.TenantWide));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Store.All.Single().Scope, Is.EqualTo(LatticeScope.TenantWide(TenantId.Parse(Tenant))));
            Assert.That(view.ScopeKind, Is.EqualTo(TenantRuleScopeKind.TenantWide));
            Assert.That(view.TreeName, Is.Null);
        });
    }

    [TestCase(TenantRuleScopeKind.Key, LatticeScopeKind.Key)]
    [TestCase(TenantRuleScopeKind.Prefix, LatticeScopeKind.Prefix)]
    public async Task PutRule_maps_a_key_or_prefix_draft(TenantRuleScopeKind kind, LatticeScopeKind expected)
    {
        var harness = new Harness();
        var view = await harness.Create().PutRuleAsync(Tenant, Draft(scopeKind: kind, keyOrPrefix: "k/"));

        var scope = harness.Store.All.Single().Scope;
        Assert.Multiple(() =>
        {
            Assert.That(scope.Kind, Is.EqualTo(expected));
            Assert.That(scope.TreeId, Is.EqualTo("t/acme/orders"));
            Assert.That(scope.KeyOrPrefix, Is.EqualTo("k/"));
            Assert.That(view.ScopeKind, Is.EqualTo(kind));
            Assert.That(view.KeyOrPrefix, Is.EqualTo("k/"));
        });
    }

    [TestCase("a/inventory/items", TestName = "PutRule_refuses_a_tenant_app_tree")]
    [TestCase("sys-audit", TestName = "PutRule_refuses_a_sys_tree")]
    [TestCase("_lattice_queue", TestName = "PutRule_refuses_a_lattice_system_tree")]
    [TestCase("*", TestName = "PutRule_refuses_the_tenant_wide_sentinel_as_a_tree")]
    public void PutRule_refuses_a_tree_outside_the_tenant_layer(string treeName)
    {
        var harness = new Harness();

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => harness.Create().PutRuleAsync(Tenant, Draft(treeName: treeName)));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.RuleTree));
            Assert.That(ex.TenantId, Is.EqualTo(Tenant));
            Assert.That(harness.Store.Writes, Is.Zero);
        });
    }

    [TestCase(LatticeOperation.Telemetry)]
    [TestCase(LatticeOperation.TreeLifecycle)]
    [TestCase(LatticeOperation.Replication)]
    [TestCase(LatticeOperation.AppInstall)]
    [TestCase(LatticeOperation.Read | LatticeOperation.Telemetry)]
    [TestCase(LatticeOperation.None)]
    public void PutRule_refuses_operations_outside_the_data_plane_mask(LatticeOperation operations)
    {
        var harness = new Harness();

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => harness.Create().PutRuleAsync(Tenant, Draft(operations: operations)));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.RuleOperations));
            Assert.That(harness.Store.Writes, Is.Zero);
        });
    }

    [TestCase("tenant:acme:r1")]
    [TestCase("tenant:globex:r1")]
    [TestCase("app:shop:reader:1")]
    public void PutRule_refuses_a_local_id_carrying_a_reserved_prefix(string ruleId)
    {
        var harness = new Harness();

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => harness.Create().PutRuleAsync(Tenant, Draft(ruleId: ruleId)));
        Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.ReservedRuleId));
    }

    [TestCase("t/globex/readers")]
    [TestCase("t/acme/readers")]
    public void PutRule_refuses_a_cluster_group_in_the_reserved_tenant_group_namespace(string groupId)
    {
        var harness = new Harness();

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => harness.Create().PutRuleAsync(Tenant, Draft(subjectId: groupId, subjectKind: TenantSubjectKind.ClusterGroup)));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
            Assert.That(harness.Store.Writes, Is.Zero);
        });
    }

    [Test]
    public void PutRule_refuses_another_tenants_tree_by_construction_because_names_are_local()
    {
        // The only way to address globex's tree is to call for globex, which an acme
        // admin is not authorized to do.
        var harness = new Harness();

        Assert.That(
            () => harness.Create().PutRuleAsync(OtherTenant, Draft()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(harness.Store.Writes, Is.Zero);
    }

    private static IEnumerable<TestCaseData> MalformedDrafts()
    {
        yield return new TestCaseData(Draft(ruleId: "")).SetName("{m}(empty id)");
        yield return new TestCaseData(Draft(subjectId: "")).SetName("{m}(empty subject)");
        yield return new TestCaseData(Draft(treeName: null)).SetName("{m}(tree scope without tree)");
        yield return new TestCaseData(Draft(keyOrPrefix: "k")).SetName("{m}(tree scope with key)");
        yield return new TestCaseData(Draft(scopeKind: TenantRuleScopeKind.Key)).SetName("{m}(key scope without key)");
        yield return new TestCaseData(Draft(scopeKind: TenantRuleScopeKind.Prefix, keyOrPrefix: "")).SetName("{m}(prefix scope with empty prefix)");
        yield return new TestCaseData(Draft(scopeKind: TenantRuleScopeKind.TenantWide)).SetName("{m}(tenant-wide with tree)");
        yield return new TestCaseData(Draft(treeName: null, scopeKind: TenantRuleScopeKind.TenantWide, keyOrPrefix: "k")).SetName("{m}(tenant-wide with key)");
        yield return new TestCaseData(Draft(scopeKind: (TenantRuleScopeKind)42)).SetName("{m}(undefined scope kind)");
        yield return new TestCaseData(Draft(subjectKind: (TenantSubjectKind)42)).SetName("{m}(undefined subject kind)");
        yield return new TestCaseData(Draft(effect: (LatticeEffect)42)).SetName("{m}(undefined effect)");
        yield return new TestCaseData(Draft(subjectId: "Bad Name", subjectKind: TenantSubjectKind.TenantGroup)).SetName("{m}(malformed tenant group name)");
    }

    [TestCaseSource(nameof(MalformedDrafts))]
    public void PutRule_refuses_a_malformed_draft_as_an_argument_error_not_a_confinement(TenantRuleDraft draft)
    {
        var harness = new Harness();

        var ex = Assert.ThrowsAsync(Is.InstanceOf<ArgumentException>(), () => harness.Create().PutRuleAsync(Tenant, draft));
        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.Not.InstanceOf<TenantAccessConfinementException>());
            Assert.That(harness.Store.Writes, Is.Zero);
        });
    }

    [Test]
    public void PutRule_with_a_null_draft_throws_argument_null()
    {
        Assert.That(() => new Harness().Create().PutRuleAsync(Tenant, null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task PutRule_at_the_rule_cap_is_refused_with_the_quota_exception_and_writes_nothing()
    {
        var harness = new Harness(new TenantQuotas { MaxTenantRules = 2 });
        var facade = harness.Create();
        await facade.PutRuleAsync(Tenant, Draft(ruleId: "r1"));
        await facade.PutRuleAsync(Tenant, Draft(ruleId: "r2"));

        // Another tenant's rules and operator rules never count against acme's cap.
        harness.Store.Seed(TenantRule(OtherTenant, "x", "orders"));
        harness.Store.Seed(OperatorRule("op", "t/acme/orders"));
        var writes = harness.Store.Writes;

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(() => facade.PutRuleAsync(Tenant, Draft(ruleId: "r3")));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(TenantAccessCaps.TenantRulesDimension));
            Assert.That(ex.Limit, Is.EqualTo(2));
            Assert.That(harness.Store.Writes, Is.EqualTo(writes), "nothing is written past the cap");
        });
    }

    [Test]
    public async Task PutRule_replacing_an_existing_rule_at_the_cap_is_admitted()
    {
        var harness = new Harness(new TenantQuotas { MaxTenantRules = 1 });
        var facade = harness.Create();
        await facade.PutRuleAsync(Tenant, Draft(ruleId: "r1"));

        var view = await facade.PutRuleAsync(Tenant, Draft(ruleId: "r1", effect: LatticeEffect.Deny));

        Assert.Multiple(() =>
        {
            Assert.That(view.Effect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(harness.Store.All.Single().Effect, Is.EqualTo(LatticeEffect.Deny));
        });
    }

    [Test]
    public void PutRule_applies_the_default_rule_cap_when_none_is_set()
    {
        var harness = new Harness();
        for (var i = 0; i < TenantQuotas.DefaultMaxTenantRules; i++)
        {
            harness.Store.Seed(TenantRule(Tenant, $"seed{i:D4}", "orders"));
        }

        Assert.That(
            () => harness.Create().PutRuleAsync(Tenant, Draft(ruleId: "one-too-many")),
            Throws.TypeOf<LatticeQuotaExceededException>());
    }

    [Test]
    public async Task PutRule_moving_a_rule_to_another_tree_removes_the_copy_it_replaced()
    {
        var harness = new Harness();
        var facade = harness.Create();
        await facade.PutRuleAsync(Tenant, Draft(treeName: "orders"));

        await facade.PutRuleAsync(Tenant, Draft(treeName: "invoices"));

        var stored = harness.Store.All.Single();
        Assert.Multiple(() =>
        {
            Assert.That(stored.Scope.TreeId, Is.EqualTo("t/acme/invoices"));
            Assert.That(harness.Store.TenantWriteOrigins, Is.All.True);
        });
    }

    [Test]
    public async Task GetRule_returns_the_tenant_rule_by_its_local_id()
    {
        var harness = new Harness();
        harness.Store.Seed(TenantRule(Tenant, "r1", "orders"));

        var view = await harness.Create().GetRuleAsync(Tenant, "r1");

        Assert.Multiple(() =>
        {
            Assert.That(view, Is.Not.Null);
            Assert.That(view!.RuleId, Is.EqualTo("r1"));
            Assert.That(view.TreeName, Is.EqualTo("orders"));
            Assert.That(view.Editable, Is.True);
        });
    }

    [Test]
    public async Task GetRule_returns_null_for_another_tenants_rule_and_for_an_operator_rule()
    {
        var harness = new Harness();
        harness.Store.Seed(TenantRule(OtherTenant, "r1", "orders"));
        harness.Store.Seed(OperatorRule("r1", "t/acme/orders"));

        Assert.That(await harness.Create().GetRuleAsync(Tenant, "r1"), Is.Null);
    }

    [TestCase(null)]
    [TestCase("")]
    public void GetRule_and_RemoveRule_refuse_an_empty_rule_id(string? ruleId)
    {
        var facade = new Harness().Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.GetRuleAsync(Tenant, ruleId!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => facade.RemoveRuleAsync(Tenant, ruleId!), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task RemoveRule_removes_the_tenant_rule_under_system_origin_and_is_idempotent()
    {
        var harness = new Harness();
        harness.Store.Seed(TenantRule(Tenant, "r1", "orders"));
        var facade = harness.Create();

        var first = await facade.RemoveRuleAsync(Tenant, "r1");
        var second = await facade.RemoveRuleAsync(Tenant, "r1");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(second, Is.False);
            Assert.That(harness.Store.All, Is.Empty);
            Assert.That(harness.Store.TenantWriteOrigins, Is.EqualTo(new[] { true }));
        });
    }

    [Test]
    public async Task RemoveRule_never_removes_a_platform_rule_or_another_tenants_rule()
    {
        var harness = new Harness();
        harness.Store.Seed(OperatorRule("r1", "t/acme/orders"));
        harness.Store.Seed(TenantRule(OtherTenant, "r1", "orders"));

        var removed = await harness.Create().RemoveRuleAsync(Tenant, "r1");

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.False);
            Assert.That(harness.Store.All.Count(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ListRules_lists_tenant_rules_as_editable_and_operator_rules_on_tenant_trees_as_read_only_platform()
    {
        var harness = new Harness();
        harness.Store.Seed(TenantRule(Tenant, "mine", "orders"));
        harness.Store.Seed(OperatorRule("guardrail", "t/acme/orders", effect: LatticeEffect.Deny));
        harness.Store.Seed(OperatorRule("everywhere", LatticeScope.ClusterWideTreeId));
        harness.Store.Seed(OperatorRule("app:shop:reader:1", "t/acme/a/shop/items", LatticeSubjectSelector.Group("t/acme/readers")));
        harness.Store.Seed(OperatorRule("legacy", "orders"));
        harness.Store.Seed(OperatorRule("theirs", "t/globex/orders"));
        harness.Store.Seed(TenantRule(OtherTenant, "theirs", "orders"));

        var page = await harness.Create().ListRulesAsync(Tenant, new TenantAccessPageRequest());

        Assert.That(page.Entries.Select(e => e.RuleId), Is.EquivalentTo(new[] { "mine", "guardrail" }));
        var platform = page.Entries.Single(e => e.RuleId == "guardrail");
        Assert.Multiple(() =>
        {
            Assert.That(platform.Layer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(platform.Origin, Is.EqualTo(TenantRuleOrigin.PlatformTree));
            Assert.That(platform.Editable, Is.False);
            Assert.That(platform.SubjectId, Is.EqualTo(Member), "a platform rule on the tenant's own tree is shown in full");
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListRules_pages_in_a_stable_order_with_a_continuation_token()
    {
        var harness = new Harness();
        foreach (var tree in new[] { "a-tree", "b-tree", "c-tree" })
        {
            harness.Store.Seed(TenantRule(Tenant, $"r-{tree}", tree));
            harness.Store.Seed(OperatorRule($"op-{tree}", $"t/acme/{tree}"));
        }

        var facade = harness.Create();
        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await facade.ListRulesAsync(Tenant, new TenantAccessPageRequest { PageSize = 4, PageToken = token });
            seen.AddRange(page.Entries.Select(e => e.RuleId));
            token = page.NextPageToken;
            pages++;
        }
        while (token is not null);

        Assert.Multiple(() =>
        {
            Assert.That(pages, Is.EqualTo(2));
            Assert.That(seen, Is.EqualTo(new[]
            {
                "op-a-tree", "r-a-tree", "op-b-tree", "r-b-tree", "op-c-tree", "r-c-tree",
            }), "ordered by tree, then stored rule id, with no gap or repeat across pages");
        });
    }

    [Test]
    public void ListRules_with_a_null_page_throws_argument_null()
    {
        Assert.That(() => new Harness().Create().ListRulesAsync(Tenant, null!), Throws.ArgumentNullException);
    }
}
