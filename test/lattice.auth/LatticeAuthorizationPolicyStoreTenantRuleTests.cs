using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the tenant-tier guards on <see cref="LatticeAuthorizationPolicyStore"/>
/// (epic #4154): the <c>tenant:</c> rule-id write guard (refused off system origin,
/// before anything is read), and the confinement guards D4 (tenant group subjects),
/// D7 (tenant-tier rule shape) and D9 (tenant-wide scope), which apply to every
/// caller. Each confinement guard is exercised for an operator caller (no system
/// origin, as the cluster auth facade or a direct store user writes) and for the
/// system-origin writer the tenant facades use on a tenant admin's behalf. The store
/// is driven against substitute grains, so a rejection is proven to issue no read or
/// write.
/// </summary>
[TestFixture]
public sealed class LatticeAuthorizationPolicyStoreTenantRuleTests
{
    private const string Tree = "t/contoso/orders";
    private const string ContosoGroup = "t/contoso/readers";
    private const string FabrikamGroup = "t/fabrikam/readers";

    private static readonly TenantId Contoso = TenantId.Parse("contoso");
    private static readonly TenantId Fabrikam = TenantId.Parse("fabrikam");
    private static readonly string TenantRuleId = LatticeTenantRuleIds.For(Contoso, "readers");

    private IGrainFactory _grainFactory = null!;
    private ILattice _policy = null!;
    private LatticeAuthorizationPolicyStore _store = null!;

    public enum Caller
    {
        Operator,
        SystemOrigin,
    }

    [SetUp]
    public void SetUp()
    {
        _grainFactory = Substitute.For<IGrainFactory>();
        _policy = Substitute.For<ILattice>();
        _policy.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(true);
        _grainFactory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(_policy);

        var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        options.CurrentValue.Returns(new LatticeAuthOptions());
        var initializer = new AuthInitializer(_grainFactory, Substitute.For<IServiceProvider>(), options);
        _store = new LatticeAuthorizationPolicyStore(_grainFactory, initializer, options);
    }

    private static LatticeAuthorizationRule Rule(
        string ruleId,
        LatticeSubjectSelector subject,
        LatticeScope scope,
        LatticeOperation ops = LatticeOperation.Read) =>
        new(ruleId, subject, scope, ops, LatticeEffect.Allow);

    private async Task PutAsync(Caller caller, LatticeAuthorizationRule rule)
    {
        if (caller == Caller.SystemOrigin)
        {
            using (LatticeSystemOrigin.Enter())
            {
                await _store.PutRuleAsync(rule);
            }

            return;
        }

        await _store.PutRuleAsync(rule);
    }

    private void AssertRefused(Caller caller, LatticeAuthorizationRule rule, string because)
    {
        var ex = Assert.ThrowsAsync(Is.InstanceOf<ArgumentException>(), () => PutAsync(caller, rule));
        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.Not.InstanceOf<LatticeTenantOwnedRuleException>(), "a confinement refusal is not the origin guard");
            Assert.That(ex!.Message, Does.Contain(because));
            Assert.That(_grainFactory.ReceivedCalls(), Is.Empty, "a refused write must not touch the policy tree");
        });
    }

    private async Task AssertAdmitted(Caller caller, LatticeAuthorizationRule rule)
    {
        await PutAsync(caller, rule);
        await _policy.Received(1).SetAsync(
            rule.Scope.TreeId + AuthConstants.RuleKeySeparator + rule.RuleId, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    // ---- Tenant-tier rule id origin guard ------------------------------------

    [Test]
    public void PutRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading()
    {
        var rule = Rule(TenantRuleId, LatticeSubjectSelector.User("alice"), LatticeScope.Tree(Tree));

        var ex = Assert.ThrowsAsync<LatticeTenantOwnedRuleException>(() => _store.PutRuleAsync(rule));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.RuleId, Is.EqualTo(TenantRuleId));
            Assert.That(ex.ParamName, Is.EqualTo("rule"));
            Assert.That(_grainFactory.ReceivedCalls(), Is.Empty);
        });
    }

    [Test]
    public void RemoveRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading()
    {
        var ex = Assert.ThrowsAsync<LatticeTenantOwnedRuleException>(() => _store.RemoveRuleAsync(Tree, TenantRuleId));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.RuleId, Is.EqualTo(TenantRuleId));
            Assert.That(ex.ParamName, Is.EqualTo("ruleId"));
            Assert.That(_grainFactory.ReceivedCalls(), Is.Empty, "a refused delete must not disclose whether the rule exists");
        });
    }

    [Test]
    public void PutRuleAsync_refuses_a_malformed_tenant_rule_id_off_system_origin()
    {
        var rule = Rule("tenant:", LatticeSubjectSelector.User("alice"), LatticeScope.Tree(Tree));

        Assert.ThrowsAsync<LatticeTenantOwnedRuleException>(() => _store.PutRuleAsync(rule));
    }

    [Test]
    public async Task PutRuleAsync_admits_a_confined_tenant_rule_under_system_origin()
    {
        await AssertAdmitted(Caller.SystemOrigin, Rule(TenantRuleId, LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Prefix(Tree, "p/")));
    }

    [Test]
    public async Task RemoveRuleAsync_admits_a_tenant_rule_id_under_system_origin()
    {
        bool removed;
        using (LatticeSystemOrigin.Enter())
        {
            removed = await _store.RemoveRuleAsync(Tree, TenantRuleId);
        }

        Assert.That(removed, Is.True);
    }

    [Test]
    public async Task GetRuleAsync_reads_a_tenant_rule_id_for_an_operator()
    {
        // Reads are not guarded: an operator may list and inspect tenant-tier rules.
        _policy.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));

        var rule = await _store.GetRuleAsync(Tree, TenantRuleId);

        Assert.That(rule, Is.Null);
        await _policy.Received(1).GetAsync(Tree + AuthConstants.RuleKeySeparator + TenantRuleId, Arg.Any<CancellationToken>());
    }

    // ---- D7: tenant-tier rule shape (system origin, which the origin guard admits) ----

    [TestCase("t/fabrikam/orders", TestName = "D7_refuses_a_tenant_rule_on_another_tenants_tree")]
    [TestCase("t/contoso/a/billing/ledger", TestName = "D7_refuses_a_tenant_rule_on_an_app_owned_tree")]
    [TestCase("orders", TestName = "D7_refuses_a_tenant_rule_on_a_legacy_tree")]
    [TestCase("sys-membership-groups", TestName = "D7_refuses_a_tenant_rule_on_a_system_tree")]
    [TestCase("t/contoso/sys-x", TestName = "D7_refuses_a_tenant_rule_on_a_system_local_name")]
    [TestCase("*", TestName = "D7_refuses_a_tenant_rule_on_the_all_trees_sentinel")]
    public void D7_refuses_a_tenant_rule_outside_its_tenants_data_trees(string treeId)
    {
        AssertRefused(Caller.SystemOrigin, Rule(TenantRuleId, LatticeSubjectSelector.User("alice"), LatticeScope.Tree(treeId)), "not a tree tenant");
    }

    [Test]
    public void D7_refuses_a_tenant_wide_scope_over_another_tenant()
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule(TenantRuleId, LatticeSubjectSelector.User("alice"), LatticeScope.TenantWide(Fabrikam)),
            "only govern its own tenant");
    }

    [TestCase(LatticeOperation.Telemetry)]
    [TestCase(LatticeOperation.Replication)]
    [TestCase(LatticeOperation.TreeLifecycle)]
    [TestCase(LatticeOperation.AppInstall)]
    [TestCase(LatticeOperation.Read | LatticeOperation.Telemetry)]
    [TestCase(LatticeOperation.None)]
    public void D7_refuses_a_tenant_rule_outside_the_data_plane_mask(LatticeOperation ops)
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule(TenantRuleId, LatticeSubjectSelector.User("alice"), LatticeScope.Tree(Tree), ops),
            "data-plane operations");
    }

    [Test]
    public void D7_refuses_a_tenant_rule_naming_another_tenants_group()
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule(TenantRuleId, LatticeSubjectSelector.Group(FabrikamGroup), LatticeScope.Tree(Tree)),
            "not a group of tenant 'contoso'");
    }

    [Test]
    public void D7_refuses_a_malformed_tenant_rule_id_under_system_origin()
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule("tenant:default:x", LatticeSubjectSelector.User("alice"), LatticeScope.Tree(Tree)),
            "is not of the form");
    }

    [Test]
    public async Task D7_admits_a_tenant_rule_naming_a_user_a_cluster_group_or_an_own_group()
    {
        await AssertAdmitted(Caller.SystemOrigin, Rule(LatticeTenantRuleIds.For(Contoso, "u"), LatticeSubjectSelector.User("alice"), LatticeScope.Key(Tree, "k")));
        await AssertAdmitted(Caller.SystemOrigin, Rule(LatticeTenantRuleIds.For(Contoso, "c"), LatticeSubjectSelector.Group("entra-readers"), LatticeScope.Tree(Tree)));
        await AssertAdmitted(
            Caller.SystemOrigin,
            Rule(LatticeTenantRuleIds.For(Contoso, "g"), LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.TenantWide(Contoso), LatticeAuthOperations.All));
    }

    // ---- D9: tenant-wide scope is tenant-layer only ----------------------------

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public void D9_refuses_a_tenant_wide_scope_on_an_operator_rule(Caller caller)
    {
        AssertRefused(caller, Rule("ops-wide", LatticeSubjectSelector.User("alice"), LatticeScope.TenantWide(Contoso)), "tenant-wide scope");
    }

    [Test]
    public void D9_refuses_an_app_rule_on_a_tenant_wide_scope_under_system_origin()
    {
        // Off system origin the app-owned id guard refuses first (see the app-owned
        // store tests); under system origin the tenant-wide guard still refuses it.
        AssertRefused(
            Caller.SystemOrigin,
            Rule(LatticeAppRuleIds.Prefix + "x/role", LatticeSubjectSelector.User("alice"), LatticeScope.TenantWide(Contoso)),
            "tenant-wide scope");
    }

    [Test]
    public void D9_refuses_a_key_scope_on_the_tenant_wide_sentinel_tree()
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule(TenantRuleId, LatticeSubjectSelector.User("alice"), LatticeScope.Key("t/contoso/*", "k")),
            "not a tenant-wide scope");
    }

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public void D9_refuses_an_operator_rule_on_the_default_tenant_sentinel(Caller caller)
    {
        AssertRefused(caller, Rule("ops", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("t/default/*")), "tenant-wide scope");
    }

    // ---- D4: tenant group subject confinement, for every caller ---------------

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public void D4_refuses_a_tenant_group_in_an_all_trees_rule(Caller caller)
    {
        AssertRefused(caller, Rule("ops", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.ClusterWide()), "names tenant group");
    }

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public void D4_refuses_a_tenant_group_on_another_tenants_tree(Caller caller)
    {
        AssertRefused(caller, Rule("ops", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Tree("t/fabrikam/orders")), "names tenant group");
    }

    [TestCase(Caller.Operator, "orders")]
    [TestCase(Caller.SystemOrigin, "orders")]
    [TestCase(Caller.Operator, "sys-membership-groups")]
    [TestCase(Caller.SystemOrigin, "sys-tenant-registry")]
    [TestCase(Caller.Operator, "_lattice_tenant_admin_contoso")]
    [TestCase(Caller.SystemOrigin, "t/contoso/sys-x")]
    public void D4_refuses_a_tenant_group_on_a_legacy_reserved_or_system_tree(Caller caller, string treeId)
    {
        AssertRefused(caller, Rule("ops", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Tree(treeId)), "names tenant group");
    }

    [Test]
    public void D4_refuses_a_tenant_group_on_the_reserved_policy_tree()
    {
        // The confinement guard runs before the reserved-namespace authoring guard.
        AssertRefused(Caller.Operator, Rule("ops", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Tree("sys-auth-policy"), LatticeOperation.Admin), "names tenant group");
    }

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public void D4_refuses_a_tenant_group_on_its_own_app_owned_tree_in_a_non_app_rule(Caller caller)
    {
        AssertRefused(caller, Rule("ops", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Tree("t/contoso/a/billing/ledger")), "names tenant group");
    }

    [Test]
    public async Task D4_admits_a_tenant_group_on_its_own_app_owned_tree_in_an_app_rule_under_system_origin()
    {
        await AssertAdmitted(
            Caller.SystemOrigin,
            Rule(LatticeAppRuleIds.Prefix + "billing/reader/ledger", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Prefix("t/contoso/a/billing/ledger", "p/")));
    }

    [Test]
    public void D4_refuses_a_tenant_group_on_another_tenants_app_owned_tree_in_an_app_rule()
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule(LatticeAppRuleIds.Prefix + "billing/reader/ledger", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Tree("t/fabrikam/a/billing/ledger")),
            "names tenant group");
    }

    [Test]
    public void D4_refuses_a_tenant_group_on_a_legacy_tree_in_an_app_rule()
    {
        AssertRefused(
            Caller.SystemOrigin,
            Rule(LatticeAppRuleIds.Prefix + "billing/reader/ledger", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Tree("a/billing/ledger")),
            "names tenant group");
    }

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public async Task D4_admits_a_tenant_group_on_its_own_data_tree_in_an_operator_rule(Caller caller)
    {
        await AssertAdmitted(caller, Rule("ops", LatticeSubjectSelector.Group(ContosoGroup), LatticeScope.Key(Tree, "k")));
    }

    [TestCase(Caller.Operator, "t/x")]
    [TestCase(Caller.Operator, "t/default/readers")]
    [TestCase(Caller.SystemOrigin, "t/contoso/Readers")]
    [TestCase(Caller.SystemOrigin, "t/")]
    public void D4_refuses_a_malformed_group_in_the_reserved_t_namespace(Caller caller, string groupId)
    {
        AssertRefused(caller, Rule("ops", LatticeSubjectSelector.Group(groupId), LatticeScope.Tree(Tree)), "not a well-formed tenant group id");
    }

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public async Task D4_leaves_a_cluster_group_rule_on_any_tree_unaffected(Caller caller)
    {
        await AssertAdmitted(caller, Rule("ops", LatticeSubjectSelector.Group("entra-readers"), LatticeScope.Tree("orders")));
    }

    [TestCase(Caller.Operator)]
    [TestCase(Caller.SystemOrigin)]
    public async Task D4_leaves_a_user_whose_id_looks_like_a_tenant_group_unaffected(Caller caller)
    {
        // Confinement is about group subjects; a user selector is never a tenant group.
        await AssertAdmitted(caller, Rule("ops", LatticeSubjectSelector.User(ContosoGroup), LatticeScope.Tree("orders")));
    }
}
