using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// The introspection half of <see cref="LatticeAuthAdminTenantTierTests"/>: explain
/// cites tenant-wide rules and reports the deciding layer and rule from the decision
/// engine's trace (D8), and effective permissions labels each rule with its layer.
/// The engine is the real one, compiled over the in-memory policy store.
/// </summary>
public sealed partial class LatticeAuthAdminTenantTierTests
{
    private static async Task<LatticeDecisionEngine> CreateEngineAsync(InMemoryPolicyStore store, bool tenantLayerActive)
    {
        var layer = new SwitchableTenantRuleLayer { IsActive = tenantLayerActive };
        var maintainer = new CompiledPolicySnapshotMaintainer(
            store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance, timeProvider: null, layer);
        await maintainer.RebuildNowAsync();
        var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        options.CurrentValue.Returns(new LatticeAuthOptions());
        return new LatticeDecisionEngine(maintainer, options, layer);
    }

    private static ILatticeMembershipDirectory DirectoryWithNoGroups()
    {
        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.GroupsOfAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyCollection<string>>(Array.Empty<string>()));
        return directory;
    }

    private static async Task<AuthExplanation> ExplainAsync(
        InMemoryPolicyStore store, LatticeScope scope, bool tenantLayerActive = true, bool withEngine = true)
    {
        var engine = withEngine ? await CreateEngineAsync(store, tenantLayerActive) : null;
        var admin = CreateAdmin(DirectoryWithNoGroups(), store, decisionEngine: engine);
        return await admin.ExplainAsync(UserId, LatticeOperation.Read, scope);
    }

    // ----- ExplainAsync: tenant-wide citations and the deciding layer -----

    [Test]
    public async Task ExplainAsync_cites_the_owning_tenants_tenant_wide_rule()
    {
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var store = new InMemoryPolicyStore().Seed(wide, TenantRule(Globex, "wide", LatticeScope.TenantWide(Globex)));

        var explanation = await ExplainAsync(store, LatticeScope.Key(TenantTree, "k"));

        Assert.That(explanation.MatchedRules, Is.EqualTo(new[] { wide }));
    }

    [Test]
    public async Task ExplainAsync_reports_the_tenant_layer_when_only_a_tenant_rule_decides()
    {
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var store = new InMemoryPolicyStore().Seed(wide);

        var explanation = await ExplainAsync(store, LatticeScope.Key(TenantTree, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo(wide.RuleId));
        });
    }

    [Test]
    public async Task ExplainAsync_reports_the_platform_layer_when_an_operator_rule_decides_over_a_tenant_rule()
    {
        var operatorDeny = OperatorRule("op-deny", LatticeScope.Tree(TenantTree), LatticeEffect.Deny);
        var tenantAllow = TenantRule(Acme, "read", LatticeScope.Key(TenantTree, "k"));
        var store = new InMemoryPolicyStore().Seed(operatorDeny, tenantAllow);

        var explanation = await ExplainAsync(store, LatticeScope.Key(TenantTree, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("op-deny"));
            Assert.That(explanation.MatchedRules, Is.EquivalentTo(new[] { operatorDeny, tenantAllow }));
        });
    }

    [Test]
    public async Task ExplainAsync_reports_the_platform_layer_for_an_all_trees_rule()
    {
        var allTrees = OperatorRule("all", LatticeScope.Tree(LatticeScope.ClusterWideTreeId));
        var store = new InMemoryPolicyStore().Seed(allTrees);
        var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        options.CurrentValue.Returns(new LatticeAuthOptions { AllTreesGrantsEnabled = true });
        var maintainer = new CompiledPolicySnapshotMaintainer(store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();
        var admin = CreateAdmin(DirectoryWithNoGroups(), store, decisionEngine: new LatticeDecisionEngine(maintainer, options));

        var explanation = await admin.ExplainAsync(UserId, LatticeOperation.Read, LatticeScope.Key("orders", "k"));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("all"));
        });
    }

    [Test]
    public async Task ExplainAsync_reports_no_layer_when_the_default_effect_decides()
    {
        var store = new InMemoryPolicyStore().Seed(OperatorRule("op-other", LatticeScope.Tree("orders")));

        var explanation = await ExplainAsync(store, LatticeScope.Key(TenantTree, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRuleId, Is.Null);
        });
    }

    [Test]
    public async Task ExplainAsync_reports_no_layer_for_a_tenant_rule_while_the_tenant_layer_is_off()
    {
        var store = new InMemoryPolicyStore().Seed(TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme)));

        var explanation = await ExplainAsync(store, LatticeScope.Key(TenantTree, "k"), tenantLayerActive: false);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.Null, "an inert tenant rule decides nothing");
            Assert.That(explanation.DecidingRuleId, Is.Null);
            Assert.That(explanation.MatchedRules, Has.Count.EqualTo(1), "the authored rule is still cited");
        });
    }

    [Test]
    public async Task ExplainAsync_reports_the_layer_for_a_collection_scope_one_rule_decides_uniformly()
    {
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var store = new InMemoryPolicyStore().Seed(wide);

        var explanation = await ExplainAsync(store, LatticeScope.Tree(TenantTree));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo(wide.RuleId));
        });
    }

    [Test]
    public async Task ExplainAsync_reports_no_layer_for_a_collection_scope_decided_per_key()
    {
        var store = new InMemoryPolicyStore().Seed(
            TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme)),
            OperatorRule("op-deny-k", LatticeScope.Key(TenantTree, "k"), LatticeEffect.Deny));

        var explanation = await ExplainAsync(store, LatticeScope.Tree(TenantTree));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRuleId, Is.Null);
        });
    }

    [Test]
    public async Task ExplainAsync_reports_no_layer_without_a_decision_engine()
    {
        var store = new InMemoryPolicyStore().Seed(TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme)));

        var explanation = await ExplainAsync(store, LatticeScope.Key(TenantTree, "k"), withEngine: false);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRuleId, Is.Null);
        });
    }

    [Test]
    public async Task ExplainAsync_reports_no_layer_for_a_host_replaced_decision_engine()
    {
        var store = new InMemoryPolicyStore().Seed(TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme)));
        var replaced = Substitute.For<ILatticeDecisionEngine>();
        var admin = CreateAdmin(DirectoryWithNoGroups(), store, decisionEngine: replaced);

        var explanation = await admin.ExplainAsync(UserId, LatticeOperation.Read, LatticeScope.Key(TenantTree, "k"));

        Assert.That(explanation.DecidingLayer, Is.Null);
    }

    // ----- EffectivePermissionsAsync: each rule's layer -----

    [Test]
    public async Task EffectivePermissionsAsync_labels_each_rule_with_its_layer()
    {
        var operatorRule = OperatorRule("op-read", LatticeScope.Tree(TenantTree));
        var tenantRule = TenantRule(Acme, "read", LatticeScope.Tree(TenantTree));
        var wide = TenantRule(Acme, "wide", LatticeScope.TenantWide(Acme));
        var store = new InMemoryPolicyStore().Seed(tenantRule, wide, operatorRule);

        var permissions = await CreateAdmin(DirectoryWithNoGroups(), store).EffectivePermissionsAsync(UserId);

        Assert.Multiple(() =>
        {
            Assert.That(permissions.Rules, Is.EqualTo(new[] { wide, operatorRule, tenantRule }));
            Assert.That(permissions.RuleLayers,
                Is.EqualTo(new[] { TenantRuleLayer.Tenant, TenantRuleLayer.Platform, TenantRuleLayer.Tenant }));
        });
    }

    [Test]
    public async Task EffectivePermissionsAsync_leaves_the_layers_empty_when_every_rule_is_an_operator_rule()
    {
        var store = new InMemoryPolicyStore().Seed(
            OperatorRule("op-1", LatticeScope.Tree("orders")),
            OperatorRule("op-2", LatticeScope.Tree(TenantTree)));

        var permissions = await CreateAdmin(DirectoryWithNoGroups(), store).EffectivePermissionsAsync(UserId);

        Assert.Multiple(() =>
        {
            Assert.That(permissions.Rules, Has.Count.EqualTo(2));
            Assert.That(permissions.RuleLayers, Is.Empty);
        });
    }

    [Test]
    public void RuleLayersOf_an_empty_list_is_empty() =>
        Assert.That(LatticeAuthAdmin.RuleLayersOf(Array.Empty<LatticeAuthorizationRule>()), Is.Empty);
}
