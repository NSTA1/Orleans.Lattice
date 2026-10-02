using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Tests for how the decision engine and snapshot maintainer consume the
/// <see cref="ITenantRuleLayer"/> seam: the snapshot carries a tenant partition only
/// when it was compiled with the layer active, an engine that finds the layer
/// switched on over a snapshot without the partition requests a rebuild (adding no
/// grant meanwhile), and switching the layer off takes effect on the next decision.
/// </summary>
[TestFixture]
public sealed class TenantLayerEngineTests
{
    private static readonly LatticeAuthorizationRule WideAllow =
        Tenant("wide-allow", LatticeSubjectSelector.User("alice"), LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow);

    private sealed class MutableLayer : ITenantRuleLayer
    {
        public bool IsActive { get; set; }
    }

    private static (LatticeDecisionEngine Engine, CompiledPolicySnapshotMaintainer Maintainer) Create(ITenantRuleLayer? layer)
    {
        var store = new CovPolicyStore();
        store.Rules.Add(WideAllow);
        var maintainer = new CompiledPolicySnapshotMaintainer(
            store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance, timeProvider: null, layer);
        var engine = new LatticeDecisionEngine(maintainer, new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()), layer);
        return (engine, maintainer);
    }

    private static bool Allowed(LatticeDecisionEngine engine) =>
        engine.Evaluate(Subject("alice"), TenantTree, LatticeOperation.Read, "k").Allowed;

    [Test]
    public async Task Maintainer_without_a_layer_builds_no_tenant_partition()
    {
        var (engine, maintainer) = Create(layer: null);
        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.TenantLayerIncluded, Is.False);
            Assert.That(maintainer.Current.Tenant, Is.Null);
            Assert.That(Allowed(engine), Is.False);
        });
    }

    [Test]
    public async Task Maintainer_with_an_active_layer_builds_the_tenant_partition()
    {
        var (engine, maintainer) = Create(new MutableLayer { IsActive = true });
        await maintainer.RebuildNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.Current.TenantLayerIncluded, Is.True);
            Assert.That(maintainer.Current.Tenant, Is.Not.Null);
            Assert.That(Allowed(engine), Is.True);
        });
    }

    [Test]
    public async Task Switching_the_layer_on_adds_no_grant_until_the_snapshot_includes_the_partition()
    {
        var layer = new MutableLayer();
        var (engine, maintainer) = Create(layer);
        await maintainer.RebuildNowAsync();
        var epoch = maintainer.CurrentEpoch;

        layer.IsActive = true;

        // The snapshot predates the switch: the tenant layer has no rules yet.
        Assert.That(Allowed(engine), Is.False);

        // The engine asked for a rebuild; one deterministic rebuild later the grant applies.
        await maintainer.RebuildNowAsync();
        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(epoch));
            Assert.That(Allowed(engine), Is.True);
        });
    }

    [Test]
    public async Task Switching_the_layer_off_takes_effect_on_the_next_decision()
    {
        var layer = new MutableLayer { IsActive = true };
        var (engine, maintainer) = Create(layer);
        await maintainer.RebuildNowAsync();
        Assert.That(Allowed(engine), Is.True, "precondition");

        layer.IsActive = false;

        Assert.That(Allowed(engine), Is.False);
    }

    [Test]
    public async Task Detailed_evaluate_surfaces_the_tenant_layer_trace()
    {
        var (engine, maintainer) = Create(new MutableLayer { IsActive = true });
        await maintainer.RebuildNowAsync();

        engine.Evaluate(Subject("alice"), TenantTree, LatticeOperation.Read, "k", null, null, out var match);

        Assert.Multiple(() =>
        {
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Tenant));
            Assert.That(match.TenantWide, Is.True);
            Assert.That(match.RuleId, Is.EqualTo(WideAllow.RuleId));
        });
    }

    [Test]
    public async Task HasAnyGrant_consults_the_active_layer()
    {
        var layer = new MutableLayer { IsActive = true };
        var (engine, maintainer) = Create(layer);
        await maintainer.RebuildNowAsync();

        Assert.That(engine.HasAnyGrant(Subject("alice"), TenantTree, LatticeOperation.Read), Is.True);

        layer.IsActive = false;
        Assert.That(engine.HasAnyGrant(Subject("alice"), TenantTree, LatticeOperation.Read), Is.False);
    }

    [Test]
    public void Compile_with_the_layer_active_and_no_rules_reports_the_layer_included()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CompiledPolicy.Compile(Array.Empty<LatticeAuthorizationRule>(), includeTenantLayer: true).TenantLayerIncluded, Is.True);
            Assert.That(CompiledPolicy.Compile(Array.Empty<LatticeAuthorizationRule>(), includeTenantLayer: false).TenantLayerIncluded, Is.False);
            Assert.That(CompiledPolicy.Empty.TenantLayerIncluded, Is.False);
        });
    }

    [Test]
    public async Task Engine_resolved_from_the_container_uses_the_registered_tenant_layer()
    {
        // The container composes the maintainer and engine with the registered seam.
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAuthorizationPolicyStore>(new CovPolicyStore { Rules = { WideAllow } });
        services.AddSingleton<ITenantRuleLayer>(new MutableLayer { IsActive = true });
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IOptionsMonitor<LatticeAuthOptions>>(new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()));
        services.AddSingleton<CompiledPolicySnapshotMaintainer>();
        services.AddSingleton<LatticeDecisionEngine>();
        using var provider = services.BuildServiceProvider();

        var maintainer = provider.GetRequiredService<CompiledPolicySnapshotMaintainer>();
        var engine = provider.GetRequiredService<LatticeDecisionEngine>();
        await maintainer.RebuildNowAsync();

        Assert.That(Allowed(engine), Is.True);
    }
}
