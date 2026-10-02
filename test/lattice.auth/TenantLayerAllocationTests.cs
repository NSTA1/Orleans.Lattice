using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Tests.Fakes;
using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Allocation tests for the tenant layer (epic #4154, D11). A warm point decision
/// through the decision engine stays allocation-free on the allow path with the
/// layer inactive (the null seam, a pre-existing tenant rule notwithstanding) and
/// active (whether the operator or the tenant layer decides), and a decision the
/// inactive layer answers allocates exactly what the operator-only evaluator
/// allocates for the same request.
/// </summary>
[TestFixture]
public sealed class TenantLayerAllocationTests
{
    private const int Small = 100;
    private const int Large = 10_000;

    private static readonly LatticeSubjectSelector Alice = LatticeSubjectSelector.User("alice");

    private static readonly LatticeAuthorizationRule[] Rules =
    [
        Operator("ops-key", Alice, LatticeScope.Key(TenantTree, "ops"), LatticeOperation.Read, LatticeEffect.Allow),
        Tenant("tenant-prefix", Alice, LatticeScope.Prefix(TenantTree, "t/"), LatticeOperation.Read, LatticeEffect.Allow),
        Tenant("wide-deny", LatticeSubjectSelector.User("mallory"), LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Deny),
    ];

    private sealed class SwitchableLayer(bool active) : ITenantRuleLayer
    {
        public bool IsActive { get; } = active;
    }

    private static async Task<LatticeDecisionEngine> CreateEngineAsync(ITenantRuleLayer? layer)
    {
        var store = new CovPolicyStore();
        store.Rules.AddRange(Rules);
        var maintainer = new CompiledPolicySnapshotMaintainer(
            store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance, timeProvider: null, layer);
        await maintainer.RebuildNowAsync();
        return new LatticeDecisionEngine(maintainer, new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()), layer);
    }

    private static long Growth(LatticeDecisionEngine engine, string key)
    {
        var subject = Subject("alice");
        return AllocationProbe.Growth(
            prepare: _ => engine,
            measure: (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    if (state.Evaluate(subject, TenantTree, LatticeOperation.Read, key).Allowed)
                    {
                        allowed++;
                    }
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: Small,
            largeSize: Large);
    }

    [Test]
    public async Task Inactive_layer_warm_allow_decision_allocates_nothing()
    {
        var engine = await CreateEngineAsync(layer: null);

        Assert.That(Growth(engine, "ops"), Is.Zero);
    }

    [Test]
    public async Task Active_layer_warm_operator_decided_allow_allocates_nothing()
    {
        var engine = await CreateEngineAsync(new SwitchableLayer(active: true));

        Assert.That(Growth(engine, "ops"), Is.Zero);
    }

    [Test]
    public async Task Active_layer_warm_tenant_decided_allow_allocates_nothing()
    {
        var engine = await CreateEngineAsync(new SwitchableLayer(active: true));
        Assert.That(engine.Evaluate(Subject("alice"), TenantTree, LatticeOperation.Read, "t/1").Allowed, Is.True, "precondition");

        Assert.That(Growth(engine, "t/1"), Is.Zero);
    }

    [Test]
    public async Task Inactive_layer_deny_allocates_what_the_operator_only_evaluator_allocates()
    {
        var engine = await CreateEngineAsync(new SwitchableLayer(active: false));
        var operatorOnly = CompiledPolicy.Compile(Rules.Where(r => !LatticeTenantRuleIds.IsTenantOwned(r.RuleId)));
        var options = new LatticeAuthOptions();
        var subject = Subject("alice");

        var engineGrowth = Growth(engine, "t/1");
        var baselineGrowth = AllocationProbe.Growth(
            prepare: _ => operatorOnly,
            measure: (policy, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    if (PolicyEvaluator.Evaluate(policy, options, subject, TenantTree, LatticeOperation.Read, "t/1", null, null).Allowed)
                    {
                        allowed++;
                    }
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: Small,
            largeSize: Large);

        Assert.Multiple(() =>
        {
            Assert.That(baselineGrowth, Is.GreaterThan(0), "precondition: a default deny builds its reason");
            Assert.That(engineGrowth, Is.EqualTo(baselineGrowth));
        });
    }

    [Test]
    public void Tenant_layer_classification_and_bucket_lookup_allocate_nothing()
    {
        var partition = CompiledPolicy.Compile(Rules, includeTenantLayer: true).Tenant!;
        string[] trees = [TenantTree, "t/contoso/other", "t/contoso/a/app/x", "orders", "t/fabrikam/x"];

        var growth = AllocationProbe.Growth(
            prepare: _ => partition,
            measure: (state, size) =>
            {
                long hits = 0;
                for (var i = 0; i < size; i++)
                {
                    foreach (var tree in trees)
                    {
                        if (state.TryGetBuckets(tree, out _, out _))
                        {
                            hits++;
                        }
                    }
                }

                AllocationProbe.ScalarSink += hits;
            },
            smallSize: Small,
            largeSize: Large);

        Assert.That(growth, Is.Zero);
    }
}
