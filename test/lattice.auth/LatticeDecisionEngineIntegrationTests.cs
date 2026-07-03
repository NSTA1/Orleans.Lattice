using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Integration tests for the decision engine and its snapshot maintainer against
/// a live single-silo <see cref="Orleans.TestingHost.TestCluster"/>. Proves the
/// core acceptance criterion: after a policy edit through
/// <see cref="ILatticeAuthorizationPolicyStore"/>, the engine's decision changes
/// without a restart once the change-feed settles, and
/// <see cref="ILatticeDecisionEngine.CurrentEpoch"/> advances. Also confirms the
/// engine is inert - registering it does not replace the core no-op access gate.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeDecisionEngineIntegrationTests
{
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Policy_edit_changes_the_decision_without_restart_and_advances_the_epoch()
    {
        var engine = _fixture.SiloServices.GetRequiredService<ILatticeDecisionEngine>();
        var maintainer = _fixture.SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();
        var store = _fixture.Store;

        const string treeId = "decision-tree";
        var alice = new LatticeSubject("alice");

        await maintainer.EnsureWarmAsync();
        var epochBefore = engine.CurrentEpoch;

        var before = engine.Evaluate(alice, treeId, LatticeOperation.Read, key: "k");
        Assert.That(before.Allowed, Is.False, "no rule yet, so the default effect (Deny) applies");

        await store.PutRuleAsync(new LatticeAuthorizationRule(
            "grant-alice",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Tree(treeId),
            LatticeOperation.Read,
            LatticeEffect.Allow));

        // The change-feed rebuild is asynchronous; wait for the epoch to advance.
        var settled = await PollAsync(() =>
        {
            var decision = engine.Evaluate(alice, treeId, LatticeOperation.Read, key: "k");
            return engine.CurrentEpoch > epochBefore && decision.Allowed ? (bool?)true : null;
        });

        Assert.That(settled, Is.True, "the engine must reflect the committed rule once the change-feed settles");

        var after = engine.Evaluate(alice, treeId, LatticeOperation.Read, key: "k");
        Assert.That(after.Allowed, Is.True, "the decision flips to Allow without any restart");
        Assert.That(engine.CurrentEpoch, Is.GreaterThan(epochBefore), "a committed policy change advances the monotonic epoch");
    }

    [Test]
    public void Registering_the_engine_leaves_the_core_access_gate_a_no_op()
    {
        var gate = _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>();

        Assert.That(gate.GetType().Name, Is.EqualTo("NullLatticeAccessGate"),
            "this feature adds a decision surface only; enforcement wiring is out of scope, so the gate must stay the default no-op");
    }

    private static async Task<bool> PollAsync(Func<bool?> probe, int timeoutMs = 5000)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            if (probe() is true)
            {
                return true;
            }

            await Task.Delay(50);
        }

        return probe() is true;
    }
}
