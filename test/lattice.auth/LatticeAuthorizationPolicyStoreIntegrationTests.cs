using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Integration tests for the authorization policy store against a live single-silo
/// <see cref="Orleans.TestingHost.TestCluster"/>. Covers the issue's acceptance
/// criteria: every rule shape round-trips (user/group x tree/key/prefix x op-set x
/// Allow/Deny), rules survive re-resolution from the durable backing tree, every
/// policy change leaves a durable history entry with no extra configuration, the
/// by-tree prefix scan is isolated, and a rule cannot be scoped at the reserved
/// <c>sys-auth-*</c> namespace.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeAuthorizationPolicyStoreIntegrationTests
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

    /// <summary>Every combination of subject, scope, op-set, and effect a rule can take.</summary>
    private static IEnumerable<LatticeAuthorizationRule> RuleShapes()
    {
        LatticeSubjectSelector[] subjects =
        {
            LatticeSubjectSelector.User("alice"),
            LatticeSubjectSelector.Group("admins"),
        };
        LatticeEffect[] effects = { LatticeEffect.Allow, LatticeEffect.Deny };
        LatticeOperation[] opSets =
        {
            LatticeOperation.Read,
            LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete,
            LatticeOperation.All,
        };

        var index = 0;
        foreach (var subject in subjects)
        {
            foreach (var effect in effects)
            {
                foreach (var ops in opSets)
                {
                    // A distinct governed tree per case keeps the round-trip
                    // reads independent.
                    var treeId = $"rt-tree-{index}";
                    yield return new LatticeAuthorizationRule($"rule-{index}", subject, LatticeScope.Tree(treeId), ops, effect);
                    index++;
                    yield return new LatticeAuthorizationRule($"rule-{index}", subject, LatticeScope.Key($"rt-tree-{index}", "k1"), ops, effect);
                    index++;
                    yield return new LatticeAuthorizationRule($"rule-{index}", subject, LatticeScope.Prefix($"rt-tree-{index}", "p/"), ops, effect);
                    index++;
                }
            }
        }
    }

    [TestCaseSource(nameof(RuleShapes))]
    public async Task PutRuleAsync_then_GetRuleAsync_round_trips_the_rule(LatticeAuthorizationRule rule)
    {
        var store = _fixture.Store;
        await store.PutRuleAsync(rule);

        var read = await store.GetRuleAsync(rule.Scope.TreeId, rule.RuleId);

        Assert.That(read, Is.EqualTo(rule),
            "the persisted rule must round-trip byte-for-byte through the store");
    }

    [Test]
    public async Task Rules_survive_reresolution_from_the_durable_backing_tree()
    {
        var rule = new LatticeAuthorizationRule(
            "durable-1",
            LatticeSubjectSelector.Group("ops"),
            LatticeScope.Prefix("dur-tree", "tenant-1/"),
            LatticeOperation.Read | LatticeOperation.Enumerate,
            LatticeEffect.Allow);

        await _fixture.Store.PutRuleAsync(rule);

        // Read the rule directly from the durable reserved tree through a fresh
        // ILattice reference - exactly what a store would do after a restart.
        var policyTree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(LatticeAuthReservedTrees.PolicyTreeId);
        LatticeAuthorizationRule? fromTree = null;
        await foreach (var entry in policyTree.EntriesAsync<LatticeAuthorizationRule>())
        {
            if (entry.Value is { RuleId: "durable-1" } r)
            {
                fromTree = r;
            }
        }

        Assert.That(fromTree, Is.EqualTo(rule),
            "the rule must live in the durable backing tree, not in store-local memory");

        // A freshly-resolved store instance (post-restart consumer) reads it back.
        var freshStore = ActivatorUtilities.CreateInstance<LatticeAuthorizationPolicyStore>(_fixture.SiloServices);
        var reread = await freshStore.GetRuleAsync("dur-tree", "durable-1");
        Assert.That(reread, Is.EqualTo(rule));
    }

    [Test]
    public async Task Policy_changes_produce_durable_history_with_no_extra_configuration()
    {
        var store = _fixture.Store;
        var v1 = new LatticeAuthorizationRule("hist-1", LatticeSubjectSelector.User("bob"), LatticeScope.Tree("hist-tree"), LatticeOperation.Read, LatticeEffect.Allow);
        var v2 = v1 with { Operations = LatticeOperation.All };
        await store.PutRuleAsync(v1);
        await store.PutRuleAsync(v2);

        var policyTree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(LatticeAuthReservedTrees.PolicyTreeId);

        var retention = await policyTree.GetHistoryRetentionAsync();
        Assert.That(retention.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly),
            "the policy tree must have durable history retention enabled by default");

        var key = $"hist-tree\u001fhist-1";
        var page = await PollAsync(async () =>
        {
            var history = await policyTree.ScanEntryHistoryAsync(key, null, null, 100, null);
            return history.Revisions.Count > 0 ? history : null;
        });

        Assert.That(page, Is.Not.Null);
        Assert.That(page!.Revisions, Is.Not.Empty,
            "successive rule writes must leave a durable revision timeline with no extra configuration");
    }

    [Test]
    public async Task ListRulesForTreeAsync_returns_only_that_trees_rules()
    {
        var store = _fixture.Store;
        await store.PutRuleAsync(new LatticeAuthorizationRule("a1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("list-tree-a"), LatticeOperation.Read, LatticeEffect.Allow));
        await store.PutRuleAsync(new LatticeAuthorizationRule("a2", LatticeSubjectSelector.User("u"), LatticeScope.Key("list-tree-a", "k"), LatticeOperation.Write, LatticeEffect.Deny));
        await store.PutRuleAsync(new LatticeAuthorizationRule("b1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("list-tree-b"), LatticeOperation.Read, LatticeEffect.Allow));

        var forA = new List<string>();
        await foreach (var rule in store.ListRulesForTreeAsync("list-tree-a"))
        {
            forA.Add(rule.RuleId);
        }

        Assert.That(forA, Is.EquivalentTo(new[] { "a1", "a2" }),
            "a by-tree scan must return exactly that tree's rules and no others");
    }

    [Test]
    public async Task ListRulesAsync_returns_rules_across_trees()
    {
        var store = _fixture.Store;
        await store.PutRuleAsync(new LatticeAuthorizationRule("all-1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("all-tree-x"), LatticeOperation.Read, LatticeEffect.Allow));
        await store.PutRuleAsync(new LatticeAuthorizationRule("all-2", LatticeSubjectSelector.User("u"), LatticeScope.Tree("all-tree-y"), LatticeOperation.Read, LatticeEffect.Allow));

        var seen = new List<string>();
        await foreach (var rule in store.ListRulesAsync())
        {
            seen.Add(rule.RuleId);
        }

        Assert.That(seen, Is.SupersetOf(new[] { "all-1", "all-2" }));
    }

    [Test]
    public async Task RemoveRuleAsync_removes_the_rule_and_reports_absence()
    {
        var store = _fixture.Store;
        await store.PutRuleAsync(new LatticeAuthorizationRule("rm-1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("rm-tree"), LatticeOperation.Read, LatticeEffect.Allow));

        var removed = await store.RemoveRuleAsync("rm-tree", "rm-1");
        Assert.That(removed, Is.True);
        Assert.That(await store.GetRuleAsync("rm-tree", "rm-1"), Is.Null);

        var removedAgain = await store.RemoveRuleAsync("rm-tree", "rm-1");
        Assert.That(removedAgain, Is.False, "removing an absent rule must report false");
    }

    [Test]
    public void PutRuleAsync_rejects_a_rule_scoped_to_the_reserved_namespace()
    {
        var rule = new LatticeAuthorizationRule(
            "bad-1",
            LatticeSubjectSelector.User("u"),
            LatticeScope.Tree("sys-auth-policy"),
            LatticeOperation.All,
            LatticeEffect.Allow);

        Assert.That(async () => await _fixture.Store.PutRuleAsync(rule), Throws.ArgumentException,
            "a rule may not be scoped at the reserved sys-auth-* namespace that backs the store");
    }

    [Test]
    public void PutRuleAsync_with_null_rule_throws()
    {
        Assert.That(async () => await _fixture.Store.PutRuleAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void GetRuleAsync_with_empty_arguments_throws()
    {
        Assert.That(async () => await _fixture.Store.GetRuleAsync("", "r"), Throws.ArgumentException);
        Assert.That(async () => await _fixture.Store.GetRuleAsync("t", ""), Throws.ArgumentException);
    }

    private static async Task<T> PollAsync<T>(Func<Task<T?>> probe, int timeoutMs = 5000)
        where T : class
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            var result = await probe();
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(50);
        }

        return await probe() ?? throw new TimeoutException("Condition not met within the poll timeout.");
    }
}
