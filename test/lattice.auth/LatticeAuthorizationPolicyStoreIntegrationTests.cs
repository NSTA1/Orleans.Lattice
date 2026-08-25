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
            LatticeAuthOperations.All,
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
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow);

        await _fixture.Store.PutRuleAsync(rule);

        // Quiesce the snapshot maintainer: the PutRuleAsync above fires the
        // mutation observer, which rebuilds the compiled snapshot by scanning the
        // reserved policy tree. Awaiting an explicit rebuild here drains that
        // in-flight scan so the diagnostic scan below does not needlessly race a
        // concurrent enumeration of the same policy-tree activation. The scan
        // itself uses the resilient ScanEntriesAsync wrapper, which transparently
        // reconnects on EnumerationAbortedException should the enumerator still be
        // aborted by a deactivation or idle-expiry mid-walk.
        await _fixture.RebuildPolicyAsync();

        // Read the rule directly from the durable reserved tree through a fresh
        // ILattice reference - exactly what a store would do after a restart.
        // The policy tree is now enforced, so this raw (non-store) read is
        // authorized as a bootstrap administrator; the store's own scans run
        // under system-origin.
        var policyTree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(LatticeAuthReservedTrees.PolicyTreeId);
        LatticeAuthorizationRule? fromTree = null;
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await foreach (var entry in policyTree.ScanEntriesAsync<LatticeAuthorizationRule>())
            {
                if (entry.Value is { RuleId: "durable-1" } r)
                {
                    fromTree = r;
                }
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
        var v2 = v1 with { Operations = LatticeAuthOperations.All };
        await store.PutRuleAsync(v1);
        await store.PutRuleAsync(v2);

        var policyTree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(LatticeAuthReservedTrees.PolicyTreeId);

        // Raw (non-store) reads of the now-enforced policy tree are authorized as
        // a bootstrap administrator.
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var retention = await policyTree.GetHistoryRetentionAsync();
            Assert.That(retention.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly),
                "the policy tree must have durable history retention enabled by default");

            var key = $"hist-tree\u001fhist-1";
            var page = await PollAsync(async () =>
            {
                var history = await policyTree.ScanEntryHistoryAsync(key, null, null, 100, null);
                return history.Revisions.Count > 0 ? history : null;
            }, timeoutMs: 20000);

            Assert.That(page, Is.Not.Null);
            Assert.That(page!.Revisions, Is.Not.Empty,
                "successive rule writes must leave a durable revision timeline with no extra configuration");
        }
    }

    [Test]
    public async Task ListRulesForTreeAsync_returns_only_that_trees_rules()
    {
        var store = _fixture.Store;
        await store.PutRuleAsync(new LatticeAuthorizationRule("a1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("list-tree-a"), LatticeOperation.Read, LatticeEffect.Allow));
        await store.PutRuleAsync(new LatticeAuthorizationRule("a2", LatticeSubjectSelector.User("u"), LatticeScope.Key("list-tree-a", "k"), LatticeOperation.Write, LatticeEffect.Deny));
        await store.PutRuleAsync(new LatticeAuthorizationRule("b1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("list-tree-b"), LatticeOperation.Read, LatticeEffect.Allow));

        // The store's list surface is a full/prefix scan of the policy tree that
        // the compiled-snapshot maintainer rescans in the background on every
        // edit. A caller scan that overlaps a maintainer scan of the same
        // activation can transiently omit a just-written key (tracked as a core
        // scan-under-concurrent-scan concern, OC-6); the durable keys converge on
        // a subsequent pass, so assert eventual convergence rather than a single
        // racy snapshot. This is an admin-read robustness allowance only - it does
        // not touch enforcement, which reads the authoritative in-memory snapshot.
        var forA = await ScanUntilAsync(
            () => CollectRuleIdsAsync(store.ListRulesForTreeAsync("list-tree-a")),
            ids => ids.Count == 2);

        Assert.That(forA, Is.EquivalentTo(new[] { "a1", "a2" }),
            "a by-tree scan must return exactly that tree's rules and no others");
    }

    [Test]
    public async Task ListRulesAsync_returns_rules_across_trees()
    {
        var store = _fixture.Store;
        await store.PutRuleAsync(new LatticeAuthorizationRule("all-1", LatticeSubjectSelector.User("u"), LatticeScope.Tree("all-tree-x"), LatticeOperation.Read, LatticeEffect.Allow));
        await store.PutRuleAsync(new LatticeAuthorizationRule("all-2", LatticeSubjectSelector.User("u"), LatticeScope.Tree("all-tree-y"), LatticeOperation.Read, LatticeEffect.Allow));

        // Eventual-convergence assertion: see ListRulesForTreeAsync_returns_only_
        // that_trees_rules for why a full-tree admin scan overlapping the
        // background maintainer is asserted to converge (OC-6).
        var seen = await ScanUntilAsync(
            () => CollectRuleIdsAsync(store.ListRulesAsync()),
            ids => ids.Contains("all-1") && ids.Contains("all-2"));

        Assert.That(seen, Is.SupersetOf(new[] { "all-1", "all-2" }));
    }

    /// <summary>Collects the rule ids emitted by a store list enumeration.</summary>
    private static async Task<List<string>> CollectRuleIdsAsync(IAsyncEnumerable<LatticeAuthorizationRule> rules)
    {
        var ids = new List<string>();
        await foreach (var rule in rules)
        {
            ids.Add(rule.RuleId);
        }

        return ids;
    }

    /// <summary>
    /// Re-runs <paramref name="scan"/> until <paramref name="satisfied"/> holds or
    /// a short deadline elapses, returning the last result either way (so a
    /// timeout surfaces as the caller's own assertion on the last observation).
    /// Converges an admin-read scan that transiently overlaps the background
    /// policy maintainer (OC-6); the durable keys always converge quickly.
    /// </summary>
    private static async Task<List<string>> ScanUntilAsync(
        Func<Task<List<string>>> scan,
        Func<List<string>, bool> satisfied)
    {
        var ids = await scan();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!satisfied(ids) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
            ids = await scan();
        }

        return ids;
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
            LatticeAuthOperations.All,
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
