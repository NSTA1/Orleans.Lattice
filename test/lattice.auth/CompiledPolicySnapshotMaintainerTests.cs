using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledPolicySnapshotMaintainer"/> over an in-memory
/// fake policy store (no cluster). Covers the warm-up build, the monotonic epoch,
/// change-feed-driven rebuilds filtered to the reserved policy tree, and the
/// atomic snapshot swap.
/// </summary>
[TestFixture]
public sealed class CompiledPolicySnapshotMaintainerTests
{
    private static CompiledPolicySnapshotMaintainer CreateMaintainer(FakePolicyStore store) =>
        new(store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance);

    private static LatticeAuthorizationRule Rule(string treeId, string ruleId, LatticeEffect effect) =>
        new(ruleId, LatticeSubjectSelector.User("alice"), LatticeScope.Tree(treeId), LatticeOperation.Read, effect);

    private static async Task<bool> WaitForEpochAtLeast(CompiledPolicySnapshotMaintainer maintainer, long target, int timeoutMs = 5000)
    {
        var start = Environment.TickCount64;
        while (Environment.TickCount64 - start < timeoutMs)
        {
            if (maintainer.CurrentEpoch >= target)
            {
                return true;
            }

            await Task.Delay(20);
        }

        return maintainer.CurrentEpoch >= target;
    }

    [Test]
    public void Fresh_maintainer_starts_at_epoch_zero_with_empty_snapshot()
    {
        var maintainer = CreateMaintainer(new FakePolicyStore());

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0));
        Assert.That(maintainer.Current.TreeCount, Is.EqualTo(0));
    }

    [Test]
    public async Task EnsureWarmAsync_builds_the_snapshot_and_advances_the_epoch_once()
    {
        var store = new FakePolicyStore();
        store.Rules.Add(Rule("app", "r", LatticeEffect.Allow));
        var maintainer = CreateMaintainer(store);

        await maintainer.EnsureWarmAsync();

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
        Assert.That(maintainer.Current.TryGetTree("app", out _), Is.True);

        // Second warm is a no-op (idempotent) once warm.
        await maintainer.EnsureWarmAsync();
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public async Task OnMutationAsync_on_the_policy_tree_rebuilds_and_advances_the_epoch()
    {
        var store = new FakePolicyStore();
        var maintainer = CreateMaintainer(store);
        await maintainer.EnsureWarmAsync();
        var epochBefore = maintainer.CurrentEpoch;

        store.Rules.Add(Rule("app", "r", LatticeEffect.Allow));
        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = AuthConstants.PolicyTree }, CancellationToken.None);

        Assert.That(await WaitForEpochAtLeast(maintainer, epochBefore + 1), Is.True, "a policy-tree mutation must trigger a rebuild");
        Assert.That(maintainer.Current.TryGetTree("app", out _), Is.True, "the rebuilt snapshot must reflect the new rule");
    }

    [Test]
    public async Task OnMutationAsync_on_a_non_policy_tree_does_not_rebuild()
    {
        var store = new FakePolicyStore();
        var maintainer = CreateMaintainer(store);
        await maintainer.EnsureWarmAsync();
        var epochBefore = maintainer.CurrentEpoch;

        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = "some-app-tree" }, CancellationToken.None);
        await Task.Delay(200);

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epochBefore), "an unrelated tree mutation must not rebuild the policy snapshot");
    }

    [Test]
    public async Task RebuildNowAsync_reflects_the_latest_store_state()
    {
        var store = new FakePolicyStore();
        var maintainer = CreateMaintainer(store);
        await maintainer.RebuildNowAsync();
        Assert.That(maintainer.Current.TreeCount, Is.EqualTo(0));

        store.Rules.Add(Rule("app", "r", LatticeEffect.Deny));
        var epoch = await maintainer.RebuildNowAsync();

        Assert.That(epoch, Is.EqualTo(2));
        Assert.That(maintainer.Current.TryGetTree("app", out _), Is.True);
    }

    /// <summary>
    /// A minimal in-memory <see cref="ILatticeAuthorizationPolicyStore"/>: only
    /// <see cref="ListRulesAsync"/> is exercised by the maintainer.
    /// </summary>
    private sealed class FakePolicyStore : ILatticeAuthorizationPolicyStore
    {
        public List<LatticeAuthorizationRule> Rules { get; } = new();

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        {
            Rules.Add(rule);
            return Task.CompletedTask;
        }

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult<LatticeAuthorizationRule?>(null);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in Rules)
            {
                if (string.Equals(rule.Scope.TreeId, treeId, StringComparison.Ordinal))
                {
                    yield return rule;
                }
            }

            await Task.CompletedTask;
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Snapshot to avoid mutation-during-enumeration when a test edits the
            // list between rebuilds.
            foreach (var rule in Rules.ToArray())
            {
                yield return rule;
            }

            await Task.CompletedTask;
        }
    }
}
