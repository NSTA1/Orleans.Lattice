using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Regression tests for a snapshot that warmed over an empty policy and then kept
/// answering after the first rules committed (the repository-context host's seeded
/// grant read as "no matching rule" after #4143).
/// </summary>
/// <remarks>
/// <para>
/// Since #4143 the cold warm-up of a host that has never written a rule returns at
/// once: the policy tree is unregistered, so the store answers empty without
/// scanning. The gate is then warm over an empty snapshot. A host that seeds its
/// grants next schedules a background rebuild per write, and that rebuild scans a
/// tree whose shards the seed writes are still creating, so it can take seconds.
/// Every gated request in that window was evaluated against the empty (or a
/// partial, pre-seed) snapshot and denied. Before #4143 the cold scan itself paid
/// the shard seeding, the epoch stayed at zero, and requests waited for it.
/// </para>
/// <para>
/// The scans here are held on <see cref="TaskCompletionSource"/> gates, so each
/// interleaving is forced rather than raced.
/// </para>
/// </remarks>
[TestFixture]
public sealed class PolicyAccessGateFirstRuleWarmupTests
{
    private static LatticeAuthorizationRule Grant(string treeId) =>
        new($"grant-{treeId}", LatticeSubjectSelector.User("agent"), LatticeScope.Tree(treeId), LatticeOperation.Write, LatticeEffect.Allow);

    private static LatticeAccessRequest WriteRequest(string treeId) =>
        new(treeId, LatticeOperation.Write, new LatticeSubject("agent"), "k");

    private static LatticeMutation PolicyMutation() => new() { TreeId = AuthConstants.PolicyTree };

    private static (PolicyAccessGate Gate, CompiledPolicySnapshotMaintainer Maintainer) CreateGate(HoldablePolicyStore store)
    {
        var options = new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny });
        var maintainer = new CompiledPolicySnapshotMaintainer(store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance);
        var engine = new LatticeDecisionEngine(maintainer, options);
        var observer = new LatticeAuthDecisionObserver(
            Array.Empty<ILatticeAuthAuditSink>(), options, NullLogger<LatticeAuthDecisionObserver>.Instance);
        return (new PolicyAccessGate(engine, maintainer, observer, options, new NullTenantGateEnforcer()), maintainer);
    }

    [Test]
    public async Task A_request_after_the_first_rule_commits_waits_for_a_rebuild_rather_than_the_empty_warm_snapshot()
    {
        var store = new HoldablePolicyStore();
        var (gate, maintainer) = CreateGate(store);

        // The host warms over a policy that holds nothing yet.
        await maintainer.EnsureWarmAsync();
        Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(0), "precondition: the empty snapshot is published");

        // The seed rule commits, and its change-feed rebuild is caught inside its scan.
        var scan = store.HoldNextScan();
        store.Rules.Add(Grant("app"));
        await maintainer.OnMutationAsync(PolicyMutation(), CancellationToken.None);
        await scan.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var pending = gate.AuthorizeAsync(WriteRequest("app")).AsTask();

        Assert.That(
            pending.IsCompleted,
            Is.False,
            "a request issued after the first rule committed must not be answered from the snapshot built "
            + "before it (it would be: " + (pending.IsCompleted ? pending.Result.Reason : "pending") + ")");

        scan.Release.SetResult();
        var decision = await pending.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(decision.Allowed, Is.True, decision.Reason);
    }

    [Test]
    public async Task A_rebuild_that_overlapped_a_later_seed_write_does_not_mark_the_partial_snapshot_warm()
    {
        var store = new HoldablePolicyStore();
        var (gate, maintainer) = CreateGate(store);
        await maintainer.EnsureWarmAsync();

        // First seed: its rebuild captures the rule set as it stood then, and is held.
        var first = store.HoldNextScan();
        store.Rules.Add(Grant("structural"));
        await maintainer.OnMutationAsync(PolicyMutation(), CancellationToken.None);
        await first.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Second seed commits while that scan is in flight; its rebuild is queued.
        var second = store.HoldNextScan();
        store.Rules.Add(Grant("memory"));
        await maintainer.OnMutationAsync(PolicyMutation(), CancellationToken.None);

        // The first rebuild publishes a snapshot holding only the first grant, and the
        // queued follow-up is caught inside its scan.
        var epochBefore = maintainer.CurrentEpoch;
        first.Release.SetResult();
        await second.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(epochBefore), "precondition: the partial snapshot is published");
        Assert.That(maintainer.Current.TryGetTree("memory", out _), Is.False, "precondition: the published snapshot is partial");

        var pending = gate.AuthorizeAsync(WriteRequest("memory")).AsTask();

        Assert.That(
            pending.IsCompleted,
            Is.False,
            "a snapshot whose scan overlapped a later seed write must not be served as warm (it answered: "
            + (pending.IsCompleted ? pending.Result.Reason : "pending") + ")");

        second.Release.SetResult();
        var decision = await pending.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(decision.Allowed, Is.True, decision.Reason);
    }

    [Test]
    public async Task A_warm_snapshot_holding_rules_still_answers_synchronously_while_a_rebuild_is_in_flight()
    {
        // The documented eventual path is unchanged once the policy holds rules: an
        // edit's rebuild never makes requests wait.
        var store = new HoldablePolicyStore();
        store.Rules.Add(Grant("app"));
        var (gate, maintainer) = CreateGate(store);
        await maintainer.EnsureWarmAsync();

        var scan = store.HoldNextScan();
        store.Rules.Add(Grant("other"));
        await maintainer.OnMutationAsync(PolicyMutation(), CancellationToken.None);
        await scan.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var decision = gate.AuthorizeAsync(WriteRequest("app"));
        try
        {
            Assert.That(decision.IsCompletedSuccessfully, Is.True, "a warm, non-empty snapshot answers on the fast path");
            Assert.That(decision.Result.Allowed, Is.True);
        }
        finally
        {
            scan.Release.SetResult();
        }
    }

    [Test]
    public async Task A_host_that_never_writes_a_rule_stays_warm_after_its_first_build()
    {
        var store = new HoldablePolicyStore();
        var (gate, maintainer) = CreateGate(store);
        await maintainer.EnsureWarmAsync();

        var decision = gate.AuthorizeAsync(WriteRequest("app"));

        Assert.That(maintainer.IsWarm, Is.True, "a first build over an untouched empty policy is warm");
        Assert.That(decision.IsCompletedSuccessfully, Is.True, "an empty policy that nothing writes keeps the fast path");
        Assert.That(decision.Result.Allowed, Is.False);
        Assert.That(store.ScanCount, Is.EqualTo(1), "no request rescans an empty policy that has not changed");
    }

    /// <summary>
    /// A policy store whose next scan can be held: the scan captures the rule set at
    /// entry (as a real scan's cursor passes a key before a later write lands
    /// there), signals <see cref="ScanHold.Entered"/>, and yields only once
    /// <see cref="ScanHold.Release"/> completes.
    /// </summary>
    private sealed class HoldablePolicyStore : ILatticeAuthorizationPolicyStore
    {
        private readonly Queue<ScanHold> _holds = new();
        private int _scanCount;

        public List<LatticeAuthorizationRule> Rules { get; } = new();

        public int ScanCount => Volatile.Read(ref _scanCount);

        public ScanHold HoldNextScan()
        {
            var hold = new ScanHold();
            lock (_holds)
            {
                _holds.Enqueue(hold);
            }

            return hold;
        }

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        {
            Rules.Add(rule);
            return Task.CompletedTask;
        }

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult<LatticeAuthorizationRule?>(null);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _scanCount);
            var captured = Rules.ToArray();
            ScanHold? hold = null;
            lock (_holds)
            {
                _holds.TryDequeue(out hold);
            }

            if (hold is not null)
            {
                hold.Entered.TrySetResult();
                await hold.Release.Task.ConfigureAwait(false);
            }

            foreach (var rule in captured)
            {
                yield return rule;
            }
        }
    }

    private sealed class ScanHold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
