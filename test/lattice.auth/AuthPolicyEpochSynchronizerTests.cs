using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthPolicyEpochSynchronizer"/>, which keeps a
/// silo's compiled policy snapshot current with policy writes committed on other
/// silos. Before it, the core change feed was the only rebuild trigger and it
/// fires only on the silo hosting the committing leaf, so a revocation never
/// reached the rest of the cluster.
/// </summary>
[TestFixture]
public sealed class AuthPolicyEpochSynchronizerTests
{
    private const string PolicyTree = "sys-auth-policy";

    private static readonly Guid Incarnation = Guid.NewGuid();

    [Test]
    public async Task A_policy_write_committed_on_another_silo_is_enforced_after_the_next_poll()
    {
        var rig = Rig.Create();
        rig.Store.Rules.Add(AliceReadsPayroll());
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True, "the first poll builds the snapshot");
        Assert.That(rig.AliceMayRead(), Is.True, "precondition: the grant admits");

        // Another silo revokes the grant; this silo's change feed never sees it.
        rig.Store.Rules.Clear();
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False, "an unmoved epoch rebuilds nothing");
        Assert.That(rig.AliceMayRead(), Is.True, "nothing has told this silo about the write yet");

        rig.Epoch.GetAsync().Returns(new AuthPolicyEpoch(Incarnation, 1));

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True);
            Assert.That(rig.AliceMayRead(), Is.False, "the revocation is enforced once the epoch moves");
        });
    }

    [Test]
    public async Task An_unchanged_epoch_and_membership_never_rebuild()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);
        var epoch = rig.Maintainer.CurrentEpoch;

        for (var i = 0; i < 3; i++)
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False);
        }

        Assert.That(rig.Maintainer.CurrentEpoch, Is.EqualTo(epoch), "the steady state costs no rebuild");
    }

    [Test]
    public async Task A_restarted_epoch_grain_rebuilds_even_at_a_lower_version()
    {
        var rig = Rig.Create();
        rig.Epoch.GetAsync().Returns(new AuthPolicyEpoch(Incarnation, 7));
        await rig.Synchronizer.PollOnceAsync(default);

        rig.Epoch.GetAsync().Returns(new AuthPolicyEpoch(Guid.NewGuid(), 0));

        Assert.That(
            await rig.Synchronizer.PollOnceAsync(default),
            Is.True,
            "a new incarnation may have lost an advance, so it must read as a change");
    }

    [Test]
    public async Task A_membership_change_rebuilds()
    {
        var membership = Substitute.For<IClusterMembershipService>();
        membership.CurrentSnapshot.Returns(Snapshot(1));
        var rig = Rig.Create(membership);
        await rig.Synchronizer.PollOnceAsync(default);
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False, "precondition: nothing moved");

        membership.CurrentSnapshot.Returns(Snapshot(2));

        Assert.That(
            await rig.Synchronizer.PollOnceAsync(default),
            Is.True,
            "a silo may have died between committing a write and publishing it");
    }

    [Test]
    public async Task A_failed_rebuild_is_retried_on_the_next_poll()
    {
        var rig = Rig.Create();
        rig.Store.Rules.Add(AliceReadsPayroll());
        await rig.Synchronizer.PollOnceAsync(default);
        rig.Store.Rules.Clear();
        rig.Epoch.GetAsync().Returns(new AuthPolicyEpoch(Incarnation, 1));
        rig.Store.FailuresRemaining = 1;

        Assert.That(async () => await rig.Synchronizer.PollOnceAsync(default), Throws.InvalidOperationException);
        Assert.That(rig.AliceMayRead(), Is.True, "precondition: the failed rebuild left the old snapshot in force");

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True, "the same epoch is retried");
            Assert.That(rig.AliceMayRead(), Is.False);
        });
    }

    [Test]
    public async Task A_policy_write_observed_here_advances_the_cluster_epoch()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation(PolicyTree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(1).AdvanceAsync();
    }

    [Test]
    public async Task A_write_to_any_other_tree_publishes_nothing()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation("orders"), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.DidNotReceive().AdvanceAsync();
    }

    [Test]
    public async Task A_failed_advance_is_retried_until_it_lands()
    {
        var rig = Rig.Create();
        rig.Epoch.AdvanceAsync().Returns(
            _ => Task.FromException<AuthPolicyEpoch>(new InvalidOperationException("epoch grain unreachable")),
            _ => Task.FromResult(new AuthPolicyEpoch(Incarnation, 1)));

        await rig.Synchronizer.OnMutationAsync(Mutation(PolicyTree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(2).AdvanceAsync();
    }

    [Test]
    public async Task The_poll_loop_follows_the_cluster_epoch_on_its_own_and_stops_cleanly()
    {
        var rig = Rig.Create(pollInterval: TimeSpan.FromMilliseconds(20));
        rig.Store.Rules.Add(AliceReadsPayroll());

        await rig.Synchronizer.StartAsync(default);
        try
        {
            await WaitUntilAsync(() => rig.Maintainer.CurrentEpoch > 0 && rig.AliceMayRead());
            rig.Store.Rules.Clear();
            rig.Epoch.GetAsync().Returns(new AuthPolicyEpoch(Incarnation, 1));

            await WaitUntilAsync(() => !rig.AliceMayRead());
        }
        finally
        {
            await rig.Synchronizer.StopAsync(default);
        }

        Assert.That(rig.Synchronizer.PollLoop.IsCompleted, Is.True);
    }

    [Test]
    public void The_poll_interval_must_be_positive() =>
        Assert.That(
            () => Rig.Create(pollInterval: TimeSpan.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    private static LatticeAuthorizationRule AliceReadsPayroll() =>
        new("alice-payroll", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("payroll"), LatticeOperation.Read, LatticeEffect.Allow);

    private static LatticeMutation Mutation(string treeId) =>
        new() { TreeId = treeId, Kind = MutationKind.Set, Key = "k" };

    private static ClusterMembershipSnapshot Snapshot(long version) =>
        new(ImmutableDictionary<SiloAddress, ClusterMember>.Empty, new MembershipVersion(version));

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!condition())
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "the condition never held");
            await Task.Delay(10);
        }
    }

    private sealed record Rig(
        AuthPolicyEpochSynchronizer Synchronizer,
        IAuthPolicyEpochGrain Epoch,
        FlakyPolicyStore Store,
        CompiledPolicySnapshotMaintainer Maintainer,
        LatticeDecisionEngine Engine)
    {
        public static Rig Create(IClusterMembershipService? membership = null, TimeSpan? pollInterval = null)
        {
            var store = new FlakyPolicyStore();
            var maintainer = new CompiledPolicySnapshotMaintainer(store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance);
            var engine = new LatticeDecisionEngine(maintainer, new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()));
            var epoch = Substitute.For<IAuthPolicyEpochGrain>();
            epoch.GetAsync().Returns(new AuthPolicyEpoch(Incarnation, 0));
            epoch.AdvanceAsync().Returns(new AuthPolicyEpoch(Incarnation, 1));
            var grains = Substitute.For<IGrainFactory>();
            grains.GetGrain<IAuthPolicyEpochGrain>(IAuthPolicyEpochGrain.GrainKey, Arg.Any<string?>()).Returns(epoch);
            var synchronizer = new AuthPolicyEpochSynchronizer(
                grains,
                maintainer,
                NullLogger<AuthPolicyEpochSynchronizer>.Instance,
                membership,
                timeProvider: null,
                pollInterval ?? TimeSpan.FromMinutes(5));
            return new Rig(synchronizer, epoch, store, maintainer, engine);
        }

        public bool AliceMayRead() =>
            Engine.Evaluate(new LatticeSubject("alice"), "payroll", LatticeOperation.Read, key: "k").Allowed;
    }

    /// <summary>An in-memory policy store whose next scans can be made to fail.</summary>
    private sealed class FlakyPolicyStore : ILatticeAuthorizationPolicyStore
    {
        private readonly CovPolicyStore _inner = new();

        public List<LatticeAuthorizationRule> Rules => _inner.Rules;

        public int FailuresRemaining { get; set; }

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default) =>
            _inner.PutRuleAsync(rule, cancellationToken);

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            _inner.GetRuleAsync(treeId, ruleId, cancellationToken);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            _inner.RemoveRuleAsync(treeId, ruleId, cancellationToken);

        public IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, CancellationToken cancellationToken = default) =>
            _inner.ListRulesForTreeAsync(treeId, cancellationToken);

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (FailuresRemaining > 0)
            {
                FailuresRemaining--;
                throw new InvalidOperationException("transient policy scan failure");
            }

            await foreach (var rule in _inner.ListRulesAsync(cancellationToken))
            {
                yield return rule;
            }
        }
    }
}
