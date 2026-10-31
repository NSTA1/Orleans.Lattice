using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression coverage for #5462: a restore into a replicated tree must never fall
/// back to an uncoordinated local replacement because this silo's membership
/// snapshot is stale. In the Explorer journeys run a tree enabled at runtime on
/// another silo still read "not replicated" on the silo that served the restore,
/// so <see cref="RestoreSagaDispatcher"/> declined, the tree was replaced locally,
/// and the peer silently kept the writes the replacement discarded. The
/// dispatcher now confirms every decline against a refreshed snapshot, and
/// refuses the restore when it cannot read one.
/// </summary>
[TestFixture]
public sealed class RestoreSagaDispatcherStaleMembershipTests
{
    private const string TargetTree = "factory-orders";
    private const string OtherTree = "factory-telemetry";
    private const string BackupId = "orders-baseline";
    private const string SetId = "set-1";
    private const string Self = "west";
    private const string Peer = "east";

    [Test]
    public async Task A_replicated_target_the_local_snapshot_has_not_caught_up_with_still_runs_the_saga()
    {
        var h = new Harness();
        h.Membership.EnabledElsewhere.Add(TargetTree);

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Not.Null, "a stale snapshot must not send a replicated tree down the local path");
        Assert.That(h.Membership.RefreshCount, Is.EqualTo(1));
        await h.Coordinator.Received(1).RunAsync(
            Arg.Is<List<string>>(l => l.Contains(Self) && l.Contains(Peer)), TargetTree, BackupId, Self);
    }

    [Test]
    public async Task A_stale_host_with_no_replicated_trees_in_its_snapshot_refreshes_before_declining()
    {
        var h = new Harness();
        h.Membership.EnabledElsewhere.Add(TargetTree);

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Not.Null, "the manifest-resolved replicated target runs the saga");
        Assert.That(h.Membership.RefreshCount, Is.EqualTo(1), "one refresh serves the whole dispatch");
    }

    [Test]
    public async Task A_target_that_is_still_unreplicated_after_the_refresh_takes_the_local_path()
    {
        var h = new Harness();
        h.Membership.Replicated.Add(OtherTree);

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Null);
        Assert.That(h.Membership.RefreshCount, Is.EqualTo(1));
        Assert.That(h.Engine.ProbeCount, Is.EqualTo(0), "the explicit-target fast path still avoids manifest I/O");
    }

    [Test]
    public async Task A_target_the_snapshot_already_reports_replicated_is_not_refreshed()
    {
        var h = new Harness();
        h.Membership.Replicated.Add(TargetTree);

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Not.Null);
        Assert.That(h.Membership.RefreshCount, Is.EqualTo(0), "only a decline is confirmed");
    }

    [Test]
    public void A_restore_is_refused_when_the_replication_configuration_cannot_be_read()
    {
        var h = new Harness();
        h.Membership.RefreshFailure = new InvalidOperationException("config tree unreachable");

        var ex = Assert.ThrowsAsync<LatticeRestoreValidationException>(async () => await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover)));

        Assert.That(ex!.Message, Does.Contain("refused").And.Contain("replicated"));
        Assert.That(h.Engine.BuildCount, Is.EqualTo(0), "nothing was replaced");
    }

    [Test]
    public void Cancellation_of_the_refresh_propagates_as_cancellation()
    {
        var h = new Harness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        h.Membership.RefreshFailure = new OperationCanceledException(cts.Token);

        Assert.ThrowsAsync<OperationCanceledException>(async () => await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover),
            cts.Token));
    }

    [Test]
    public async Task A_set_holding_a_tree_the_local_snapshot_has_not_caught_up_with_runs_one_saga()
    {
        var h = new Harness();
        h.Membership.EnabledElsewhere.Add(TargetTree);

        var result = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(result, Is.Not.Null, "a stale snapshot must not restore a replicated member uncoordinated");
        Assert.That(h.Membership.RefreshCount, Is.EqualTo(1));
        await h.Coordinator.Received(1).RunAsync(
            Arg.Any<List<string>>(), SetId, SetId, Self, SetId);
    }

    [Test]
    public async Task A_set_with_no_replicated_member_after_the_refresh_takes_the_local_path()
    {
        var h = new Harness();

        var result = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(result, Is.Null);
        Assert.That(h.Membership.RefreshCount, Is.EqualTo(1));
    }

    [Test]
    public void A_set_restore_is_refused_when_the_replication_configuration_cannot_be_read()
    {
        var h = new Harness();
        h.Membership.RefreshFailure = new InvalidOperationException("config tree unreachable");

        Assert.ThrowsAsync<LatticeRestoreValidationException>(async () =>
            await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover));
    }

    /// <summary>
    /// A membership whose snapshot misses the trees enabled on another silo until
    /// it is refreshed, as the per-silo compiled snapshot did before #5462.
    /// </summary>
    private sealed class StaleSnapshotMembership : IReplicatedTreeMembership, IRefreshableReplicatedTreeMembership
    {
        public HashSet<string> Replicated { get; } = new(StringComparer.Ordinal);

        public HashSet<string> EnabledElsewhere { get; } = new(StringComparer.Ordinal);

        public Exception? RefreshFailure { get; set; }

        public int RefreshCount { get; private set; }

        public IReadOnlyCollection<string> ReplicatedTrees => Replicated;

        public bool IsReplicated(string treeId) => Replicated.Contains(treeId);

        public Task RefreshAsync(CancellationToken cancellationToken = default)
        {
            RefreshCount++;
            if (RefreshFailure is { } failure)
            {
                return Task.FromException(failure);
            }

            Replicated.UnionWith(EnabledElsewhere);
            return Task.CompletedTask;
        }
    }

    private sealed class Harness
    {
        public Harness()
        {
            var topology = Substitute.For<IReplicationTopology>();
            topology.CurrentPeers.Returns([Peer]);

            var capacity = Substitute.For<IRestoreCapacityProbe>();
            capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(true));

            var channel = Substitute.For<ISagaControlChannel>();
            channel.GetStatusAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new SagaControlResponse()));

            var setResolver = Substitute.For<ILatticeBackupSetResolver>();
            setResolver.ResolveMembersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<BackupSetMember>>(
                [
                    new BackupSetMember(BackupId, TargetTree),
                    new BackupSetMember("backup-2", OtherTree),
                ]));

            Coordinator.RunAsync(Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
                .Returns(Task.FromResult(CrossClusterSagaOutcome.Committed));
            Coordinator.RunAsync(
                    Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>())
                .Returns(Task.FromResult(CrossClusterSagaOutcome.Committed));

            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<ICrossClusterSagaCoordinatorGrain>(Arg.Any<string>()).Returns(Coordinator);
            factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(Substitute.For<ISagaWriteFenceGrain>());

            var participant = new RestoreParticipant(
                Engine, Engine, capacity, factory, NullLogger<RestoreParticipant>.Instance, setResolver);

            var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
            options.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = Self });

            Dispatcher = new RestoreSagaDispatcher(
                Membership, topology, Engine, capacity, channel, factory, participant, options,
                NullLogger<RestoreSagaDispatcher>.Instance, setResolver);
        }

        public StaleSnapshotMembership Membership { get; } = new();

        public ICrossClusterSagaCoordinatorGrain Coordinator { get; } = Substitute.For<ICrossClusterSagaCoordinatorGrain>();

        public FakeCoordinatedRestoreEngine Engine { get; } = new() { TargetTree = TargetTree };

        public RestoreSagaDispatcher Dispatcher { get; }
    }
}
