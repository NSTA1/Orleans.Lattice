using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the <see cref="RestoreSagaDispatcher"/> paths that
/// <see cref="RestoreSagaDispatcherTests"/> does not reach: the
/// backup-package-absent decline, the target-tree-resolved-then-declined case,
/// the whole <c>TryDispatchSetAsync</c> surface (decline arms, saga dispatch,
/// cached local results and the synthesized fallback), and the cancellation
/// rethrow inside the peer-reachability probe.
/// </summary>
[TestFixture]
public sealed class RestoreSagaDispatcherSetRestoreTests
{
    private const string TargetTree = "orders";
    private const string OtherTree = "invoices";
    private const string BackupId = "backup-1";
    private const string SetId = "set-1";
    private const string Self = "site-a";
    private const string Peer = "site-b";

    [Test]
    public async Task TryDispatchAsync_declines_when_the_backup_package_is_not_wired()
    {
        // A replication-only host registers the dispatcher but resolves no
        // coordinated-restore engine. There is nothing to promote to a saga, so
        // the dispatcher declines and the caller runs its plain local restore.
        var h = new Harness(withEngine: false);

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task TryDispatchAsync_declines_when_the_manifest_resolved_target_is_not_replicated()
    {
        // No explicit target, so the effective target comes from the manifest
        // chain. The decision is keyed on that resolved tree's status now, and
        // it is not replicated - so the restore takes the local path even though
        // this host does replicate some other tree.
        var h = new Harness();
        h.Membership.IsReplicated(TargetTree).Returns(false);
        h.Membership.ReplicatedTrees.Returns([OtherTree]);

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Null);
        Assert.That(h.Engine.ProbeCount, Is.EqualTo(1), "the target had to be resolved from the manifest first");
    }

    [Test]
    public async Task TryDispatchAsync_returns_the_cached_local_result_when_the_participant_prepared()
    {
        // In production the coordinator drives every participant's prepare,
        // including the local one, before returning Committed - so the local
        // build result is cached under the saga id and returned verbatim rather
        // than synthesized.
        var h = new Harness();
        h.PrepareLocallyDuringSaga();

        var result = await h.Dispatcher.TryDispatchAsync(
            new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.OperationId, Is.EqualTo("op-" + BackupId),
            "the cached participant result carries the engine's own operation id, not the saga id");
        Assert.That(h.Engine.BuildCount, Is.EqualTo(1));
    }

    [Test]
    public async Task TryDispatchSetAsync_declines_when_no_set_resolver_is_wired()
    {
        var h = new Harness(withSetResolver: false);

        var result = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task TryDispatchSetAsync_declines_when_the_id_is_not_a_set_id()
    {
        // An empty member list means the id is a single-tree backup id, so the
        // caller handles it as such.
        var h = new Harness();
        h.SetResolver.ResolveMembersAsync(SetId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<BackupSetMember>>([]));

        var result = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task TryDispatchSetAsync_declines_when_no_member_tree_is_replicated()
    {
        // If NO member is replicated the whole set takes the plain local
        // multi-tree restore; no saga is started.
        var h = new Harness();
        h.Membership.IsReplicated(Arg.Any<string>()).Returns(false);

        var result = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(result, Is.Null);
        await h.Coordinator.DidNotReceive().RunAsync(
            Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public async Task TryDispatchSetAsync_runs_one_saga_and_synthesizes_a_result_per_member()
    {
        // ANY replicated member makes the set run one saga so it stays cross-tree
        // atomic, with local-only members riding along. With no cached local
        // group result (the reactivation case), one committed summary per member
        // is synthesized.
        var h = new Harness();

        var results = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(results, Is.Not.Null);
        Assert.That(results!, Has.Count.EqualTo(2));
        Assert.That(results.Select(r => r.TargetTreeId), Is.EquivalentTo(new[] { TargetTree, OtherTree }));
        Assert.That(results.Select(r => r.BackupId), Is.EquivalentTo(new[] { BackupId, "backup-2" }));
        Assert.That(results, Has.All.Matches<LatticeRestoreResult>(
            r => r.Mode == LatticeRestoreMode.ShadowCutover && r.EntriesApplied == 0));

        // Exactly one saga, carrying the set id on every argument that names it.
        await h.Coordinator.Received(1).RunAsync(
            Arg.Is<List<string>>(l => l.Contains(Self) && l.Contains(Peer)), SetId, SetId, Self, SetId);
    }

    [Test]
    public async Task TryDispatchSetAsync_returns_the_cached_local_group_result_when_the_participant_prepared()
    {
        var h = new Harness();
        h.PrepareSetLocallyDuringSaga();

        var results = await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover);

        Assert.That(results, Is.Not.Null);
        Assert.That(results!, Has.Count.EqualTo(2));
        Assert.That(results, Has.All.Matches<LatticeRestoreResult>(r => r.OperationId.StartsWith("op-")),
            "the cached group result comes from the participant's own builds");
    }

    [Test]
    public void TryDispatchSetAsync_aborted_saga_throws_all_or_nothing()
    {
        var h = new Harness(outcome: CrossClusterSagaOutcome.Aborted);

        Assert.That(async () => await h.Dispatcher.TryDispatchSetAsync(SetId, LatticeRestoreMode.ShadowCutover),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public void TryDispatchSetAsync_rejects_an_empty_set_id()
    {
        var h = new Harness();

        Assert.That(async () => await h.Dispatcher.TryDispatchSetAsync(string.Empty, LatticeRestoreMode.ShadowCutover),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void TryDispatchAsync_rethrows_cancellation_from_the_peer_reachability_probe()
    {
        // Cancellation is not an unreachable peer: it must propagate rather than
        // be folded into the all-or-nothing refusal, so the caller sees the
        // cancellation it asked for.
        var h = new Harness();
        using var cts = new CancellationTokenSource();
        h.Channel.GetStatusAsync(Peer, Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns<Task<SagaControlResponse>>(_ =>
            {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });

        Assert.That(
            async () => await h.Dispatcher.TryDispatchAsync(
                new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover),
                cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Builds a <see cref="RestoreSagaDispatcher"/> over a real
    /// <see cref="RestoreParticipant"/> so the dispatcher's local-result read-back
    /// can be driven by an actual participant prepare rather than a stub.
    /// </summary>
    private sealed class Harness
    {
        public Harness(
            bool withEngine = true,
            bool withSetResolver = true,
            CrossClusterSagaOutcome outcome = CrossClusterSagaOutcome.Committed)
        {
            Membership.IsReplicated(Arg.Any<string>()).Returns(true);
            Membership.ReplicatedTrees.Returns([TargetTree]);

            var topology = Substitute.For<IReplicationTopology>();
            topology.CurrentPeers.Returns([Peer]);

            var capacity = Substitute.For<IRestoreCapacityProbe>();
            capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(true));

            Channel.GetStatusAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new SagaControlResponse()));

            SetResolver.ResolveMembersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<BackupSetMember>>(
                [
                    new BackupSetMember(BackupId, TargetTree),
                    new BackupSetMember("backup-2", OtherTree),
                ]));

            Coordinator.RunAsync(
                    Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
                    Arg.Any<string?>())
                .Returns(_ => Task.FromResult(outcome));

            var factory = Substitute.For<IGrainFactory>();
            // The saga id is derived inside the dispatcher, so capture it off the
            // grain lookup - that is the only place it surfaces to a collaborator.
            factory.GetGrain<ICrossClusterSagaCoordinatorGrain>(Arg.Do<string>(id => SagaId = id))
                .Returns(Coordinator);
            factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(Substitute.For<ISagaWriteFenceGrain>());

            Participant = new RestoreParticipant(
                Engine, Engine, capacity, factory, NullLogger<RestoreParticipant>.Instance,
                withSetResolver ? SetResolver : null);

            var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
            options.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = Self });

            Dispatcher = new RestoreSagaDispatcher(
                Membership,
                topology,
                withEngine ? Engine : null,
                capacity,
                Channel,
                factory,
                Participant,
                options,
                NullLogger<RestoreSagaDispatcher>.Instance,
                withSetResolver ? SetResolver : null);
        }

        public IReplicatedTreeMembership Membership { get; } = Substitute.For<IReplicatedTreeMembership>();

        public ISagaControlChannel Channel { get; } = Substitute.For<ISagaControlChannel>();

        public ILatticeBackupSetResolver SetResolver { get; } = Substitute.For<ILatticeBackupSetResolver>();

        public ICrossClusterSagaCoordinatorGrain Coordinator { get; } =
            Substitute.For<ICrossClusterSagaCoordinatorGrain>();

        public FakeCoordinatedRestoreEngine Engine { get; } = new() { TargetTree = TargetTree };

        public RestoreParticipant Participant { get; }

        public RestoreSagaDispatcher Dispatcher { get; }

        /// <summary>The saga id the dispatcher derived, captured off the grain lookup.</summary>
        public string? SagaId { get; private set; }

        /// <summary>
        /// Makes the coordinator drive this cluster's own participant prepare
        /// before reporting Committed, exactly as the real coordinator does, so
        /// the dispatcher finds a cached local result to return.
        /// </summary>
        public void PrepareLocallyDuringSaga() =>
            Coordinator.RunAsync(
                    Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
                    Arg.Any<string?>())
                .Returns(async _ =>
                {
                    await Participant.PrepareAsync(new SagaControlRequest
                    {
                        SagaId = SagaId!,
                        TargetTree = TargetTree,
                        ManifestId = BackupId,
                        CoordinatorClusterId = Self,
                    });
                    return CrossClusterSagaOutcome.Committed;
                });

        /// <summary>The set-restore equivalent of <see cref="PrepareLocallyDuringSaga"/>.</summary>
        public void PrepareSetLocallyDuringSaga() =>
            Coordinator.RunAsync(
                    Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
                    Arg.Any<string?>())
                .Returns(async _ =>
                {
                    await Participant.PrepareAsync(new SagaControlRequest
                    {
                        SagaId = SagaId!,
                        TargetTree = SetId,
                        ManifestId = SetId,
                        CoordinatorClusterId = Self,
                        SetId = SetId,
                    });
                    return CrossClusterSagaOutcome.Committed;
                });
    }
}
