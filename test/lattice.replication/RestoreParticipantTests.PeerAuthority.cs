using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the participant's <b>peer authority</b> (#5443). A participant acting
/// on a remote coordinator's saga has no user identity, so its restore steps run in a
/// system-origin scope - but only for a current replication peer, on a tree this
/// cluster replicates, replaying backups of trees this cluster replicates. Every
/// engine call records whether system origin was active when it ran, so each test
/// proves exactly which steps were elevated, and that nothing else was.
/// </summary>
public partial class RestoreParticipantTests
{
    private const string LocalCluster = "site-b";

    /// <summary>Records one engine call; the set paths fan their calls out, so the list is guarded.</summary>
    private static void Record(List<(string Call, bool Elevated)> calls, (string Call, bool Elevated) call)
    {
        lock (calls)
        {
            calls.Add(call);
        }
    }

    /// <summary>
    /// A healthy engine substitute that records, per call, whether a system-origin
    /// scope was active. <paramref name="sourceTreeId"/> overrides the captured
    /// source the admission report names (default: the target itself).
    /// </summary>
    private static (ILatticeCoordinatedRestoreEngine Engine, List<(string Call, bool Elevated)> Calls)
        ObservedEngine(string? sourceTreeId = null, bool reportNoSources = false)
    {
        var calls = new List<(string Call, bool Elevated)>();
        var engine = Substitute.For<ILatticeCoordinatedRestoreEngine>();
        engine.ProbeAdmissionAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                Record(calls, ("probe", LatticeSystemOrigin.IsActive));
                var tree = TreeOf(ci, TargetTree);
                var report = ReportFor(tree, sourceTreeId);
                return Task.FromResult(reportNoSources
                    ? new RestoreAdmissionReport(report.BackupId, tree, 1024, 1, 1, report.ManifestChain)
                    : report);
            });
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                Record(calls, ("build", LatticeSystemOrigin.IsActive));
                return Task.FromResult(ResultFor(TreeOf(ci, TargetTree)));
            });
        engine.CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Record(calls, ("commit", LatticeSystemOrigin.IsActive));
                return Task.CompletedTask;
            });
        engine.DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Record(calls, ("delete", LatticeSystemOrigin.IsActive));
                return Task.CompletedTask;
            });
        engine.ResolveShadowTreeId(Arg.Any<LatticeRestoreRequest>())
            .Returns(ci => TreeOf(ci, TargetTree) + "-shadow");
        return (engine, calls);
    }

    /// <summary>A participant on <see cref="LocalCluster"/> that replicates <paramref name="replicated"/> with <paramref name="peers"/>.</summary>
    private static RestoreParticipant PeerParticipant(
        ILatticeCoordinatedRestoreEngine engine,
        string[] replicated,
        string[]? peers,
        ILatticeBackupRestoreService? restoreService = null,
        ILatticeBackupSetResolver? setResolver = null) =>
        Participant(
            engine,
            FactoryFor(Substitute.For<ISagaWriteFenceGrain>()),
            restoreService: restoreService,
            setResolver: setResolver,
            membership: MembershipFor(replicated),
            options: OptionsFor(LocalCluster),
            topology: peers is null ? null : TopologyFor(peers));

    [Test]
    public async Task PrepareAsync_from_a_current_peer_runs_probe_and_build_with_system_origin()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        var result = await participant.PrepareAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit), result.Detail);
            Assert.That(calls, Is.EqualTo(new[] { ("probe", true), ("build", true) }));
            Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the scope ends with the saga step");
        });
    }

    [Test]
    public async Task CommitAsync_from_a_current_peer_cuts_over_with_system_origin()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);
        await participant.PrepareAsync(Request());
        calls.Clear();

        await participant.CommitAsync(Request());

        Assert.That(calls, Is.EqualTo(new[] { ("commit", true) }));
    }

    [Test]
    public async Task AbortAsync_from_a_current_peer_compensates_with_system_origin()
    {
        var (engine, calls) = ObservedEngine();
        var restoreService = Substitute.For<ILatticeBackupRestoreService>();
        var reverts = new List<bool>();
        restoreService.RevertRestoreAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                reverts.Add(LatticeSystemOrigin.IsActive);
                return Task.CompletedTask;
            });
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster], restoreService);
        await participant.PrepareAsync(Request());
        await participant.CommitAsync(Request());
        calls.Clear();

        await participant.AbortAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(reverts, Is.EqualTo(new[] { true }));
            Assert.That(calls, Is.EqualTo(new[] { ("delete", true) }));
        });
    }

    [Test]
    public async Task PrepareAsync_from_this_clusters_own_coordinator_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        var request = Request() with { CoordinatorClusterId = LocalCluster };
        await participant.PrepareAsync(request);

        Assert.That(calls.Select(c => c.Elevated), Is.All.False,
            "a loopback saga carries the operator's own identity and is authorized as such");
    }

    [Test]
    public async Task PrepareAsync_from_a_cluster_that_is_not_a_current_peer_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], ["site-c"]);

        await participant.PrepareAsync(Request());

        Assert.That(calls.Select(c => c.Elevated), Is.All.False);
    }

    [TestCase(null)]
    [TestCase("")]
    public async Task PrepareAsync_with_no_coordinator_runs_as_the_ambient_caller(string? coordinator)
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        await participant.PrepareAsync(Request() with { CoordinatorClusterId = coordinator! });

        Assert.That(calls.Select(c => c.Elevated), Is.All.False);
    }

    [Test]
    public async Task PrepareAsync_without_the_topology_seam_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], peers: null);

        await participant.PrepareAsync(Request());

        Assert.That(calls.Select(c => c.Elevated), Is.All.False, "a missing seam is a refusal, never a pass");
    }

    [Test]
    public async Task PrepareAsync_from_a_peer_for_an_unreplicated_target_is_refused_before_any_engine_call()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, ["some-other-tree"], [CoordinatorCluster]);

        var result = await participant.PrepareAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(calls, Is.Empty);
        });
    }

    [Test]
    public async Task PrepareAsync_from_a_peer_replaying_a_backup_of_an_unreplicated_tree_votes_abort_without_building()
    {
        var (engine, calls) = ObservedEngine(sourceTreeId: "ledger-local");
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        var result = await participant.PrepareAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(result.Detail, Does.Contain("captured from a tree this cluster does not replicate"));
            Assert.That(calls.Select(c => c.Call), Does.Not.Contain("build"));
        });
    }

    [Test]
    public async Task PrepareAsync_from_a_peer_with_a_report_naming_no_source_votes_abort()
    {
        var (engine, calls) = ObservedEngine(reportNoSources: true);
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        var result = await participant.PrepareAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort), "an unknown source is refused, not assumed");
            Assert.That(calls.Select(c => c.Call), Does.Not.Contain("build"));
        });
    }

    [Test]
    public async Task PrepareAsync_from_a_peer_replaying_a_backup_of_another_replicated_tree_builds()
    {
        var (engine, calls) = ObservedEngine(sourceTreeId: "orders-archive");
        var participant = PeerParticipant(engine, [TargetTree, "orders-archive"], [CoordinatorCluster]);

        var result = await participant.PrepareAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit), result.Detail);
            Assert.That(calls, Does.Contain(("build", true)));
        });
    }

    [Test]
    public async Task PrepareAsync_as_the_ambient_caller_does_not_apply_the_source_confinement()
    {
        // The coordinator cluster authorizes the operator's captured sources itself,
        // as that operator; the confinement exists only for the peer's authority.
        var (engine, calls) = ObservedEngine(sourceTreeId: "ledger-local");
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        var result = await participant.PrepareAsync(Request() with { CoordinatorClusterId = LocalCluster });

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That(calls, Does.Contain(("build", false)));
        });
    }

    [Test]
    public async Task PrepareSetAsync_from_a_current_peer_builds_every_replicated_member_with_system_origin()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(
            engine, [TreeA, TreeB], [CoordinatorCluster], setResolver: TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Commit), result.Detail);
            Assert.That(calls.Count(c => c.Call == "build"), Is.EqualTo(2));
            Assert.That(calls.Select(c => c.Elevated), Is.All.True);
        });
    }

    [Test]
    public async Task PrepareSetAsync_from_a_peer_refuses_a_member_replaying_an_unreplicated_source()
    {
        var (engine, calls) = ObservedEngine(sourceTreeId: "ledger-local");
        var participant = PeerParticipant(
            engine, [TreeA, TreeB], [CoordinatorCluster], setResolver: TwoMemberResolver());

        var result = await participant.PrepareAsync(SetRequestFor(SetId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(calls.Select(c => c.Call), Does.Not.Contain("build"));
            Assert.That(participant.TryGetLocalSetResult(SetId, out _), Is.False);
        });
    }

    [Test]
    public async Task CommitSetAsync_and_AbortSetAsync_from_a_current_peer_run_with_system_origin()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(
            engine, [TreeA, TreeB], [CoordinatorCluster], setResolver: TwoMemberResolver());
        await participant.PrepareAsync(SetRequestFor(SetId));
        calls.Clear();

        await participant.CommitAsync(SetRequestFor(SetId));
        await participant.AbortAsync(SetRequestFor(SetId));

        Assert.Multiple(() =>
        {
            Assert.That(calls.Count(c => c.Call == "commit"), Is.EqualTo(2));
            Assert.That(calls.Count(c => c.Call == "delete"), Is.EqualTo(2));
            Assert.That(calls.Select(c => c.Elevated), Is.All.True);
        });
    }
}
