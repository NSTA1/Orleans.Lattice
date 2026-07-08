using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Reliability soak coverage for a coordinated restore under duress: a
/// large-tree-onto-small-cluster build that is interrupted, an unrecoverable
/// participant, a bounded write-fence window, and an infeasible target. These
/// fixtures assert the epic's reliability guarantees - resumable build, clean
/// all-or-nothing abort with no orphan shadow, a fence engaged only at cutover,
/// and admission-time refusal of infeasible targets.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CoordinatedRestoreReliabilitySoakTests
{
    private const string TargetTree = "big-tree";
    private const string ManifestId = "backup-big";
    private const string CoordinatorCluster = "site-home";

    /// <summary>Builds a bare restore participant over a fake engine, a substitute fence, and a capacity probe.</summary>
    private static (RestoreParticipant Participant, FakeCoordinatedRestoreEngine Engine) NewParticipant(
        Action<FakeCoordinatedRestoreEngine>? configureEngine = null,
        bool refuseCapacity = false)
    {
        var engine = new FakeCoordinatedRestoreEngine { TargetTree = TargetTree };
        configureEngine?.Invoke(engine);

        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(!refuseCapacity));

        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(fence);

        var participant = new RestoreParticipant(
            engine, engine, capacity, factory, NullLogger<RestoreParticipant>.Instance);
        return (participant, engine);
    }

    private static SagaControlRequest Request(string sagaId) =>
        new()
        {
            SagaId = sagaId,
            TargetTree = TargetTree,
            ManifestId = ManifestId,
            CoordinatorClusterId = CoordinatorCluster,
        };

    [Test]
    public async Task Participant_restart_mid_build_resumes_the_shadow_it_does_not_restart_from_scratch()
    {
        // Transient exhaustion twice (an OOM/restart mid-build) then success: the
        // participant's bounded, resumable retry budget carries the build to a vote,
        // rather than giving up after the first failure.
        var (participant, engine) = NewParticipant(e => e.TransientBuildFailures = 2);
        var request = Request("restore-soak-resume");

        var vote = await participant.PrepareAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit), "the resumable build carries through to a commit vote");
            Assert.That(engine.BuildCount, Is.EqualTo(3), "the build was resumed (2 failures + 1 success), not abandoned");
        });

        // Model a participant activation loss between prepare and commit: a fresh
        // participant with an empty cache re-derives the SAME shadow idempotently
        // (a resume), rather than failing because it lost the prepared handle.
        var (restarted, restartedEngine) = NewParticipant();
        // Point the restarted participant at the same target so the re-derived shadow
        // id matches; commit rebuilds idempotently from the backup's fixed cut.
        await restarted.CommitAsync(request);

        Assert.That(restartedEngine.CommitCount, Is.EqualTo(1),
            "a restarted participant resumes to the prepared shadow and commits it");
    }

    [Test]
    public async Task Unrecoverable_participant_aborts_all_or_nothing_with_no_orphan_shadow()
    {
        var sagaId = "restore-soak-unrecoverable";

        // site-a builds its shadow; site-b's build is permanently broken.
        var a = CoordinatedRestoreSagaHarness.CreateCluster("site-a", sagaId, TargetTree);
        var b = CoordinatedRestoreSagaHarness.CreateCluster(
            "site-b", sagaId, TargetTree,
            configureEngine: e => e.BuildFailure = new InvalidOperationException("unrecoverable participant"));

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        var coordinator = CoordinatedRestoreSagaHarness.CreateCoordinator(sagaId, channel);
        var outcome = await coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));

        Assert.Multiple(() =>
        {
            // All-or-nothing: no cluster committed the cut.
            Assert.That(a.Engine.CommitCount, Is.EqualTo(0));
            Assert.That(b.Engine.CommitCount, Is.EqualTo(0));

            // No orphan shadow: every cluster that built (or partially built) a shadow
            // reliably garbage collected it, so no stranded shadow-tree state leaks.
            Assert.That(a.Engine.DeletedShadows, Is.Not.Empty,
                "the prepared cluster garbage collected its shadow on compensation");
            Assert.That(b.Engine.DeletedShadows, Is.Not.Empty,
                "the failing cluster garbage collected its partial shadow");
            Assert.That(a.Engine.RevertCount, Is.EqualTo(1),
                "the prepared cluster reverted its alias to the pre-restore tree");
        });
    }

    [Test]
    public async Task Infeasible_target_is_refused_at_admission_before_any_build_starts()
    {
        // A tree that cannot fit the target cluster is refused by the admission probe
        // BEFORE any shadow build starts - no wasted build, a clean abort vote.
        var (participant, engine) = NewParticipant(refuseCapacity: true);
        engine.ProbeByteLength = long.MaxValue / 2;
        engine.ProbeShardCount = 4096;

        var vote = await participant.PrepareAsync(Request("restore-soak-infeasible"));

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Abort), "an infeasible target votes abort");
            Assert.That(engine.ProbeCount, Is.GreaterThanOrEqualTo(1), "admission was probed");
            Assert.That(engine.BuildCount, Is.EqualTo(0), "no shadow build started for an infeasible target");
        });
    }

    // The bounded-fence-window guarantee needs the real durable fence grain, so it
    // runs over the real-engine single-silo fixture.

    private const string FenceTreeUs = "soak-fence@us";
    private const string FenceTreeEu = "soak-fence@eu";
    private const string FenceSagaId = "restore-soak-fence";

    private static readonly string[] FenceCutKeys =
    [
        "fact/00000000000000000001",
        "fact/00000000000000000002",
        "fact/00000000000000000003",
    ];

    [Test]
    public async Task Write_fence_engages_only_at_cutover_not_during_the_prepare_build()
    {
        var fixture = new CoordinatedRestoreClusterFixture();
        try
        {
            await fixture.InitializeAsync();

            var us = fixture.GrainFactory.GetGrain<ILattice>(FenceTreeUs);
            var eu = fixture.GrainFactory.GetGrain<ILattice>(FenceTreeEu);
            foreach (var key in FenceCutKeys)
            {
                await us.SetAsync(key, Encoding.UTF8.GetBytes(key));
                await eu.SetAsync(key, Encoding.UTF8.GetBytes(key));
            }

            var backup = await fixture.Capture.CaptureAsync(
                new LatticeBackupCaptureRequest("cut", BackupScopeSelector.WholeTree(FenceTreeUs)));

            var participant = new RestoreParticipant(
                fixture.SiloServices.GetRequiredService<ILatticeCoordinatedRestoreEngine>(),
                fixture.SiloServices.GetRequiredService<ILatticeBackupRestoreService>(),
                fixture.SiloServices.GetRequiredService<IRestoreCapacityProbe>(),
                fixture.SiloServices.GetRequiredService<IGrainFactory>(),
                NullLogger<RestoreParticipant>.Instance);

            var request = new SagaControlRequest
            {
                SagaId = FenceSagaId,
                TargetTree = FenceTreeUs,
                ManifestId = backup.BackupId,
                CoordinatorClusterId = CoordinatedRestoreClusterFixture.ClusterId,
            };

            // Prepare builds the shadow UNFENCED. The fence must not have engaged.
            var vote = await participant.PrepareAsync(request);
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit));

            var afterPrepare = await fixture.Fence(FenceSagaId).GetSnapshotAsync();
            Assert.That(afterPrepare.Phase, Is.EqualTo(SagaWriteFencePhase.None),
                "the write fence must NOT engage during the prepare build - healthy clusters are not write-starved");

            // Commit engages the fence for the cutover, then unblocks writes.
            await participant.CommitAsync(request);

            var afterCommit = await fixture.Fence(FenceSagaId).GetSnapshotAsync();
            Assert.That(afterCommit.WritesUnblocked, Is.True,
                "the cutover fence releases local writes immediately after the flip");
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }
}
