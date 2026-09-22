using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3393. The checkpoint flush is two things wearing one name: a BODY
/// that performs the durable write, and a TAIL that runs three follow-up
/// notifications. The tail's steps are contained independently and
/// deliberately (issue #2220) because the durable write has already committed
/// and a failed notification must not tear the activation down.
/// </summary>
/// <remarks>
/// <para>
/// That containment is correct and these tests do not challenge it. What it
/// cost was OBSERVABILITY: a tail fault produced a log line and nothing else,
/// while the enclosing <c>checkpoint_flush</c> deactivation barrier still
/// reported SUCCESS. A production drain recorded 1,699 inline-digest-publish
/// faults against a barrier counter of zero, so the only instrument an
/// operator would consult read clean while every leaf in the silo faulted.
/// </para>
/// <para>
/// The middle test is the load-bearing one. It reproduces that exact
/// signature - tail counter non-zero, barrier counter zero - which is the
/// pair that must be distinguishable. Asserting only that the tail counter
/// fires would pass even if the fault had also been misattributed to the
/// barrier, and the two have very different meanings: a barrier fault says
/// the durable write did not happen, a tail fault says it did and a
/// notification did not land.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private const string FlushTailTreeId = "tree-3393-flush-tail";

    /// <summary>
    /// The leaf's parent. The inline digest publish returns early when the
    /// leaf has no parent, so without this the tail's second step would be a
    /// no-op and could not be faulted at all.
    /// </summary>
    private static readonly GrainId FlushTailParentId =
        GrainId.Create("internal", "tree-3393-flush-tail-parent");

    /// <summary>
    /// Collects the <c>reason</c> tag of every checkpoint-flush tail failure
    /// recorded while the returned listener is alive.
    /// </summary>
    private static IDisposable ListenForFlushTailFailures(List<string> reasons) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.LeafCheckpointFlushTailFailures,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var t in tags)
                {
                    if (t.Key == "reason" && t.Value is string reason)
                    {
                        reasons.Add(reason);
                    }
                }
            }));

    /// <summary>
    /// Builds a leaf whose cursor reporter is returned to the caller, so a
    /// fault can be injected into the tail's FIRST step specifically.
    /// </summary>
    /// <remarks>
    /// The cursor report is chosen as the injection point because it is the
    /// only tail step reachable through a registered service, so the fault
    /// lands inside the tail without disturbing the durable write that
    /// precedes it. That separation is the whole point: a test that broke the
    /// write as well could not tell a tail failure from a body failure.
    /// </remarks>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State,
        ILeafCursorReporter Reporter, IBPlusInternalGrain Parent)
        CreateLeafForFlushTail(ILeafReplayCoordinatorGrain coordinator)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var reporter = Substitute.For<ILeafCursorReporter>();
        var parent = Substitute.For<IBPlusInternalGrain>();

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = FlushTailTreeId;
        state.State.ParentId = FlushTailParentId;

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(parent);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = Timeout.InfiniteTimeSpan,
                MaterialiserCheckpointEntries = 1_000_000,
                WalPartitions = 1,
                LeafSnapshotReClassifyEveryNCheckpoints = 1000,

                // Same rationale as the barrier-containment fixture: pinning
                // the coalescing window to 0 makes the checkpoint flush the
                // first writer in the deactivation hook, so the tail under
                // test is reached rather than absorbed a barrier earlier.
                DigestCoalescingWindowMs = 0,

                // The tail's second step is a no-op unless the leaf maintains
                // a projection digest.
                //
                // DigestPublishTimeout is deliberately LEFT AT ITS DEFAULT
                // (15s, finite). Pinning it to Timeout.InfiniteTimeSpan would
                // be the obvious way to keep a fault surfacing as itself, but
                // that value is documented as restoring the HISTORICAL
                // unbounded-await behaviour: it takes the branch that awaits
                // the parent directly, whereas a deployed leaf takes the
                // CancellationTokenSource deadline branch. The test would then
                // prove attribution on a path production does not run. The
                // injected fault throws immediately, so the deadline never
                // fires, the `when (deadline.IsCancellationRequested)` filter
                // does not match, and the exception propagates unrewritten -
                // no wall-clock dependence is introduced by keeping the
                // default, and no ManualTimeProvider is needed.
                MaintainProjectionDigest = true,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return (new BPlusLeafGrain(
                context, state, grainFactory, optionsResolver,
                TestMutationObservers.NoObservers(),
                TestOriginClusterIdResolver.Default()),
            state,
            reporter,
            parent);
    }

    /// <summary>
    /// Queues a checkpoint advance the coalescing predicate will not flush, so
    /// the deactivation hook is guaranteed to perform a durable write and
    /// therefore to run the tail.
    /// </summary>
    private static async Task QueuePendingFlushTailCheckpointAsync(
        BPlusLeafGrain grain,
        FakePersistentState<LeafNodeState> state,
        GrowingWal wal)
    {
        var projection = AsProjection(grain);
        var target = wal.Head + 1;
        projection.Apply(BuildSet(
            $"k{target}", Encoding.UTF8.GetBytes("v"), hlcPhysical: 10, treeId: FlushTailTreeId));

        var writesBefore = state.WriteCount;
        await projection.SetCheckpointOffsetAsync(target, default);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
            "precondition: the advance must still be PENDING when the deactivation hook runs, "
            + "otherwise the hook performs no durable write, the tail never runs, and every "
            + "test below would pass against a completely reverted change.");

        // The tail's cursor-report step early-returns on a Zero clock, which
        // would make every assertion in this fixture vacuous - the step would
        // never reach the injected fault and the failure list would be empty
        // for a reason that has nothing to do with the code under test. Set it
        // explicitly and assert it rather than relying on Apply to have done
        // so.
        if (state.State.Clock <= HybridLogicalClock.Zero)
        {
            state.State.Clock = HybridLogicalClock.Tick(new HybridLogicalClock());
        }

        Assert.That(state.State.Clock > HybridLogicalClock.Zero, Is.True,
            "precondition: the leaf must carry a non-Zero clock, or ReportCursorIfActiveAsync "
            + "returns before it can reach the cursor reporter and the fault under test is "
            + "never triggered.");
    }

    /// <summary>
    /// The attribution half. A faulting tail step must name itself, because
    /// the three steps have different consequences.
    /// </summary>
    [Test]
    public async Task Checkpoint_flush_tail_fault_is_attributed_to_the_step_that_faulted()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, reporter, parent) = CreateLeafForFlushTail(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingFlushTailCheckpointAsync(grain, state, wal);

        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new InvalidOperationException("injected inline digest publish fault"));

        var steps = new List<string>();
        using (ListenForFlushTailFailures(steps))
        {
            // Must not throw. The tail's containment guarantee predates this
            // change and making the fault visible must not trade it away.
            await DeactivateLeafAsync(grain);
        }

        Assert.Multiple(() =>
        {
            Assert.That(steps, Does.Contain(LatticeMetrics.CheckpointFlushTailInlineDigestPublish.Value),
                "the fault must be attributed to the inline-digest-publish step specifically. A "
                + "generic 'the tail failed' count would not tell an operator whether a digest "
                + "republish was skipped (the parent's fold goes stale) or the coverage-lag arming "
                + "was skipped (which can leave a Zero block pin retaining the tree's shared WAL).");

            Assert.That(steps, Does.Not.Contain(LatticeMetrics.CheckpointFlushTailCursorReport.Value),
                "and it must not be misattributed to an adjacent step. The cursor report is wired "
                + "to a healthy substitute in this test, so a cursor_report tag here would prove "
                + "the reason tag is decorative and a reader acting on it is sent to the wrong code.");
        });
    }

    /// <summary>
    /// THE load-bearing case. Reproduces the production signature that made
    /// this defect invisible: the tail faults while the enclosing barrier
    /// reports success.
    /// </summary>
    [Test]
    public async Task A_faulting_tail_step_is_counted_without_being_charged_to_the_checkpoint_flush_barrier()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, _, parent) = CreateLeafForFlushTail(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingFlushTailCheckpointAsync(grain, state, wal);

        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new InvalidOperationException("injected inline digest publish fault"));

        var tailSteps = new List<string>();
        var barrierReasons = new List<string>();
        using (ListenForFlushTailFailures(tailSteps))
        using (ListenForBarrierFailures(barrierReasons))
        {
            await DeactivateLeafAsync(grain);
        }

        Assert.Multiple(() =>
        {
            Assert.That(tailSteps, Is.Not.Empty,
                "before this change the tail produced a log line and NO metric at all, so this "
                + "list was empty and an operator had no series to alert on.");

            Assert.That(barrierReasons, Does.Not.Contain(
                    LatticeMetrics.DeactivationBarrierCheckpointFlush.Value),
                "and the barrier must stay clean, because the durable write DID succeed. "
                + "Charging a tail failure to the barrier would say the checkpoint was not "
                + "banked, which is false and would send an operator looking for data loss that "
                + "did not occur. This pair - tail non-zero, barrier zero - is the exact "
                + "production signature the separate instrument exists to express.");
        });

        // Proves the durable write really did land, so the assertion above is
        // about a genuinely successful barrier rather than one that was
        // skipped. Without this the test would pass if the flush never ran.
        Assert.That(state.WriteCount, Is.GreaterThan(0),
            "the checkpoint body must have committed; a tail-only fault is only meaningful if "
            + "the write it follows actually happened.");
    }

    /// <summary>
    /// The discriminating control. Without an injected fault the counter must
    /// stay silent, so a non-zero reading in production means a real fault
    /// rather than ordinary flush traffic.
    /// </summary>
    [Test]
    public async Task Checkpoint_flush_tail_records_no_failure_when_every_step_succeeds()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, reporter, parent) = CreateLeafForFlushTail(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingFlushTailCheckpointAsync(grain, state, wal);

        var steps = new List<string>();
        using (ListenForFlushTailFailures(steps))
        {
            await DeactivateLeafAsync(grain);
        }

        Assert.Multiple(() =>
        {
            Assert.That(steps, Is.Empty,
                "a clean flush must not record a tail failure. Without this control the counter "
                + "could be incrementing on every checkpoint and the two tests above would still "
                + "pass, which would make the series useless for alerting.");

            Assert.That(state.WriteCount, Is.GreaterThan(0),
                "and this control must have exercised the same path rather than passing because "
                + "no flush occurred at all.");
        });

        // The non-vacuity proof, and the reason this control is worth having.
        // "No failure recorded" is the same observation whether the steps ran
        // cleanly or never ran at all, so without this the control would pass
        // against a tail that had been removed entirely. Both faultable steps
        // are asserted to have actually executed. Awaited directly rather than
        // inside Assert.Multiple: an async lambda there binds to the Action
        // overload and the assertion would run unobserved.
        await reporter.Received().ReportAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
        await parent.Received().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }
}
