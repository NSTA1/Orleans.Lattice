using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3366: the graceful-deactivation hook runs four durability barriers -
/// digest publish, checkpoint flush, snapshot capture, durable frontier pin -
/// and they previously shared a single <c>try</c> with a single anonymous bare
/// <c>catch</c> that had no logger.
/// </summary>
/// <remarks>
/// <para>
/// <b>What was wrong, and why it was invisible.</b> The barriers are ordered
/// most-fragile-first, so a fault in an early one silently cancelled every later
/// one: a checkpoint-flush failure also skipped the snapshot capture AND the
/// durable frontier pin, and the whole teardown emitted nothing at all - no
/// counter, no log line, no trace. The catch's stated justification ("the
/// persisted offset still bounds replay cost on the next activation") is sound
/// for replay <i>cost</i>, but it is not a justification for skipping the two
/// barriers that establish durable <i>coverage</i>.
/// </para>
/// <para>
/// <b>Why that mattered more than an ordinary swallowed exception.</b> Three
/// materially different failures rendered as byte-identical silence - an early
/// barrier throwing, the capture's uninstrumented <c>TreeId is null</c> early
/// return, and the hook never running at all - and each demands a different
/// remedy. No exported series distinguished them, so an investigation could not
/// even establish which one was occurring, let alone fix it. An absent series
/// and a proven zero carry opposite meanings; before this change the path could
/// only ever produce the former.
/// </para>
/// <para>
/// <b>What these tests pin.</b> Not merely that something is counted, but both
/// halves of the repair: that the fault is <i>attributed to the specific
/// barrier</i>, and that the barriers ordered after it <i>still execute</i>.
/// The second is the behavioural half and is what a silent-catch regression
/// would break first.
/// </para>
/// <para>
/// <b>Why the fixture pins an infinite checkpoint interval.</b>
/// <c>FlushPendingCheckpointAsync</c> persists only when an advance is actually
/// pending, so a fixture that let the coalescing predicate fire during setup
/// would reach the deactivation hook with nothing to flush - the injected fault
/// would never be touched and the test would pass against a fully reverted fix.
/// <c>HasIntervalElapsed</c> returns <c>false</c> unconditionally for
/// <see cref="Timeout.InfiniteTimeSpan"/>, which makes the predicate
/// deterministically false rather than merely unlikely, and the write-count
/// precondition below asserts that outcome instead of assuming it.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private const string BarrierContainmentTreeId = "tree-3366-barrier-containment";

    /// <summary>
    /// Collects the <c>reason</c> tag of every deactivation-barrier failure
    /// recorded while the returned listener is alive.
    /// </summary>
    private static IDisposable ListenForBarrierFailures(List<string> reasons) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.LeafDeactivationBarrierFailures,
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

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot)
        CreateLeafForBarrierContainment(ILeafReplayCoordinatorGrain coordinator)
    {
        // No pre-existing snapshot: the leaf's durable coverage starts behind
        // any checkpoint it banks, so the capture barrier has real work to do
        // and its execution is observable as a SaveAsync on this stub.
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = BarrierContainmentTreeId;

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                // See the class remarks: this is what makes the queued advance
                // still-pending when the deactivation hook runs.
                MaterialiserCheckpointInterval = Timeout.InfiniteTimeSpan,
                MaterialiserCheckpointEntries = 1_000_000,
                WalPartitions = 1,
                LeafSnapshotReClassifyEveryNCheckpoints = 1000,

                // Disarms the digest-publish barrier (the hook runs it only
                // when this window is > 0). Measured, not assumed: with the
                // default window the barrier persists state FIRST, consumes the
                // one-shot injected fault, and swallows it in its own internal
                // catch - after FlushPendingCheckpointAsync has already cleared
                // the pending advance. The checkpoint-flush barrier then found
                // nothing to persist, never wrote, and never faulted, so the
                // test asserted against a fault that had been absorbed one
                // barrier earlier. Pinning the window to 0 makes the checkpoint
                // flush the first writer in the hook, so the fault lands on the
                // barrier under test.
                DigestCoalescingWindowMs = 0,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        return (new BPlusLeafGrain(
                context, state, grainFactory, optionsResolver,
                TestMutationObservers.NoObservers(),
                TestOriginClusterIdResolver.Default()),
            state,
            snapshotStub);
    }

    /// <summary>
    /// Queues a checkpoint advance that the coalescing predicate will NOT
    /// flush, and asserts that outcome rather than assuming it, so the
    /// injected fault below is guaranteed to be reached.
    /// </summary>
    /// <remarks>
    /// The advance is banked at the EXCLUSIVE WAL head the fixture activated
    /// against, one offset <i>above</i> its newest entry (issue #2680). Activation
    /// replay banks that newest entry itself, so a
    /// hard-coded low offset would be refused by the monotonicity guard rather
    /// than queued, and the fault under test would never be reached.
    /// </remarks>
    private static async Task<int> QueuePendingCheckpointAsync(
        BPlusLeafGrain grain,
        FakePersistentState<LeafNodeState> state,
        GrowingWal wal)
    {
        var projection = AsProjection(grain);
        var target = wal.Head;
        projection.Apply(BuildSet(
            $"k{target}", Encoding.UTF8.GetBytes("v"), hlcPhysical: 10, treeId: BarrierContainmentTreeId));

        var writesBefore = state.WriteCount;
        await projection.SetCheckpointOffsetAsync(target, default);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
            "precondition: the advance must still be PENDING when the deactivation hook runs. If "
            + "the coalescing predicate flushed it here, the checkpoint-flush barrier would have "
            + "nothing to persist, the injected fault would never fire, and both tests below would "
            + "pass against a completely reverted fix.");

        return state.WriteCount;
    }

    /// <summary>
    /// The observability half. A faulting barrier must name itself.
    /// </summary>
    [Test]
    public async Task Deactivation_barrier_fault_is_attributed_to_the_barrier_that_faulted()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, _) = CreateLeafForBarrierContainment(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingCheckpointAsync(grain, state, wal);

        state.ThrowOnWrite = new InvalidOperationException("injected checkpoint-flush storage fault");

        var reasons = new List<string>();
        using (ListenForBarrierFailures(reasons))
        {
            // Must not throw: a storage failure on shutdown still has to not
            // block deactivation. That guarantee predates this fix and the
            // containment must preserve it, not trade it away for visibility.
            await DeactivateLeafAsync(grain);
        }

        Assert.Multiple(() =>
        {
            Assert.That(reasons, Does.Contain(LatticeMetrics.DeactivationBarrierCheckpointFlush.Value),
                "the fault must be attributed to the checkpoint-flush barrier specifically. A "
                + "generic 'a barrier failed' count would not tell an operator whether durable "
                + "checkpoint progress was lost (this barrier) or merely a staleness-tolerant "
                + "digest republish was skipped (the first barrier), which have very different "
                + "consequences.");

            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DeactivationBarrierDigestPublish.Value),
                "and it must not be misattributed to an adjacent barrier - otherwise the reason "
                + "tag would be decorative and a reader acting on it would be sent to the wrong "
                + "code.");
        });
    }

    /// <summary>
    /// THE load-bearing case, and the behavioural half of the repair. A fault
    /// in an early barrier must no longer cancel the barriers after it.
    /// </summary>
    [Test]
    public async Task A_faulting_checkpoint_flush_no_longer_cancels_the_snapshot_capture_barrier()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, snapshotStub) = CreateLeafForBarrierContainment(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingCheckpointAsync(grain, state, wal);

        state.ThrowOnWrite = new InvalidOperationException("injected checkpoint-flush storage fault");

        // Setup itself captures a snapshot, so a bare Received() below would be
        // satisfied by that earlier call and would hold under BOTH shapes -
        // passing against a fully reverted fix. Clearing here narrows the
        // assertion to calls made BY THE DEACTIVATION HOOK, which is the only
        // thing this test is about. The DidNotReceive immediately after is the
        // proof that the clear took effect, so the narrowing is asserted rather
        // than assumed.
        snapshotStub.ClearReceivedCalls();
        await snapshotStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        await DeactivateLeafAsync(grain);

        // Under the shared-catch shape this assertion fails: the throw from the
        // checkpoint flush unwound straight past the capture, so no blob was
        // ever written and the leaf went dormant with no durable coverage - the
        // exact state that makes a subsequent restart lose everything the
        // in-memory projection was still holding.
        await snapshotStub.Received().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// The discriminating control. Without an injected fault the counter must
    /// stay silent, so a non-zero reading in production means a real fault
    /// rather than ordinary teardown traffic.
    /// </summary>
    [Test]
    public async Task Deactivation_records_no_barrier_failure_when_every_barrier_succeeds()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, snapshotStub) = CreateLeafForBarrierContainment(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingCheckpointAsync(grain, state, wal);

        var reasons = new List<string>();
        // Same narrowing as the test above, and for the same reason: setup
        // captures a snapshot, so the assertion below would otherwise be
        // satisfied by a call the deactivation hook never made.
        snapshotStub.ClearReceivedCalls();

        using (ListenForBarrierFailures(reasons))
        {
            await DeactivateLeafAsync(grain);
        }

        Assert.That(reasons, Is.Empty,
            "a clean teardown must not record a barrier failure. Without this control the "
            + "counter could be incrementing on every deactivation and the two tests above "
            + "would still pass, which would make the series useless for alerting.");

        // Proves this control exercised the same path rather than passing
        // because the barriers were skipped for some unrelated reason. Awaited
        // directly rather than inside Assert.Multiple: an async lambda there
        // binds to the Action overload, so the assertion would run unobserved
        // and a failure would be swallowed.
        await snapshotStub.Received().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }
}
