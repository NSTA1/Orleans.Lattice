using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the leaf-driven proactive snapshot capture seam.
/// The leaf observes the
/// <see cref="FallOffLogDecision.SnapshotPending"/> advisory itself
/// (at activation and on a configurable per-checkpoint cadence)
/// instead of relying on a maintenance-grain fan-out. The tests here
/// verify that:
/// <list type="bullet">
/// <item>An activation-time advisory triggers exactly one capture
/// after the WAL tail replay completes.</item>
/// <item>A non-advisory activation does not capture.</item>
/// <item>The periodic recheck fires on the configured Nth
/// checkpoint persist and, on advisory, drives a capture.</item>
/// <item>The single-flight guard suppresses an overlapping
/// capture.</item>
/// <item>Setting the periodic-recheck cadence to <c>0</c> disables
/// the recheck entirely.</item>
/// </list>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ProactiveTreeId = "tree-proactive";

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain SnapshotStub, ILatticeFallOffLogDetector Detector) CreateGrainForProactiveCapture(
        FallOffLogDecision activationDecision,
        long persistedCheckpoint = 0,
        long walHead = 0,
        int reClassifyEveryN = LatticeOptions.DefaultLeafSnapshotReClassifyEveryNCheckpoints,
        FallOffLogDecision? periodicDecision = null)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(walHead));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        // First call (activation) returns the supplied activationDecision.
        // Subsequent calls (periodic recheck) return periodicDecision when
        // supplied, otherwise repeat the activation decision.
        var callCount = 0;
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1)
                    return Task.FromResult(activationDecision);
                return Task.FromResult(periodicDecision ?? activationDecision);
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(detector);
        var services = sc.BuildServiceProvider();

        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ProactiveTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var baseOptions = new LatticeOptions
        {
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            LeafSnapshotReClassifyEveryNCheckpoints = reClassifyEveryN,
            // Pin WalPartitions=1: these tests stub a single ILeafReplayCoordinatorGrain
            // and assert exact ClassifyAsync call counts. The silo-wide
            // default flipped to 8 with multi-partition WAL replay, which
            // would otherwise drive 8 classifier calls per activation
            // instead of 1.
            WalPartitions = 1,
        };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state, snapshotStub, detector);
    }

    [Test]
    public async Task Activation_SnapshotPending_advisory_drives_one_capture_after_replay()
    {
        var (grain, state, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.SnapshotPending,
            persistedCheckpoint: 12,
            walHead: 12);

        // The capture path requires a non-negative persisted checkpoint
        // (already 12 above) and a non-empty cache row set; seed one row
        // so the snapshot blob is meaningful.
        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        await snapshotStub.Received(1).SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Activation_TailReplay_decision_does_not_capture()
    {
        var (grain, _, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: 5,
            walHead: 5);

        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        await snapshotStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Periodic_recheck_captures_on_Nth_checkpoint_persist_when_advisory_fires()
    {
        // Activation classifier returns TailReplay so the activation-
        // time capture does not run; periodic classifier returns
        // SnapshotPending so the Nth checkpoint persist drives capture.
        const int threshold = 2;
        var (grain, _, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: 0,
            walHead: 0,
            reClassifyEveryN: threshold,
            periodicDecision: FallOffLogDecision.SnapshotPending);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Activation alone did not capture.
        await snapshotStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        // Drive two successful checkpoint advances through the
        // ILeafProjection materialiser seam; with MaterialiserCheckpointInterval
        // = TimeSpan.Zero each advance forces an immediate
        // FlushPendingCheckpointAsync persist. The 2nd persist hits the
        // threshold and drives the periodic recheck which observes
        // SnapshotPending and captures.
        var projection = (ILeafProjection)grain;
        await projection.SetCheckpointOffsetAsync(1, CancellationToken.None);
        await projection.SetCheckpointOffsetAsync(2, CancellationToken.None);

        await snapshotStub.Received().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Periodic_recheck_captures_on_Nth_persist_unconditionally_regardless_of_advisory()
    {
        // REWRITTEN for the guaranteed-cadence snapshot capture (residual
        // cold-restart prefix-loss fix, Part 2). This test was originally
        // `Periodic_recheck_does_not_capture_when_advisory_does_not_fire`: it
        // asserted the periodic recheck captures ONLY when the fall-off-log
        // classifier raises the SnapshotPending advisory. That advisory gate
        // is UNSAFE under the coverage-gated durable pin. The pin now BLOCKS
        // trimming a checkpointed prefix until a snapshot covers it, so the WAL
        // tail stays low and the classifier's proximity heuristic (tail near
        // checkpoint) never fires - meaning the block would be held forever and
        // the WAL would grow unbounded, reintroducing the #1489/#1490 growth
        // class. The fix drives capture on the fixed checkpoint CADENCE,
        // unconditionally, so every blocked prefix is covered within at most
        // LeafSnapshotReClassifyEveryNCheckpoints checkpoints and the pin can
        // then advance and the GC trim. The strengthened contract this test now
        // pins: the Nth checkpoint persist captures regardless of the
        // classifier decision (here TailReplay, which previously SUPPRESSED
        // capture).
        const int threshold = 2;
        var (grain, _, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: 0,
            walHead: 0,
            reClassifyEveryN: threshold,
            periodicDecision: FallOffLogDecision.TailReplay);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Activation alone did not capture (activation-advisory path is
        // unchanged: TailReplay does not latch a pending capture).
        await snapshotStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        var projection = (ILeafProjection)grain;
        await projection.SetCheckpointOffsetAsync(1, CancellationToken.None);
        await projection.SetCheckpointOffsetAsync(2, CancellationToken.None);

        // The 2nd persist hits the cadence threshold and captures even though
        // the classifier decision is TailReplay - the advisory no longer gates.
        await snapshotStub.Received().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Periodic_recheck_disabled_when_threshold_is_zero()
    {
        // reClassifyEveryN: 0 disables the periodic recheck completely.
        // Even with a SnapshotPending decision the leaf must not call
        // ClassifyAsync on checkpoint persists, and the snapshot save
        // never runs.
        var (grain, _, snapshotStub, detector) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: 0,
            walHead: 0,
            reClassifyEveryN: 0,
            periodicDecision: FallOffLogDecision.SnapshotPending);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // One activation-time classify already happened; subsequent
        // checkpoint persists must not trigger any further classifies.
        var classifyCallsAtActivation = detector.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeFallOffLogDetector.ClassifyAsync));

        var projection = (ILeafProjection)grain;
        for (var i = 1; i <= 10; i++)
            await projection.SetCheckpointOffsetAsync(i, CancellationToken.None);

        var classifyCallsAfter = detector.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeFallOffLogDetector.ClassifyAsync));

        Assert.That(classifyCallsAfter, Is.EqualTo(classifyCallsAtActivation),
            "Periodic recheck must not call ClassifyAsync when the cadence option is 0.");
        await snapshotStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Single_flight_guard_suppresses_overlapping_capture()
    {
        // Hold the snapshot SaveAsync in a TaskCompletionSource so the
        // first capture is "in flight". While it is blocked, kick off a
        // second CaptureSnapshotAsync directly; the single-flight guard
        // on the leaf must short-circuit the second call without
        // touching SaveAsync a second time.
        var firstSaveTcs = new TaskCompletionSource();
        var saveCallCount = 0;
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Interlocked.Increment(ref saveCallCount);
                return firstSaveTcs.Task;
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ProactiveTreeId;
        state.State.ProjectionCheckpointOffset = 7;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

        // Start the first capture and yield control so it enters
        // SaveAsync and parks on firstSaveTcs.
        var first = grain.CaptureSnapshotAsync();
        await Task.Yield();

        // Now kick off a second capture; the single-flight guard must
        // short-circuit it before SaveAsync is touched again.
        var second = grain.CaptureSnapshotAsync();
        await second;

        Assert.That(saveCallCount, Is.EqualTo(1),
            "Single-flight guard must suppress the overlapping capture.");

        // Release the first capture so the test does not leak the
        // pending Task.
        firstSaveTcs.SetResult();
        await first;
    }

    [Test]
    public async Task Activation_advisory_capture_failure_does_not_block_activation()
    {
        // Snapshot SaveAsync throws; the leaf must still complete
        // activation cleanly (the proactive capture is best-effort and
        // the next periodic recheck or reactivation re-attempts).
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("transient storage failure"));

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(3L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.SnapshotPending));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(detector);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ProactiveTreeId;
        state.State.ProjectionCheckpointOffset = 3;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero },
            maxLeafKeys: 128, shardCount: 1, factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

        Assert.That(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None),
            Throws.Nothing,
            "A best-effort proactive snapshot capture must not block activation.");
    }

    [Test]
    public async Task Periodic_recheck_skips_classifier_when_checkpoint_unchanged_since_last_capture()
    {
        // After an activation-time capture lands, the periodic recheck
        // path must short-circuit before invoking the classifier
        // whenever state.State.ProjectionCheckpointOffset has not moved
        // since the last capture: a fresh classify would re-derive an
        // identical decision and any follow-on capture would write a
        // byte-identical blob. The short-circuit avoids the classifier
        // RPC pair (head + tail) and the redundant SaveAsync.
        const int threshold = 1;
        var (grain, _, snapshotStub, detector) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.SnapshotPending,
            persistedCheckpoint: 7,
            walHead: 7,
            reClassifyEveryN: threshold,
            periodicDecision: FallOffLogDecision.SnapshotPending);

        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

        // Activation drives the first capture; this also stamps the
        // last-captured checkpoint at 7.
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        await snapshotStub.Received(1).SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        // One classify call so far - the activation-time pre-replay
        // classification.
        var classifiesAfterActivation = detector.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeFallOffLogDetector.ClassifyAsync));

        // Re-advance the checkpoint to the same value the snapshot was
        // stamped at. The materialiser still flushes (which increments
        // the persist counter to threshold) but the recheck must
        // short-circuit because the checkpoint matches the last-
        // captured offset.
        var projection = (ILeafProjection)grain;
        await projection.SetCheckpointOffsetAsync(7, CancellationToken.None);

        // No additional classify, and no additional capture.
        var classifiesAfterPersist = detector.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeFallOffLogDetector.ClassifyAsync));
        Assert.That(classifiesAfterPersist, Is.EqualTo(classifiesAfterActivation),
            "Periodic recheck must short-circuit before the classifier when checkpoint == last-captured offset.");
        await snapshotStub.Received(1).SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }
}
