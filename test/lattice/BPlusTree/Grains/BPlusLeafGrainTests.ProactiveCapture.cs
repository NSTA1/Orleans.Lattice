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
/// Unit tests for the leaf-driven proactive snapshot capture seam
/// added in R-120 step 7.5. The leaf observes the
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
    public async Task Periodic_recheck_does_not_capture_when_advisory_does_not_fire()
    {
        const int threshold = 2;
        var (grain, _, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: 0,
            walHead: 0,
            reClassifyEveryN: threshold,
            periodicDecision: FallOffLogDecision.TailReplay);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        var projection = (ILeafProjection)grain;
        await projection.SetCheckpointOffsetAsync(1, CancellationToken.None);
        await projection.SetCheckpointOffsetAsync(2, CancellationToken.None);

        await snapshotStub.DidNotReceive().SaveAsync(
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
}
