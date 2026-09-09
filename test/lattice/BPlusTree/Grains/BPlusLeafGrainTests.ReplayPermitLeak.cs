using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the monotonic replay-gate permit leak in leaf
/// activation (issue #2256).
/// <para>
/// <c>OnActivateAsync</c> acquires a permit from the process-wide replay
/// concurrency gate and then, before entering the <c>try</c> whose
/// <c>finally</c> releases it, ran an observation block: a metric increment, a
/// tenant-label resolution, the cold/warm totals sample, a logger resolution,
/// an <c>IsEnabled</c> probe and a templated log call. A throw anywhere in that
/// window lost the permit for the lifetime of the process, because the gate is
/// a <see cref="SemaphoreSlim"/> sized once on first use and never re-created
/// or topped up. The gate defaults to <see cref="Environment.ProcessorCount"/>,
/// which honours a container CPU quota only while <c>DOTNET_PROCESSOR_COUNT</c>
/// does not override it (issue #2278 found a deployed host where it did), so on
/// a 2-vCPU host two such throws - ever - permanently stop the silo activating
/// leaves, and the symptom is a silent hang rather than an error.
/// </para>
/// <para>
/// The injection point is a throwing <see cref="ILoggerFactory"/>, which is the
/// most plausible real trigger (an environmental logging sink fault) and is the
/// one the issue names. It faults <c>ResolveLogger</c>, which sits inside the
/// former unprotected window.
/// </para>
/// <para>
/// <b>The instrument is validated inside the test, not assumed.</b> A test that
/// only asserted "the gate's count is the same afterwards" would pass against
/// broken source for the wrong reason if the injected fault happened to throw
/// <i>before</i> the permit was ever acquired - no permit taken means no permit
/// leaked. So the throwing factory samples the gate's remaining count at the
/// instant it throws, and the test asserts that sample is exactly one below the
/// baseline. That is the known non-zero answer which proves the fault really is
/// inside the window, and it is what makes the unchanged-afterwards assertion
/// mean what it claims.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The activation-temperature log line is rate limited per tree, and the
    /// permit-holding window is only entered as far as the logger when that
    /// line is due. A tree id never seen before is always due, so each test
    /// mints its own.
    /// </summary>
    private static string UniqueReplayPermitTree() => $"replay-permit-{Guid.NewGuid():N}";

    [Test]
    public async Task Activation_releases_the_replay_permit_when_the_post_acquire_observation_throws()
    {
        // Size the gate and leave it quiescent. It is lazily created by the
        // first activation that resolves options, so a baseline read before any
        // activation would see no gate at all.
        var (warmGrain, warmState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        warmState.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)warmGrain).OnActivateAsync(CancellationToken.None);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null,
            "a completed activation with a tree id must have sized the process-wide replay gate");
        var baseline = gate!.CurrentCount;

        var observedWhileThrowing = -1;
        var loggerFactory = new ReplayPermitProbeLoggerFactory(() => observedWhileThrowing = gate.CurrentCount);
        var (grain, state) = CreateGrainWithLoggerFactory(loggerFactory);
        state.State.TreeId = UniqueReplayPermitTree();

        Assert.ThrowsAsync<ReplayPermitProbeException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None),
            "the injected observation fault must propagate out of activation, exactly as a real "
            + "logging-sink fault would");

        Assert.Multiple(() =>
        {
            // Instrument validation: proves the fault fired while a permit was
            // held, so the assertion below is about a permit that existed.
            Assert.That(observedWhileThrowing, Is.EqualTo(baseline - 1),
                "the injected fault must fire while the activation holds a replay permit - otherwise "
                + "this fixture proves nothing about the leak, because no permit was ever taken");

            Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                "the permit must be returned to the gate when the post-acquisition observation throws. "
                + "The gate is sized once per process and never restored, so every permit lost here is "
                + "a permanent reduction in leaf-activation concurrency, and exhaustion presents as a "
                + "silent hang on WaitAsync rather than as an error");
        });
    }

    [Test]
    public async Task Activation_that_completes_normally_leaves_the_replay_gate_unchanged()
    {
        // The control for the test above: the same gate, the same activation
        // path, no injected fault. Without it, a fix that simply stopped taking
        // a permit at all would satisfy the leak assertion.
        var (warmGrain, warmState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        warmState.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)warmGrain).OnActivateAsync(CancellationToken.None);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null);
        var baseline = gate!.CurrentCount;

        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        state.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
            "a clean activation must return the permit it took");
    }

    /// <summary>
    /// Builds a leaf whose activation service provider yields
    /// <paramref name="loggerFactory"/>, so <c>ResolveLogger</c> runs the
    /// injected behaviour. Mirrors
    /// <see cref="CreateGrainWithSnapshotAndCoordinator"/> with no snapshot and
    /// an empty WAL, which is the cold-activation shape that reaches the
    /// observation block.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateGrainWithLoggerFactory(
        ILoggerFactory loggerFactory)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(loggerFactory);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = 0;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    /// <summary>
    /// Stands in for a logging sink that faults: the transient, environmental
    /// class of failure the issue identifies as the most plausible trigger.
    /// Runs <c>probe</c> first so the caller can sample the gate at the instant
    /// of the throw.
    /// </summary>
    private sealed class ReplayPermitProbeLoggerFactory(Action probe) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName)
        {
            probe();
            throw new ReplayPermitProbeException();
        }

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// A sentinel so the test asserts on the fault it injected rather than on
    /// any exception the activation path might otherwise raise.
    /// </summary>
    private sealed class ReplayPermitProbeException()
        : Exception("Injected logging-sink fault inside the leaf activation replay window.");
}
