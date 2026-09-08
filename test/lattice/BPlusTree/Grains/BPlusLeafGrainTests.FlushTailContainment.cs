using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the remaining two contained steps of the
/// post-persist checkpoint-flush tail (issue #2220): the cursor report and the
/// periodic snapshot recheck.
/// </summary>
/// <remarks>
/// <para>
/// The invariant: once <c>PersistAsync</c> has committed the advanced
/// checkpoint, this activation is durably correct. Everything after it is
/// notification, not part of the durability contract, so no step may propagate
/// out of the flush and destroy the activation. The field mechanism was the
/// inline upward digest publish, and
/// <c>BPlusLeafGrainTests.DigestFlushHardening</c> pins that middle step. The
/// two steps either side of it are contained by the same rule and are covered
/// here, so a future refactor cannot quietly narrow the containment to the one
/// step that happened to fail in the field.
/// </para>
/// <para>
/// Each test asserts the branch by its durable side effect - the persisted
/// checkpoint advanced and a write landed - and separately confirms the fault
/// was really injected on the path, so a no-throw result reads as containment
/// rather than as a step that was skipped.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private const string TailContainmentTreeId = "tree-tail-containment";

    /// <summary>
    /// Builds a leaf whose post-persist tail runs against a supplied cursor
    /// reporter, with the periodic snapshot recheck disabled so the cursor
    /// report is the only step that can fail.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateTailContainmentLeaf(
        ILeafCursorReporter reporter)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "tail-containment-leaf"));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = TailContainmentTreeId;

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                LeafSnapshotReClassifyEveryNCheckpoints = 0,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state);
    }

    /// <summary>
    /// A cursor reporter that faults the synchronous durable-frontier note. That
    /// call sits outside the per-consumer try/catch inside
    /// <c>ReportCursorIfActiveAsync</c>, so it is what actually escapes that
    /// method and reaches the flush tail's containment.
    /// </summary>
    [Test]
    public async Task Checkpoint_flush_when_the_cursor_report_faults_persists_durably_and_does_not_throw()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter
            .When(r => r.NoteDurableMaterialiserFrontier(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>()))
            .Do(_ => throw new InvalidOperationException("simulated cursor-report cascade (#2220)"));

        var (grain, state) = CreateTailContainmentLeaf(reporter);
        var projection = AsProjection(grain);

        // Clock must leave Zero for the cursor report to run at all.
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));

        // First flush crosses the Zero block pin through the batched flush and
        // latches _durableFrontierBarriered; the debounced note is not used yet.
        await projection.SetCheckpointOffsetAsync(1, CancellationToken.None);
        await projection.FlushCheckpointAsync(CancellationToken.None);

        var writesBefore = state.WriteCount;

        // Second flush takes the debounced mirror, which is the faulting call.
        await projection.SetCheckpointOffsetAsync(2, CancellationToken.None);
        await projection.FlushCheckpointAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                state.State.ProjectionCheckpointOffset,
                Is.EqualTo(2),
                "the durable advance must stand even though the cursor report faulted");
            Assert.That(
                state.WriteCount,
                Is.GreaterThan(writesBefore),
                "PersistAsync must have durably committed the advance before the report ran");
        });

        // Confirm the fault was injected on this path, so the no-throw result is
        // containment rather than a step that never executed.
        reporter.Received().NoteDurableMaterialiserFrontier(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>());
    }

    /// <summary>
    /// The periodic snapshot recheck is the last step of the tail. It resolves
    /// options before doing anything else, so a registry that refuses the
    /// structural pin faults the step; the flush must still return and the
    /// durable advance must stand.
    /// </summary>
    /// <remarks>
    /// No cursor reporter is registered, so the report step returns early
    /// without resolving options and cannot mask which step faulted.
    /// </remarks>
    [Test]
    public async Task Checkpoint_flush_when_the_snapshot_recheck_faults_persists_durably_and_does_not_throw()
    {
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var grainFactory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry
            .GetEntryAsync(Arg.Any<string>())
            .ThrowsAsync(new InvalidOperationException("simulated registry outage during the flush tail (#2220)"));

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "tail-recheck-leaf"));
        // No ILeafCursorReporter registered: the report step returns early.
        context.ActivationServices.Returns(new ServiceCollection().BuildServiceProvider());

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = TailContainmentTreeId;

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            new LatticeOptionsResolver(grainFactory, optionsMonitor),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        var projection = AsProjection(grain);

        var writesBefore = state.WriteCount;

        // Idempotent re-assert at the current offset is a force-flush signal, and
        // it reaches FlushPendingCheckpointAsync before SetCheckpointOffsetAsync
        // has resolved options - so the tail is the first thing to touch the
        // registry, and the fault lands inside the contained tail.
        await projection.SetCheckpointOffsetAsync(0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                state.WriteCount,
                Is.GreaterThan(writesBefore),
                "the force-flush must persist before the tail runs");
            Assert.That(
                state.State.ProjectionCheckpointOffset,
                Is.EqualTo(0),
                "the re-assert does not advance the offset, it only commits accumulated Apply work");
        });

        // Confirm the fault was really reached on this path.
        await registry.Received().GetEntryAsync(Arg.Any<string>());
    }

    /// <summary>
    /// The prepare clamp's silent no-op arm. An unresolved saga prepare pins the
    /// checkpoint at <c>minPrepare - 1</c>, because a resumed replay must re-read
    /// that prepare to rebuild the pending-transaction map. When the prepare sits
    /// <em>below</em> the already-materialised position the clamp drives the
    /// requested offset backwards past it, and the advance must then be dropped
    /// silently rather than persisted or rejected.
    /// </summary>
    /// <remarks>
    /// Persisting the clamped value would move the durable checkpoint backwards;
    /// throwing would fail a caller that did nothing wrong, since the requested
    /// offset was a legitimate forward advance. Dropping it is correct: the
    /// checkpoint stays where it is until the prepare resolves.
    /// </remarks>
    [Test]
    public async Task SetCheckpointOffset_is_a_silent_noop_when_an_open_prepare_clamps_below_the_current_offset()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        var (grain, state) = CreateTailContainmentLeaf(reporter);
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));

        // Materialise the checkpoint at 10.
        await projection.SetCheckpointOffsetAsync(10, CancellationToken.None);
        await projection.FlushCheckpointAsync(CancellationToken.None);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(10));

        // An unresolved prepare recorded at WAL offset 5 - below the current
        // materialised position - so the clamp floor is 4.
        using (LatticeApplyOffsetContext.BeginScope(partition: 0, offset: 5))
        {
            projection.Apply(BuildPreparedDelete(Guid.NewGuid(), "k1", hlcPhysical: 150));
        }

        var writesBefore = state.WriteCount;

        // A legitimate forward advance that the clamp drags back to 4, which is
        // behind the current position of 10.
        await projection.SetCheckpointOffsetAsync(11, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                state.State.ProjectionCheckpointOffset,
                Is.EqualTo(10),
                "the clamped advance must be dropped, never persisted backwards");
            Assert.That(
                state.WriteCount,
                Is.EqualTo(writesBefore),
                "a clamped-below-current advance must not write state at all");
            Assert.That(
                grain.GetCurrentCheckpointForPartition(0),
                Is.EqualTo(10),
                "the materialiser's view is unchanged by the dropped advance");
        });
    }
}
