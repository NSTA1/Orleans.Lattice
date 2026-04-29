using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for leaf-grain cursor-registry integration. The
/// <see cref="BPlusLeafGrain"/> reports its highest applied
/// <see cref="HybridLogicalClock"/> via the silo-scoped
/// <see cref="ILeafCursorReporter"/> seam after every successful
/// projection-checkpoint persist, lazy-gated on
/// <c>state.State.Clock &gt; HybridLogicalClock.Zero</c>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string CursorTreeId = "tree-cursor";
    private const string CursorReplicaId = "leaf-cursor-test";

    private static (BPlusLeafGrain Grain, ILeafCursorReporter Reporter, FakePersistentState<LeafNodeState> State) CreateGrainWithReporter(
        string? treeId = CursorTreeId,
        ILeafCursorReporter? reporter = null,
        bool registerReporter = true,
        IServiceProvider? servicesOverride = null)
    {
        reporter ??= Substitute.For<ILeafCursorReporter>();

        IServiceProvider? services;
        if (servicesOverride is not null)
        {
            services = servicesOverride;
        }
        else if (registerReporter)
        {
            var sc = new ServiceCollection();
            sc.AddSingleton(reporter);
            services = sc.BuildServiceProvider();
        }
        else
        {
            services = new ServiceCollection().BuildServiceProvider();
        }

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", CursorReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        if (treeId is not null)
            state.State.TreeId = treeId;

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(maxLeafKeys: 128, shardCount: 1, factory: grainFactory);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsResolver, TestMutationObservers.NoObservers());
        return (grain, reporter, state);
    }

    [Test]
    public async Task ReportCursor_does_not_fire_when_clock_is_zero()
    {
        var (grain, reporter, _) = CreateGrainWithReporter();
        var projection = AsProjection(grain);

        // No Apply has run -> Clock stays at Zero -> the lazy-registration
        // gate keeps the WAL from being pinned at offset zero.
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        await reporter.DidNotReceive().ReportAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_fires_after_apply_advances_clock()
    {
        var (grain, reporter, state) = CreateGrainWithReporter();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        await reporter.Received(1).ReportAsync(
            CursorTreeId,
            Arg.Any<string>(),
            state.State.Clock,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_uses_canonical_consumer_id_pattern()
    {
        var (grain, reporter, _) = CreateGrainWithReporter();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        await reporter.Received().ReportAsync(
            CursorTreeId,
            Arg.Is<string>(s => s.StartsWith("_lattice_materialiser_") && s.Contains(CursorTreeId)),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_advances_monotonically_across_flushes()
    {
        var (grain, reporter, state) = CreateGrainWithReporter();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);
        var firstClock = state.State.Clock;

        projection.Apply(BuildSet("k2", Encoding.UTF8.GetBytes("v2"), hlcPhysical: 200));
        await projection.SetCheckpointOffsetAsync(2, default);
        await projection.FlushCheckpointAsync(default);
        var secondClock = state.State.Clock;

        Assert.That(secondClock, Is.GreaterThan(firstClock));
        await reporter.Received().ReportAsync(
            CursorTreeId, Arg.Any<string>(), firstClock, Arg.Any<CancellationToken>());
        await reporter.Received().ReportAsync(
            CursorTreeId, Arg.Any<string>(), secondClock, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_is_no_op_when_reporter_not_registered()
    {
        // Service provider exists but does not contain ILeafCursorReporter.
        var reporter = Substitute.For<ILeafCursorReporter>();
        var (grain, _, _) = CreateGrainWithReporter(reporter: reporter, registerReporter: false);
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        Assert.DoesNotThrowAsync(async () => await projection.FlushCheckpointAsync(default));

        await reporter.DidNotReceive().ReportAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_is_no_op_when_activation_services_null()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        var (grain, _, _) = CreateGrainWithReporter(reporter: reporter, servicesOverride: null!, registerReporter: false);
        // Override servicesOverride manually so context.ActivationServices is null.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", CursorReplicaId));
        context.ActivationServices.Returns((IServiceProvider?)null!);
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = CursorTreeId;
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(maxLeafKeys: 128, shardCount: 1, factory: grainFactory);
        var nullSvcGrain = new BPlusLeafGrain(context, state, grainFactory, optionsResolver, TestMutationObservers.NoObservers());
        var projection = AsProjection(nullSvcGrain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        Assert.DoesNotThrowAsync(async () => await projection.FlushCheckpointAsync(default));

        await reporter.DidNotReceive().ReportAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_swallows_reporter_exceptions_and_completes_flush()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.ReportAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("transient")));

        var (grain, _, state) = CreateGrainWithReporter(reporter: reporter);
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);

        Assert.DoesNotThrowAsync(async () => await projection.FlushCheckpointAsync(default));
        // Persist still happened: the offset was committed even though the
        // cursor report failed, so the next flush will catch up monotonically.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L));
    }

    [Test]
    public async Task ReportCursor_is_no_op_when_tree_id_unset()
    {
        var (grain, reporter, _) = CreateGrainWithReporter(treeId: null);
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100, treeId: ""));
        await projection.SetCheckpointOffsetAsync(1, default);
        Assert.DoesNotThrowAsync(async () => await projection.FlushCheckpointAsync(default));

        await reporter.DidNotReceive().ReportAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_includes_grain_id_in_consumer_id()
    {
        var (grain, reporter, _) = CreateGrainWithReporter();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        // Consumer id must include the grain's identity so each leaf
        // advances its own cursor independently.
        await reporter.Received().ReportAsync(
            CursorTreeId,
            Arg.Is<string>(s => s.Contains(CursorReplicaId)),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportCursor_never_fires_under_pre_promotion_foreground_writes()
    {
        // Acceptance fixture (pre-WAL-as-sole-commit-point baseline): a
        // host that has not yet flipped the projection seam drives
        // foreground writes through SetAsync / DeleteAsync only. The
        // leaf's own state.State.Clock advances on every write (the
        // foreground commit path ticks the HLC for LWW timestamps),
        // but the projection-checkpoint persist seam is never invoked
        // and ReportCursorIfActiveAsync is therefore never reached.
        // The leaf must NOT register a cursor in this state - if it
        // did, the WAL GC would pin against a leaf that is not in
        // fact consuming the WAL and the trim point would never
        // advance.
        var (grain, reporter, state) = CreateGrainWithReporter();

        for (var i = 0; i < 100; i++)
        {
            await grain.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"v{i}"));
        }
        for (var i = 0; i < 50; i++)
        {
            await grain.DeleteAsync($"k{i}");
        }

        // Foreground writes have advanced the leaf's clock past Zero:
        Assert.That(state.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero));
        // ...but the cursor reporter has never been invoked, because
        // ReportCursorIfActiveAsync is reachable only from the
        // projection-seam persist (FlushPendingCheckpointAsync), which
        // is dormant in pre-promotion hosts.
        await reporter.DidNotReceive().ReportAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
        await reporter.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}