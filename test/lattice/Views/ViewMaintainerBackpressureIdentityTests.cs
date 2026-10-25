using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression coverage for source back-pressure after a source physical-identity
/// swap (resize / undo / restore / revert). The WAL saturation sampler keys its
/// signal by the physical WAL tree id, so the maintainer must read saturation
/// under the physical id it drains, not under the logical alias. Reading the
/// alias names a log nobody samples, which reports Healthy forever and drains
/// at full rate onto a saturated source.
/// </summary>
[TestFixture]
public class ViewMaintainerBackpressureIdentityTests
{
    private const string ViewName = "orders-view";
    private const string LogicalSource = "orders";
    private const string PhysicalSource = "orders-resized";
    private const int FullBatch = 256;
    private const int SaturatedBatch = 16;
    private const int SaturatedPause = 500;

    private sealed class FixedCatalog(ViewRegistration registration) : IViewCatalog
    {
        public ViewRegistration? TryGet(string viewName) => registration;

        public void Register(ViewRegistration registration)
        {
        }

        public void Remove(string viewName)
        {
        }

        public IReadOnlyCollection<ViewRegistration> All() => new[] { registration };
    }

    private sealed class StubAggregationProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "agg-v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) => [];
    }

    private sealed record Harness(ViewMaintainerGrain Grain, List<int> DrainBatchSizes);

    private static Harness Create(
        bool aggregation,
        string saturatedTreeId,
        WalSaturationState pressure = WalSaturationState.Saturated)
    {
        var registration = aggregation
            ? new ViewRegistration(ViewName, LogicalSource, Projection: null, AggregationProjection: new StubAggregationProjection())
            : new ViewRegistration(ViewName, LogicalSource, Substitute.For<ILatticeViewProjection>());

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(PhysicalSource);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = 1,
                PhysicalTreeId = PhysicalSource,
            }));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var trees = new Dictionary<string, ILattice>(StringComparer.Ordinal);
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var treeId = call.ArgAt<string>(0);
            if (!trees.TryGetValue(treeId, out var tree))
            {
                tree = Substitute.For<ILattice>();
                tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Returns(Task.FromResult<byte[]?>(null));
                tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
                    .Returns(_ => Task.FromResult(new Dictionary<string, byte[]>(StringComparer.Ordinal)));
                trees[treeId] = tree;
            }

            return tree;
        });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));

        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(Arg.Any<string>()).Returns(new LatticeViewOptions
        {
            BatchSize = FullBatch,
            ObeySourceBackpressure = true,
            ThrottledBatchRatio = 0.5,
            SaturatedBatchSize = SaturatedBatch,
            SaturatedPauseMs = SaturatedPause,
        });

        var latticeOptions = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        latticeOptions.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalRetention = TimeSpan.Zero });

        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.IsReplicationEnabled.Returns(false);

        // The sampler publishes under exactly one tree id; every other id is a log
        // it has never observed, which the real signal reports as Healthy.
        var saturationSignal = Substitute.For<IWalSaturationSignal>();
        saturationSignal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);
        saturationSignal.GetCurrentState(saturatedTreeId).Returns(pressure);

        var batchSizes = new List<int>();
        var subscriber = Substitute.For<IWalSubscriber>();
        subscriber
            .DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                batchSizes.Add(call.ArgAt<WalSubscriptionContext>(0).BatchSize);
                return Task.FromResult(new WalDrainResult());
            });

        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = PhysicalSource;

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            reminderRegistry: null!,
            NullLogger<ViewMaintainerGrain>.Instance,
            new FixedCatalog(registration),
            commitLogReader: Substitute.For<ICommitLogReader>(),
            subscriber: subscriber,
            cursorRegistry: Substitute.For<IWalCursorRegistry>(),
            optionsResolver: TestOptionsResolver.ForFactory(factory, new LatticeOptions()),
            viewOptions,
            latticeOptions,
            replicationContext,
            saturationSignal,
            historyRowCodec: null!,
            state);

        return new Harness(grain, batchSizes);
    }

    [TestCase(false, WalSaturationState.Saturated, TestName = "Projection_drain_after_an_identity_swap_obeys_saturation_of_the_bound_physical_log")]
    [TestCase(true, WalSaturationState.Saturated, TestName = "Aggregation_drain_after_an_identity_swap_obeys_saturation_of_the_bound_physical_log")]
    [TestCase(false, WalSaturationState.Throttled, TestName = "Projection_drain_after_an_identity_swap_obeys_throttling_of_the_bound_physical_log")]
    [TestCase(true, WalSaturationState.Throttled, TestName = "Aggregation_drain_after_an_identity_swap_obeys_throttling_of_the_bound_physical_log")]
    [TestCase(false, WalSaturationState.Healthy, TestName = "Projection_drain_after_an_identity_swap_keeps_full_size_for_a_healthy_bound_physical_log")]
    [TestCase(true, WalSaturationState.Healthy, TestName = "Aggregation_drain_after_an_identity_swap_keeps_full_size_for_a_healthy_bound_physical_log")]
    public async Task Drain_obeys_saturation_of_the_bound_physical_log(bool aggregation, WalSaturationState pressure)
    {
        var h = Create(aggregation, saturatedTreeId: PhysicalSource, pressure);

        await h.Grain.DrainAsync();

        var expected = pressure switch
        {
            WalSaturationState.Saturated => SaturatedBatch,
            WalSaturationState.Throttled => FullBatch / 2,
            _ => FullBatch,
        };
        Assert.That(h.DrainBatchSizes, Is.EqualTo(new[] { expected }),
            "The pass must apply the bound physical log's full, Throttled or Saturated batch size.");
    }

    [TestCase(false, TestName = "Projection_drain_ignores_a_saturation_signal_published_under_the_logical_alias")]
    [TestCase(true, TestName = "Aggregation_drain_ignores_a_saturation_signal_published_under_the_logical_alias")]
    public async Task Drain_ignores_saturation_under_the_logical_alias(bool aggregation)
    {
        var h = Create(aggregation, saturatedTreeId: LogicalSource);

        await h.Grain.DrainAsync();

        Assert.That(h.DrainBatchSizes, Is.EqualTo(new[] { FullBatch }),
            "The logical alias is not the log the view reads, so its signal must not throttle the drain.");
    }

    [Test]
    public void Background_tick_deferral_obeys_saturation_of_the_bound_physical_log()
    {
        var h = Create(aggregation: false, saturatedTreeId: PhysicalSource);

        Assert.That(h.Grain.ComputeBackpressurePauseMs(), Is.EqualTo(SaturatedPause),
            "The tick-skip window must arm while the bound physical log is saturated.");
    }

    [Test]
    public void Background_tick_deferral_ignores_a_saturation_signal_published_under_the_logical_alias()
    {
        var h = Create(aggregation: false, saturatedTreeId: LogicalSource);

        Assert.That(h.Grain.ComputeBackpressurePauseMs(), Is.Zero,
            "A signal under the logical alias does not describe the drained log.");
    }
}