using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class ShardRootGrainPartitionHeadCaptureTests
{
    private const string TreeId = "partition-heads";

    private static (ShardRootGrain Grain, ILeafReplayCoordinatorGrain[] Coordinators, ILatticeRegistry Registry) CreateGrain(
        int partitions, int? configuredPartitions = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", TreeId + "/0"));
        var state = new FakePersistentState<ShardRootState>();
        var leafKey = Guid.NewGuid();
        state.State.RootNodeId = GrainId.Create("leaf", leafKey.ToString("N"));
        state.State.RootIsLeaf = true;
        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        factory.GetGrain<IBPlusLeafGrain>(leafKey).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId.Value).Returns(leaf);
        leaf.GetProjectionCheckpointAndNextSiblingAsync().Returns(new LeafCheckpointPage
        {
            CheckpointOffset = 7,
            Link = new LeafChainLink { KeyRange = new LeafKeyRange() },
        });
        var coordinators = new ILeafReplayCoordinatorGrain[partitions];
        for (var p = 0; p < partitions; p++)
        {
            coordinators[p] = Substitute.For<ILeafReplayCoordinatorGrain>();
            factory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/{p}").Returns(coordinators[p]);
        }
        var options = new LatticeOptions
        {
            WalPartitions = configuredPartitions ?? partitions,
            MaxScanPageStallDuration = Timeout.InfiniteTimeSpan,
        };
        var resolver = TestOptionsResolver.Create(options, factory: factory);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        if (configuredPartitions is not null)
            registry.GetEntryAsync(TreeId).Returns(new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = 1,
                WalPartitions = partitions,
            });
        var grain = new ShardRootGrain(context, state, factory, resolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());
        return (grain, coordinators, registry);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Head_capture_overlaps_reads_and_preserves_partition_order(bool lag)
    {
        var (grain, coordinators, _) = CreateGrain(3);
        var gates = Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource<long>()).ToArray();
        for (var p = 0; p < gates.Length; p++)
            coordinators[p].GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(gates[p].Task);

        var capture = lag
            ? CaptureLagHeadsAsync(grain)
            : grain.SnapshotWalHeadAsync(CancellationToken.None);

        foreach (var coordinator in coordinators)
            _ = coordinator.Received(1).GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        gates[2].SetResult(30);
        gates[0].SetResult(10);
        Assert.That(capture.IsCompleted, Is.False);
        gates[1].SetResult(20);
        Assert.That(await capture.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo(new long[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Head_capture_preserves_the_single_partition_fast_path()
    {
        var (grain, coordinators, _) = CreateGrain(1);
        coordinators[0].GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(42L);

        Assert.That(await grain.SnapshotWalHeadAsync(CancellationToken.None), Is.EqualTo(new[] { 42L }));
        _ = coordinators[0].Received(1).GetHeadOffsetAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Lag_continuation_does_not_capture_partition_heads_again()
    {
        var (grain, coordinators, _) = CreateGrain(3);

        var page = await grain.GetShardMaterialiserLagBoundedAsync("continued", CancellationToken.None);

        Assert.That(page.WalHeadOffsets, Is.Empty);
        foreach (var coordinator in coordinators)
            _ = coordinator.DidNotReceive().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
    }

    [TestCase(false, 1)]
    [TestCase(false, 3)]
    [TestCase(true, 1)]
    [TestCase(true, 3)]
    public async Task Head_capture_reuses_pinned_partition_count_but_reads_fresh_heads(bool lag, int partitions)
    {
        var (grain, coordinators, registry) = CreateGrain(partitions, configuredPartitions: partitions + 4);
        foreach (var coordinator in coordinators)
            coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(42L);

        var first = lag ? await CaptureLagHeadsAsync(grain) : await grain.SnapshotWalHeadAsync(CancellationToken.None);
        Assert.That(first, Is.EqualTo(Enumerable.Repeat(42L, partitions)));
        _ = registry.Received(1).GetEntryAsync(TreeId);
        registry.ClearReceivedCalls();
        foreach (var coordinator in coordinators)
            coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(99L);

        var second = lag ? await CaptureLagHeadsAsync(grain) : await grain.SnapshotWalHeadAsync(CancellationToken.None);
        Assert.That(second, Is.EqualTo(Enumerable.Repeat(99L, partitions)));
        _ = registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
        _ = registry.DidNotReceive().RegisterAndGetEntryAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Legacy_partition_fallback_is_shared_between_both_capture_sites(bool lagFirst)
    {
        var (grain, _, registry) = CreateGrain(3);
        if (lagFirst)
            await CaptureLagHeadsAsync(grain);
        else
            await grain.SnapshotWalHeadAsync(CancellationToken.None);
        registry.ClearReceivedCalls();

        var heads = lagFirst
            ? await grain.SnapshotWalHeadAsync(CancellationToken.None)
            : await CaptureLagHeadsAsync(grain);

        Assert.That(heads, Has.Length.EqualTo(3));
        _ = registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Head_capture_bounds_wide_trees()
    {
        var (grain, coordinators, _) = CreateGrain(BoundedFanOut.DefaultWidth + 1);
        var gate = new TaskCompletionSource<long>();
        for (var p = 0; p < BoundedFanOut.DefaultWidth; p++)
            coordinators[p].GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(gate.Task);
        coordinators[^1].GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(99L));

        var capture = grain.SnapshotWalHeadAsync(CancellationToken.None);

        await coordinators[^1].DidNotReceive().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        Assert.That(capture.IsCompleted, Is.False);
        gate.SetResult(1);
        var heads = await capture.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(heads, Has.Length.EqualTo(coordinators.Length));
        Assert.That(heads[^1], Is.EqualTo(99));
        await coordinators[^1].Received(1).GetHeadOffsetAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Head_capture_propagates_fault_only_after_other_reads_settle()
    {
        var (grain, coordinators, _) = CreateGrain(2);
        var gate = new TaskCompletionSource<long>();
        coordinators[0].GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new InvalidOperationException("head unavailable")));
        coordinators[1].GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(gate.Task);

        var capture = grain.SnapshotWalHeadAsync(CancellationToken.None);
        Assert.That(capture.IsCompleted, Is.False);
        gate.SetResult(20);
        var error = Assert.ThrowsAsync<InvalidOperationException>(() => capture.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.That(error!.Message, Is.EqualTo("head unavailable"));
        await coordinators[1].Received(1).GetHeadOffsetAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void Head_capture_observes_pre_cancelled_tokens()
    {
        var (grain, _, _) = CreateGrain(3);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(() => grain.SnapshotWalHeadAsync(cancellation.Token));
    }

    private static async Task<long[]> CaptureLagHeadsAsync(ShardRootGrain grain) =>
        (await grain.GetShardMaterialiserLagBoundedAsync(null, CancellationToken.None)).WalHeadOffsets;
}
