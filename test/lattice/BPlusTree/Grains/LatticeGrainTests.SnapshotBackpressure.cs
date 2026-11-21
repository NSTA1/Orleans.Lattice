using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    [TestCase(WalSaturationState.Healthy, true, false)]
    [TestCase(WalSaturationState.Throttled, true, false)]
    [TestCase(WalSaturationState.Saturated, true, false)]
    [TestCase(WalSaturationState.Saturated, false, false)]
    [TestCase(WalSaturationState.Saturated, true, true)]
    public void OpenSnapshotEntryCursorAsync_sheds_only_the_observed_logical_Saturated_tree_with_its_own_source(
        WalSaturationState pressure, bool enabled, bool unobservedAlias)
    {
        const string tree = "snapshot-backpressure-member";
        var signal = new WalSaturationSignal();
        // (#3395) The shed reads the partition backing the cursor's start; an
        // unbounded forward cursor starts at the empty key.
        signal.UpdatePartitionState(
            unobservedAlias ? "physical-snapshot-copy" : tree,
            WalPartitionHash.Compute(string.Empty, LatticeOptions.DefaultWalPartitions),
            pressure);
        var (grain, factory) = CreateGrain(tree,
            options: new LatticeOptions { ShedSnapshotOpensWhenSaturated = enabled },
            saturationSignal: signal);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var reachedRouting = new IOException("snapshot reached routing after signal admission");
        if (enabled)
        {
            // Opt-out never consults the partition count, so its first entry read is routing.
            PinRegistryEntryWithoutShardMapRead(registry, LatticeOptions.DefaultWalPartitions, reachedRouting);
        }
        registry.GetShardMapAsync(tree).Returns(Task.FromException<ShardMap?>(reachedRouting));
        registry.ClearReceivedCalls();
        try
        {
            if (enabled && !unobservedAlias && pressure == WalSaturationState.Saturated)
            {
                var error = Assert.ThrowsAsync<LatticeSaturatedException>(
                    () => grain.OpenSnapshotEntryCursorAsync());
                Assert.Multiple(() =>
                {
                    Assert.That(error!.TreeId, Is.EqualTo(tree));
                    Assert.That(error.SaturationSource, Is.EqualTo(LatticeSaturationSource.SnapshotCursorOpen));
                    Assert.That(registry.ReceivedCalls().Any(call =>
                        call.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)), Is.False,
                        "shed before routing and the expensive per-shard capture");
                });
            }
            else
            {
                var error = Assert.ThrowsAsync<IOException>(() => grain.OpenSnapshotEntryCursorAsync());
                Assert.That(error, Is.SameAs(reachedRouting),
                    "Healthy, Throttled, opt-out and unobserved aliases must reach the real post-admission path");
            }
        }
        finally
        {
            signal.ResetForTesting();
        }
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public void OpenSnapshotEntryCursorAsync_sheds_only_when_the_partition_backing_the_start_position_is_Saturated(
        bool reverse, bool currentPartitionSaturated)
    {
        const string tree = "snapshot-backpressure-partition";
        const int partitions = 4;
        const string low = "alpha";
        const string high = "omega";
        var position = reverse ? high : low;
        var current = WalPartitionHash.Compute(position, partitions);
        var sibling = (current + 1) % partitions;
        var signal = new WalSaturationSignal();
        // Every partition except the one under test is Saturated, so a
        // tree-wide or sibling-partition read would shed the idle case.
        for (var partition = 0; partition < partitions; partition++)
        {
            signal.UpdatePartitionState(tree, partition,
                partition != current || currentPartitionSaturated
                    ? WalSaturationState.Saturated
                    : WalSaturationState.Healthy);
        }
        signal.UpdateState(tree, WalSaturationState.Saturated);
        Assert.That(signal.GetCurrentState(tree, sibling), Is.EqualTo(WalSaturationState.Saturated));
        var (grain, factory) = CreateGrain(tree,
            options: new LatticeOptions { ShedSnapshotOpensWhenSaturated = true, WalPartitions = partitions },
            saturationSignal: signal);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var reachedRouting = new IOException("snapshot reached routing after signal admission");
        PinRegistryEntryWithoutShardMapRead(registry, partitions, reachedRouting);
        registry.GetShardMapAsync(tree).Returns(Task.FromException<ShardMap?>(reachedRouting));
        try
        {
            if (currentPartitionSaturated)
            {
                var error = Assert.ThrowsAsync<LatticeSaturatedException>(
                    () => grain.OpenSnapshotEntryCursorAsync(low, high, reverse));
                Assert.That(error!.SaturationSource, Is.EqualTo(LatticeSaturationSource.SnapshotCursorOpen));
            }
            else
            {
                var error = Assert.ThrowsAsync<IOException>(
                    () => grain.OpenSnapshotEntryCursorAsync(low, high, reverse));
                Assert.That(error, Is.SameAs(reachedRouting),
                    "a saturated sibling partition must not stall a cursor whose own partition is idle");
            }
        }
        finally
        {
            signal.ResetForTesting();
        }
    }

    /// <summary>
    /// Serves the shed's partition-count lookup (the first entry read) from a
    /// pinned entry and faults every later entry read with
    /// <paramref name="reachedRouting"/>, so a cursor that passes admission
    /// trips the routing sentinel while the shed itself never does. The
    /// substitute registry's default entry read consults
    /// <see cref="ILatticeRegistry.GetShardMapAsync"/>, which would otherwise
    /// fault the lookup before the shed is evaluated.
    /// </summary>
    private static void PinRegistryEntryWithoutShardMapRead(
        ILatticeRegistry registry, int walPartitions, Exception reachedRouting) =>
        registry.GetEntryAsync(Arg.Any<string>()).Returns(
            Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = 4,
                WalPartitions = walPartitions,
            }),
            Task.FromException<TreeRegistryEntry?>(reachedRouting));
}
