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
        signal.UpdateState(unobservedAlias ? "physical-snapshot-copy" : tree, pressure);
        var (grain, factory) = CreateGrain(tree,
            options: new LatticeOptions { ShedSnapshotOpensWhenSaturated = enabled },
            saturationSignal: signal);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var reachedRouting = new IOException("snapshot reached routing after signal admission");
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
}
