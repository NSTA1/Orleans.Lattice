using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Review N1 of #5833: a failed arm is never silent. The partition stays pre-arm and
/// advances by replay, and the failure is counted on
/// <c>orleans.lattice.wal.append_fence.arm_failures</c>, tagged with the leaf's tree.
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    [Test]
    public async Task A_failed_arm_is_counted_and_leaves_the_partition_pre_arm()
    {
        var h = new Harness();
        var tree = "arm-failures-" + Guid.NewGuid().ToString("N");
        var fence = new LeafAppendEpochFence(
            Leaf,
            new SettableGate(open: true),
            h.Storage,
            h.Superseded.Add,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, tree),
            LatticeTenantLabel.ForTree(tree));
        h.Shard.ArmAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<Guid>(), Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<WalAppendFenceArmResult>(new TimeoutException("injected: the arm call timed out")));
        var trees = new List<string?>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalAppendFenceArmFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, tree))
                    {
                        lock (trees)
                        {
                            trees.Add(tag.Value as string);
                        }
                    }
                }
            }));

        var heads = await fence.ArmAsync(_ => h.Shard, [(0, WalRoutingInitiator.None, 0L)], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(heads, Is.EqualTo(new (long, long)?[] { null }), "a failed arm reported a head");
            Assert.That(fence.TryGetArmingView(0, out _, out _), Is.False, "the partition armed");
            Assert.That(trees, Is.EqualTo(new[] { tree }), "the failed arm was not counted once, tagged with its tree");
        });
    }
}
