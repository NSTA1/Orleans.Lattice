using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The resize mirror is a late-forward source, like the split's hot-path
/// forward: its call is abandoned at <see cref="LatticeOptions.ShardForwardTimeout"/>
/// and can still land on the resized copy after the saga's terminal. The
/// destination leaf refuses a late prepare on the registry's decision, or on the
/// saga's retired participant row, only when the call is marked as a forwarded
/// prepare naming the tree the saga decides under (issues #4445 and #4632). The
/// marking is done by <c>ShardRootGrain.ForwardWithDeadlineAsync</c> for every
/// shadow forward, the resize mirror included.
/// </summary>
public partial class ShardRootGrainShadowForwardTests
{
    [Test]
    public async Task SetManyAsync_prepare_mirrored_to_the_resize_destination_is_marked_as_a_forwarded_prepare()
    {
        var h = CreateHarness();
        bool? forwarded = null;
        string? registryTreeId = null;
        h.ShadowTarget.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            forwarded = LatticeForwardedPrepareContext.Current;
            registryTreeId = LatticeForwardedPrepareContext.RegistryTreeId;
            return Task.CompletedTask;
        });
        h.Leaf.GetOriginalPrepareStampsAsync(Arg.Any<Guid>())
            .Returns(Task.FromResult<Dictionary<string, HybridLogicalClock?>?>(
                new() { ["k1"] = new HybridLogicalClock { WallClockTicks = 7_000, Counter = 1 } }));
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        using (LatticePreparedContext.BeginScope())
        {
            LatticeTransactionContext.Set(Guid.NewGuid());
            try
            {
                await h.Grain.SetManyAsync([new("k1", [1])]);
            }
            finally
            {
                LatticeTransactionContext.Set(Guid.Empty);
            }
        }

        await h.ShadowTarget.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        Assert.That(forwarded, Is.True,
            "a mirrored prepare that outlives its deadline must reach the destination as a forwarded prepare, or no late-prepare refusal applies to it");
        Assert.That(registryTreeId, Is.EqualTo(TreeId),
            "the forwarded prepare names the tree the saga records its decision under");
        Assert.That(LatticeForwardedPrepareContext.Current, Is.False, "the marker must not outlive the forward");
    }
}
