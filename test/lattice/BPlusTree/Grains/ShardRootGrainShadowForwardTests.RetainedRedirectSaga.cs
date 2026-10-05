using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A saga bound to the copy a shadow-cutover restore retained
/// (spec/shard-ownership/ShardOwnershipCutover.tla). The retained redirect
/// refuses the saga's routed prepares, so the saga re-binds to the copy the tree
/// resolves to, and it admits a call addressed to the copy directly, so the
/// terminal broadcast reaches the copy the saga decided on even after the restore
/// has armed it.
/// </summary>
public partial class ShardRootGrainShadowForwardTests
{
    private const string RedirectedLogicalTreeId = "logical-tree";

    private static void SetRetainedRedirect(FakePersistentState<ShardRootState> state) =>
        state.State.RetainedRedirect = new RetainedRedirectState
        {
            DestinationPhysicalTreeId = $"{TreeId}-bkprestore-1",
            OperationId = "restore-op",
            LogicalTreeId = RedirectedLogicalTreeId,
        };

    [Test]
    public void SetManyAsync_prepared_by_a_saga_bound_to_a_retained_copy_is_refused_when_routed_through_the_alias()
    {
        // Unlike a resize fence, a restore's redirect admits no bound saga: the
        // retained copy mirrors nowhere, so a prepare it took would be left
        // behind when the saga re-binds to the restored copy.
        var h = CreateHarness();
        SetRetainedRedirect(h.State);
        List<KeyValuePair<string, byte[]>> entries = [new("k1", [1])];

        RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, RedirectedLogicalTreeId);
        try
        {
            using (LatticePreparedContext.BeginScope())
            using (LatticeAtomicBindingContext.With(TreeId))
            {
                Assert.ThrowsAsync<StaleTreeRoutingException>(() => h.Grain.SetManyAsync(entries));
            }
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        }

        h.Leaf.DidNotReceiveWithAnyArgs().SetManyAsync(default!);
    }

    [Test]
    public async Task AppendTxTerminalAsync_addressed_directly_to_a_retained_copy_is_applied()
    {
        // The saga addresses its terminals to the physical copy it decided on,
        // without a routed-logical stamp, so a redirect the restore armed after
        // the decision does not leave the batch decided on some shards only.
        var h = CreateHarness();
        SetRetainedRedirect(h.State);
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true, new Dictionary<string, byte[]> { ["k1"] = [1] });

        await h.Leaf.Received(1).ApplyTxTerminalAsync(txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>());
    }

    [Test]
    public void AppendTxTerminalAsync_routed_through_the_alias_is_refused_by_a_retained_copy()
    {
        var h = CreateHarness();
        SetRetainedRedirect(h.State);
        RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, RedirectedLogicalTreeId);
        try
        {
            Assert.ThrowsAsync<StaleTreeRoutingException>(() =>
                h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true, new Dictionary<string, byte[]> { ["k1"] = [1] }));
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        }

        h.Leaf.DidNotReceiveWithAnyArgs().ApplyTxTerminalAsync(default, default, default);
    }
}
