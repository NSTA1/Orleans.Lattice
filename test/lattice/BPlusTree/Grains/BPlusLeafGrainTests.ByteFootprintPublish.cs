using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers <c>BPlusLeafGrain.TryPublishByteFootprintAsync</c>: the
/// per-persist hop that pushes this leaf's
/// <see cref="LeafByteFootprint"/> to its owning <see cref="IShardRootGrain"/>
/// so the shard root's running storage-usage totals stay current
/// without ever walking the leaf chain on the read path. Specifically
/// pins the retry behaviour after a transient publish failure: the
/// next publish carrying identical byte totals must still hop, because
/// the previous attempt never landed on the shard root.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds a leaf grain with a Guid-keyed activation (production
    /// shape) and a stub <see cref="IShardRootGrain"/> wired through
    /// the substitute <see cref="IGrainFactory"/>. The Guid key is
    /// required because the publish helper short-circuits when
    /// <c>GrainId.GetGuidKey()</c> throws (the production-key contract).
    /// </summary>
    private static (BPlusLeafGrain grain, IShardRootGrain shardRoot, FakePersistentState<LeafNodeState> state) CreateGuidKeyedLeafWithShardRoot(
        string treeId = "byte-fp-tree",
        int shardIndex = 0)
    {
        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        var state = new FakePersistentState<LeafNodeState>
        {
            State =
            {
                TreeId = treeId,
                ShardIndex = shardIndex,
            },
        };

        var grainFactory = Substitute.For<IGrainFactory>();
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}").Returns(shardRoot);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return (grain, shardRoot, state);
    }

    [Test]
    public async Task TryPublishByteFootprint_publishes_changed_totals_to_the_owning_shard_root()
    {
        var (grain, shardRoot, _) = CreateGuidKeyedLeafWithShardRoot();
        shardRoot.PublishLeafByteFootprintAsync(Arg.Any<Guid>(), Arg.Any<LeafByteFootprint>())
            .Returns(Task.CompletedTask);

        // Any write funnels through PersistAsync (topology / wired-in
        // bookkeeping) and through the digest publication path on every
        // commit boundary. SetShardIndexAsync persists once, which is
        // enough to drive TryPublishByteFootprintAsync once.
        await grain.SetShardIndexAsync(0);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await shardRoot.ReceivedWithAnyArgs().PublishLeafByteFootprintAsync(default, default);
    }

    [Test]
    public async Task TryPublishByteFootprint_skips_republish_when_totals_unchanged_since_last_hop()
    {
        var (grain, shardRoot, _) = CreateGuidKeyedLeafWithShardRoot();
        shardRoot.PublishLeafByteFootprintAsync(Arg.Any<Guid>(), Arg.Any<LeafByteFootprint>())
            .Returns(Task.CompletedTask);

        // Two writes of the SAME key+value: LWW stamps a fresh HLC each
        // time so the digest path runs, but Cache.StateBytes is byte-
        // identical, so the publish helper's watermark short-circuits
        // the second cross-grain hop.
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        shardRoot.ClearReceivedCalls();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await shardRoot.DidNotReceiveWithAnyArgs().PublishLeafByteFootprintAsync(default, default);
    }

    [Test]
    public async Task TryPublishByteFootprint_retries_after_transient_publish_failure_with_identical_totals()
    {
        var (grain, shardRoot, _) = CreateGuidKeyedLeafWithShardRoot();
        var callCount = 0;
        shardRoot.PublishLeafByteFootprintAsync(Arg.Any<Guid>(), Arg.Any<LeafByteFootprint>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1)
                {
                    throw new InvalidOperationException("transient shard-root failure");
                }
                return Task.CompletedTask;
            });

        // Two consecutive writes of the SAME key+value: the LWW funnel
        // stamps a fresh HLC each time and re-flags the digest as dirty
        // (so PublishCurrentDigestAsync runs and chains into the byte-
        // footprint publish), but Cache.StateBytes is byte-identical
        // across both calls. The publish helper must therefore re-attempt
        // on the second write rather than skipping behind a watermark
        // the first (failed) hop should never have advanced.
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(callCount, Is.GreaterThanOrEqualTo(2),
            "the publish helper must re-attempt the hop after a transient failure, even when the cache state is unchanged");
    }
}
