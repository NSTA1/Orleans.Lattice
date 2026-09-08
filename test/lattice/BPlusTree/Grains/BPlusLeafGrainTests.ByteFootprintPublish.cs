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

    private static readonly GrainId ByteFootprintParentId =
        GrainId.Create("internal", "byte-fp-parent");

    private const string ByteFootprintProbeTreeId = "byte-fp-invalid-activation-tree";

    private const long ByteFootprintAdvancedOffset = 160973;

    /// <summary>
    /// Builds a production-shaped leaf (Guid activation key, resolved
    /// tree id and shard index, wired parent) whose checkpoint flush
    /// runs the inline upward digest publish on every call, so a single
    /// <c>SetCheckpointOffsetAsync</c> drives
    /// <c>PublishCurrentDigestAndClearDirtyAsync</c> and the byte-footprint
    /// publish piggybacked on it. Optionally shares a persisted row and a
    /// parent stub with an earlier activation, so a test can model a
    /// reactivation over state a previous activation left behind.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusInternalGrain ParentStub)
        CreateInvalidActivationProbeLeaf(
            FakePersistentState<LeafNodeState>? sharedState = null,
            IBPlusInternalGrain? sharedParent = null)
    {
        const int shardIndex = 0;

        var grainFactory = Substitute.For<IGrainFactory>();
        var parentStub = sharedParent ?? Substitute.For<IBPlusInternalGrain>();
        grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(parentStub);

        var shardRoot = Substitute.For<IShardRootGrain>();
        shardRoot.PublishLeafByteFootprintAsync(Arg.Any<Guid>(), Arg.Any<LeafByteFootprint>())
            .Returns(Task.CompletedTask);
        grainFactory.GetGrain<IShardRootGrain>($"{ByteFootprintProbeTreeId}/{shardIndex}").Returns(shardRoot);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = sharedState ?? new FakePersistentState<LeafNodeState>
        {
            State =
            {
                ParentId = ByteFootprintParentId,
                TreeId = ByteFootprintProbeTreeId,
                ShardIndex = shardIndex,
                ProjectionCheckpointOffset = 0,
            },
        };

        var options = new LatticeOptions
        {
            // Every-entry flush, so one SetCheckpointOffsetAsync reaches
            // PersistAsync and the inline publish with no coalescing
            // window in the way.
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            // Leave the upward publish as the only step in the
            // post-persist tail that can touch the invalidated state.
            LeafSnapshotReClassifyEveryNCheckpoints = 0,
        };

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: options,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state, parentStub);
    }

    [Test]
    public async Task Footprint_publish_on_an_invalidated_activation_returns_instead_of_throwing()
    {
        // Issue #2264, reproducing the field stack exactly: the upward
        // digest publish SUCCEEDS, and the activation is collected during
        // that parent hop. The piggybacked byte-footprint publish that
        // runs immediately afterwards is therefore the first statement to
        // touch state.State on a now-invalid activation, and before the
        // fix its unguarded `state.State.TreeId` read threw straight out
        // of a helper whose own doc and whose only call-site comment both
        // promise it cannot fail.
        var (grain, state, parentStub) = CreateInvalidActivationProbeLeaf();
        parentStub
            .OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(_ =>
            {
                state.ThrowOnStateAccess = new InvalidOperationException(
                    "Attempt to access an invalid activation: [Activation: bplusleaf/probe]");
                return Task.CompletedTask;
            });

        // Before the fix this still returned - CompleteCheckpointFlushTailAsync
        // contains the tail - but it logged the escape as an upward digest
        // publish failure and left the digest dirty. The discriminator is
        // the dirty flag below, not this call.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(
            ByteFootprintAdvancedOffset, CancellationToken.None);

        // The activation is notionally gone; lift the fault so the probe
        // below exercises the dirty flag rather than the fault itself.
        state.ThrowOnStateAccess = null;

        // Confirm the scenario actually ran: the digest DID reach the
        // parent, so a re-publish would be re-sending what it already has.
        await parentStub.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
        parentStub.ClearReceivedCalls();

        // PublishCurrentDigestAndClearDirtyAsync clears _digestDirty only
        // if the publish it awaits returns. Before the fix the footprint
        // helper threw through it, the flag stayed set, and this drain
        // re-published a digest the parent already held.
        await grain.FlushPendingDigestPublishAsync();

        await parentStub.DidNotReceiveWithAnyArgs()
            .OnChildDigestPublishedAsync(default, default);
    }

    [Test]
    public async Task Digest_dirty_flag_is_activation_scoped_so_a_fresh_activation_inherits_no_dirt()
    {
        // Pins the severity claim in #2264's title. "Permanently dirty"
        // overstates it: _digestDirty is a plain instance field with no
        // durable backing (LeafNodeState carries no dirty-digest member),
        // so it cannot outlive the activation that set it. A leaf that
        // dies dirty is replaced by one that starts clean over the same
        // persisted row, which bounds the defect to a single already-
        // doomed activation rather than to the tree's lifetime.
        var publishFaults = true;
        var (dyingGrain, state, parentStub) = CreateInvalidActivationProbeLeaf();
        parentStub
            .OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(_ => publishFaults
                ? Task.FromException(new TimeoutException("upward publish fault leaves the digest dirty"))
                : Task.CompletedTask);

        await ((ILeafProjection)dyingGrain).SetCheckpointOffsetAsync(
            ByteFootprintAdvancedOffset, CancellationToken.None);

        // A second activation over the SAME persisted row and parent.
        var (freshGrain, _, _) = CreateInvalidActivationProbeLeaf(
            sharedState: state, sharedParent: parentStub);
        publishFaults = false;
        parentStub.ClearReceivedCalls();

        await freshGrain.FlushPendingDigestPublishAsync();

        await parentStub.DidNotReceiveWithAnyArgs()
            .OnChildDigestPublishedAsync(default, default);

        // Control arm: the dirt is real on the activation that took it,
        // so the assertion above discriminates rather than passing
        // vacuously against a probe that never publishes.
        await dyingGrain.FlushPendingDigestPublishAsync();

        await parentStub.ReceivedWithAnyArgs(1)
            .OnChildDigestPublishedAsync(default, default);
    }
}
