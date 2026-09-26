using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the interleavable optimistic point read
/// <see cref="ShardRootGrain.TryGetOptimisticAsync"/> (issue #3474): reads on one
/// shard root overlap, and any routing mutation observed between the snapshot and the
/// leaf's reply forces the serial fallback rather than returning a stale answer
/// (the U9h-C "key missing mid-chaos" invariant).
/// </summary>
[TestFixture]
public sealed partial class ShardRootGrainOptimisticReadTests
{
    private const string ShardKey = "optimistic-tree/0";
    private static readonly GrainId RootLeafId = GrainId.Create("leaf", "root-leaf");
    private static readonly Guid LeafEpoch = Guid.NewGuid();

    private static VersionedValue Stamped(byte[]? value, long generation = 1) => new()
    {
        Value = value,
        LeafRoutingEpoch = LeafEpoch,
        LeafRoutingGeneration = generation,
    };

    private static void SeedRoutingStamp(ShardRootGrain grain, GrainId leafId, Guid epoch, long generation)
    {
        // Arrange a warmed cache only. OwnershipTests drives the real serial
        // warmup path separately so these focused epoch tests do not hide it.
        var field = typeof(ShardRootGrain).GetField("_leafRoutingStamps",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "Ownership cache renamed; update the test arrangement.");
        var stamps = (Dictionary<GrainId, (Guid, long)>)field!.GetValue(grain)!;
        stamps[leafId] = (epoch, generation);
    }

    private static (ShardRootGrain Grain, FakePersistentState<ShardRootState> State, IBPlusLeafGrain Leaf, ILeafCacheGrain Cache) CreateGrain(
        bool optimisticReads = true,
        bool seedRoot = true,
        IGrainContext? context = null,
        string shardKey = ShardKey,
        IGrainFactory? factory = null,
        bool warmStamp = true)
    {
        context ??= Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", shardKey));

        var state = new FakePersistentState<ShardRootState>();
        if (seedRoot)
        {
            state.State.RootNodeId = RootLeafId;
            state.State.RootIsLeaf = true;
        }

        factory ??= Substitute.For<IGrainFactory>();
        var cache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(cache);
        var leaf = Substitute.For<IBPlusLeafGrain>();
        // Proof-focused tests take the raw-miss arm unless they arrange a value.
        leaf.GetAsync(Arg.Any<string>()).Returns((byte[]?)null);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { OptimisticShardRootPointReads = optimisticReads },
            factory: factory);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        if (warmStamp) SeedRoutingStamp(grain, RootLeafId, LeafEpoch, 1);
        return (grain, state, leaf, cache);
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task TryGetOptimisticAsync_returns_validated_value_in_steady_state()
    {
        var (grain, _, leaf, cache) = CreateGrain(warmStamp: false);
        leaf.GetAsync("k1").Returns(Bytes("v1"));

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.True);
        Assert.That(Encoding.UTF8.GetString(result.Value!), Is.EqualTo("v1"));
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_absent_key_with_matching_ownership_stamp_validates()
    {
        var (grain, _, leaf, _) = CreateGrain();
        leaf.GetWithVersionAsync("missing").Returns(Stamped(null));

        var result = await grain.TryGetOptimisticAsync("missing");

        Assert.That(result.IsValidated, Is.True);
        Assert.That(result.Value, Is.Null);
    }

    [Test]
    public async Task TryGetOptimisticAsync_reads_the_primary_leaf_not_the_leaf_cache()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        leaf.GetWithVersionAsync("k1").Returns(Stamped(Bytes("v1")));

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.True);
        await leaf.Received(1).GetWithVersionAsync("k1");
        await leaf.Received(1).GetAsync("k1");
        await cache.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_concurrent_reads_are_in_flight_together_against_a_gated_leaf()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        var gateA = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        var gateB = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("a").Returns(gateA.Task);
        leaf.GetWithVersionAsync("b").Returns(gateB.Task);

        var readA = grain.TryGetOptimisticAsync("a");
        var readB = grain.TryGetOptimisticAsync("b");

        // Both reads reached the leaf before either leaf reply arrived: they overlap
        // on the one shard root rather than being serialised behind each other.
        await leaf.Received(1).GetWithVersionAsync("a");
        await leaf.Received(1).GetWithVersionAsync("b");
        Assert.That(readA.IsCompleted, Is.False);
        Assert.That(readB.IsCompleted, Is.False);

        gateB.SetResult(Stamped(Bytes("vb")));
        gateA.SetResult(Stamped(Bytes("va")));
        var resultA = await readA;
        var resultB = await readB;

        Assert.That(resultA.IsValidated, Is.True);
        Assert.That(resultB.IsValidated, Is.True);
        Assert.That(Encoding.UTF8.GetString(resultA.Value!), Is.EqualTo("va"));
        Assert.That(Encoding.UTF8.GetString(resultB.Value!), Is.EqualTo("vb"));
    }

    [Test]
    public async Task TryGetOptimisticAsync_routing_mutation_between_snapshot_and_leaf_reply_forces_serial_retry()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        var gate = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k1").Returns(gate.Task);

        var read = grain.TryGetOptimisticAsync("k1");

        // A simulated promotion / move-away publish runs to completion while the
        // read is parked on the leaf. The leaf then answers with a value, which the
        // epoch check alone must refuse: the routing it was resolved against moved.
        grain.BeginRoutingMutation();
        grain.EndRoutingMutation();
        gate.SetResult(Stamped(Bytes("stale")));

        var result = await read;

        Assert.That(result.IsValidated, Is.False);
        Assert.That(result.Value, Is.Null);
    }

    [Test]
    public async Task TryGetOptimisticAsync_serial_fallback_after_routing_mutation_returns_the_present_value()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        var gate = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k1").Returns(gate.Task);

        var read = grain.TryGetOptimisticAsync("k1");
        grain.BeginRoutingMutation();
        grain.EndRoutingMutation();
        gate.SetResult(Stamped(null));
        var optimistic = await read;

        // The caller then takes the serial path against settled routing, which
        // finds the key.
        cache.GetAsync("k1").Returns(Bytes("present"));
        var serial = await grain.GetAsync("k1");

        Assert.That(optimistic.IsValidated, Is.False);
        Assert.That(Encoding.UTF8.GetString(serial!), Is.EqualTo("present"));
    }

    [Test]
    public async Task TryGetOptimisticAsync_mutation_still_in_flight_at_snapshot_forces_serial_retry_without_leaf_call()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        grain.BeginRoutingMutation();

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
        grain.EndRoutingMutation();
    }

    [Test]
    public async Task TryGetOptimisticAsync_leaf_fault_while_routing_moved_maps_to_serial_retry()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        var gate = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k1").Returns(gate.Task);

        var read = grain.TryGetOptimisticAsync("k1");
        grain.BeginRoutingMutation();
        gate.SetException(new InvalidOperationException("leaf retired mid-read"));
        var result = await read;
        grain.EndRoutingMutation();

        Assert.That(result.IsValidated, Is.False);
    }

    [Test]
    public void TryGetOptimisticAsync_leaf_fault_with_stable_routing_propagates()
    {
        var (grain, _, leaf, cache) = CreateGrain();
        leaf.GetWithVersionAsync("k1").Returns(Task.FromException<VersionedValue>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.TryGetOptimisticAsync("k1"));
    }

    [Test]
    public async Task TryGetOptimisticAsync_flag_off_restores_serial_behaviour()
    {
        var (grain, _, leaf, cache) = CreateGrain(optimisticReads: false);
        leaf.GetWithVersionAsync("k1").Returns(Stamped(Bytes("v1")));

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_unseeded_root_defers_to_serial_prepare()
    {
        var (grain, _, leaf, cache) = CreateGrain(seedRoot: false);

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_pending_promotion_defers_to_serial_path()
    {
        var (grain, state, leaf, cache) = CreateGrain();
        state.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "m",
            NewSiblingId = GrainId.Create("leaf", "sibling"),
        };

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_split_in_progress_defers_to_serial_path()
    {
        var (grain, state, leaf, cache) = CreateGrain();
        state.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.BeginShadowWrite,
            ShadowTargetShardIndex = 1,
            MovedSlots = [1],
            VirtualShardCount = 64,
        };

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_moved_away_slot_defers_to_serial_instead_of_throwing()
    {
        var (grain, state, leaf, cache) = CreateGrain();
        const int virtualShardCount = 64;
        var slot = ShardMap.GetVirtualSlot("k1", virtualShardCount);
        state.State.MovedAwayVirtualShardCount = virtualShardCount;
        state.State.MovedAwaySlots[slot] = 3;

        // No-livelock pin: a stale-routing throw from the optimistic read would be
        // absorbed by the caller's retry loop and re-enter the optimistic read, never
        // reaching the serial path. The gate must surface as a serial retry instead,
        // leaving the serial read to raise (or settle) the moved-away verdict.
        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
        Assert.ThrowsAsync<StaleShardRoutingException>(() => grain.GetAsync("k1"));
    }

    [Test]
    public async Task TryGetOptimisticAsync_deleted_tree_defers_to_serial_instead_of_throwing()
    {
        var (grain, state, leaf, cache) = CreateGrain();
        state.State.IsDeleted = true;

        var result = await grain.TryGetOptimisticAsync("k1");

        Assert.That(result.IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.GetAsync("k1"));
    }
}
