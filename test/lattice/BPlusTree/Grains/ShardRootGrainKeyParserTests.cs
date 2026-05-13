using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for malformed <see cref="ShardRootGrain"/> activation
/// keys. The expected key shape is <c>{treeId}/{shardIndex}</c>; every
/// public entry point must surface a typed
/// <see cref="InvalidOperationException"/> for malformed keys instead of
/// leaking a low-level <see cref="ArgumentOutOfRangeException"/>,
/// <see cref="FormatException"/>, or silently returning a misparsed
/// (empty / zero) value that mis-tags metrics and corrupts routing.
/// Sibling grains <c>WalShardGrain</c> and <c>LeafReplayCoordinatorGrain</c>
/// already follow this contract; this fixture pins
/// <see cref="ShardRootGrain"/> to the same shape.
/// </summary>
public class ShardRootGrainKeyParserTests
{
    private static ShardRootGrain CreateGrain(string shardKey)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", shardKey));

        var state = new FakePersistentState<ShardRootState>();
        // Pre-populate so EnsureRootAsync is a no-op and the malformed-key
        // signal reaches RecordRead / metric tagging unimpeded.
        state.State.RootNodeId = GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: grainFactory);

        var leafGrain = Substitute.For<IBPlusLeafGrain>();
        leafGrain.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leafGrain);

        var cacheGrain = Substitute.For<ILeafCacheGrain>();
        cacheGrain.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        grainFactory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cacheGrain);

        return new ShardRootGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
    }

    // --- TreeId parser (ComputeTreeId) via GetAsync -> RecordRead -> TreeId getter ---

    // Note: an empty grain key is unreachable via the Orleans surface because
    // GrainId.Create itself rejects null/whitespace at construction. The
    // string.IsNullOrEmpty guard inside ParseShardGrainKey is defensive,
    // covering the case where a future Orleans change relaxes that check.

    [Test]
    public void GetAsync_with_no_slash_grain_key_throws_InvalidOperationException()
    {
        var grain = CreateGrain(shardKey: "no-slash-key");
        Assert.That(
            async () => await grain.GetAsync("key"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetAsync_with_leading_slash_only_throws_InvalidOperationException()
    {
        // Worst variant: LastIndexOf returns 0, key[..0] returns "" silently,
        // and "0" parses cleanly as the shard index. Today this routes
        // mutations against TreeId="" with no diagnostic, corrupting metrics
        // and registry lookups. Must surface as a typed validation error.
        var grain = CreateGrain(shardKey: "/0");
        Assert.That(
            async () => await grain.GetAsync("key"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetAsync_with_trailing_slash_only_throws_InvalidOperationException()
    {
        // LastIndexOf returns key.Length - 1, the shard-index slice is
        // empty, int.TryParse fails, and ShardIndex silently returns 0 -
        // mis-tagging every metric data point. Must throw.
        var grain = CreateGrain(shardKey: "tree-without-shard/");
        Assert.That(
            async () => await grain.GetAsync("key"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetAsync_with_non_integer_shard_suffix_throws_InvalidOperationException()
    {
        // ShardIndex's int.TryParse fails -> silent 0; MyShardIndex's
        // int.Parse throws raw FormatException. Must surface as typed
        // InvalidOperationException either way.
        var grain = CreateGrain(shardKey: "tree-id/not-a-number");
        Assert.That(
            async () => await grain.GetAsync("key"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetAsync_with_negative_shard_suffix_throws_InvalidOperationException()
    {
        // A negative shard index is structurally meaningless (shard indices
        // are non-negative integers). Sibling WalShardGrain rejects this;
        // ShardRootGrain must too.
        var grain = CreateGrain(shardKey: "tree-id/-1");
        Assert.That(
            async () => await grain.GetAsync("key"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // --- MyShardIndex parser via BeginSplitAsync ---

    [Test]
    public void BeginSplitAsync_with_no_slash_grain_key_throws_InvalidOperationException_not_FormatException()
    {
        // BeginSplitAsync's first check is `targetShardIndex == MyShardIndex`,
        // which forces int.Parse on the suffix. Today that throws
        // FormatException for "no-slash-key" (LastIndexOf returns -1,
        // int.Parse("no-slash-key") fails). Must be a typed validation
        // exception instead.
        var grain = CreateGrain(shardKey: "no-slash-key");
        Assert.That(
            async () => await grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [0], virtualShardCount: 16),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // --- Happy-path regression: well-formed keys still work ---

    [Test]
    public async Task GetAsync_with_well_formed_grain_key_succeeds()
    {
        var grain = CreateGrain(shardKey: "happy-tree/3");
        // No exception means TreeId="happy-tree" and ShardIndex=3 parsed cleanly.
        var result = await grain.GetAsync("key");
        Assert.That(result, Is.Null);
    }
}