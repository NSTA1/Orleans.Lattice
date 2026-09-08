using System.Text;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the leaf bucketing in <c>ShardRootGrain.GetRawEntriesAsync</c> - the raw
/// batch read the atomic-write prepare phase captures a shard through.
///
/// <para>
/// The bucketing is lazy: because the saga capture path resolves its affected
/// keys from a single leaf set, the common shape is single-leaf, so no bucket
/// map is built at all until a SECOND distinct leaf is actually observed. At
/// that point the already-routed prefix has to be back-filled into the first
/// bucket from the caller's key list. That back-fill is the interesting failure
/// mode, and it is invisible to a single-leaf test: get the prefix width wrong
/// and the multi-leaf result silently mis-associates values with keys, or drops
/// them, while every single-leaf test still passes.
/// </para>
///
/// <para>
/// So the fixture walks three shapes deliberately: the flat single-leaf tree,
/// a multi-leaf batch whose leaves INTERLEAVE (so the prefix is one key wide and
/// scattering by position is load-bearing), and a multi-leaf batch whose second
/// leaf appears late (so the back-filled prefix is several keys wide). All three
/// assert index alignment against the caller's request order, which is the
/// contract <c>AtomicWriteGrain.CaptureShardAsync</c> depends on.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainRawBatchReadBucketingTests
{
    private const string TreeId = "raw-batch-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// Builds a shard whose root is an INTERNAL node routing by first letter to
    /// three distinct leaves, and returns the per-leaf recorded call arguments so
    /// a test can assert how the batch was bucketed as well as what it returned.
    /// </summary>
    private static (ShardRootGrain Grain, Dictionary<string, List<List<string>>> Calls) CreateThreeLeafShard(
        params string[] tombstonedKeys)
    {
        var tombstones = new HashSet<string>(tombstonedKeys, StringComparer.Ordinal);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var internalRootId = GrainId.Create("internal", "root");
        var leafIds = new[]
        {
            GrainId.Create("leaf", "leaf-a"),
            GrainId.Create("leaf", "leaf-b"),
            GrainId.Create("leaf", "leaf-c"),
        };

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = internalRootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

        // Route() walks separators backwards and takes the first that is null or
        // <= key, so "a*" -> leaf-a, "b*" -> leaf-b, "c*" -> leaf-c.
        var internalRoot = Substitute.For<IBPlusInternalGrain>();
        internalRoot.GetRoutingTableAsync()
            .Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[] { null, "b", "c" },
                ChildIds = leafIds,
                ChildrenAreLeaves = true,
            }));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalRoot);

        var calls = new Dictionary<string, List<List<string>>>(StringComparer.Ordinal);
        var leavesById = new Dictionary<GrainId, IBPlusLeafGrain>();
        foreach (var leafId in leafIds)
        {
            var recorded = new List<List<string>>();
            calls[leafId.ToString()] = recorded;

            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.GetRawEntriesAsync(Arg.Any<List<string>>())
                .Returns(call =>
                {
                    var requested = call.Arg<List<string>>();

                    // Snapshot the argument: the shipped single-leaf path hands
                    // over the caller's own list, so recording the reference
                    // would let a later mutation rewrite history.
                    recorded.Add([.. requested]);

                    var entries = new List<LwwEntry?>(requested.Count);
                    foreach (var key in requested)
                    {
                        entries.Add(tombstones.Contains(key)
                            ? new LwwEntry { Key = key, Value = null, IsTombstone = true }
                            : new LwwEntry { Key = key, Value = Encoding.UTF8.GetBytes("v:" + key) });
                    }

                    return Task.FromResult(entries);
                });

            leavesById[leafId] = leaf;
        }

        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leavesById[call.ArgAt<GrainId>(0)]);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, calls);
    }

    /// <summary>Builds a shard whose root IS the leaf, i.e. the flat-tree shape.</summary>
    private static (ShardRootGrain Grain, List<List<string>> Calls) CreateFlatShard()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootLeafId = GrainId.Create("leaf", "root-leaf");
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootLeafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

        var calls = new List<List<string>>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(call =>
            {
                var requested = call.Arg<List<string>>();
                calls.Add([.. requested]);

                var entries = new List<LwwEntry?>(requested.Count);
                foreach (var key in requested)
                {
                    entries.Add(new LwwEntry { Key = key, Value = Encoding.UTF8.GetBytes("v:" + key) });
                }

                return Task.FromResult(entries);
            });

        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, calls);
    }

    private static string? Read(LwwEntry? entry) =>
        entry?.Value is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    [Test]
    public async Task Flat_tree_reads_every_key_in_one_call_and_preserves_request_order()
    {
        var (grain, calls) = CreateFlatShard();

        var result = await grain.GetRawEntriesAsync(["c1", "a1", "b1"]);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Has.Count.EqualTo(1),
                "A flat tree has exactly one leaf, so the batch must reach it in a single call.");
            Assert.That(calls[0], Is.EqualTo(new[] { "c1", "a1", "b1" }),
                "The keys must be handed to the leaf in the caller's order, because the response is "
                + "scattered back by position.");
            Assert.That(result.Select(Read), Is.EqualTo(new[] { "v:c1", "v:a1", "v:b1" }));
        });
    }

    [Test]
    public async Task Single_leaf_batch_over_an_internal_root_still_takes_one_call()
    {
        var (grain, calls) = CreateThreeLeafShard();

        var result = await grain.GetRawEntriesAsync(["a3", "a1", "a2"]);

        Assert.Multiple(() =>
        {
            Assert.That(calls["leaf/leaf-a"], Has.Count.EqualTo(1));
            Assert.That(calls["leaf/leaf-b"], Is.Empty);
            Assert.That(calls["leaf/leaf-c"], Is.Empty);
            Assert.That(calls["leaf/leaf-a"][0], Is.EqualTo(new[] { "a3", "a1", "a2" }));
            Assert.That(result.Select(Read), Is.EqualTo(new[] { "v:a3", "v:a1", "v:a2" }));
        });
    }

    [Test]
    public async Task Interleaved_leaves_scatter_back_into_request_order()
    {
        var (grain, calls) = CreateThreeLeafShard();

        // a, b, a, c, b - the second key already forces the bucket map, so the
        // back-filled prefix is exactly one key wide.
        var result = await grain.GetRawEntriesAsync(["a1", "b1", "a2", "c1", "b2"]);

        Assert.Multiple(() =>
        {
            Assert.That(calls["leaf/leaf-a"][0], Is.EqualTo(new[] { "a1", "a2" }),
                "The prefix routed before the map existed must be back-filled ahead of the later key.");
            Assert.That(calls["leaf/leaf-b"][0], Is.EqualTo(new[] { "b1", "b2" }));
            Assert.That(calls["leaf/leaf-c"][0], Is.EqualTo(new[] { "c1" }));
            Assert.That(
                result.Select(Read),
                Is.EqualTo(new[] { "v:a1", "v:b1", "v:a2", "v:c1", "v:b2" }),
                "Every value must land back on the index its key occupied in the request.");
        });
    }

    [Test]
    public async Task A_second_leaf_appearing_late_back_fills_the_whole_routed_prefix()
    {
        var (grain, calls) = CreateThreeLeafShard();

        // Three keys route to one leaf before the fourth reveals a second, so the
        // back-fill is three keys wide. An off-by-one here loses or duplicates a
        // key while the interleaved case above still passes.
        var result = await grain.GetRawEntriesAsync(["a1", "a2", "a3", "b1"]);

        Assert.Multiple(() =>
        {
            Assert.That(calls["leaf/leaf-a"][0], Is.EqualTo(new[] { "a1", "a2", "a3" }));
            Assert.That(calls["leaf/leaf-b"][0], Is.EqualTo(new[] { "b1" }));
            Assert.That(result.Select(Read), Is.EqualTo(new[] { "v:a1", "v:a2", "v:a3", "v:b1" }));
        });
    }

    [Test]
    public async Task Tombstones_surface_as_null_on_both_the_sole_leaf_and_multi_leaf_paths()
    {
        var (soleGrain, _) = CreateThreeLeafShard("a2");
        var sole = await soleGrain.GetRawEntriesAsync(["a1", "a2", "a3"]);

        var (multiGrain, _) = CreateThreeLeafShard("a2", "b1");
        var multi = await multiGrain.GetRawEntriesAsync(["a1", "a2", "b1", "a3"]);

        Assert.Multiple(() =>
        {
            Assert.That(sole.Select(Read), Is.EqualTo(new[] { "v:a1", null, "v:a3" }),
                "The single-leaf path must match single-key GetRawEntryAsync semantics.");
            Assert.That(multi.Select(Read), Is.EqualTo(new[] { "v:a1", null, null, "v:a3" }),
                "And the bucketed path must agree with it.");
        });
    }

    [Test]
    public async Task An_empty_batch_reads_no_leaf_at_all()
    {
        var (grain, calls) = CreateThreeLeafShard();

        var result = await grain.GetRawEntriesAsync([]);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Empty);
            Assert.That(calls.Values.SelectMany(c => c), Is.Empty,
                "An empty batch must not reach a leaf; the lazy bucketing has no sole leaf to call.");
        });
    }
}
