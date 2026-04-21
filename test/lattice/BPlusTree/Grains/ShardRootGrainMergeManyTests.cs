using System.IO;
using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="ShardRootGrain.MergeManyAsync"/> covering the
/// leaf-grouped routing optimisation: entries are grouped by target leaf
/// and each leaf receives exactly one batched merge call.
/// </summary>
[TestFixture]
public class ShardRootGrainMergeManyTests
{
    private const string TreeId = "merge-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required Dictionary<GrainId, IBPlusLeafGrain> Leaves { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public IBPlusInternalGrain? Root { get; init; }
    }

    /// <summary>
    /// Creates a two-leaf harness with one internal root routing keys
    /// lexicographically &lt; <paramref name="separator"/> to the left leaf
    /// and ≥ the separator to the right leaf.
    /// </summary>
    private static Harness CreateTwoLeafHarness(string separator = "m")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var leftId = GrainId.Create("leaf", "left");
        var rightId = GrainId.Create("leaf", "right");
        var rootId = GrainId.Create("internal", "root");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var leftLeaf = Substitute.For<IBPlusLeafGrain>();
        leftLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        var rightLeaf = Substitute.For<IBPlusLeafGrain>();
        rightLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));

        var root = Substitute.For<IBPlusInternalGrain>();
        root.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(ci =>
            {
                var k = ci.Arg<string>();
                var childId = string.Compare(k, separator, StringComparison.Ordinal) < 0 ? leftId : rightId;
                return (childId, true);
            });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusLeafGrain>(leftId).Returns(leftLeaf);
        factory.GetGrain<IBPlusLeafGrain>(rightId).Returns(rightLeaf);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver),
            Leaves = new Dictionary<GrainId, IBPlusLeafGrain>
            {
                [leftId] = leftLeaf,
                [rightId] = rightLeaf,
            },
            State = state,
            Root = root,
        };
    }

    /// <summary>
    /// Creates a single-leaf (root-is-leaf) harness.
    /// </summary>
    private static Harness CreateRootLeafHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var leafId = GrainId.Create("leaf", "only");
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver),
            Leaves = new Dictionary<GrainId, IBPlusLeafGrain> { [leafId] = leaf },
            State = state,
        };
    }

    /// <summary>
    /// Creates a three-leaf harness with one internal root routing by two
    /// separators: keys &lt; <paramref name="sep1"/> → left, keys &lt; <paramref name="sep2"/> → middle, else → right.
    /// </summary>
    private static (Harness H, IBPlusLeafGrain Left, IBPlusLeafGrain Middle, IBPlusLeafGrain Right) CreateThreeLeafHarness(
        string sep1 = "h", string sep2 = "p")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var leftId = GrainId.Create("leaf", "left");
        var midId = GrainId.Create("leaf", "mid");
        var rightId = GrainId.Create("leaf", "right");
        var rootId = GrainId.Create("internal", "root");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var leftLeaf = Substitute.For<IBPlusLeafGrain>();
        var midLeaf = Substitute.For<IBPlusLeafGrain>();
        var rightLeaf = Substitute.For<IBPlusLeafGrain>();
        foreach (var l in new[] { leftLeaf, midLeaf, rightLeaf })
            l.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
                .Returns(Task.FromResult<SplitResult?>(null));

        var root = Substitute.For<IBPlusInternalGrain>();
        root.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(ci =>
            {
                var k = ci.Arg<string>();
                GrainId childId = string.Compare(k, sep1, StringComparison.Ordinal) < 0 ? leftId
                    : string.Compare(k, sep2, StringComparison.Ordinal) < 0 ? midId
                    : rightId;
                return (childId, true);
            });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusLeafGrain>(leftId).Returns(leftLeaf);
        factory.GetGrain<IBPlusLeafGrain>(midId).Returns(midLeaf);
        factory.GetGrain<IBPlusLeafGrain>(rightId).Returns(rightLeaf);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var h = new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver),
            Leaves = new Dictionary<GrainId, IBPlusLeafGrain>
            {
                [leftId] = leftLeaf,
                [midId] = midLeaf,
                [rightId] = rightLeaf,
            },
            State = state,
        };
        return (h, leftLeaf, midLeaf, rightLeaf);
    }

    private static Dictionary<string, LwwValue<byte[]>> BuildEntries(params string[] keys)
    {
        var result = new Dictionary<string, LwwValue<byte[]>>(keys.Length);
        var clock = HybridLogicalClock.Zero;
        foreach (var k in keys)
        {
            clock = HybridLogicalClock.Tick(clock);
            result[k] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes(k), clock);
        }
        return result;
    }

    [Test]
    public async Task MergeMany_returns_without_grain_calls_when_entries_empty()
    {
        var h = CreateTwoLeafHarness();

        await h.Grain.MergeManyAsync([]);

        foreach (var leaf in h.Leaves.Values)
        {
            await leaf.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        }
    }

    [Test]
    public async Task MergeMany_routes_all_entries_to_root_leaf_in_one_call_when_root_is_leaf()
    {
        var h = CreateRootLeafHarness();
        var leaf = h.Leaves.Values.Single();
        var entries = BuildEntries("a", "m", "z");

        await h.Grain.MergeManyAsync(entries);

        await leaf.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(
            d => d.Count == 3 && d.ContainsKey("a") && d.ContainsKey("m") && d.ContainsKey("z")));
    }

    [Test]
    public async Task MergeMany_groups_entries_by_target_leaf_and_calls_each_leaf_once()
    {
        var h = CreateTwoLeafHarness(separator: "m");
        var leftLeaf = h.Leaves.First().Value;
        var rightLeaf = h.Leaves.Last().Value;
        var entries = BuildEntries("a", "b", "c", "n", "o", "p");

        await h.Grain.MergeManyAsync(entries);

        await leftLeaf.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(
            d => d.Count == 3 && d.ContainsKey("a") && d.ContainsKey("b") && d.ContainsKey("c")));
        await rightLeaf.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(
            d => d.Count == 3 && d.ContainsKey("n") && d.ContainsKey("o") && d.ContainsKey("p")));
    }

    [Test]
    public async Task MergeMany_calls_only_target_leaf_when_all_entries_land_in_same_leaf()
    {
        var h = CreateTwoLeafHarness(separator: "m");
        var leftLeaf = h.Leaves.First().Value;
        var rightLeaf = h.Leaves.Last().Value;
        var entries = BuildEntries("a", "b", "c");

        await h.Grain.MergeManyAsync(entries);

        await leftLeaf.Received(1).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        await rightLeaf.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
    }

    [Test]
    public async Task MergeMany_retries_group_on_transient_exception()
    {
        var h = CreateTwoLeafHarness();
        var leftLeaf = h.Leaves.First().Value;

        // First call throws a transient IOException; second succeeds.
        leftLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(
                _ => throw new IOException("transient"),
                _ => Task.FromResult<SplitResult?>(null));

        var entries = BuildEntries("a", "b");

        await h.Grain.MergeManyAsync(entries);

        await leftLeaf.Received(2).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
    }

    [Test]
    public async Task MergeMany_propagates_non_transient_exception_without_retry()
    {
        var h = CreateTwoLeafHarness();
        var leftLeaf = h.Leaves.First().Value;

        leftLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw new InvalidOperationException("permanent"));

        var entries = BuildEntries("a", "b");

        Assert.That(async () => await h.Grain.MergeManyAsync(entries),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("permanent"));
        await leftLeaf.Received(1).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
    }

    [Test]
    public async Task MergeMany_groups_across_three_leaves_and_calls_each_once()
    {
        var (h, left, mid, right) = CreateThreeLeafHarness(sep1: "h", sep2: "p");
        var entries = BuildEntries("a", "b", "c", "i", "j", "k", "q", "r", "s");

        await h.Grain.MergeManyAsync(entries);

        await left.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(
            d => d.Count == 3 && d.ContainsKey("a") && d.ContainsKey("b") && d.ContainsKey("c")));
        await mid.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(
            d => d.Count == 3 && d.ContainsKey("i") && d.ContainsKey("j") && d.ContainsKey("k")));
        await right.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(
            d => d.Count == 3 && d.ContainsKey("q") && d.ContainsKey("r") && d.ContainsKey("s")));
    }

    [Test]
    public async Task MergeMany_propagates_leaf_split_result_up_the_path()
    {
        var h = CreateTwoLeafHarness();
        var leftLeaf = h.Leaves.First().Value;

        // Left leaf reports a split on its merge; the shard root must
        // propagate (PromotedKey, NewSiblingId) up to the parent via
        // AcceptSplitAsync.
        var promotedKey = "b";
        var newSiblingId = GrainId.Create("leaf", "left-sibling");
        var splitResult = new SplitResult { PromotedKey = promotedKey, NewSiblingId = newSiblingId };
        leftLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(splitResult));

        h.Root!.AcceptSplitAsync(promotedKey, newSiblingId)
            .Returns(Task.FromResult<SplitResult?>(null));

        var entries = BuildEntries("a", "b");

        await h.Grain.MergeManyAsync(entries);

        await h.Root.Received(1).AcceptSplitAsync(promotedKey, newSiblingId);
    }

    [Test]
    public async Task MergeMany_retries_transient_exception_during_grouping_traversal()
    {
        var leftId = GrainId.Create("leaf", "left");
        var rightId = GrainId.Create("leaf", "right");

        // Build a bespoke harness where the first RouteWithMetadataAsync call
        // throws a transient IOException before settling into normal routing,
        // so we can verify the grouping phase absorbs it instead of failing
        // the whole batch.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var rootId = GrainId.Create("internal", "root");
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var leftLeaf = Substitute.For<IBPlusLeafGrain>();
        leftLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        var rightLeaf = Substitute.For<IBPlusLeafGrain>();
        rightLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));

        var callCount = 0;
        var root = Substitute.For<IBPlusInternalGrain>();
        root.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(ci =>
            {
                callCount++;
                if (callCount == 1) throw new IOException("transient");
                var k = ci.Arg<string>();
                var childId = string.Compare(k, "m", StringComparison.Ordinal) < 0 ? leftId : rightId;
                return (childId, true);
            });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusLeafGrain>(leftId).Returns(leftLeaf);
        factory.GetGrain<IBPlusLeafGrain>(rightId).Returns(rightLeaf);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var grain = new ShardRootGrain(context, state, factory, optionsResolver);
        var entries = BuildEntries("a", "b");

        await grain.MergeManyAsync(entries);

        // Retry absorbed the transient failure; grouping completed and the
        // leaf still received its batched merge.
        await leftLeaf.Received(1).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
        Assert.That(callCount, Is.GreaterThanOrEqualTo(2),
            "The transient-throwing call should have been retried.");
    }

    [Test]
    public async Task MergeMany_gives_up_after_retry_budget_on_persistent_transient_failure()
    {
        var h = CreateTwoLeafHarness();
        var leftLeaf = h.Leaves.First().Value;

        // Always throws transient — retry budget is MaxRetries=2 so the leaf
        // is called 3 times (attempt 0 + 2 retries) before giving up.
        leftLeaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw new IOException("persistent-transient"));

        var entries = BuildEntries("a", "b");

        Assert.That(async () => await h.Grain.MergeManyAsync(entries),
            Throws.InstanceOf<IOException>());
        await leftLeaf.Received(3).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
    }
}
