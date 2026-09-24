using System.IO;
using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards the <see cref="ShardRootGrain.MergeManyAsync"/> re-grouping added by
/// issue #2125. A batch is grouped by routing each key once; when routing moves
/// under the batch before a group is dispatched (or the dispatch is retried),
/// every key of that group must be re-routed so each lands on the leaf that
/// declares it, instead of being handed wholesale to the leaf its first key
/// routes to and left to the leaf's span forward or fail-open commit.
/// </summary>
[TestFixture]
public class ShardRootGrainMergeRegroupTests
{
    private const string ShardKey = "merge-regroup-tree/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Left { get; init; }
        public required IBPlusLeafGrain Right { get; init; }
        public required GrainId RootId { get; init; }
        public required List<(string Leaf, string[] Keys)> Calls { get; init; }

        /// <summary>The live separator the internal root routes by.</summary>
        public string Separator { get; set; } = "m";

        /// <summary>
        /// Runs once, inside the first merge call received by the named leaf,
        /// before that call returns or throws.
        /// </summary>
        public Action? OnFirstLeftCall { get; set; }

        /// <summary>When set, the first merge call to the left leaf throws it.</summary>
        public Exception? FailFirstLeftCallWith { get; set; }
    }

    /// <summary>
    /// A two-leaf harness whose internal root routes keys ordinally below
    /// <see cref="Harness.Separator"/> to the left leaf and the rest to the
    /// right leaf. The separator is read on every routing-table fetch, so a
    /// test moves the boundary by changing it and invalidating the cached
    /// snapshot.
    /// </summary>
    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var leftId = GrainId.Create("leaf", "regroup-left");
        var rightId = GrainId.Create("leaf", "regroup-right");
        var rootId = GrainId.Create("internal", "regroup-root");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var left = Substitute.For<IBPlusLeafGrain>();
        var right = Substitute.For<IBPlusLeafGrain>();
        var root = Substitute.For<IBPlusInternalGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusLeafGrain>(leftId).Returns(left);
        factory.GetGrain<IBPlusLeafGrain>(rightId).Returns(right);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var h = new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver, Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers()),
            Left = left,
            Right = right,
            RootId = rootId,
            Calls = [],
        };

        root.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(ci => (string.Compare(ci.Arg<string>(), h.Separator, StringComparison.Ordinal) < 0 ? leftId : rightId, true));
        root.GetRoutingTableAsync()
            .Returns(_ => Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[] { null, h.Separator },
                ChildIds = new[] { leftId, rightId },
                ChildrenAreLeaves = true,
            }));

        var leftCalls = 0;
        left.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(ci =>
            {
                h.Calls.Add(("left", ci.Arg<Dictionary<string, LwwValue<byte[]>>>().Keys.Order(StringComparer.Ordinal).ToArray()));
                if (leftCalls++ == 0)
                {
                    h.OnFirstLeftCall?.Invoke();
                    if (h.FailFirstLeftCallWith is { } ex) throw ex;
                }

                return Task.FromResult<SplitResult?>(null);
            });
        right.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(ci =>
            {
                h.Calls.Add(("right", ci.Arg<Dictionary<string, LwwValue<byte[]>>>().Keys.Order(StringComparer.Ordinal).ToArray()));
                return Task.FromResult<SplitResult?>(null);
            });

        return h;
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

    /// <summary>
    /// Moves the leaf boundary to <paramref name="separator"/> the way a
    /// completed split does: the parent's children change and the shard root
    /// invalidates its cached routing snapshot for that parent.
    /// </summary>
    private static void MoveBoundary(Harness h, string separator)
    {
        h.Separator = separator;
        h.Grain.InvalidateRoutingTable(h.RootId);
    }

    [Test]
    public async Task MergeMany_with_unchanged_routing_takes_the_pivot_path_and_never_regroups()
    {
        var h = CreateHarness();

        await h.Grain.MergeManyAsync(BuildEntries("a", "b", "n", "o"));

        Assert.That(h.Grain.MergeRegroupCount, Is.Zero);
        Assert.That(h.Calls, Has.Count.EqualTo(2));
        Assert.That(h.Calls, Has.One.Matches<(string Leaf, string[] Keys)>(c => c.Leaf == "left" && c.Keys.SequenceEqual(["a", "b"])));
        Assert.That(h.Calls, Has.One.Matches<(string Leaf, string[] Keys)>(c => c.Leaf == "right" && c.Keys.SequenceEqual(["n", "o"])));
    }

    [Test]
    public async Task MergeMany_group_dispatched_after_routing_moved_is_regrouped_onto_the_declaring_leaves()
    {
        var h = CreateHarness();
        // Groups: left {a}, right {n, o}. While the left group is merging, a
        // split moves the boundary to "o", so "n" now belongs to the left leaf.
        h.OnFirstLeftCall = () => MoveBoundary(h, "o");

        await h.Grain.MergeManyAsync(BuildEntries("a", "n", "o"));

        Assert.That(h.Grain.MergeRegroupCount, Is.EqualTo(1));
        Assert.That(h.Calls, Has.Count.EqualTo(3));
        Assert.That(h.Calls[0].Leaf, Is.EqualTo("left"));
        Assert.That(h.Calls[0].Keys, Is.EqualTo(new[] { "a" }));
        Assert.That(h.Calls, Has.One.Matches<(string Leaf, string[] Keys)>(c => c.Leaf == "left" && c.Keys.SequenceEqual(["n"])),
            "\"n\" must be re-routed to the left leaf, which now declares it.");
        Assert.That(h.Calls, Has.One.Matches<(string Leaf, string[] Keys)>(c => c.Leaf == "right" && c.Keys.SequenceEqual(["o"])),
            "the right leaf must receive only the key it still declares.");
    }

    [Test]
    public async Task MergeMany_retried_group_is_regrouped_against_the_current_routing()
    {
        var h = CreateHarness();
        // One group, left {a, b, c}. Its first dispatch fails transiently after
        // a split moved the boundary to "b", so only "a" is still the left
        // leaf's.
        h.OnFirstLeftCall = () => MoveBoundary(h, "b");
        h.FailFirstLeftCallWith = new IOException("transient");

        await h.Grain.MergeManyAsync(BuildEntries("a", "b", "c"));

        Assert.That(h.Grain.MergeRegroupCount, Is.EqualTo(1));
        Assert.That(h.Calls, Has.Count.EqualTo(3));
        Assert.That(h.Calls[0].Leaf, Is.EqualTo("left"));
        Assert.That(h.Calls[0].Keys, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(h.Calls, Has.One.Matches<(string Leaf, string[] Keys)>(c => c.Leaf == "left" && c.Keys.SequenceEqual(["a"])));
        Assert.That(h.Calls, Has.One.Matches<(string Leaf, string[] Keys)>(c => c.Leaf == "right" && c.Keys.SequenceEqual(["b", "c"])));
    }

    [Test]
    public async Task MergeMany_retry_takes_the_regroup_path_even_when_routing_did_not_move()
    {
        var h = CreateHarness();
        h.FailFirstLeftCallWith = new IOException("transient");

        await h.Grain.MergeManyAsync(BuildEntries("a", "b"));

        Assert.That(h.Grain.MergeRegroupCount, Is.EqualTo(1),
            "a retry must re-group unconditionally, not only when the local routing generation moved.");
        Assert.That(h.Calls, Has.Count.EqualTo(2));
        Assert.That(h.Calls.Select(c => c.Leaf), Is.All.EqualTo("left"));
        Assert.That(h.Calls[1].Keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task MergeMany_cross_shard_migration_regroup_preserves_the_migration_flag()
    {
        var h = CreateHarness();
        h.OnFirstLeftCall = () => MoveBoundary(h, "o");

        await h.Grain.MergeManyAsync(BuildEntries("a", "n", "o"), isCrossShardMigration: true);

        Assert.That(h.Grain.MergeRegroupCount, Is.EqualTo(1));
        await h.Left.Received(2).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), true);
        await h.Right.Received(1).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), true);
        await h.Left.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), false);
        await h.Right.DidNotReceive().MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), false);
    }
}
