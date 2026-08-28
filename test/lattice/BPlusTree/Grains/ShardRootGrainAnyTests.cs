using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="IShardRootGrain.AnyAsync"/>, the short-circuiting
/// existence probe used by reshard initiation in place of a full
/// <see cref="IShardRootGrain.CountAsync()"/> chain walk.
/// <para>
/// The probe exists because emptiness is a far cheaper question than a count,
/// and - unlike a count - needs no reconciliation against a moving shard map: a
/// split only ever moves keys between shards, so a key that exists is observed
/// by at least one shard wherever the split has got to. The tests below pin the
/// two properties that makes it safe to rely on: it stops at the first
/// non-empty leaf, and it reports non-empty for keys whose slots have been
/// moved away rather than filtering them out (over-reporting is the harmless
/// direction; under-reporting would let a reshard repin a non-empty tree).
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainAnyTests
{
    private const string TreeId = "any-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IReadOnlyList<IBPlusLeafGrain> Leaves { get; init; }
    }

    // Builds a shard whose leaf chain has one leaf per entry in leafCounts.
    private static Harness CreateHarness(
        int[] leafCounts,
        Dictionary<int, int>? movedAwaySlots = null,
        int? movedAwayVirtualShardCount = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();

        if (movedAwaySlots is not null)
            foreach (var (k, v) in movedAwaySlots) state.State.MovedAwaySlots[k] = v;
        if (movedAwayVirtualShardCount is not null)
            state.State.MovedAwayVirtualShardCount = movedAwayVirtualShardCount;

        var leaves = new List<IBPlusLeafGrain>(leafCounts.Length);
        for (int i = 0; i < leafCounts.Length; i++)
        {
            var leafId = GrainId.Create("leaf", $"any-leaf-{i}");
            if (i == 0)
            {
                state.State.RootNodeId = leafId;
                state.State.RootIsLeaf = true;
            }

            var count = leafCounts[i];
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.CountAsync().Returns(Task.FromResult(count));
            leaf.CountAsync(Arg.Any<string?>(), Arg.Any<string?>()).Returns(Task.FromResult(count));
            factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);
            leaves.Add(leaf);
        }

        // Chain the leaves together; the last has no next sibling.
        for (int i = 0; i < leaves.Count; i++)
        {
            var next = i + 1 < leaves.Count
                ? GrainId.Create("leaf", $"any-leaf-{i + 1}")
                : (GrainId?)null;
            leaves[i].GetNextSiblingAsync().Returns(Task.FromResult(next));
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 1, factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers()),
            Leaves = leaves,
        };
    }

    [Test]
    public async Task AnyAsync_returns_false_when_every_leaf_in_the_chain_is_empty()
    {
        var harness = CreateHarness([0, 0, 0]);

        Assert.That(await harness.Grain.AnyAsync(), Is.False);
    }

    [Test]
    public async Task AnyAsync_returns_true_on_the_first_leaf_without_walking_the_rest()
    {
        // The whole point of the probe: a non-empty shard must cost one leaf
        // call, not a full chain walk as CountAsync pays.
        var harness = CreateHarness([3, 5, 7]);

        Assert.That(await harness.Grain.AnyAsync(), Is.True);

        await harness.Leaves[0].Received(1).CountAsync(Arg.Any<string?>(), Arg.Any<string?>());
        await harness.Leaves[0].DidNotReceive().GetNextSiblingAsync();
        await harness.Leaves[1].DidNotReceive().CountAsync(Arg.Any<string?>(), Arg.Any<string?>());
        await harness.Leaves[2].DidNotReceive().CountAsync(Arg.Any<string?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task AnyAsync_walks_past_leading_empty_leaves_to_find_keys()
    {
        // A chain whose head leaves have been emptied by deletes must still
        // report the keys that remain further along it.
        var harness = CreateHarness([0, 0, 2]);

        Assert.That(await harness.Grain.AnyAsync(), Is.True);

        await harness.Leaves[2].Received(1).CountAsync(Arg.Any<string?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task AnyAsync_reports_non_empty_for_keys_whose_slots_moved_away()
    {
        // Deliberate one-sidedness. CountAsync filters moved-away slots so a
        // migrating key is not counted twice (it is briefly present on both the
        // source and the destination). For an existence question that key still
        // means "the tree holds keys", so the probe does not filter: it may
        // over-report while a migration drains, which only declines the fast
        // path, but it can never report empty while a key exists.
        var harness = CreateHarness(
            [4],
            movedAwaySlots: new Dictionary<int, int> { [0] = 1, [1] = 1 },
            movedAwayVirtualShardCount: 16);

        Assert.That(await harness.Grain.AnyAsync(), Is.True,
            "an existence probe must not filter moved-away slots - under-reporting would let a reshard repin a non-empty tree");
    }
}
