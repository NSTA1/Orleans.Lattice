using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Routing convergence on the conditional batch path across a quiesce
/// boundary (issue #3358). The chaos predicate fixture's conditional batches
/// kept missing the same guard-matching keys on 31 consecutive passes over a
/// quiescent topology (issue #2663). A quiesce can only heal a miss if the
/// routing the shard root serves converges on the durable tree once splits
/// stop, so these pin that it does, for the two ways it can fail to: a
/// division linked where no descent finds it, and a routing table fetched
/// across a link and cached past it. Either one routes every later pass to the
/// same wrong leaf for as long as nothing splits again, which after a quiesce
/// is forever.
/// </summary>
public sealed partial class ShardRootGrainSplitLinkTests
{
    // The number of quiesce-and-retry passes the chaos fixture ran against the
    // misses in issue #2663 before giving up.
    private const int QuiescedPasses = 31;

    private static readonly LatticePredicateNode AnyGuard = LatticePredicateNode.Member("Score");

    private static List<KeyValuePair<string, byte[]>> ConditionalBatch(string key) => [new(key, [1])];

    /// <summary>
    /// Makes <paramref name="leaf"/> report every entry it is handed as written,
    /// returning <paramref name="splits"/> on successive calls and then none.
    /// </summary>
    private static void StubConditionalWrites(IBPlusLeafGrain leaf, params SplitResult?[] splits)
    {
        var call = 0;
        leaf.SetManyWherePredicateAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<LatticePredicateNode>())
            .Returns(info =>
            {
                var slice = info.ArgAt<List<KeyValuePair<string, byte[]>>>(0);
                var split = call < splits.Length ? splits[call] : null;
                call++;
                return Task.FromResult(new ConditionalSetManyResult
                {
                    WrittenKeys = slice.Select(e => e.Key).ToList(),
                    Split = split,
                });
            });
    }

    private static Task<ConditionalSetManyResult> ReceivedConditionalWrite(IBPlusLeafGrain leaf, int count, string key) =>
        leaf.Received(count).SetManyWherePredicateAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(b => b.Count == 1 && b[0].Key == key),
            Arg.Any<LatticePredicateNode>());

    [Test]
    public async Task A_conditional_batch_links_a_forwarded_division_under_the_parent_a_fresh_descent_finds()
    {
        var h = CreateTwoLevelHarness();
        h.AddLeaf(SiblingId);
        foreach (var leaf in h.Leaves.Values)
        {
            StubConditionalWrites(leaf);
        }

        // The batch lands on the left leaf, which forwards part of it along the
        // chain to the leaf declaring it under the right parent; that leaf
        // divides at "t" (completing an interrupted split, say) and its
        // division rides back on the result. Linked against the ancestor path
        // the batch captured for "c", the separator would land in the left
        // parent, whose range excludes "t", and route nothing to the new leaf.
        StubConditionalWrites(h.Leaf(LeftLeafId), SplitResult.Forward(LeafSplit("t")));

        var written = await h.Grain.SetManyWherePredicateAsync(ConditionalBatch("c"), AnyGuard);

        Assert.That(written, Is.EqualTo(new[] { "c" }));
        await h.Node(RightParentId).Received(1).AcceptSplitAsync("t", SiblingId);
        await h.Node(LeftParentId).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task Every_quiesced_conditional_pass_routes_to_the_leaf_a_forwarded_division_created()
    {
        var h = CreateTwoLevelHarness();
        h.AddLeaf(SiblingId);
        foreach (var leaf in h.Leaves.Values)
        {
            StubConditionalWrites(leaf);
        }

        StubConditionalWrites(h.Leaf(LeftLeafId), SplitResult.Forward(LeafSplit("t")));
        h.Node(RightParentId).AcceptSplitAsync("t", SiblingId).Returns(_ =>
        {
            h.Routing[RightParentId] = Table(true, (null, RightLeafId), ("t", SiblingId));
            return Task.FromResult<SplitResult?>(null);
        });

        await h.Grain.SetManyWherePredicateAsync(ConditionalBatch("c"), AnyGuard);

        // Quiesced: nothing divides from here on, so nothing will invalidate a
        // routing table again, and whatever the shard root routes "u" by now is
        // what it routes by for good.
        for (var pass = 0; pass < QuiescedPasses; pass++)
        {
            await h.Grain.SetManyWherePredicateAsync(ConditionalBatch("u"), AnyGuard);
        }

        await ReceivedConditionalWrite(h.Leaf(SiblingId), QuiescedPasses, "u");
        await h.Leaf(RightLeafId).DidNotReceive().SetManyWherePredicateAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<LatticePredicateNode>());
    }

    [Test]
    public async Task A_conditional_pass_whose_routing_fetch_overlapped_a_split_link_does_not_pin_the_pre_split_table()
    {
        var h = CreateOneLevelHarness();
        h.AddLeaf(SiblingId);
        foreach (var leaf in h.Leaves.Values)
        {
            StubConditionalWrites(leaf);
        }

        h.Leaf(RightLeafId).SetAsync("x", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("t")));
        h.Node(RootId).AcceptSplitAsync("t", SiblingId).Returns(_ =>
        {
            h.Routing[RootId] = Table(true, (null, LeftLeafId), ("m", RightLeafId), ("t", SiblingId));
            return Task.FromResult<SplitResult?>(null);
        });

        // The root serves the batch's routing fetch before it accepts the
        // split, but the reply reaches the shard root only after the split's
        // link has invalidated the cache: Orleans does not order the two
        // replies. Every other fetch is answered at once with the table the
        // root holds when it is asked.
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fetches = 0;
        h.Node(RootId).GetRoutingTableAsync().Returns(_ =>
        {
            var served = h.Routing[RootId];
            return Interlocked.Increment(ref fetches) == 1
                ? reply.Task.ContinueWith(_ => served, TaskScheduler.Default)
                : Task.FromResult(served);
        });

        var overlapped = h.Grain.SetManyWherePredicateAsync(ConditionalBatch("u"), AnyGuard);
        Assert.That(overlapped.IsCompleted, Is.False, "precondition: the batch must be parked on its routing fetch");

        await h.Grain.SetAsync("x", [1]);
        await h.Node(RootId).Received(1).AcceptSplitAsync("t", SiblingId);

        reply.SetResult();
        await overlapped;

        for (var pass = 0; pass < QuiescedPasses; pass++)
        {
            await h.Grain.SetManyWherePredicateAsync(ConditionalBatch("u"), AnyGuard);
        }

        // The overlapped batch routed by the table it was handed, so its one
        // dispatch to the donor is the in-flight window. Every pass after it
        // must see the separator the link installed.
        await ReceivedConditionalWrite(h.Leaf(RightLeafId), 1, "u");
        await ReceivedConditionalWrite(h.Leaf(SiblingId), QuiescedPasses, "u");
    }
}
