using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4478: once a resize has completed, a split of the resized copy can
/// move a slot off the shard the replaced copy mirrors to by index. The mirror
/// follows the refusal to the shard that owns the slot now, splits a refused
/// batch into one forward per entry, and fails the mirrored write once the hops
/// run out; a terminal of a saga bound to the replaced copy reaches every shard
/// of the resized copy the split records lead to, and any refusal there fails it.
/// </summary>
public partial class ShardRootGrainShadowForwardTests
{
    private const int MovedTo = 5;

    private static IShardRootGrain DestinationShard(GrainHarness h, int index)
    {
        var shard = Substitute.For<IShardRootGrain>();
        shard.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);
        shard.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.CompletedTask);
        shard.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>()).Returns(Task.CompletedTask);
        shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));
        h.Factory.GetGrain<IShardRootGrain>($"{DestTreeId}/{index}").Returns(shard);
        return shard;
    }

    private static StaleShardRoutingException MovedSlot(int from, int to) => new(from, to, virtualSlot: 7);

    [Test]
    public async Task SetAsync_mirror_refused_for_a_moved_slot_is_resent_to_the_shard_that_owns_it_now()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);
        var first = DestinationShard(h, ShardIndex);
        var owner = DestinationShard(h, MovedTo);
        first.MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey("k")), false).ThrowsAsync(MovedSlot(ShardIndex, MovedTo));

        await h.Grain.SetAsync("k", [1]);

        await owner.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => MirrorsRow(h, d, "k")), false);
    }

    [Test]
    public async Task SetManyAsync_mirror_refused_for_one_moved_slot_is_split_so_each_entry_reaches_its_owner()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);
        var first = DestinationShard(h, ShardIndex);
        var owner = DestinationShard(h, MovedTo);
        first.MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey("moved")), false)
            .ThrowsAsync(MovedSlot(ShardIndex, MovedTo));
        List<KeyValuePair<string, byte[]>> entries = [new("stays", [1]), new("moved", [2])];

        await h.Grain.SetManyAsync(entries);

        await first.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && MirrorsRow(h, d, "stays")), false);
        await owner.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && MirrorsRow(h, d, "moved")), false);
        await owner.DidNotReceive().MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey("stays")), Arg.Any<bool>());
    }

    [Test]
    public void SetAsync_fails_once_the_mirror_is_still_refused_after_the_last_hop()
    {
        // Fails closed: the mirrored write is not acknowledged, so the caller
        // (a saga prepare) retries, aborts or re-binds rather than believing the
        // resized copy holds the write.
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);
        var shards = new IShardRootGrain[ShadowForwardRefusal.MaxHops + 2];
        for (var i = 0; i < shards.Length; i++)
        {
            shards[i] = DestinationShard(h, i);
            shards[i].MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>()).ThrowsAsync(MovedSlot(i, i + 1));
        }

        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.SetAsync("k", [1]));

        shards[ShadowForwardRefusal.MaxHops + 1].DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public void SetAsync_surfaces_a_refusal_that_names_no_owner()
    {
        // A shard an online consolidation retired refuses without naming the
        // shard that absorbed its slots; there is nowhere to follow it.
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);
        DestinationShard(h, ShardIndex).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>()).ThrowsAsync(MovedSlot(ShardIndex, -1));

        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.SetAsync("k", [1]));
    }

    [Test]
    public async Task AppendTxTerminalAsync_of_a_rejecting_copy_reaches_every_shard_the_resized_copys_splits_lead_to()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        var first = DestinationShard(h, ShardIndex);
        var owner = DestinationShard(h, MovedTo);
        first.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int> { MovedTo }));
        var txid = Guid.NewGuid();
        var committedValues = new Dictionary<string, byte[]> { ["k"] = [1] };

        await h.Grain.AppendTxTerminalAsync(txid, committed: true, committedValues);

        await first.Received(1).AppendTxTerminalAsync(
            txid, true, Arg.Is<IReadOnlyDictionary<string, byte[]>?>(v => v != null && v.ContainsKey("k")), Arg.Any<CancellationToken>(), Arg.Any<bool>());
        await owner.Received(1).AppendTxTerminalAsync(
            txid, true, null, Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public void AppendTxTerminalAsync_fails_when_a_shard_the_resized_copys_split_leads_to_refuses_it()
    {
        // The saga's broadcast must retry rather than count the terminal as
        // delivered, or the bucket the split's sweep replayed is stranded.
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        var first = DestinationShard(h, ShardIndex);
        var owner = DestinationShard(h, MovedTo);
        first.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int> { MovedTo }));
        owner.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .ThrowsAsync(MovedSlot(MovedTo, ShardIndex));

        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: false));
    }

    [Test]
    public async Task AppendTxTerminalAsync_before_the_swap_reaches_only_the_shard_with_the_same_index()
    {
        // No migration of the resized copy can run while the resize is in flight,
        // so the closure is not walked.
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);
        var first = DestinationShard(h, ShardIndex);
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await first.Received(1).AppendTxTerminalAsync(
            txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>());
        await first.DidNotReceive().GetSplitForwardTargetsAsync();
    }
}
