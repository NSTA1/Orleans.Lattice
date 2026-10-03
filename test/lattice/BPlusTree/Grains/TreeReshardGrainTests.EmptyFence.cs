using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The empty-tree reshard fast path must fence every slot it moves on that
/// slot's previous owner before it publishes the new map, so a router still
/// holding the old map is refused rather than writing to a shard the new map
/// no longer reads (#4066).
/// </summary>
public partial class TreeReshardGrainTests
{
    private const int FenceVirtualShardCount = 16;

    private static (Dictionary<int, IShardRootGrain> Shards, List<string> Order) WireEmptyShards(
        IGrainFactory grainFactory, ILatticeRegistry registry, int shardSlots)
    {
        var order = new List<string>();
        var shards = new Dictionary<int, IShardRootGrain>();
        for (var i = 0; i < shardSlots; i++)
        {
            var index = i;
            var shard = Substitute.For<IShardRootGrain>();
            shard.AnyBoundedAsync(Arg.Any<string?>()).Returns(Task.FromResult(new ShardAnyPage { Found = false }));
            shard.FenceMovedSlotsAsync(Arg.Any<int[]>(), Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(_ => { order.Add($"fence:{index}"); return Task.CompletedTask; });
            shard.ReviveAsync(Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(_ => { order.Add($"revive:{index}"); return Task.CompletedTask; });
            shard.ReclaimSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(_ => { order.Add($"reclaim:{index}"); return Task.FromResult(0); });
            shards[i] = shard;
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shard);
        }

        registry.SetShardMapAsync(TreeId, Arg.Any<ShardMap>())
            .Returns(_ => { order.Add("map"); return Task.CompletedTask; });
        return (shards, order);
    }

    /// <summary>
    /// The slots each old owner gives up, with the owner each one moves to,
    /// computed independently of the production diff.
    /// </summary>
    private static Dictionary<int, (int[] Slots, int[] Owners)> ExpectedFences(int from, int to)
    {
        var before = ShardMap.CreateDefault(FenceVirtualShardCount, from).Slots;
        var after = ShardMap.CreateDefault(FenceVirtualShardCount, to).Slots;
        var result = new Dictionary<int, (List<int> Slots, List<int> Owners)>();
        for (var s = 0; s < FenceVirtualShardCount; s++)
        {
            if (before[s] == after[s]) continue;
            if (!result.TryGetValue(before[s], out var entry))
            {
                entry = ([], []);
                result[before[s]] = entry;
            }

            entry.Slots.Add(s);
            entry.Owners.Add(after[s]);
        }

        return result.ToDictionary(kv => kv.Key, kv => (kv.Value.Slots.ToArray(), kv.Value.Owners.ToArray()));
    }

    private static int[]? FenceSlotsReceived(IShardRootGrain shard, out int[]? owners, out int vsc)
    {
        owners = null;
        vsc = -1;
        var call = shard.ReceivedCalls()
            .SingleOrDefault(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.FenceMovedSlotsAsync));
        if (call is null) return null;
        var args = call.GetArguments();
        owners = (int[])args[1]!;
        vsc = (int)args[2]!;
        return (int[])args[0]!;
    }

    [TestCase(2, 4)]
    [TestCase(4, 2)]
    [TestCase(2, 3)]
    public async Task An_empty_tree_reshard_fences_every_moved_slot_on_its_old_owner_before_publishing_the_map(
        int from, int to)
    {
        var (grain, _, grainFactory, registry) = CreateGrain(
            virtualShardCount: FenceVirtualShardCount, physicalShardCount: from);
        var (shards, order) = WireEmptyShards(grainFactory, registry, Math.Max(from, to));
        var expected = ExpectedFences(from, to);
        Assert.That(expected, Is.Not.Empty, "the test case must move at least one slot");

        await grain.ReshardAsync(to);

        foreach (var (owner, shard) in shards)
        {
            var slots = FenceSlotsReceived(shard, out var owners, out var vsc);
            if (expected.TryGetValue(owner, out var fence))
            {
                Assert.That(slots, Is.EqualTo(fence.Slots), $"shard {owner} must be fenced on exactly the slots it gives up");
                Assert.That(owners, Is.EqualTo(fence.Owners), $"shard {owner} must record each moved slot's new owner");
                Assert.That(vsc, Is.EqualTo(FenceVirtualShardCount));
                Assert.That(order.IndexOf($"fence:{owner}"), Is.LessThan(order.IndexOf("map")),
                    $"shard {owner} must be fenced before the new map is published");
            }
            else
            {
                Assert.That(slots, Is.Null, $"shard {owner} gives up no slot and must not be fenced");
            }
        }
    }

    [Test]
    public async Task An_empty_tree_reshard_reclaims_each_new_owners_slots_after_reviving_it_and_before_the_map()
    {
        // A stale fence from an earlier grow-then-shrink on a shard the new map
        // routes to would otherwise refuse the slots it is now meant to own.
        var (grain, _, grainFactory, registry) = CreateGrain(
            virtualShardCount: FenceVirtualShardCount, physicalShardCount: 2);
        var (shards, order) = WireEmptyShards(grainFactory, registry, 4);
        var target = ShardMap.CreateDefault(FenceVirtualShardCount, 4);

        await grain.ReshardAsync(4);

        for (var owner = 0; owner < 4; owner++)
        {
            var owned = Enumerable.Range(0, FenceVirtualShardCount).Where(s => target.Slots[s] == owner).ToArray();
            await shards[owner].Received(1).ReclaimSlotsAsync(
                Arg.Is<int[]>(s => s.SequenceEqual(owned)), FenceVirtualShardCount);
            Assert.That(order.IndexOf($"revive:{owner}"), Is.LessThan(order.IndexOf($"reclaim:{owner}")));
            Assert.That(order.IndexOf($"reclaim:{owner}"), Is.LessThan(order.IndexOf("map")));
        }
    }

    [Test]
    public async Task An_empty_tree_reshard_fences_old_owners_before_reviving_new_ones()
    {
        // Fencing first closes the window in which both the old and the new
        // owner accept a write for the same slot.
        var (grain, _, grainFactory, registry) = CreateGrain(
            virtualShardCount: FenceVirtualShardCount, physicalShardCount: 2);
        var (_, order) = WireEmptyShards(grainFactory, registry, 4);

        await grain.ReshardAsync(4);

        var lastFence = order.FindLastIndex(e => e.StartsWith("fence:", StringComparison.Ordinal));
        var firstRevive = order.FindIndex(e => e.StartsWith("revive:", StringComparison.Ordinal));
        Assert.That(lastFence, Is.GreaterThanOrEqualTo(0));
        Assert.That(lastFence, Is.LessThan(firstRevive));
    }

    [Test]
    public void An_empty_tree_reshard_does_not_publish_the_map_when_fencing_fails()
    {
        var (grain, _, grainFactory, registry) = CreateGrain(
            virtualShardCount: FenceVirtualShardCount, physicalShardCount: 2);
        var (shards, order) = WireEmptyShards(grainFactory, registry, 4);
        shards[1].FenceMovedSlotsAsync(Arg.Any<int[]>(), Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(Task.FromException(new InvalidOperationException("fence failed")));

        Assert.That(async () => await grain.ReshardAsync(4), Throws.InvalidOperationException);
        Assert.That(order, Does.Not.Contain("map"));
    }
}
