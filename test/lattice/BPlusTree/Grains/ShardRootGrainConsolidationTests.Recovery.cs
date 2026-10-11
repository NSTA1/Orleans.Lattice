using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainConsolidationTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task ReclaimSlots_after_leaf_timeout_and_shard_reactivation_restores_retained_key(bool otherSlotsRemain)
    {
        const string key = "seed-00408";
        byte[] value = [4, 8];
        var slot = ShardMap.GetVirtualSlot(key, VirtualShardCount);
        var unrelatedSlot = (slot + 1) % VirtualShardCount;
        var unrelatedKey = Enumerable.Range(0, 100_000)
            .Select(i => $"unowned-{i}")
            .First(k => ShardMap.GetVirtualSlot(k, VirtualShardCount) == unrelatedSlot);
        var (leaf, leafState) = CreateRetainedLeaf();
        await leaf.SetAsync(key, value);
        await leaf.SetAsync(unrelatedKey, [9]);
        var sealedSlots = otherSlotsRemain
            ? new[] { slot, unrelatedSlot }.Order().ToArray()
            : [slot];
        await leaf.MarkSlotsMovedAwayAsync(sealedSlots, VirtualShardCount);

        var h = CreateHarness();
        SealSlots(h, sealedSlots);
        ShardRootState persisted = new()
        {
            RootNodeId = h.State.State.RootNodeId,
            RootIsLeaf = true,
            MovedAwaySlots = new(h.State.State.MovedAwaySlots),
            MovedAwayVirtualShardCount = VirtualShardCount,
        };
        h.State.OnWriteState = s => persisted = new ShardRootState
        {
            RootNodeId = s.RootNodeId,
            RootIsLeaf = s.RootIsLeaf,
            MovedAwaySlots = new(s.MovedAwaySlots),
            MovedAwayVirtualShardCount = s.MovedAwayVirtualShardCount,
        };
        h.Leaf.UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount)
            .Returns(Task.FromException(new TimeoutException("leaf unseal did not complete")));

        Assert.That(leaf.EntriesForTest.Keys, Does.Contain(key), "The seeded population must actually exist.");
        Assert.That(await leaf.GetAsync(key), Is.Null, "The leaf seal, not missing data, hides the row.");
        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.ReclaimSlotsAsync([slot], VirtualShardCount));
        var writesBeforeRetry = leafState.WriteCount;

        // Consolidation SwapAsync retries ReclaimSlotsAsync after a failed leaf
        // RPC; ForceDeactivateAsync can make that retry use the persisted shard.
        var reactivated = CreateHarness(persistentState: new FakePersistentState<ShardRootState>
        {
            State = new ShardRootState
            {
                RootNodeId = persisted.RootNodeId,
                RootIsLeaf = persisted.RootIsLeaf,
                MovedAwaySlots = new(persisted.MovedAwaySlots),
                MovedAwayVirtualShardCount = persisted.MovedAwayVirtualShardCount,
            },
        });
        reactivated.Leaf.UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount)
            .Returns(c => leaf.UnmarkSlotsMovedAwayAsync(c.Arg<int[]>(), VirtualShardCount));
        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(key).Returns(_ => leaf.GetAsync(key));
        reactivated.Factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        await reactivated.Grain.ReclaimSlotsAsync([slot], VirtualShardCount);

        Assert.That(await reactivated.Grain.GetAsync(key), Is.EqualTo(value).AsCollection,
            "A successful consolidation retry must not acknowledge reclaim while its leaf still returns null.");
        Assert.That(leaf.EntriesForTest.Keys, Does.Contain(key));
        Assert.That(persisted.MovedAwaySlots.Keys, Does.Contain(slot),
            "Failure must leave a persisted retry obligation for a fresh activation.");
        Assert.That(leafState.WriteCount, Is.EqualTo(writesBeforeRetry + 1));
        Assert.That(reactivated.Grain.IsSlotMovedAway(key), Is.False);
        Assert.That(leafState.State.MovedAwaySlots ?? [], Does.Not.Contain(slot));
        if (otherSlotsRemain)
        {
            Assert.That(reactivated.State.State.MovedAwaySlots.Keys, Does.Contain(unrelatedSlot));
            Assert.That(leafState.State.MovedAwaySlots, Does.Contain(unrelatedSlot),
                "Retry cleanup must not unseal an unrelated, unowned slot.");
            Assert.That(await leaf.GetAsync(unrelatedKey), Is.Null);
            Assert.ThrowsAsync<StaleShardRoutingException>(() => reactivated.Grain.GetAsync(unrelatedKey));
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ReclaimSlots_without_a_matching_reclaim_obligation_does_not_expose_retained_unowned_key(bool activeSplit)
    {
        const string key = "seed-00408";
        var slot = ShardMap.GetVirtualSlot(key, VirtualShardCount);
        var (leaf, leafState) = CreateRetainedLeaf();
        await leaf.SetAsync(key, [4, 8]);
        await leaf.MarkSlotsMovedAwayAsync([slot], VirtualShardCount);
        var h = CreateHarness();
        SealSlots(h, (slot + 1) % VirtualShardCount);
        h.Leaf.UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount)
            .Returns(c => leaf.UnmarkSlotsMovedAwayAsync(c.Arg<int[]>(), VirtualShardCount));
        if (activeSplit)
        {
            h.State.State.SplitInProgress = new ShardSplitInProgress
            {
                Phase = ShardSplitPhase.Reject,
                ShadowTargetShardIndex = 7,
                MovedSlots = [slot],
                VirtualShardCount = VirtualShardCount,
            };
            Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.ReclaimSlotsAsync([slot], VirtualShardCount));
        }
        else
        {
            Assert.That(await h.Grain.ReclaimSlotsAsync([slot], VirtualShardCount), Is.Zero);
        }

        await h.Leaf.DidNotReceive().UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount);
        Assert.That(leaf.EntriesForTest.Keys, Does.Contain(key));
        Assert.That(leafState.State.MovedAwaySlots, Does.Contain(slot));
        Assert.That(await leaf.GetAsync(key), Is.Null,
            "Retry safety must not lift an unowned leaf seal without a matching reclaim obligation.");
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateRetainedLeaf()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "leaf-0"));
        var factory = Substitute.For<IGrainFactory>();
        var grain = new BPlusLeafGrain(
            context, state, factory, TestOptionsResolver.Create(factory: factory),
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state);
    }

    [Test]
    public async Task ReclaimSlots_after_shard_persist_failure_retains_fence_and_retries_leaf_walk()
    {
        var h = CreateHarness();
        SealSlots(h, 1, 3);
        h.State.ThrowOnWrite = new IOException("shard state write failed");

        Assert.ThrowsAsync<IOException>(() => h.Grain.ReclaimSlotsAsync([1], VirtualShardCount));

        Assert.That(h.State.State.MovedAwaySlots.Keys, Is.EquivalentTo(new[] { 1, 3 }));
        Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        Assert.That(await h.Grain.ReclaimSlotsAsync([1], VirtualShardCount), Is.EqualTo(1));
        await h.Leaf.Received(2).UnmarkSlotsMovedAwayAsync(
            Arg.Is<int[]>(s => s.Length == 1 && s[0] == 1), VirtualShardCount);
        Assert.That(h.State.State.MovedAwaySlots.Keys, Is.EquivalentTo(new[] { 3 }));
    }
}
