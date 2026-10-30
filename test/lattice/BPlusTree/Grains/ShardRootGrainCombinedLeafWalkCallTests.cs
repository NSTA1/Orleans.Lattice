using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the per-hop grain-call budget of three shard-root leaf-chain walks:
/// the bounded range delete, the bounded range-clock probe, and the
/// snapshot-baseline freeze pass. Each now reads its payload, the leaf's
/// bounds and its sibling pointer in a single leaf call per hop, where it
/// previously paid a separate <c>GetNextSiblingAsync</c> (and, for the delete
/// and the clock probe, <c>GetKeyRangeAsync</c>) on top. The leaves here are
/// bare substitutes that answer only the combined call, so a regression to the
/// separate calls fails loudly rather than being composed away.
/// </summary>
[TestFixture]
public sealed class ShardRootGrainCombinedLeafWalkCallTests
{
    private const string TreeId = "combined-walk-tree";
    private const string ShardKey = TreeId + "/0";
    private const int LeafCount = 4;

    private static (ShardRootGrain Grain, IBPlusLeafGrain[] Leaves, IGrainFactory Factory, GrainId[] Ids, Guid[] Keys)
        CreateShard(LatticeOptions options, Action<ShardRootState>? configure = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var keys = new Guid[LeafCount];
        var ids = new GrainId[LeafCount];
        for (var i = 0; i < LeafCount; i++)
        {
            keys[i] = Guid.NewGuid();
            ids[i] = GrainId.Create("leaf", keys[i].ToString("N"));
        }

        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;
        configure?.Invoke(state.State);

        var factory = Substitute.For<IGrainFactory>();
        var leaves = new IBPlusLeafGrain[LeafCount];
        for (var i = 0; i < LeafCount; i++)
        {
            leaves[i] = Substitute.For<IBPlusLeafGrain>();
            leaves[i].GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaves[i]);
            factory.GetGrain<IBPlusLeafGrain>(keys[i]).Returns(leaves[i]);
        }

        var grain = new ShardRootGrain(context, state, factory,
            TestOptionsResolver.Create(baseOptions: options, shardCount: 1, factory: factory),
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());
        return (grain, leaves, factory, ids, keys);
    }

    private static LeafKeyRange Bounds(int index) => new()
    {
        LowKeyInclusive = $"k{index}",
        HighKeyExclusive = index + 1 < LeafCount ? $"k{index + 1}" : null,
    };

    private static GrainId? Next(GrainId[] ids, int index) =>
        index + 1 < LeafCount ? ids[index + 1] : null;

    private static void AssertNoSeparateLinkReads(IBPlusLeafGrain leaf)
    {
        leaf.DidNotReceive().GetNextSiblingAsync();
        leaf.DidNotReceive().GetKeyRangeAndNextSiblingAsync();
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task Split_aware_counts_make_one_leaf_call_per_hop(int path)
    {
        const int virtualShards = 16;
        var movedSlot = ShardMap.GetVirtualSlot("k0", virtualShards);
        var (grain, leaves, _, ids, _) = CreateShard(new LatticeOptions
        {
            MaxLeavesPerScanPage = 1000,
            MaxScanPageDuration = TimeSpan.Zero,
        }, state =>
        {
            state.MovedAwaySlots.Add(movedSlot, 1);
            state.MovedAwayVirtualShardCount = virtualShards;
        });
        for (var i = 0; i < LeafCount; i++)
        {
            leaves[i].GetKeysAndNextSiblingAsync(Arg.Any<string?>(), Arg.Any<string?>())
                .Returns((new List<string> { $"k{i}" },
                    new LeafChainLink { KeyRange = Bounds(i), NextSibling = Next(ids, i) }));
        }

        var expected = Enumerable.Range(0, LeafCount)
            .Count(i => ShardMap.GetVirtualSlot($"k{i}", virtualShards) != movedSlot);
        if (path == 0)
            Assert.That((await grain.CountBoundedAsync(null, null)).Count, Is.EqualTo(expected));
        else if (path == 1)
        {
            var page = await grain.CountWithMovedAwayBoundedAsync(null);
            Assert.That(page.Count, Is.EqualTo(expected));
            Assert.That(page.MovedAwaySlots, Is.EqualTo(new[] { movedSlot }));
        }
        else
        {
            var slots = Enumerable.Range(0, virtualShards).Where(s => s != movedSlot).ToArray();
            Assert.That((await grain.CountForSlotsBoundedAsync(slots, virtualShards, null, null)).Count,
                Is.EqualTo(expected));
        }
        foreach (var leaf in leaves)
        {
            await leaf.Received(1).GetKeysAndNextSiblingAsync(null, null);
            await leaf.DidNotReceive().GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>());
            AssertNoSeparateLinkReads(leaf);
        }
    }

    [Test]
    public async Task Projection_rebuild_makes_one_leaf_call_per_hop()
    {
        var (grain, leaves, _, ids, _) = CreateShard(new LatticeOptions
        {
            MaxLeavesPerScanPage = 1000,
            MaxScanPageDuration = TimeSpan.Zero,
        });
        for (var i = 0; i < LeafCount; i++)
            leaves[i].RebuildProjectionAndNextSiblingAsync().Returns(
                new LeafChainLink { KeyRange = Bounds(i), NextSibling = Next(ids, i) });

        var page = await grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);

        Assert.That(page.LeavesRebuilt, Is.EqualTo(LeafCount));
        Assert.That(page.ResumeFromInclusive, Is.Null);
        foreach (var leaf in leaves)
        {
            await leaf.Received(1).RebuildProjectionAndNextSiblingAsync();
            AssertNoSeparateLinkReads(leaf);
        }
    }

    [Test]
    public async Task Bounded_range_delete_makes_one_leaf_call_per_hop()
    {
        var (grain, leaves, _, ids, _) = CreateShard(new LatticeOptions
        {
            MaxLeavesPerScanPage = 1000,
            MaxScanPageDuration = TimeSpan.Zero,
        });
        for (var i = 0; i < LeafCount; i++)
        {
            var index = i;
            leaves[i].DeleteRangeAndNextSiblingAsync(
                    Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>())
                .Returns(Task.FromResult(new LeafRangeDeletePage
                {
                    Result = new RangeDeleteResult { Deleted = 0, PastRange = false, MatchedKeys = null },
                    KeyRange = Bounds(index),
                    NextSibling = Next(ids, index),
                }));
        }

        var page = await grain.DeleteRangeBoundedAsync("k0", "k9", null);

        Assert.That(page.ResumeFromInclusive, Is.Null, "the walk must run to the chain tail");
        foreach (var leaf in leaves)
        {
            await leaf.Received(1).DeleteRangeAndNextSiblingAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>());
            await leaf.DidNotReceive().DeleteRangeAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>());
            AssertNoSeparateLinkReads(leaf);
        }
    }

    [Test]
    public async Task Bounded_range_clock_probe_makes_one_leaf_call_per_hop()
    {
        var (grain, leaves, _, ids, _) = CreateShard(new LatticeOptions
        {
            MaxLeavesPerScanPage = 1000,
            MaxScanPageDuration = TimeSpan.Zero,
        });
        for (var i = 0; i < LeafCount; i++)
        {
            var index = i;
            leaves[i].GetClockAndNextSiblingAsync()
                .Returns(Task.FromResult(new LeafClockPage
                {
                    Clock = new HybridLogicalClock { WallClockTicks = 100 + index },
                    KeyRange = Bounds(index),
                    NextSibling = Next(ids, index),
                }));
        }

        var page = await grain.GetRangeClockBoundedAsync("k0", "k9");

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Null);
            Assert.That(page.MaxClock.WallClockTicks, Is.EqualTo(100 + LeafCount - 1),
                "every leaf's clock must reach the maximum");
        });
        foreach (var leaf in leaves)
        {
            await leaf.Received(1).GetClockAndNextSiblingAsync();
            await leaf.DidNotReceive().GetClockAsync();
            AssertNoSeparateLinkReads(leaf);
        }
    }

    [Test]
    public async Task Materialiser_lag_walk_makes_one_leaf_call_per_hop()
    {
        var (grain, leaves, factory, ids, _) = CreateShard(new LatticeOptions
        {
            WalPartitions = 1,
            MaxLeavesPerScanPage = 1000,
            MaxScanPageDuration = TimeSpan.Zero,
        });
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(100L));
        factory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/0").Returns(coordinator);
        for (var i = 0; i < LeafCount; i++)
        {
            leaves[i].GetProjectionCheckpointAndNextSiblingAsync().Returns(new LeafCheckpointPage
            {
                CheckpointOffset = 20 - i,
                Link = new LeafChainLink { KeyRange = Bounds(i), NextSibling = Next(ids, i) },
            });
        }

        var page = await grain.GetShardMaterialiserLagBoundedAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Null);
            Assert.That(page.WalHeadOffsets, Is.EqualTo(new[] { 100L }));
            Assert.That(page.MinCheckpointOffset, Is.EqualTo(20 - LeafCount + 1));
        });
        foreach (var leaf in leaves)
        {
            await leaf.Received(1).GetProjectionCheckpointAndNextSiblingAsync();
            AssertNoSeparateLinkReads(leaf);
        }
    }

    [TestCase(1)]
    [TestCase(3)]
    public async Task Snapshot_baseline_freezes_every_leaf_before_capturing_any_partition_head(int partitions)
    {
        var (grain, leaves, factory, ids, _) = CreateShard(new LatticeOptions
        {
            WalPartitions = partitions,
            MaxScanPageDuration = TimeSpan.Zero,
            MaxScanPageStallDuration = Timeout.InfiniteTimeSpan,
        });
        for (var i = 0; i < LeafCount; i++)
        {
            var index = i;
            leaves[i].FreezeProjectionAndNextSiblingAsync(Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new LeafBaselineFreezePage
                {
                    Freeze = new LeafBaselineFreeze { Rows = [], FrontierPerPartition = new long[partitions], Pending = [] },
                    NextSibling = Next(ids, index),
                }));
            leaves[i].FoldTailOntoFrozenAsync(
                    Arg.Any<LeafBaselineFreeze>(), Arg.Any<long[]>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<LeafSnapshotRow>>([]));
        }

        var coordinators = new ILeafReplayCoordinatorGrain[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
            coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult((long)partition));
            factory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/{partition}").Returns(coordinator);
            coordinators[partition] = coordinator;
        }
        var snapshotLeaf = Substitute.For<ISnapshotLeafGrain>();
        factory.GetGrain<ISnapshotLeafGrain>(Arg.Any<string>()).Returns(snapshotLeaf);
        var lastFreeze = new TaskCompletionSource<LeafBaselineFreezePage>();
        leaves[^1].FreezeProjectionAndNextSiblingAsync(Arg.Any<CancellationToken>()).Returns(lastFreeze.Task);

        var capture = grain.CaptureSnapshotBaselineAsync(Guid.NewGuid(), CancellationToken.None);
        foreach (var coordinator in coordinators)
            _ = coordinator.DidNotReceive().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        Assert.That(capture.IsCompleted, Is.False);
        lastFreeze.SetResult(new LeafBaselineFreezePage
        {
            Freeze = new LeafBaselineFreeze { Rows = [], FrontierPerPartition = new long[partitions], Pending = [] },
        });
        var result = await capture.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(result.CapturedHeadPerPartition, Is.EqualTo(Enumerable.Range(0, partitions).Select(p => (long)p)));
        foreach (var coordinator in coordinators)
            _ = coordinator.Received(1).GetHeadOffsetAsync(Arg.Any<CancellationToken>());

        foreach (var leaf in leaves)
        {
            await leaf.Received(1).FreezeProjectionAndNextSiblingAsync(Arg.Any<CancellationToken>());
            await leaf.Received(1).FoldTailOntoFrozenAsync(
                Arg.Any<LeafBaselineFreeze>(), Arg.Any<long[]>(), Arg.Any<CancellationToken>());
            AssertNoSeparateLinkReads(leaf);
        }
    }
}
