using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusInternalGrainTests
{
    [TestCase(true, 1)]
    [TestCase(true, 2)]
    [TestCase(false, 1)]
    [TestCase(false, 2)]
    public async Task Initialize_seeds_children_with_one_dispatch_each(bool leaves, int count)
    {
        var (grain, state, factory) = CreateDigestGrain();
        var ids = new[] { Child0, Child1 }.Take(count).ToList();
        var snapshots = ids.Select((_, i) => new ChildDigestSnapshot
        {
            Hash = Bytes16((byte)(i + 1)),
            EntryCount = i + 2,
            CheckpointOffset = i + 10,
            PublishSequence = 1,
        }).ToArray();
        var leafStubs = ids.Select(_ => Substitute.For<IBPlusLeafGrain>()).ToArray();
        var nodeStubs = ids.Select(_ => Substitute.For<IBPlusInternalGrain>()).ToArray();
        for (var i = 0; i < count; i++)
        {
            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leafStubs[i]);
            factory.GetGrain<IBPlusInternalGrain>(ids[i]).Returns(nodeStubs[i]);
            leafStubs[i].SetParentAndGetChildDigestSnapshotAsync(Arg.Any<GrainId?>()).Returns(snapshots[i]);
            nodeStubs[i].SetParentAndGetChildDigestSnapshotAsync(Arg.Any<GrainId?>()).Returns(snapshots[i]);
        }

        await grain.InitializeWithChildrenForTreeAsync(
            "seed-tree", ids.Select((_, i) => i == 0 ? null : "m").ToList(), ids, leaves);

        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(snapshots.Sum(s => s.EntryCount)));
        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(snapshots.Max(s => s.CheckpointOffset)));
        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(Bytes16(count == 1 ? (byte)1 : (byte)3)));
        for (var i = 0; i < count; i++)
        {
            Assert.That(state.State.ChildDigests[ids[i]], Is.EqualTo(snapshots[i]));
            if (leaves)
                await leafStubs[i].Received(1).SetParentAndGetChildDigestSnapshotAsync(GrainId.Create("internal", "digest-self"));
            else
                await nodeStubs[i].Received(1).SetParentAndGetChildDigestSnapshotAsync(GrainId.Create("internal", "digest-self"));
            await leafStubs[i].DidNotReceiveWithAnyArgs().SetParentAsync(default);
            await leafStubs[i].DidNotReceive().GetChildDigestSnapshotAsync();
            await nodeStubs[i].DidNotReceiveWithAnyArgs().SetParentAsync(default);
            await nodeStubs[i].DidNotReceive().GetChildDigestSnapshotAsync();
        }
    }

    [Test]
    public async Task Combined_parent_seed_drains_deferred_publications_before_returning_snapshot()
    {
        var (grain, state, factory) = CreateDigestGrain();
        var parent = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parent);
        state.State.Children = [new ChildEntry { ChildId = Child0 }];
        var queued = false;
        state.BeforeWrite = async () =>
        {
            if (queued) return;
            queued = true;
            await grain.OnChildDigestPublishedAsync(Child0, new ChildDigestSnapshot
            {
                Hash = Bytes16(7), EntryCount = 8, CheckpointOffset = 9, PublishSequence = 10,
            });
        };

        var snapshot = await grain.SetParentAndGetChildDigestSnapshotAsync(DigestParent);

        Assert.That(queued, Is.True);
        Assert.That(snapshot.Hash, Is.EqualTo(Bytes16(7)));
        Assert.That(snapshot.EntryCount, Is.EqualTo(8));
        Assert.That(snapshot.CheckpointOffset, Is.EqualTo(9));
        Assert.That(state.WriteCount, Is.EqualTo(2), "binding and deferred fold must both persist");
        snapshot.Hash![0] = 0;
        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(Bytes16(7)));
        await parent.DidNotReceiveWithAnyArgs().OnChildDigestPublishedAsync(default, default);
    }

    [Test]
    public async Task Combined_parent_seed_failed_write_rolls_back_and_retry_is_idempotent()
    {
        var (grain, state, _) = CreateDigestGrain();
        state.ThrowOnWrite = new IOException("seed write failed");

        Assert.ThrowsAsync<IOException>(() => grain.SetParentAndGetChildDigestSnapshotAsync(DigestParent));
        Assert.That(state.State.ParentId, Is.Null);
        Assert.That(state.State.DigestPublishSequence, Is.Zero, "failed binding must not produce a snapshot");

        var first = await grain.SetParentAndGetChildDigestSnapshotAsync(DigestParent);
        var writes = state.WriteCount;
        var second = await grain.SetParentAndGetChildDigestSnapshotAsync(DigestParent);

        Assert.That(state.State.ParentId, Is.EqualTo(DigestParent));
        Assert.That(state.WriteCount, Is.EqualTo(writes));
        Assert.That(second.PublishSequence, Is.GreaterThan(first.PublishSequence));
    }

    [TestCase(null)]
    [TestCase("foreign")]
    [TestCase("ORDERS")]
    public async Task Tree_topology_foreign_or_unbound_node_never_traverses_children(string? boundTree)
    {
        var state = new FakePersistentState<InternalNodeState>();
        var (grain, _, factory) = CreateDigestGrain(state);
        state.State.TreeId = boundTree;
        state.State.ChildrenAreLeaves = false;
        state.State.Children = [new ChildEntry { ChildId = Child0 }];

        Assert.That(await grain.GetTopologyForTreeAsync("orders", 3), Is.Null);
        factory.DidNotReceiveWithAnyArgs().GetGrain<IBPlusInternalGrain>(default(GrainId));
        Assert.That(state.State.SubtreeProjectionHash, Is.Null, "denial must not even build the topology");
    }

    [TestCase(0)]
    [TestCase(1)]
    public async Task Tree_topology_bound_node_preserves_depth_and_summary(int depth)
    {
        var (grain, state, factory) = CreateDigestGrain();
        state.State.TreeId = "orders";
        state.State.ChildrenAreLeaves = false;
        state.State.Children = [new ChildEntry { ChildId = Child0 }];
        state.State.ChildDigests[Child0] = new ChildDigestSnapshot { ChildFanout = 2, EntryCount = 12 };
        var child = Substitute.For<IBPlusInternalGrain>();
        child.GetTopologyAsync(0).Returns(new ShardTopologyNode { NodeId = "nested", IsLeaf = false });
        factory.GetGrain<IBPlusInternalGrain>(Child0).Returns(child);

        var topology = await grain.GetTopologyForTreeAsync("orders", depth);

        Assert.That(topology, Is.Not.Null);
        Assert.That(topology!.Children, Has.Count.EqualTo(1));
        if (depth == 0)
            Assert.That(topology.Children[0].EntryCount, Is.EqualTo(12));
        Assert.That(topology.Children[0].ChildrenTruncated, Is.EqualTo(depth == 0));
        if (depth == 0)
            await child.DidNotReceiveWithAnyArgs().GetTopologyAsync(default);
        else
            await child.Received(1).GetTopologyAsync(0);
    }
}
