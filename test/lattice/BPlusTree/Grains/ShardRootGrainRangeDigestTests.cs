using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="ShardRootGrain.GetShardProjectionDigestForRangeAsync"/>
/// over an internal-rooted shard - the separator-key descent that folds only
/// the leaves and whole subtrees overlapping the query range.
/// <para>
/// The three descent outcomes each need a different topology shape and none
/// is reachable from a flat-tree fixture: a child whose range is fully inside
/// the query range folds its pre-computed subtree snapshot in a single call, a
/// child that straddles a bound is descended into recursively, and a child
/// outside the range is skipped entirely.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainRangeDigestTests
{
    private const string ShardKey = "digest-tree/1";

    [Test]
    public async Task GetShardProjectionDigestForRangeAsync_folds_leaves_subtrees_and_skips_out_of_range_children()
    {
        // Topology (separator keys in brackets):
        //   I0 [null, "m"]        -> I1, I2      (children internal)
        //     I1 [null, "d"]      -> L0, L1      (children leaves)
        //     I2 [null, "t"]      -> I3, I4      (children internal)
        //       I3 [null]         -> L2          (children leaves)
        //       I4 [null]         -> L3          (children leaves)
        //
        // Query ["b", "z"):
        //   I0 straddles "b"           -> recursive descent
        //     L0, L1                   -> per-leaf in-range snapshots
        //   I2 straddles "z"           -> recursive descent
        //     I3 ["m","t") fully inside -> single-call subtree snapshot
        //     I4 ["t", +inf) straddles  -> recursive descent -> L3
        var harness = new DigestHarness();

        var l0 = harness.Leaf("L0", Snapshot(hash: 0x01, entries: 3, checkpoint: 5));
        var l1 = harness.Leaf("L1", Snapshot(hash: 0x02, entries: 4, checkpoint: 2));
        var l2 = harness.Leaf("L2", Snapshot(hash: 0x04, entries: 5, checkpoint: 1));
        var l3 = harness.Leaf("L3", Snapshot(hash: 0x08, entries: 6, checkpoint: 9));

        var i1 = harness.Internal("I1", [null, "d"], [l0.Id, l1.Id], childrenAreLeaves: true);
        var i3 = harness.Internal("I3", [null], [l2.Id], childrenAreLeaves: true,
            subtreeSnapshot: Snapshot(hash: 0x04, entries: 5, checkpoint: 1));
        var i4 = harness.Internal("I4", [null], [l3.Id], childrenAreLeaves: true);
        var i2 = harness.Internal("I2", [null, "t"], [i3.Id, i4.Id], childrenAreLeaves: false);
        var i0 = harness.Internal("I0", [null, "m"], [i1.Id, i2.Id], childrenAreLeaves: false);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        var digest = await harness.Grain.GetShardProjectionDigestForRangeAsync("b", "z", CancellationToken.None);

        // Entry counts sum across every folded contribution: 3 + 4 + 5 + 6.
        Assert.That(digest.EntryCount, Is.EqualTo(18));
        // Checkpoint offsets max-reduce rather than summing, so the highest
        // descendant checkpoint (L3's 9) wins over L0's 5.
        Assert.That(digest.CheckpointOffset, Is.EqualTo(9));
        Assert.That(digest.Version, Is.EqualTo(LeafProjectionDigest.CurrentVersion));

        // The fully-inside subtree was folded from its pre-computed snapshot,
        // never descended into.
        await i3.Grain.Received(1).GetChildDigestSnapshotAsync();
        await i3.Grain.DidNotReceive().GetRoutingTableAsync();

        // The straddling subtrees were descended into instead.
        await i4.Grain.Received(1).GetRoutingTableAsync();
        await i4.Grain.DidNotReceive().GetChildDigestSnapshotAsync();

        // Every in-range leaf was asked for its clamped in-range snapshot.
        await l0.Grain.Received(1).GetProjectionDigestForRangeAsync("b", "z");
        await l3.Grain.Received(1).GetProjectionDigestForRangeAsync("b", "z");
        Assert.That(l2.Grain.ReceivedCalls(), Is.Empty, "L2 sits under a fully-inside subtree and must never be called.");
    }

    [Test]
    public async Task GetShardProjectionDigestForRangeAsync_skips_children_outside_the_query_range()
    {
        // I0 [null, "m"] -> L0, L1 (children leaves). Query ["a", "d") only
        // overlaps the first child, so the second is skipped without a call.
        var harness = new DigestHarness();
        var l0 = harness.Leaf("L0", Snapshot(hash: 0x11, entries: 2, checkpoint: 0));
        var l1 = harness.Leaf("L1", Snapshot(hash: 0x22, entries: 7, checkpoint: 0));
        var i0 = harness.Internal("I0", [null, "m"], [l0.Id, l1.Id], childrenAreLeaves: true);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        var digest = await harness.Grain.GetShardProjectionDigestForRangeAsync("a", "d", CancellationToken.None);

        Assert.That(digest.EntryCount, Is.EqualTo(2));
        await l0.Grain.Received(1).GetProjectionDigestForRangeAsync("a", "d");
        Assert.That(l1.Grain.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task GetShardProjectionDigestForRangeAsync_folds_a_whole_subtree_for_an_unbounded_range()
    {
        // A null/null query range makes every child fully inside, so each
        // internal child folds from its snapshot without any descent.
        var harness = new DigestHarness();
        var l0 = harness.Leaf("L0", Snapshot(hash: 0x01, entries: 1, checkpoint: 0));
        var i1 = harness.Internal("I1", [null], [l0.Id], childrenAreLeaves: true,
            subtreeSnapshot: Snapshot(hash: 0x33, entries: 11, checkpoint: 4));
        var i0 = harness.Internal("I0", [null], [i1.Id], childrenAreLeaves: false);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        var digest = await harness.Grain.GetShardProjectionDigestForRangeAsync(null, null, CancellationToken.None);

        Assert.That(digest.EntryCount, Is.EqualTo(11));
        Assert.That(digest.CheckpointOffset, Is.EqualTo(4));
        await i1.Grain.Received(1).GetChildDigestSnapshotAsync();
        Assert.That(l0.Grain.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task GetShardProjectionDigestForRangeAsync_treats_a_never_published_child_hash_as_zero()
    {
        // A child that has never published carries a null Hash. The fold must
        // treat it as the zero contribution rather than faulting, and the
        // wrapper must hash a zero block in its place.
        var harness = new DigestHarness();
        var rootLeaf = harness.Leaf("L0", new ChildDigestSnapshot
        {
            Hash = null,
            EntryCount = 2,
            CheckpointOffset = 3,
        });

        harness.State.State.RootNodeId = rootLeaf.Id;
        harness.State.State.RootIsLeaf = true;

        var digest = await harness.Grain.GetShardProjectionDigestForRangeAsync("a", "z", CancellationToken.None);

        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.EntryCount, Is.EqualTo(2));
        Assert.That(digest.CheckpointOffset, Is.EqualTo(3));

        // A wrong-length hash is treated identically to a missing one, so both
        // degrade to the same well-defined digest.
        var shortHashHarness = new DigestHarness();
        var shortHashLeaf = shortHashHarness.Leaf("L0", new ChildDigestSnapshot
        {
            Hash = [0x01, 0x02],
            EntryCount = 2,
            CheckpointOffset = 3,
        });
        shortHashHarness.State.State.RootNodeId = shortHashLeaf.Id;
        shortHashHarness.State.State.RootIsLeaf = true;

        var shortHashDigest = await shortHashHarness.Grain
            .GetShardProjectionDigestForRangeAsync("a", "z", CancellationToken.None);

        Assert.That(shortHashDigest.Hash, Is.EqualTo(digest.Hash));
    }

    [Test]
    public void GetShardProjectionDigestForRangeAsync_honours_a_cancelled_token_mid_descent()
    {
        var harness = new DigestHarness();
        var l0 = harness.Leaf("L0", Snapshot(hash: 0x01, entries: 1, checkpoint: 0));
        var i0 = harness.Internal("I0", [null], [l0.Id], childrenAreLeaves: true);
        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await harness.Grain.GetShardProjectionDigestForRangeAsync(null, null, cts.Token));
    }

    private static ChildDigestSnapshot Snapshot(byte hash, long entries, long checkpoint)
    {
        var bytes = new byte[16];
        bytes[0] = hash;
        return new ChildDigestSnapshot
        {
            Hash = bytes,
            EntryCount = entries,
            CheckpointOffset = checkpoint,
        };
    }

    /// <summary>
    /// Directly-constructed <see cref="ShardRootGrain"/> plus leaf and internal
    /// node substitutes whose routing tables drive the range descent.
    /// </summary>
    private sealed class DigestHarness
    {
        public DigestHarness()
        {
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("shard", ShardKey));

            Grain = new ShardRootGrain(
                context,
                State,
                Factory,
                TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: Factory),
                NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers());
        }

        public IGrainFactory Factory { get; } = Substitute.For<IGrainFactory>();

        public FakePersistentState<ShardRootState> State { get; } = new();

        public ShardRootGrain Grain { get; }

        public LeafNode Leaf(string key, ChildDigestSnapshot snapshot)
        {
            var id = GrainId.Create("leaf", $"{ShardKey}:{key}");
            var grain = Substitute.For<IBPlusLeafGrain>();
            grain.GetProjectionDigestForRangeAsync(Arg.Any<string?>(), Arg.Any<string?>())
                .Returns(Task.FromResult(snapshot));
            Factory.GetGrain<IBPlusLeafGrain>(id).Returns(grain);
            return new LeafNode(id, grain);
        }

        public InternalNode Internal(
            string key,
            string?[] separatorKeys,
            GrainId[] childIds,
            bool childrenAreLeaves,
            ChildDigestSnapshot? subtreeSnapshot = null)
        {
            var id = GrainId.Create("internal", $"{ShardKey}:{key}");
            var grain = Substitute.For<IBPlusInternalGrain>();
            grain.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = separatorKeys,
                ChildIds = childIds,
                ChildrenAreLeaves = childrenAreLeaves,
            }));
            grain.GetChildDigestSnapshotAsync()
                .Returns(Task.FromResult(subtreeSnapshot ?? default));
            Factory.GetGrain<IBPlusInternalGrain>(id).Returns(grain);
            return new InternalNode(id, grain);
        }

        public readonly record struct LeafNode(GrainId Id, IBPlusLeafGrain Grain);

        public readonly record struct InternalNode(GrainId Id, IBPlusInternalGrain Grain);
    }
}
