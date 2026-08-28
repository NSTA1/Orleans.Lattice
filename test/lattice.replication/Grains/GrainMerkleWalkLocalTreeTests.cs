using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit tests for <see cref="GrainMerkleWalkLocalTree"/>, the local-cluster
/// adapter the cross-cluster Merkle walk reads the tree through. It is strictly
/// read-only, so the whole contract is "resolve the right grain and project its
/// digest and routing table onto <see cref="MerkleWalkLocalNode"/>" - which is
/// exercised here against substituted shard-root, leaf, and internal grains,
/// with no cluster.
/// </summary>
[TestFixture]
public sealed class GrainMerkleWalkLocalTreeTests
{
    private const string PhysicalTreeId = "tree-a";
    private const int ShardIndex = 3;

    private static LeafProjectionDigest Digest(byte marker, long entries) => new()
    {
        Hash = [marker],
        EntryCount = entries,
        CheckpointOffset = entries * 2,
        Version = 1,
    };

    private static GrainId LeafId(string name) => GrainId.Create("leaf", name);

    private static GrainId InternalId(string name) => GrainId.Create("internal", name);

    private sealed class Harness
    {
        public required IGrainFactory Factory { get; init; }

        public required IShardRootGrain ShardRoot { get; init; }

        public required Dictionary<GrainId, IBPlusLeafGrain> Leaves { get; init; }

        public required Dictionary<GrainId, IBPlusInternalGrain> Internals { get; init; }

        public GrainMerkleWalkLocalTree Tree => new(Factory, PhysicalTreeId, ShardIndex);
    }

    private static Harness CreateHarness()
    {
        var factory = Substitute.For<IGrainFactory>();
        var shardRoot = Substitute.For<IShardRootGrain>();
        var leaves = new Dictionary<GrainId, IBPlusLeafGrain>();
        var internals = new Dictionary<GrainId, IBPlusInternalGrain>();

        // The adapter addresses the shard root by "{physicalTreeId}/{shardIndex}"
        // and every node by its GrainId.
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves[call.ArgAt<GrainId>(0)]);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>())
            .Returns(call => internals[call.ArgAt<GrainId>(0)]);

        return new Harness
        {
            Factory = factory,
            ShardRoot = shardRoot,
            Leaves = leaves,
            Internals = internals,
        };
    }

    private static IBPlusLeafGrain AddLeaf(Harness h, GrainId id, LeafProjectionDigest digest)
    {
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetProjectionDigestAsync().Returns(Task.FromResult(digest));
        h.Leaves[id] = leaf;
        return leaf;
    }

    private static IBPlusInternalGrain AddInternal(
        Harness h, GrainId id, LeafProjectionDigest digest, RoutingTableSnapshot routing)
    {
        var node = Substitute.For<IBPlusInternalGrain>();
        node.GetSubtreeProjectionDigestAsync().Returns(Task.FromResult(digest));
        node.GetRoutingTableAsync().Returns(Task.FromResult(routing));
        h.Internals[id] = node;
        return node;
    }

    [Test]
    public async Task GetRootAsync_addresses_the_shard_root_by_tree_and_shard_index()
    {
        var h = CreateHarness();
        h.ShardRoot.GetRootNodeRefAsync().Returns(Task.FromResult<ShardRootNodeRef?>(null));

        await h.Tree.GetRootAsync(CancellationToken.None);

        h.Factory.Received(1).GetGrain<IShardRootGrain>($"{PhysicalTreeId}/{ShardIndex}");
    }

    [Test]
    public async Task GetRootAsync_returns_null_for_an_empty_shard()
    {
        var h = CreateHarness();
        h.ShardRoot.GetRootNodeRefAsync().Returns(Task.FromResult<ShardRootNodeRef?>(null));

        Assert.That(await h.Tree.GetRootAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetRootAsync_resolves_a_leaf_root()
    {
        var h = CreateHarness();
        var id = LeafId("root");
        AddLeaf(h, id, Digest(0x11, 5));
        h.ShardRoot.GetRootNodeRefAsync()
            .Returns(Task.FromResult<ShardRootNodeRef?>(new ShardRootNodeRef { NodeId = id, IsLeaf = true }));

        var node = await h.Tree.GetRootAsync(CancellationToken.None);

        Assert.That(node, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(node!.Value.IsLeaf, Is.True);
            Assert.That(node.Value.Digest.EntryCount, Is.EqualTo(5L));
            Assert.That(node.Value.Children, Is.Empty);
        });
    }

    [Test]
    public async Task GetRootAsync_resolves_an_internal_root()
    {
        var h = CreateHarness();
        var rootId = InternalId("root");
        var childId = LeafId("child-0");
        AddInternal(h, rootId, Digest(0x22, 9), new RoutingTableSnapshot
        {
            SeparatorKeys = [null],
            ChildIds = [childId],
            ChildrenAreLeaves = true,
        });
        h.ShardRoot.GetRootNodeRefAsync()
            .Returns(Task.FromResult<ShardRootNodeRef?>(new ShardRootNodeRef { NodeId = rootId, IsLeaf = false }));

        var node = await h.Tree.GetRootAsync(CancellationToken.None);

        Assert.That(node, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(node!.Value.IsLeaf, Is.False);
            Assert.That(node.Value.Digest.EntryCount, Is.EqualTo(9L));
            Assert.That(node.Value.Children, Has.Count.EqualTo(1));
            Assert.That(node.Value.Children[0].NodeId, Is.EqualTo(childId));
        });
    }

    [Test]
    public async Task ResolveAsync_projects_a_leaf_digest_with_no_children()
    {
        var h = CreateHarness();
        var id = LeafId("l1");
        AddLeaf(h, id, Digest(0x33, 12));

        var node = await h.Tree.ResolveAsync(id, isLeaf: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(node.IsLeaf, Is.True);
            Assert.That(node.Digest.Hash, Is.EqualTo(new byte[] { 0x33 }));
            Assert.That(node.Digest.EntryCount, Is.EqualTo(12L));
            Assert.That(node.Children, Is.Empty);
        });
    }

    [Test]
    public async Task ResolveAsync_projects_an_internal_node_routing_table_in_order()
    {
        var h = CreateHarness();
        var id = InternalId("n1");
        var a = LeafId("a");
        var b = LeafId("b");
        var c = LeafId("c");
        AddInternal(h, id, Digest(0x44, 30), new RoutingTableSnapshot
        {
            // The leftmost child carries a null separator (it inherits the
            // parent's lower bound).
            SeparatorKeys = [null, "m", "s"],
            ChildIds = [a, b, c],
            ChildrenAreLeaves = true,
        });

        var node = await h.Tree.ResolveAsync(id, isLeaf: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(node.IsLeaf, Is.False);
            Assert.That(node.Digest.EntryCount, Is.EqualTo(30L));
            Assert.That(node.Children.Select(x => x.NodeId), Is.EqualTo(new[] { a, b, c }));
            Assert.That(node.Children.Select(x => x.SeparatorKey), Is.EqualTo(new string?[] { null, "m", "s" }));
            Assert.That(node.Children.Select(x => x.ChildIsLeaf), Is.All.True);
        });
    }

    [Test]
    public async Task ResolveAsync_marks_children_as_internal_when_the_routing_table_says_so()
    {
        var h = CreateHarness();
        var id = InternalId("n2");
        AddInternal(h, id, Digest(0x55, 1), new RoutingTableSnapshot
        {
            SeparatorKeys = [null],
            ChildIds = [InternalId("mid")],
            ChildrenAreLeaves = false,
        });

        var node = await h.Tree.ResolveAsync(id, isLeaf: false, CancellationToken.None);

        Assert.That(node.Children[0].ChildIsLeaf, Is.False);
    }

    [Test]
    public async Task ResolveAsync_returns_no_children_for_an_empty_routing_table()
    {
        var h = CreateHarness();
        var id = InternalId("n3");
        AddInternal(h, id, Digest(0x66, 0), new RoutingTableSnapshot
        {
            SeparatorKeys = [],
            ChildIds = [],
            ChildrenAreLeaves = true,
        });

        var node = await h.Tree.ResolveAsync(id, isLeaf: false, CancellationToken.None);

        Assert.That(node.Children, Is.Empty);
    }

    [Test]
    public async Task ResolveAsync_truncates_a_ragged_routing_table_to_the_shorter_array()
    {
        // Defensive pairing: a routing table whose separator and child arrays
        // disagree in length must project only the pairs that exist rather than
        // reading past the end of the shorter one.
        var h = CreateHarness();
        var id = InternalId("n4");
        var a = LeafId("a");
        AddInternal(h, id, Digest(0x77, 4), new RoutingTableSnapshot
        {
            SeparatorKeys = [null, "m", "s"],
            ChildIds = [a],
            ChildrenAreLeaves = true,
        });

        var node = await h.Tree.ResolveAsync(id, isLeaf: false, CancellationToken.None);

        Assert.That(node.Children, Has.Count.EqualTo(1));
        Assert.That(node.Children[0].NodeId, Is.EqualTo(a));
    }

    [Test]
    public void GetRootAsync_observes_cancellation_before_touching_the_shard_root()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await h.Tree.GetRootAsync(cts.Token));
        h.Factory.DidNotReceiveWithAnyArgs().GetGrain<IShardRootGrain>(default(string)!);
    }

    [Test]
    public void ResolveAsync_observes_cancellation_before_touching_a_node()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await h.Tree.ResolveAsync(LeafId("l"), isLeaf: true, cts.Token));
        h.Factory.DidNotReceiveWithAnyArgs().GetGrain<IBPlusLeafGrain>(default(GrainId));
    }
}
