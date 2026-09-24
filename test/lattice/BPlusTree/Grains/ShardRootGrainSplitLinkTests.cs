using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Runtime-free coverage of how the shard root links a split into the tree
/// (issue #3523). Internal nodes store no key range and accept whatever
/// separator they are given, so a split linked against the ancestor path a
/// writer captured on its way down lands on the wrong parent as soon as a
/// concurrent turn has reshaped that path, and the new sibling becomes
/// reachable by the leaf chain but by no descent. These tests pin that every
/// link runs one at a time per shard, re-descends from the current root for
/// its parent, records its intent durably before the parent is asked to
/// accept, carries a parent's own division up a level, and is re-delivered by
/// the next operation when it faults.
/// </summary>
[TestFixture]
public sealed class ShardRootGrainSplitLinkTests
{
    private const string ShardKey = "split-link-tree/0";

    private static readonly GrainId RootId = GrainId.Create("internal", "root");
    private static readonly GrainId LeftParentId = GrainId.Create("internal", "left-parent");
    private static readonly GrainId RightParentId = GrainId.Create("internal", "right-parent");
    private static readonly GrainId NewRootId = GrainId.Create("internal", "new-root");
    private static readonly GrainId LeftLeafId = GrainId.Create("leaf", "left");
    private static readonly GrainId RightLeafId = GrainId.Create("leaf", "right");
    private static readonly GrainId SiblingId = GrainId.Create("leaf", "sibling");

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required Dictionary<GrainId, IBPlusInternalGrain> Internals { get; init; }
        public required Dictionary<GrainId, RoutingTableSnapshot> Routing { get; init; }
        public required Dictionary<GrainId, IBPlusLeafGrain> Leaves { get; init; }
        public required IBPlusInternalGrain NewRoot { get; init; }

        public IBPlusInternalGrain Node(GrainId id) => Internals[id];

        public IBPlusLeafGrain Leaf(GrainId id) => Leaves[id];

        /// <summary>
        /// Registers an internal node whose routing table is read from
        /// <see cref="Routing"/> on every call, so a test can reshape the tree
        /// part-way through an operation.
        /// </summary>
        public void AddInternal(GrainId id, RoutingTableSnapshot routing)
        {
            Routing[id] = routing;
            Internals[id] = MakeInternal(id, Routing);
        }

        public void AddLeaf(GrainId id)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
                .Returns(Task.FromResult<SplitResult?>(null));
            Leaves[id] = leaf;
        }
    }

    private static IBPlusInternalGrain MakeInternal(GrainId id, Dictionary<GrainId, RoutingTableSnapshot> routing)
    {
        var node = Substitute.For<IBPlusInternalGrain, IGrainBase>();
        var nodeContext = Substitute.For<IGrainContext>();
        nodeContext.GrainId.Returns(id);
        ((IGrainBase)node).GrainContext.Returns(nodeContext);
        node.GetRoutingTableAsync().Returns(_ => Task.FromResult(routing[id]));
        node.GetLeftmostChildWithMetadataAsync().Returns(_ =>
            Task.FromResult((routing[id].ChildIds[0], routing[id].ChildrenAreLeaves)));
        node.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));
        return node;
    }

    private static RoutingTableSnapshot Table(bool childrenAreLeaves, params (string? Separator, GrainId Child)[] entries) => new()
    {
        SeparatorKeys = entries.Select(e => e.Separator).ToArray(),
        ChildIds = entries.Select(e => e.Child).ToArray(),
        ChildrenAreLeaves = childrenAreLeaves,
    };

    /// <summary>
    /// A two-level tree: the root splits the keyspace at <c>"m"</c> between
    /// two leaf parents, each over one leaf.
    /// </summary>
    private static Harness CreateTwoLevelHarness()
    {
        var h = CreateEmptyHarness();
        h.AddLeaf(LeftLeafId);
        h.AddLeaf(RightLeafId);
        h.AddInternal(LeftParentId, Table(true, (null, LeftLeafId)));
        h.AddInternal(RightParentId, Table(true, (null, RightLeafId)));
        h.AddInternal(RootId, Table(false, (null, LeftParentId), ("m", RightParentId)));
        return h;
    }

    /// <summary>A one-level tree: the root is the parent of two leaves.</summary>
    private static Harness CreateOneLevelHarness()
    {
        var h = CreateEmptyHarness();
        h.AddLeaf(LeftLeafId);
        h.AddLeaf(RightLeafId);
        h.AddInternal(RootId, Table(true, (null, LeftLeafId), ("m", RightLeafId)));
        return h;
    }

    private static Harness CreateEmptyHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = RootId;
        state.State.RootIsLeaf = false;

        var internals = new Dictionary<GrainId, IBPlusInternalGrain>();
        var leaves = new Dictionary<GrainId, IBPlusLeafGrain>();
        var routing = new Dictionary<GrainId, RoutingTableSnapshot>();

        var factory = Substitute.For<IGrainFactory>();

        // An unregistered node fails loudly rather than resolving to a fresh
        // substitute that reports zeroes.
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>())
            .Returns(call => internals[call.ArgAt<GrainId>(0)]);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves[call.ArgAt<GrainId>(0)]);
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Substitute.For<IShardRootGrain>());

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        // A promotion mints the new root from a deterministic Guid.
        var newRoot = MakeInternal(NewRootId, routing);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(newRoot);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>(), Arg.Any<string>()).Returns(newRoot);

        return new Harness
        {
            Grain = grain,
            State = state,
            Internals = internals,
            Routing = routing,
            Leaves = leaves,
            NewRoot = newRoot,
        };
    }

    private static SplitResult LeafSplit(string promotedKey, GrainId? sibling = null) => new()
    {
        PromotedKey = promotedKey,
        NewSiblingId = sibling ?? SiblingId,
        ChildIsLeaf = true,
    };

    [Test]
    public async Task A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds()
    {
        var h = CreateTwoLevelHarness();

        // Warm the routing cache along the left edge.
        await h.Grain.SetAsync("a", [1]);

        // While the leaf writes, a concurrent turn moves the root's boundary
        // below the new separator: "d" now belongs under the right parent, and
        // the left parent the writer descended through no longer covers it.
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(_ =>
        {
            h.Routing[RootId] = Table(false, (null, LeftParentId), ("b", RightParentId));
            return Task.FromResult<SplitResult?>(LeafSplit("d"));
        });

        await h.Grain.SetAsync("c", [2]);

        await h.Node(RightParentId).Received(1).AcceptSplitAsync("d", SiblingId);
        await h.Node(LeftParentId).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_leaf_reported_split_is_linked_at_leaf_level_even_when_its_child_flag_is_unset()
    {
        var h = CreateOneLevelHarness();

        // A producer that leaves ChildIsLeaf at its default must not be read as
        // a division of the root, which would wrap the root around a leaf.
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "d",
            NewSiblingId = SiblingId,
        }));

        await h.Grain.SetAsync("c", [1]);

        await h.Node(RootId).Received(1).AcceptSplitAsync("d", SiblingId);
        await h.NewRoot.DidNotReceive().InitializeAsync(
            Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>());
        Assert.That(h.State.State.RootNodeId, Is.EqualTo(RootId));
    }

    [Test]
    public async Task A_parent_that_divides_on_accept_has_its_division_linked_a_level_up()
    {
        var h = CreateTwoLevelHarness();
        var leftParentSibling = GrainId.Create("internal", "left-parent-sibling");
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("d")));
        h.Node(LeftParentId).AcceptSplitAsync("d", SiblingId).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "f",
            NewSiblingId = leftParentSibling,
            ChildIsLeaf = false,
        }));

        await h.Grain.SetAsync("c", [1]);

        await h.Node(RootId).Received(1).AcceptSplitAsync("f", leftParentSibling);
        await h.Node(RightParentId).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_root_that_divides_on_accept_is_wrapped_under_a_new_root()
    {
        var h = CreateOneLevelHarness();
        var rootSibling = GrainId.Create("internal", "root-sibling");
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("d")));
        h.Node(RootId).AcceptSplitAsync("d", SiblingId).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "q",
            NewSiblingId = rootSibling,
            ChildIsLeaf = false,
        }));

        await h.Grain.SetAsync("c", [1]);

        await h.NewRoot.Received(1).InitializeAsync("q", RootId, rootSibling, false);
        await h.Node(RootId).Received(1).AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootNodeId, Is.EqualTo(NewRootId));
            Assert.That(h.State.State.RootIsLeaf, Is.False);
            Assert.That(h.State.State.PendingPromotion, Is.Null);
            Assert.That(h.State.State.PendingChildLinks, Is.Empty);
        });
    }

    [Test]
    public async Task The_link_intent_is_persisted_before_the_parent_is_asked_to_accept()
    {
        var h = CreateTwoLevelHarness();
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("d")));

        var writesBefore = h.State.WriteCount;
        List<PendingChildLink>? recordedAtAccept = null;
        var writesAtAccept = 0;
        h.Node(LeftParentId).AcceptSplitAsync("d", SiblingId).Returns(_ =>
        {
            recordedAtAccept = [.. h.State.State.PendingChildLinks];
            writesAtAccept = h.State.WriteCount;
            return Task.FromResult<SplitResult?>(null);
        });

        await h.Grain.SetAsync("c", [1]);

        Assert.That(recordedAtAccept, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(recordedAtAccept!, Has.Count.EqualTo(1));
            Assert.That(recordedAtAccept![0].PromotedKey, Is.EqualTo("d"));
            Assert.That(recordedAtAccept[0].ChildId, Is.EqualTo(SiblingId));
            Assert.That(recordedAtAccept[0].ChildIsLeaf, Is.True);
            Assert.That(writesAtAccept, Is.GreaterThan(writesBefore),
                "the intent must be durable before the accept, or a crash between the two strands the sibling.");
            Assert.That(h.State.State.PendingChildLinks, Is.Empty,
                "a landed link retires its intent.");
        });
    }

    [Test]
    public void A_split_whose_intent_cannot_be_persisted_fails_without_touching_the_parent()
    {
        var h = CreateTwoLevelHarness();
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("d")));
        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await h.Grain.SetAsync("c", [1]),
            Throws.InvalidOperationException.With.Message.EqualTo("storage unavailable"));
        Assert.That(h.State.State.PendingChildLinks, Is.Empty,
            "an intent that was never persisted is withdrawn from the activation.");
        h.Node(LeftParentId).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
    }

    [Test]
    public async Task A_link_that_faults_stays_recorded_and_the_next_operation_redelivers_it()
    {
        var h = CreateTwoLevelHarness();
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("d")));

        // Not a transient fault, so the write path does not retry it and the
        // fault reaches the caller.
        h.Node(LeftParentId).AcceptSplitAsync("d", SiblingId).Returns(
            _ => Task.FromException<SplitResult?>(new InvalidOperationException("parent faulted")),
            _ => Task.FromResult<SplitResult?>(null));

        Assert.That(async () => await h.Grain.SetAsync("c", [1]), Throws.TypeOf<InvalidOperationException>());
        Assert.That(h.State.State.PendingChildLinks, Has.Count.EqualTo(1));

        // An unrelated write that splits nothing still repairs the tree first.
        await h.Grain.SetAsync("x", [2]);

        await h.Node(LeftParentId).Received(2).AcceptSplitAsync("d", SiblingId);
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_transient_link_fault_is_retried_by_the_write_and_the_stranded_link_is_redelivered()
    {
        var h = CreateTwoLevelHarness();

        // The retried write finds the leaf already divided and the key already
        // applied, so it reports no split the second time: only the durable
        // intent still knows the sibling is unlinked.
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(
            Task.FromResult<SplitResult?>(LeafSplit("d")),
            Task.FromResult<SplitResult?>(null));
        h.Node(LeftParentId).AcceptSplitAsync("d", SiblingId).Returns(
            _ => Task.FromException<SplitResult?>(new TimeoutException("parent unreachable")),
            _ => Task.FromResult<SplitResult?>(null));

        await h.Grain.SetAsync("c", [1]);

        Assert.That(h.State.State.PendingChildLinks, Has.Count.EqualTo(1));
        Assert.That(h.State.State.PendingChildLinks[0].ChildId, Is.EqualTo(SiblingId));

        await h.Grain.SetAsync("x", [2]);

        await h.Node(LeftParentId).Received(2).AcceptSplitAsync("d", SiblingId);
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task A_resumed_internal_child_link_is_delivered_at_the_level_its_height_implies()
    {
        var h = CreateTwoLevelHarness();
        var strandedParent = GrainId.Create("internal", "stranded-parent");
        var strandedLeaf = GrainId.Create("leaf", "stranded-leaf");
        h.AddLeaf(strandedLeaf);
        h.AddInternal(strandedParent, Table(true, (null, strandedLeaf)));
        h.State.State.PendingChildLinks.Add(new PendingChildLink
        {
            PromotedKey = "p",
            ChildId = strandedParent,
            ChildIsLeaf = false,
            Ancestors = [],
        });

        await h.Grain.SetAsync("x", [1]);

        // One internal level tall, so its parent is the root, not the leaf
        // parent a descent for "p" reaches.
        await h.Node(RootId).Received(1).AcceptSplitAsync("p", strandedParent);
        await h.Node(RightParentId).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }

    [Test]
    public async Task Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands()
    {
        var h = CreateTwoLevelHarness();
        var rightSibling = GrainId.Create("leaf", "right-sibling");
        h.Leaf(LeftLeafId).SetAsync("c", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(LeafSplit("d")));
        h.Leaf(RightLeafId).SetAsync("x", Arg.Any<byte[]>())
            .Returns(Task.FromResult<SplitResult?>(LeafSplit("y", rightSibling)));

        var firstAcceptEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstAccept = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Node(LeftParentId).AcceptSplitAsync("d", SiblingId).Returns(_ =>
        {
            firstAcceptEntered.TrySetResult();
            return releaseFirstAccept.Task;
        });

        var first = h.Grain.SetAsync("c", [1]);
        await firstAcceptEntered.Task;

        // Every call below the second link completes synchronously, so without
        // the gate its accept would already have been issued by the time the
        // call returns.
        var second = h.Grain.SetAsync("x", [2]);
        await h.Node(RightParentId).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());

        releaseFirstAccept.SetResult(null);
        await Task.WhenAll(first, second);

        await h.Node(RightParentId).Received(1).AcceptSplitAsync("y", rightSibling);
        Assert.That(h.State.State.PendingChildLinks, Is.Empty);
    }
}
