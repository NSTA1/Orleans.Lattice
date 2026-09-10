using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Runtime-free unit coverage for the shard root's bulk-load and bulk-append
/// graft path (<c>ShardRootGrain.BulkLoad.cs</c>).
/// <para>
/// The integration suite drives the common shapes - a flat shard growing into a
/// two-level tree - but three arms it cannot reach cheaply are where the
/// interesting failures live: the graft resumption that a crash between
/// "intent persisted" and "graft applied" depends on, the multi-level descent to
/// the rightmost leaf, and the promoted-key bubble that reaches the root and
/// grows the tree by a level. Each is exercised here by seeding the persisted
/// <see cref="PendingBulkGraft"/> directly and substituting the node tier, so
/// the shape is arranged rather than provoked.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainBulkLoadGraftTests
{
    private const string TreeId = "bulk-tree";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required Dictionary<Guid, IBPlusLeafGrain> Leaves { get; init; }
        public required Dictionary<Guid, IBPlusInternalGrain> Internals { get; init; }

        public IBPlusLeafGrain Leaf(GrainId id) => Leaves[id.GetGuidKey()];

        public IBPlusInternalGrain Internal(GrainId id) => Internals[id.GetGuidKey()];

        /// <summary>Registers a leaf substitute under a fresh id.</summary>
        public GrainId AddLeaf()
        {
            var key = Guid.NewGuid();
            var leaf = MakeLeaf(key);
            Leaves[key] = leaf;
            return GrainId.Create("leaf", key.ToString("N"));
        }

        /// <summary>
        /// Registers an internal-node substitute under a fresh id. When
        /// <paramref name="child"/> is supplied the node reports it as its only
        /// child, so the rightmost-edge descent has a consistent routing table
        /// to walk; otherwise it reports a freshly-minted leaf.
        /// </summary>
        public GrainId AddInternal(bool childrenAreLeaves, GrainId? child = null)
        {
            // Resolve the child id BEFORE configuring the node: AddLeaf builds
            // another substitute, and NSubstitute rejects a nested substitute
            // call made between a call and its Returns(...).
            var childId = child ?? AddLeaf();
            var key = Guid.NewGuid();
            var node = MakeInternal(key);
            var routing = new RoutingTableSnapshot
            {
                SeparatorKeys = [null],
                ChildIds = [childId],
                ChildrenAreLeaves = childrenAreLeaves,
            };
            node.AreChildrenLeavesAsync().Returns(Task.FromResult(childrenAreLeaves));
            node.GetRoutingTableAsync().Returns(Task.FromResult(routing));
            Internals[key] = node;
            return GrainId.Create("internal", key.ToString("N"));
        }
    }

    /// <summary>
    /// Builds a leaf substitute that also implements <see cref="IGrainBase"/>,
    /// which is the shape Orleans' <c>GetGrainId()</c> extension accepts for a
    /// non-<c>Grain</c> implementation. Bulk load calls it on every leaf it
    /// mints, so a plain proxy would throw before any assembly happened.
    /// </summary>
    private static IBPlusLeafGrain MakeLeaf(Guid key)
    {
        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var leafContext = Substitute.For<IGrainContext>();
        leafContext.GrainId.Returns(GrainId.Create("leaf", key.ToString("N")));
        ((IGrainBase)leaf).GrainContext.Returns(leafContext);

        // The append path reads the rightmost leaf's key set to decide how much
        // room is left in it. A substitute returns null for a List-typed result,
        // so seed an empty leaf explicitly.
        leaf.GetKeysAsync().ReturnsForAnyArgs(Task.FromResult(new List<string>()));
        return leaf;
    }

    private static IBPlusInternalGrain MakeInternal(Guid key)
    {
        var node = Substitute.For<IBPlusInternalGrain, IGrainBase>();
        var nodeContext = Substitute.For<IGrainContext>();
        nodeContext.GrainId.Returns(GrainId.Create("internal", key.ToString("N")));
        ((IGrainBase)node).GrainContext.Returns(nodeContext);
        node.AreChildrenLeavesAsync().Returns(Task.FromResult(true));
        node.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));
        node.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
        {
            SeparatorKeys = [null],
            ChildIds = [GrainId.Create("leaf", Guid.NewGuid().ToString("N"))],
            ChildrenAreLeaves = true,
        }));
        return node;
    }

    private static Harness CreateHarness(int maxLeafKeys = 2)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/0"));

        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();
        var leaves = new Dictionary<Guid, IBPlusLeafGrain>();
        var internals = new Dictionary<Guid, IBPlusInternalGrain>();

        // A node the test did not register would otherwise resolve to a fresh
        // substitute reporting zeroes, so an unregistered lookup fails loudly.
        // Bulk load mints deterministic ids for the leaves it creates, so those
        // are registered on demand rather than up front.
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
            ResolveLeaf(leaves, call.ArgAt<Guid>(0)));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(call =>
            ResolveLeaf(leaves, call.ArgAt<GrainId>(0).GetGuidKey()));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(call =>
            ResolveInternal(internals, call.ArgAt<Guid>(0)));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(call =>
            ResolveInternal(internals, call.ArgAt<GrainId>(0).GetGuidKey()));

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions(),
                maxLeafKeys: maxLeafKeys,
                shardCount: 1,
                factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness
        {
            Grain = grain,
            State = state,
            Factory = factory,
            Leaves = leaves,
            Internals = internals,
        };
    }

    private static IBPlusLeafGrain ResolveLeaf(Dictionary<Guid, IBPlusLeafGrain> table, Guid key)
    {
        if (!table.TryGetValue(key, out var leaf))
        {
            leaf = MakeLeaf(key);
            table[key] = leaf;
        }

        return leaf;
    }

    private static IBPlusInternalGrain ResolveInternal(Dictionary<Guid, IBPlusInternalGrain> table, Guid key)
    {
        if (!table.TryGetValue(key, out var node))
        {
            node = MakeInternal(key);
            table[key] = node;
        }

        return node;
    }

    private static List<KeyValuePair<string, byte[]>> Pairs(int count) =>
        Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create($"k{i:D4}", new byte[] { (byte)i }))
            .ToList();

    private static List<LwwEntry> RawEntries(int count) =>
        Enumerable.Range(0, count)
            .Select(i => new LwwEntry
            {
                Key = $"k{i:D4}",
                Value = [(byte)i],
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            })
            .ToList();

    // --- BulkLoadRawAsync preconditions ---

    [Test]
    public void BulkLoadRaw_refuses_a_shard_that_already_holds_data()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = h.AddLeaf();

        // The raw loader assembles a tree from scratch and installs it as the
        // root, so running it over an existing tree would orphan every node
        // already linked beneath that root rather than merge into it.
        Assert.That(
            async () => await h.Grain.BulkLoadRawAsync("op-1", RawEntries(2)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("requires an empty shard"));
    }

    [Test]
    public async Task BulkLoadRaw_is_idempotent_for_a_completed_operation_id()
    {
        var h = CreateHarness();
        h.State.State.LastCompletedBulkOperationId = "op-1";
        h.State.State.RootNodeId = h.AddLeaf();

        // Re-delivery of a completed operation must short-circuit BEFORE the
        // empty-shard guard, or a retried load would fail instead of no-opping.
        await h.Grain.BulkLoadRawAsync("op-1", RawEntries(2));

        Assert.That(h.State.WriteCount, Is.Zero);
    }

    [Test]
    public async Task BulkLoadRaw_with_no_entries_is_a_no_op()
    {
        var h = CreateHarness();

        await h.Grain.BulkLoadRawAsync("op-1", []);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootNodeId, Is.Null);
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.Null);
        });
    }

    [Test]
    public async Task BulkLoadRaw_links_the_leaf_chain_in_both_directions()
    {
        var h = CreateHarness(maxLeafKeys: 2);

        // Five entries at two keys per leaf produce three leaves, so the chain
        // has two links. A missing back-pointer is invisible to a forward scan
        // and only surfaces later as a broken reverse walk or a botched merge.
        await h.Grain.BulkLoadRawAsync("op-1", RawEntries(5));

        // The four birth-time setters collapse into one InitializeSiblingAsync
        // batch, so the chain is asserted through that call's payload. That is
        // a stronger check than counting setter invocations was: it reads the
        // pointer values the leaf was actually handed rather than observing
        // only that some setter ran.
        var inits = h.Leaves.Values
            .SelectMany(l => l.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))
                .Select(c => (SiblingInitialization)c.GetArguments()[0]!))
            .ToList();

        var forward = inits.Count(i => i.NextSibling is not null);
        var chained = inits.Count(i => i.PrevSibling is not null);

        Assert.Multiple(() =>
        {
            Assert.That(inits, Has.Count.EqualTo(3), "one initialization batch per leaf");
            Assert.That(forward, Is.EqualTo(2), "each leaf but the last points forward");
            Assert.That(chained, Is.EqualTo(2), "each leaf but the first points back");
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("op-1"));
        });
    }

    // --- Graft resumption ---

    /// <summary>
    /// Seeds the persisted intent a crash would leave behind: the new leaves are
    /// built and durable, but nothing has been linked into the tree yet.
    /// </summary>
    private static GrainId SeedPendingGraft(Harness h, string operationId, bool rootWasLeaf, int newLeafCount = 1)
    {
        var existing = h.AddLeaf();
        var entries = new List<GraftEntry>();
        for (var i = 0; i < newLeafCount; i++)
        {
            entries.Add(new GraftEntry { SeparatorKey = $"s{i:D2}", LeafId = h.AddLeaf() });
        }

        h.State.State.RootIsLeaf = rootWasLeaf;
        h.State.State.RootNodeId = rootWasLeaf ? existing : h.AddInternal(childrenAreLeaves: true);
        h.State.State.PendingBulkGraft = new PendingBulkGraft
        {
            OperationId = operationId,
            ExistingRightmostLeafId = existing,
            NewLeaves = entries,
            RootWasLeaf = rootWasLeaf,
        };

        return existing;
    }

    [Test]
    public async Task A_redelivered_append_for_the_pending_operation_completes_the_graft_and_stops()
    {
        var h = CreateHarness();
        var existing = SeedPendingGraft(h, "op-1", rootWasLeaf: false);

        // The caller retried the same operation after a crash. The durable graft
        // must be finished and the call must NOT go on to build a second set of
        // leaves for entries the first attempt already persisted.
        await h.Grain.BulkAppendAsync("op-1", Pairs(4));

        await h.Leaf(existing).Received().SetNextSiblingAsync(Arg.Any<GrainId>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public async Task A_new_append_first_completes_an_older_pending_graft()
    {
        var h = CreateHarness();
        var existing = SeedPendingGraft(h, "old-op", rootWasLeaf: false);

        // A different operation must not be layered on top of an unfinished
        // graft: the older intent is applied first so the tree is consistent
        // before the new entries are appended to its rightmost leaf.
        await h.Grain.BulkAppendAsync("new-op", Pairs(2));

        await h.Leaf(existing).Received().SetNextSiblingAsync(Arg.Any<GrainId>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("new-op"),
                "the new operation must be the one recorded as complete");
        });
    }

    [Test]
    public async Task A_graft_over_a_flat_shard_promotes_the_root()
    {
        var h = CreateHarness();
        SeedPendingGraft(h, "op-1", rootWasLeaf: true);

        await h.Grain.BulkAppendAsync("op-1", Pairs(1));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.False, "the flat shard must gain an internal root.");
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
        });
    }

    // --- Multi-level descent and root promotion ---

    [Test]
    public async Task A_graft_descends_past_an_internal_level_to_reach_the_rightmost_parent()
    {
        var h = CreateHarness();
        var existing = SeedPendingGraft(h, "op-1", rootWasLeaf: false);

        // Rebuild the seeded root as a three-level tree: root -> mid -> leaves.
        // The descent must walk to the node whose children ARE leaves; stopping
        // at the root would splice the new leaf in as a sibling of an internal
        // node and corrupt the level invariant.
        var mid = h.AddInternal(childrenAreLeaves: true);
        var root = h.AddInternal(childrenAreLeaves: false, child: mid);
        h.Internal(root).GetRightmostChildAsync().Returns(Task.FromResult(mid));
        h.State.State.RootNodeId = root;

        await h.Grain.BulkAppendAsync("op-1", Pairs(1));

        await h.Internal(root).Received().GetRightmostChildAsync();
        await h.Internal(mid).Received().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        await h.Internal(root).DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
        Assert.That(existing, Is.Not.EqualTo(default(GrainId)));
    }

    [Test]
    public async Task A_bubble_that_reaches_the_top_of_the_descent_path_promotes_a_new_root()
    {
        var h = CreateHarness();
        SeedPendingGraft(h, "op-1", rootWasLeaf: false);

        var root = h.AddInternal(childrenAreLeaves: true);
        h.State.State.RootNodeId = root;

        // The root absorbed nothing and split, so the promoted separator has
        // nowhere left to go: the tree must grow a level rather than drop the
        // bubble, which would silently orphan the new sibling subtree.
        var newSibling = h.AddInternal(childrenAreLeaves: true);
        h.Internal(root).AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(new SplitResult
            {
                PromotedKey = "promoted",
                NewSiblingId = newSibling,
                ChildIsLeaf = false,
            }));

        await h.Grain.BulkAppendAsync("op-1", Pairs(1));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootNodeId, Is.Not.EqualTo(root),
                "a bubble reaching the top must install a fresh root above the old one.");
            Assert.That(h.State.State.RootIsLeaf, Is.False);
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
        });
    }

    [Test]
    public async Task An_ordinary_operation_resumes_an_interrupted_graft_before_serving()
    {
        var h = CreateHarness();
        var existing = SeedPendingGraft(h, "op-1", rootWasLeaf: false);

        // Crash recovery: the activation persisted the graft intent and then
        // died before linking it in. Nothing re-drives BulkAppendAsync, so the
        // next ordinary operation is what must finish it - otherwise the new
        // leaves stay durable but unreachable, invisible to every read.
        _ = await h.Grain.GetAsync("k0000");

        await h.Leaf(existing).Received().SetNextSiblingAsync(Arg.Any<GrainId>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public async Task A_bubble_absorbed_by_the_parent_leaves_the_root_alone()
    {
        var h = CreateHarness();
        SeedPendingGraft(h, "op-1", rootWasLeaf: false);

        var root = h.AddInternal(childrenAreLeaves: true);
        h.State.State.RootNodeId = root;
        h.Internal(root).AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));

        await h.Grain.BulkAppendAsync("op-1", Pairs(1));

        Assert.That(h.State.State.RootNodeId, Is.EqualTo(root),
            "an absorbed split must not grow the tree.");
    }
}
