using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the root-promotion half of issue 899 / issue 1883:
/// <c>CompletePromotionAsync</c> deciding a new root's
/// <c>childrenAreLeaves</c> bit from a PERSISTED FLAG rather than from the
/// surviving root child's actual grain TYPE.
/// <para>
/// <see cref="ShardRootGrainPromotionChildTypeTests"/> pins the case where the
/// persisted <c>RootIsLeaf</c> flag is TRUTHFUL. This fixture pins the case
/// where it LIES - a shard root whose <c>RootIsLeaf</c> bit is <c>true</c>
/// while <c>RootNodeId</c> addresses an internal grain, which a census of a
/// pristine pre-epic production volume found baked into 96 of 841 shard roots.
/// Because the deeper-race guard was gated on the raw flag, a lying flag
/// SKIPPED it, and the fall-through wrapped a brand-new root above the internal
/// root while seeding it <c>childrenAreLeaves = true</c>. Downstream,
/// <c>BPlusInternalGrain.SeedChildParentAsync</c> resolved that internal child
/// through <c>IBPlusLeafGrain</c> and threw
/// <c>InvalidCastException: Unable to cast object of type 'BPlusInternalGrain'
/// to type 'IBPlusLeafGrain'</c> - observed on every reader and writer of
/// <c>ShardConsolidationChaosTests</c>. The wrap also PERSISTED a fresh root
/// whose <c>ChildrenAreLeaves</c> bit lied about its children, so each
/// occurrence baked the state that made the next one skip the guard again.
/// </para>
/// <para>
/// The guard is decided by <c>IsLeafGrainId</c>, which degrades to "everything
/// is a leaf" when the grain factory cannot yield a runtime-typed reference.
/// A plain substitute factory therefore silently no-ops the whole mechanism -
/// which is why this defect survived the existing unit coverage. The harness
/// below deliberately makes the leaf-grain-type probe RESOLVABLE, so
/// <c>IsLeafGrainId</c> is live and discriminates <c>GrainId.Create("leaf",
/// ...)</c> from <c>GrainId.Create("internal", ...)</c> exactly as the runtime
/// factory does in production.
/// </para>
/// </summary>
public sealed class ShardRootGrainPromotionInconsistentRootFlagTests
{
    private const string TreeId = "tree";
    private const int ShardIndex = 0;

    private static readonly GrainId InternalRootId = GrainId.Create("internal", "promoted-root");

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusInternalGrain Internal { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    /// <summary>
    /// Builds a shard-root harness whose grain factory resolves a real
    /// <see cref="GrainType"/> for leaf grains, so the production
    /// <c>IsLeafGrainId</c> type guard is genuinely live rather than degraded
    /// to a no-op. The leaf substitute also implements
    /// <see cref="IGrainBase"/>, which is the shape Orleans'
    /// <c>GetGrainId()</c> extension accepts for a non-<c>Grain</c>
    /// implementation, so the probe
    /// <c>GetGrain&lt;IBPlusLeafGrain&gt;(Guid.Empty).GetGrainId().Type</c>
    /// yields the "leaf" grain type instead of throwing
    /// <see cref="ArgumentException"/> and disabling the guard.
    /// </summary>
    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = InternalRootId;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var leafContext = Substitute.For<IGrainContext>();
        leafContext.GrainId.Returns(GrainId.Create("leaf", "leaf-type-probe"));
        ((IGrainBase)leaf).GrainContext.Returns(leafContext);
        leaf.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);

        var @internal = Substitute.For<IBPlusInternalGrain>();
        @internal.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        @internal.InitializeAsync(Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>())
            .Returns(Task.CompletedTask);
        @internal.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));
        @internal.GetRoutingTableAsync().Returns(LeafBearingRoutingTable());
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(@internal);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>(), Arg.Any<string>()).Returns(@internal);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(@internal);

        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Internal = @internal, State = state };
    }

    private static RoutingTableSnapshot LeafBearingRoutingTable() => new()
    {
        SeparatorKeys = [null],
        ChildIds = [GrainId.Create("leaf", "existing-leaf")],
        ChildrenAreLeaves = true,
    };

    private static RoutingTableSnapshot InternalBearingRoutingTable() => new()
    {
        SeparatorKeys = [null],
        ChildIds = [GrainId.Create("internal", "existing-inner")],
        ChildrenAreLeaves = false,
    };

    /// <summary>
    /// Sanity leg. Every assertion in this fixture is only meaningful when the
    /// leaf-grain-type probe resolves; if it does not, <c>IsLeafGrainId</c>
    /// answers <see langword="true"/> for every id and the guard under test is
    /// a no-op, which would make the other tests pass vacuously. This test
    /// fails loudly in that case by pinning the one behaviour that is
    /// observable only when the probe is live: with <c>RootIsLeaf = true</c>
    /// over an INTERNAL root id, a read must NOT take the flat-tree fast path
    /// but must descend through the internal root's routing table.
    /// </summary>
    [Test]
    public async Task Harness_resolves_the_leaf_grain_type_so_the_type_guard_is_live()
    {
        var h = CreateHarness();
        h.State.State.RootIsLeaf = true;

        await h.Grain.GetAsync("k-any");

        await h.Internal.Received().GetRoutingTableAsync();
    }

    /// <summary>
    /// Primary regression. A leaf-level pending promotion resumed against a
    /// shard root whose <c>RootIsLeaf</c> flag lies over an internal root must
    /// route the bubble INTO the existing root, never wrap a new root above it
    /// with <c>childrenAreLeaves = true</c> - the wrap is what threw
    /// <c>InvalidCastException</c> in <c>SeedChildParentAsync</c> and what
    /// persisted the next lying root.
    /// </summary>
    [Test]
    public async Task Resume_routes_via_AcceptSplitAsync_when_RootIsLeaf_flag_lies_over_an_internal_root()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = InternalRootId;
        // The baked issue-899 inconsistency: the flag claims a flat tree while
        // the persisted root id addresses an internal grain.
        h.State.State.RootIsLeaf = true;
        h.State.State.PendingPromotionRootWasLeaf = true;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-split",
            NewSiblingId = GrainId.Create("leaf", "new-leaf-sibling"),
            ChildIsLeaf = true,
        };
        h.Internal.GetRoutingTableAsync().Returns(LeafBearingRoutingTable());

        try { await h.Grain.GetAsync("k-any"); } catch { }

        await h.Internal.DidNotReceive().InitializeAsync(
            Arg.Any<string>(),
            Arg.Any<GrainId>(),
            Arg.Any<GrainId>(),
            Arg.Any<bool>());
        await h.Internal.Received(1).AcceptSplitAsync(
            "k-split",
            Arg.Is<GrainId>(id => id == GrainId.Create("leaf", "new-leaf-sibling")));
        Assert.That(h.State.State.PendingPromotion, Is.Null,
            "PendingPromotion should have been cleared after the bubble was re-routed.");
    }

    /// <summary>
    /// Deeper-race variant of the primary regression: the lying flag sits over
    /// an internal root whose own children are internal, so the leaf-level
    /// bubble cannot be spliced at this level. The stale intent must be
    /// dropped rather than used to wrap a lying root.
    /// </summary>
    [Test]
    public async Task Resume_drops_stale_pending_when_RootIsLeaf_flag_lies_over_a_deep_internal_root()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = InternalRootId;
        h.State.State.RootIsLeaf = true;
        h.State.State.PendingPromotionRootWasLeaf = true;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-mismatch",
            NewSiblingId = GrainId.Create("leaf", "stale-leaf-sibling"),
            ChildIsLeaf = true,
        };
        h.Internal.GetRoutingTableAsync().Returns(
            InternalBearingRoutingTable(),
            LeafBearingRoutingTable());

        try { await h.Grain.GetAsync("k-any"); } catch { }

        await h.Internal.DidNotReceive().InitializeAsync(
            Arg.Any<string>(),
            Arg.Any<GrainId>(),
            Arg.Any<GrainId>(),
            Arg.Any<bool>());
        await h.Internal.DidNotReceive().AcceptSplitAsync(
            Arg.Any<string>(),
            Arg.Any<GrainId>());
        Assert.That(h.State.State.PendingPromotion, Is.Null,
            "Stale pending promotion intent should have been dropped on shape mismatch.");
    }

    /// <summary>
    /// The legacy-scalar leg of the same defect. When the persisted bubble
    /// carries <c>ChildIsLeaf == false</c> the deeper-race guard is bypassed by
    /// design, and the <c>childrenAreLeaves</c> bit falls back to the
    /// <c>PendingPromotionRootWasLeaf</c> scalar - which is sampled from the
    /// same lying flag and can therefore claim leaves over an internal root.
    /// The surviving root child's actual grain type is authoritative and must
    /// clamp the bit to <see langword="false"/>, so the new root describes its
    /// children truthfully and <c>SeedChildParentAsync</c> dispatches through
    /// <c>IBPlusInternalGrain</c>.
    /// </summary>
    [Test]
    public async Task Resume_seeds_internal_children_when_PendingPromotionRootWasLeaf_lies_over_an_internal_root()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = InternalRootId;
        h.State.State.RootIsLeaf = false;
        h.State.State.PendingPromotionRootWasLeaf = true;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-deep-split",
            NewSiblingId = GrainId.Create("internal", "new-internal-sibling"),
            ChildIsLeaf = false,
        };
        h.Internal.GetRoutingTableAsync().Returns(InternalBearingRoutingTable());

        try { await h.Grain.GetAsync("k-any"); } catch { }

        await h.Internal.Received(1).InitializeAsync(
            "k-deep-split",
            Arg.Is<GrainId>(id => id == InternalRootId),
            Arg.Is<GrainId>(id => id == GrainId.Create("internal", "new-internal-sibling")),
            false);
    }
}

