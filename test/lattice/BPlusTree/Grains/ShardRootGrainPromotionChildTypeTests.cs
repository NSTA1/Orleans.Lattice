using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the U9k step 2 InvalidCastException: under
/// <c>[AlwaysInterleave]</c> on <see cref="IShardRootGrain.SetManyAsync"/>,
/// two concurrent <c>SetManyAsync</c> turns both reach
/// <c>PromoteRootAsync</c>. Before the fix, turn B could read
/// <c>state.State.RootIsLeaf</c> after turn A had already flipped it to
/// <c>false</c>, persist its own bubble against an already-promoted
/// root, then resume that intent and call <c>InitializeAsync(...)</c>
/// with the previously-promoted internal grain as one of the new
/// root's children - whose downstream <c>SeedChildParentAsync</c>
/// branch then cast the wrong grain interface and crashed with
/// <c>InvalidCastException</c>. The fix made <see cref="SplitResult"/>
/// self-describing via <see cref="SplitResult.ChildIsLeaf"/> AND
/// added an asymmetric root-shape re-check inside
/// <c>CompletePromotionAsync</c> / <c>PromoteRootAsync</c>: only when
/// the persisted bubble is leaf-level (<c>ChildIsLeaf == true</c>) AND
/// the live root has been promoted (<c>RootIsLeaf == false</c>) AND
/// the live root's <c>ChildrenAreLeaves</c> is <c>true</c> is the
/// bubble routed through the existing root via
/// <c>AcceptSplitAsync</c> instead of wrapping a second new root
/// above it. Legitimate depth->=1 root splits
/// (<c>ChildIsLeaf == false</c>) bypass the re-check entirely and
/// keep their original wrap semantics; the symmetric
/// <c>ChildrenAreLeaves == ChildIsLeaf</c> predicate the first U9k
/// step 2 fix shipped false-positived those legitimate splits and
/// silently corrupted any tree of depth >= 2 (caught by
/// <c>ChaosReshardIntegrationTests</c>,
/// <c>PublicApiContractTests.WalReactivation.Tree_with_split_leaves_survives_cluster_restart</c>,
/// and
/// <c>ReshardTopologyTests.Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard</c>).
/// This fixture pins all four legs deterministically.
/// </summary>
public sealed class ShardRootGrainPromotionChildTypeTests
{
    private const string TreeId = "tree";
    private const int ShardIndex = 0;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusInternalGrain Internal { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        // Defaults flipped by individual tests below to simulate the
        // pre-fix race window.
        state.State.RootNodeId = GrainId.Create("leaf", "root-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        // CompletePromotionAsync creates the new root via
        // grainFactory.GetGrain&lt;IBPlusInternalGrain&gt;(deterministicId)
        // where the parameter is a Guid (IBPlusInternalGrain is
        // IGrainWithGuidKey, so the Orleans overload taking Guid is the
        // one bound at compile time). Stubbing only the GrainId
        // overload causes NSubstitute to return an unrelated auto-mock
        // and the test never sees the InitializeAsync call. We stub
        // every overload the production code might bind to.
        var @internal = Substitute.For<IBPlusInternalGrain>();
        @internal.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        @internal.InitializeAsync(Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>())
            .Returns(Task.CompletedTask);
        // Default routing-table snapshot stubs the post-fix root-shape
        // re-check inside CompletePromotionAsync / PromoteRootAsync.
        // Individual tests override this via Harness.SetRootRoutingTable
        // when they need a specific ChildrenAreLeaves value.
        @internal.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null],
            ChildIds = [GrainId.Create("leaf", "stub-child")],
            ChildrenAreLeaves = true,
        });
        @internal.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));
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

    [Test]
    public async Task Resume_routes_via_AcceptSplitAsync_when_root_already_promoted()
    {
        // Reproduces the U9k step 2 race window deterministically by
        // pre-loading shard-root state with the exact shape an
        // interleaved peer would have published: a leaf-sourced
        // PendingPromotion (ChildIsLeaf = true) whose accompanying
        // PendingPromotionRootWasLeaf = false because an interleaved
        // peer's PromoteRootAsync sampled RootIsLeaf after another turn
        // had already flipped it. RootIsLeaf itself is also `false` on
        // disk for the same reason, AND the current RootNodeId now
        // points at an INTERNAL grain (which is what the prior
        // promotion installed). Under the original code,
        // CompletePromotionAsync would have wrapped a second new root
        // above that internal grain, passing the internal grain as
        // leftChild to InitializeAsync(childrenAreLeaves=true), and the
        // downstream SeedChildParentAsync would have crashed with the
        // inverse InvalidCastException (cast BPlusInternalGrain to
        // IBPlusLeafGrain). After the fix, the resume path observes
        // the current root's ChildrenAreLeaves and routes the bubble
        // INTO the existing root via AcceptSplitAsync instead.
        var h = CreateHarness();
        h.State.State.RootNodeId = GrainId.Create("internal", "promoted-root");
        h.State.State.RootIsLeaf = false;
        h.State.State.PendingPromotionRootWasLeaf = false;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-split",
            NewSiblingId = GrainId.Create("leaf", "new-leaf-sibling"),
            ChildIsLeaf = true,
        };
        // Mirror the production shape: the promoted root holds leaves.
        h.Internal.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null],
            ChildIds = [GrainId.Create("leaf", "existing-leaf")],
            ChildrenAreLeaves = true,
        });

        // GetAsync drives PrepareForOperationSlowAsync, which calls
        // ResumePendingPromotionAsync. That is the public seam through
        // which CompletePromotionAsync runs in this fixture.
        try { await h.Grain.GetAsync("k-any"); } catch { }

        // The bubble must be routed through the existing root, not
        // used to construct a second new root above it.
        await h.Internal.Received(1).AcceptSplitAsync(
            "k-split",
            Arg.Is<GrainId>(id => id == GrainId.Create("leaf", "new-leaf-sibling")));
        await h.Internal.DidNotReceive().InitializeAsync(
            Arg.Any<string>(),
            Arg.Any<GrainId>(),
            Arg.Any<GrainId>(),
            Arg.Any<bool>());
        Assert.That(h.State.State.PendingPromotion, Is.Null,
            "PendingPromotion should have been cleared after a successful re-bubble.");
    }

    [Test]
    public async Task Resume_falls_back_to_PendingPromotionRootWasLeaf_when_SplitResult_is_pre_fix_state()
    {
        // Backward-compatibility leg: an activation that crashed
        // mid-promotion under the pre-fix code path persisted a
        // SplitResult whose ChildIsLeaf field deserialises to its
        // default `false`. The only surviving signal of whether the
        // sibling holds leaves is the legacy PendingPromotionRootWasLeaf
        // scalar. CompletePromotionAsync must honour it when
        // ChildIsLeaf is false so an in-place upgrade does not lose
        // information from older persisted state. This case keeps the
        // wrap-as-new-root shape because RootIsLeaf is still true on
        // disk - the root-shape re-check in the new code is gated on
        // !RootIsLeaf.
        var h = CreateHarness();
        h.State.State.RootIsLeaf = true;
        h.State.State.PendingPromotionRootWasLeaf = true;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-split",
            NewSiblingId = GrainId.Create("leaf", "legacy-leaf-sibling"),
            // ChildIsLeaf intentionally left at default `false` to
            // simulate state persisted before the fix shipped.
        };

        try { await h.Grain.GetAsync("k-any"); } catch { }

        await h.Internal.Received(1).InitializeAsync(
            Arg.Any<string>(),
            Arg.Any<GrainId>(),
            Arg.Any<GrainId>(),
            true);
    }

    [Test]
    public async Task Resume_drops_stale_pending_when_root_level_mismatches_bubble()
    {
        // Deeper-race resume case: the persisted bubble is leaf-level
        // (ChildIsLeaf = true) but the live root's children are
        // themselves internal nodes (ChildrenAreLeaves = false). The
        // bubble's NewSiblingId belongs deeper in the tree than the
        // resume path can safely splice from here. The fix drops the
        // stale intent so the caller's write retry envelope re-routes
        // the user-visible mutation against the current topology, and
        // does NOT call InitializeAsync (which would seed the new
        // root with the wrong childrenAreLeaves bit and reproduce
        // the U9k step 2 InvalidCastException) nor AcceptSplitAsync
        // (which would inject the leaf at the wrong tree level).
        var h = CreateHarness();
        h.State.State.RootNodeId = GrainId.Create("internal", "promoted-root");
        h.State.State.RootIsLeaf = false;
        h.State.State.PendingPromotionRootWasLeaf = false;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-mismatch",
            NewSiblingId = GrainId.Create("leaf", "stale-leaf-sibling"),
            ChildIsLeaf = true,
        };
        // Live root says its children are internal nodes - bubble's
        // ChildIsLeaf = true cannot align. The second snapshot below
        // is what the inner internal child returns once the promotion
        // path has finished dropping the stale pending and GetAsync
        // continues with a normal read traversal: NSubstitute returns
        // the harness's single internal mock for every GrainId, so
        // without a leaf-bearing follow-up snapshot the descent would
        // loop forever on the same mock and hang the test host.
        h.Internal.GetRoutingTableAsync().Returns(
            new RoutingTableSnapshot
            {
                SeparatorKeys = [null],
                ChildIds = [GrainId.Create("internal", "existing-inner")],
                ChildrenAreLeaves = false,
            },
            new RoutingTableSnapshot
            {
                SeparatorKeys = [null],
                ChildIds = [GrainId.Create("leaf", "terminating-leaf")],
                ChildrenAreLeaves = true,
            });

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

    [Test]
    public async Task Resume_wraps_new_root_for_legitimate_internal_root_split()
    {
        // Acceptance leg for the U9k step 2 follow-up: the first U9k
        // step 2 fix used a symmetric `ChildrenAreLeaves == ChildIsLeaf`
        // predicate, which silently false-positived every legitimate
        // depth->=1 root split (where the splitting internal root
        // emits a same-level bubble and the live root is, correctly,
        // an internal node whose ChildrenAreLeaves matches the
        // bubble's ChildIsLeaf = false). That symmetric predicate
        // re-fed the bubble back into the splitter via
        // AcceptSplitAsync and silently corrupted any depth >= 2
        // tree, breaking Chaos/Reshard/WalReactivation suites.
        //
        // The corrected, asymmetric predicate fires only when
        // ChildIsLeaf == true (leaf-level bubble). This test pins
        // the legitimate depth->=1 case: ChildIsLeaf == false,
        // RootIsLeaf == false. The recovery branch must be skipped
        // entirely and the legacy wrap-as-new-root path must run -
        // i.e. InitializeAsync(...) MUST be called with
        // childrenAreLeaves = false to splice the new same-level
        // sibling above the existing internal root, and
        // AcceptSplitAsync MUST NOT be called (which would
        // catastrophically re-feed the bubble into the splitter).
        var h = CreateHarness();
        h.State.State.RootNodeId = GrainId.Create("internal", "current-internal-root");
        h.State.State.RootIsLeaf = false;
        h.State.State.PendingPromotionRootWasLeaf = false;
        h.State.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "k-deep-split",
            NewSiblingId = GrainId.Create("internal", "new-internal-sibling"),
            ChildIsLeaf = false,
        };
        // Live root's children are themselves internals (depth >= 2),
        // matching the same-level bubble. Under the broken symmetric
        // predicate this would trigger the rebubble fast path; under
        // the corrected asymmetric predicate it must NOT, because
        // ChildIsLeaf is false.
        h.Internal.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null],
            ChildIds = [GrainId.Create("internal", "existing-inner")],
            ChildrenAreLeaves = false,
        });

        try { await h.Grain.GetAsync("k-any"); } catch { }

        // The critical pin is the childrenAreLeaves argument: it must
        // be `false` to splice the new same-level internal sibling
        // above the existing internal root. Under the broken symmetric
        // predicate the recovery branch would short-circuit through
        // AcceptSplitAsync instead, and InitializeAsync would never
        // be invoked at all. (The subsequent `newRoot.GetGrainId()`
        // call inside CompletePromotionAsync is not satisfiable on
        // the auto-mock and throws past the try/catch above, so
        // PendingPromotion clearance is not asserted here - matching
        // the existing wrap-path test's coverage shape.)
        await h.Internal.Received(1).InitializeAsync(
            "k-deep-split",
            Arg.Is<GrainId>(id => id == GrainId.Create("internal", "current-internal-root")),
            Arg.Is<GrainId>(id => id == GrainId.Create("internal", "new-internal-sibling")),
            false);
        await h.Internal.DidNotReceive().AcceptSplitAsync(
            Arg.Any<string>(),
            Arg.Any<GrainId>());
    }
}
