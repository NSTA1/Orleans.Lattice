using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the delete half of issue #3523. A single-key delete and a
/// range delete can each divide a leaf: the leaf re-checks its declared span
/// after the WAL append, forwards a tombstone stranded by an interleaved split
/// to the sibling that now declares it, and that merge can split the sibling.
/// Only the shard root can link the resulting leaf, so these tests pin that
/// <see cref="ShardRootGrain.DeleteAsync"/> and
/// <see cref="ShardRootGrain.DeleteRangeBoundedAsync"/> link every split a
/// delete reports, and that the rolling-upgrade fallback to the untracked leaf
/// delete fires only on a fault raised before the leaf ran.
/// </summary>
[TestFixture]
public sealed class ShardRootGrainDeleteSplitLinkTests
{
    private const string TreeId = "delete-split-tree";
    private const string ShardKey = TreeId + "/0";
    private const string Separator = "m";

    private static readonly GrainId RootId = GrainId.Create("internal", "root");
    private static readonly GrainId LeftId = GrainId.Create("leaf", "left");
    private static readonly GrainId RightId = GrainId.Create("leaf", "right");
    private static readonly GrainId NewSiblingId = GrainId.Create("leaf", "new-sibling");

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusInternalGrain Root { get; init; }
        public required IBPlusLeafGrain Left { get; init; }
        public required IBPlusLeafGrain Right { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    /// <summary>
    /// An internal root over two leaves: keys below <see cref="Separator"/>
    /// route to the left leaf, the rest to the right. The left leaf's next
    /// sibling is the right leaf, and the right leaf ends the chain.
    /// </summary>
    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = RootId;
        state.State.RootIsLeaf = false;

        var left = CreateLeaf(lowInclusive: null, highExclusive: Separator, next: RightId);
        var right = CreateLeaf(lowInclusive: Separator, highExclusive: null, next: null);

        var root = Substitute.For<IBPlusInternalGrain>();
        root.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(ci => (string.CompareOrdinal(ci.Arg<string>(), Separator) < 0 ? LeftId : RightId, true));
        root.GetRoutingTableAsync()
            .Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = [null, Separator],
                ChildIds = [LeftId, RightId],
                ChildrenAreLeaves = true,
            }));
        root.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(RootId).Returns(root);
        factory.GetGrain<IBPlusLeafGrain>(LeftId).Returns(left);
        factory.GetGrain<IBPlusLeafGrain>(RightId).Returns(right);

        var cache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Substitute.For<IShardRootGrain>());

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers()),
            Root = root,
            Left = left,
            Right = right,
            State = state,
        };
    }

    private static IBPlusLeafGrain CreateLeaf(string? lowInclusive, string? highExclusive, GrainId? next)
    {
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.DeleteTrackedAsync(Arg.Any<string>())
            .Returns(Task.FromResult(new LeafDeleteResult { Deleted = true }));
        leaf.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 0, PastRange = next is null }));
        leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = lowInclusive,
            HighKeyExclusive = highExclusive,
        }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
        leaf.GetPrevSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        return leaf;
    }

    private static SplitResult LeafSplit(string promotedKey) => new()
    {
        PromotedKey = promotedKey,
        NewSiblingId = NewSiblingId,
        ChildIsLeaf = true,
    };

    /// <summary>
    /// Builds the fallback Orleans substitutes for a remote exception it cannot
    /// deserialize. Its <c>ExceptionType</c> setter is not public (the codec
    /// fills it in), so the test sets it the same way.
    /// </summary>
    private static UnavailableExceptionFallbackException Fallback(string exceptionType)
    {
        var fallback = new UnavailableExceptionFallbackException("remote fault", innerException: null!);
        typeof(UnavailableExceptionFallbackException)
            .GetProperty(nameof(UnavailableExceptionFallbackException.ExceptionType))!
            .SetValue(fallback, exceptionType);
        return fallback;
    }

    private static IEnumerable<TestCaseData> MethodUnavailableFaults()
    {
        yield return new TestCaseData(new TypeLoadException("unknown alias"))
            .SetName("IsLeafMethodUnavailableFault_type_load_exception_returns_true");
        yield return new TestCaseData(new SerializerException("unknown request type"))
            .SetName("IsLeafMethodUnavailableFault_serializer_exception_returns_true");
        yield return new TestCaseData(new OrleansException("wrapped", new TypeLoadException("unknown alias")))
            .SetName("IsLeafMethodUnavailableFault_type_load_exception_as_inner_returns_true");
        yield return new TestCaseData(new AggregateException(new OrleansException("wrapped", new SerializerException("x"))))
            .SetName("IsLeafMethodUnavailableFault_serializer_exception_nested_two_deep_returns_true");
        yield return new TestCaseData(Fallback(typeof(TypeLoadException).FullName!))
            .SetName("IsLeafMethodUnavailableFault_fallback_carrying_type_load_exception_returns_true");
        yield return new TestCaseData(Fallback("Orleans.Serialization.CodecNotFoundException"))
            .SetName("IsLeafMethodUnavailableFault_fallback_carrying_orleans_serialization_fault_returns_true");
    }

    private static IEnumerable<TestCaseData> OrdinaryFaults()
    {
        yield return new TestCaseData(new InvalidOperationException("leaf fault"))
            .SetName("IsLeafMethodUnavailableFault_invalid_operation_exception_returns_false");
        yield return new TestCaseData(new TimeoutException("slow"))
            .SetName("IsLeafMethodUnavailableFault_timeout_exception_returns_false");
        yield return new TestCaseData(new OrleansException("wrapped", new InvalidOperationException("leaf fault")))
            .SetName("IsLeafMethodUnavailableFault_ordinary_inner_exception_returns_false");
        yield return new TestCaseData(Fallback(typeof(InvalidOperationException).FullName!))
            .SetName("IsLeafMethodUnavailableFault_fallback_carrying_ordinary_fault_returns_false");
        yield return new TestCaseData(new UnavailableExceptionFallbackException("no type", innerException: null!))
            .SetName("IsLeafMethodUnavailableFault_fallback_without_exception_type_returns_false");
    }

    [TestCaseSource(nameof(MethodUnavailableFaults))]
    public void IsLeafMethodUnavailableFault_recognises_request_type_resolution_faults(Exception fault)
    {
        Assert.That(ShardRootGrain.IsLeafMethodUnavailableFault(fault), Is.True);
    }

    [TestCaseSource(nameof(OrdinaryFaults))]
    public void IsLeafMethodUnavailableFault_rejects_faults_the_leaf_may_have_raised_after_running(Exception fault)
    {
        Assert.That(ShardRootGrain.IsLeafMethodUnavailableFault(fault), Is.False);
    }

    [Test]
    public async Task DeleteAsync_links_the_split_the_leaf_delete_reports()
    {
        var h = CreateHarness();
        h.Left.DeleteTrackedAsync("c")
            .Returns(Task.FromResult(new LeafDeleteResult { Deleted = true, Split = LeafSplit("d") }));

        var deleted = await h.Grain.DeleteAsync("c");

        Assert.That(deleted, Is.True);
        await h.Root.Received(1).AcceptSplitAsync("d", NewSiblingId);
        Assert.That(h.State.State.PendingChildLinks, Is.Empty,
            "the link intent must be retired once the parent accepted the split");
    }

    [Test]
    public async Task DeleteAsync_links_every_division_a_forwarded_split_carries()
    {
        var h = CreateHarness();
        var secondSibling = GrainId.Create("leaf", "second-sibling");
        var split = LeafSplit("d") with
        {
            Forwarded = true,
            Additional = [new SplitResult { PromotedKey = "q", NewSiblingId = secondSibling, ChildIsLeaf = true }],
        };
        h.Left.DeleteTrackedAsync("c")
            .Returns(Task.FromResult(new LeafDeleteResult { Deleted = true, Split = split }));

        await h.Grain.DeleteAsync("c");

        await h.Root.Received(1).AcceptSplitAsync("d", NewSiblingId);
        await h.Root.Received(1).AcceptSplitAsync("q", secondSibling);
    }

    [Test]
    public async Task DeleteAsync_without_a_split_does_not_touch_the_parent()
    {
        var h = CreateHarness();

        var deleted = await h.Grain.DeleteAsync("c");

        Assert.That(deleted, Is.True);
        await h.Left.Received(1).DeleteTrackedAsync("c");
        await h.Root.DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
    }

    [Test]
    public async Task DeleteAsync_returns_false_for_an_absent_key_reported_by_the_tracked_delete()
    {
        var h = CreateHarness();
        h.Right.DeleteTrackedAsync("x").Returns(Task.FromResult(new LeafDeleteResult { Deleted = false }));

        var deleted = await h.Grain.DeleteAsync("x");

        Assert.That(deleted, Is.False);
        await h.Left.DidNotReceive().DeleteTrackedAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DeleteAsync_falls_back_to_the_untracked_delete_when_the_leaf_silo_predates_it()
    {
        var h = CreateHarness();
        h.Left.DeleteTrackedAsync("c").ThrowsAsync(new TypeLoadException("unknown alias"));
        h.Left.DeleteAsync("c").Returns(Task.FromResult(true));

        var deleted = await h.Grain.DeleteAsync("c");

        Assert.That(deleted, Is.True);
        await h.Left.Received(1).DeleteAsync("c");
        await h.Root.DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
    }

    [Test]
    public async Task DeleteAsync_fallback_reports_the_untracked_delete_result()
    {
        var h = CreateHarness();
        h.Left.DeleteTrackedAsync("c").ThrowsAsync(new SerializerException("unknown request type"));
        h.Left.DeleteAsync("c").Returns(Task.FromResult(false));

        var deleted = await h.Grain.DeleteAsync("c");

        Assert.That(deleted, Is.False);
    }

    [Test]
    public void DeleteAsync_does_not_reissue_a_delete_that_failed_after_the_leaf_ran()
    {
        var h = CreateHarness();
        h.Left.DeleteTrackedAsync("c").ThrowsAsync(new InvalidOperationException("leaf fault"));

        Assert.That(async () => await h.Grain.DeleteAsync("c"),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("leaf fault"));
        _ = h.Left.DidNotReceive().DeleteAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DeleteAsync_promotes_the_root_when_a_root_leaf_delete_splits()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var state = new FakePersistentState<ShardRootState>();
        var rootLeafId = GrainId.Create("leaf", "root-leaf");
        state.State.RootNodeId = rootLeafId;
        state.State.RootIsLeaf = true;

        var leaf = CreateLeaf(lowInclusive: null, highExclusive: null, next: null);
        leaf.DeleteTrackedAsync("c")
            .Returns(Task.FromResult(new LeafDeleteResult { Deleted = true, Split = LeafSplit("d") }));

        var newRoot = Substitute.For<IBPlusInternalGrain>();
        newRoot.InitializeAsync(Arg.Any<string>(), Arg.Any<GrainId>(), Arg.Any<GrainId>(), Arg.Any<bool>())
            .Returns(Task.CompletedTask);
        newRoot.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null, "d"],
            ChildIds = [rootLeafId, NewSiblingId],
            ChildrenAreLeaves = true,
        });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(newRoot);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>(), Arg.Any<string>()).Returns(newRoot);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(newRoot);
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Substitute.For<IShardRootGrain>());

        var grain = new ShardRootGrain(context, state, factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory),
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());

        // A mocked internal grain cannot report its own GrainId, so the
        // promotion may fault after seeding the new root; the seam under test
        // is that the split reached the promotion at all.
        try { await grain.DeleteAsync("c"); } catch { }

        await newRoot.Received(1).InitializeAsync("d", rootLeafId, NewSiblingId, true);
    }

    [Test]
    public async Task DeleteRangeBoundedAsync_links_the_split_a_leaf_range_delete_reports()
    {
        var h = CreateHarness();
        h.Left.DeleteRangeAsync("a", "z", Arg.Any<LatticePredicateNode?>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 2, PastRange = false, Split = LeafSplit("d") }));
        h.Right.DeleteRangeAsync("a", "z", Arg.Any<LatticePredicateNode?>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 1, PastRange = true }));

        var page = await h.Grain.DeleteRangeBoundedAsync("a", "z");

        Assert.That(page.Deleted, Is.EqualTo(3));
        await h.Root.Received(1).AcceptSplitAsync("d", NewSiblingId);
        await h.Right.Received(1).DeleteRangeAsync("a", "z", Arg.Any<LatticePredicateNode?>());
    }

    [Test]
    public async Task DeleteRangeBoundedAsync_without_a_split_does_not_touch_the_parent()
    {
        var h = CreateHarness();
        h.Left.DeleteRangeAsync("a", "z", Arg.Any<LatticePredicateNode?>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 2, PastRange = false }));
        h.Right.DeleteRangeAsync("a", "z", Arg.Any<LatticePredicateNode?>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 1, PastRange = true }));

        var page = await h.Grain.DeleteRangeBoundedAsync("a", "z");

        Assert.That(page.Deleted, Is.EqualTo(3));
        await h.Root.DidNotReceive().AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>());
    }
}
