using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Partial admission at the shard root (issue #5970): when a leaf of a batch is
/// refused for saturation, the shard reports exactly the refused leaves' keys,
/// links the splits of the leaves that committed, and otherwise keeps today's
/// rethrow.
/// </summary>
[TestFixture]
public class ShardRootGrainSetManyPartialAdmissionTests
{
    private const string TreeId = "partial-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Left { get; init; }
        public required IBPlusLeafGrain Right { get; init; }
        public required IBPlusInternalGrain Root { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    /// <summary>
    /// One internal root routing keys below "m" to the left leaf and the rest to
    /// the right leaf.
    /// </summary>
    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var leftId = GrainId.Create("leaf", "left");
        var rightId = GrainId.Create("leaf", "right");
        var rootId = GrainId.Create("internal", "root");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var left = Substitute.For<IBPlusLeafGrain>();
        left.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.FromResult<SplitResult?>(null));
        var right = Substitute.For<IBPlusLeafGrain>();
        right.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.FromResult<SplitResult?>(null));

        var root = Substitute.For<IBPlusInternalGrain>();
        root.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(ci => (string.Compare(ci.Arg<string>(), "m", StringComparison.Ordinal) < 0 ? leftId : rightId, true));
        root.GetRoutingTableAsync()
            .Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[] { null, "m" },
                ChildIds = new[] { leftId, rightId },
                ChildrenAreLeaves = true,
            }));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusLeafGrain>(leftId).Returns(left);
        factory.GetGrain<IBPlusLeafGrain>(rightId).Returns(right);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver, NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers()),
            Left = left,
            Right = right,
            Root = root,
            State = state,
        };
    }

    private static List<KeyValuePair<string, byte[]>> Batch(params string[] keys)
        => keys.Select(k => new KeyValuePair<string, byte[]>(k, [1])).ToList();

    private static LatticeSaturatedException ReplayRefusal()
        => new("replay permit admission refused", TreeId, LatticeSaturationSource.ReplayPermitAdmission);

    [Test]
    public async Task A_refused_leaf_reports_exactly_its_keys_and_the_committed_leafs_split_is_linked()
    {
        var h = CreateHarness();
        var split = new SplitResult { PromotedKey = "c", NewSiblingId = GrainId.Create("leaf", "left-sibling") };
        h.Left.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.FromResult<SplitResult?>(split));
        h.Right.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw ReplayRefusal());
        h.Root.AcceptSplitAsync(split.PromotedKey, split.NewSiblingId).Returns(Task.FromResult<SplitResult?>(null));

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await h.Grain.SetManyAsync(Batch("a", "b", "x", "y", "x")));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
            Assert.That(thrown.UnappliedKeys, Is.EquivalentTo(new[] { "x", "y" }),
                "exactly the refused leaf's keys, each once; the committed leaf's keys are confirmed");
        });
        await h.Root.Received(1).AcceptSplitAsync(split.PromotedKey, split.NewSiblingId);
    }

    [Test]
    public async Task A_split_link_that_fails_on_the_refusal_path_never_masks_the_refusal()
    {
        var h = CreateHarness();
        var split = new SplitResult { PromotedKey = "c", NewSiblingId = GrainId.Create("leaf", "left-sibling") };
        h.Left.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.FromResult<SplitResult?>(split));
        h.Right.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw ReplayRefusal());
        h.Root.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns<Task<SplitResult?>>(_ => throw new InvalidOperationException("link failed"));

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await h.Grain.SetManyAsync(Batch("a", "x")));

        Assert.That(thrown!.UnappliedKeys, Is.EquivalentTo(new[] { "x" }));
        await h.Root.Received().AcceptSplitAsync(split.PromotedKey, split.NewSiblingId);
    }

    [Test]
    public async Task A_fault_other_than_saturation_keeps_todays_rethrow_with_no_list()
    {
        var h = CreateHarness();
        h.Left.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw ReplayRefusal());
        h.Right.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw new InvalidOperationException("permanent"));

        Exception? thrown = null;
        try
        {
            await h.Grain.SetManyAsync(Batch("a", "x"));
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        Assert.That(thrown, Is.Not.Null);
        Assert.That((thrown as LatticeSaturatedException)?.UnappliedKeys, Is.Null,
            "with a fault that is not saturation the outcome cannot be attributed key by key");
    }

    [Test]
    public void A_shard_with_moved_slots_to_forward_reports_no_list()
    {
        var h = CreateHarness();
        // A slot no key of the batch maps to: the shard still has moved slots to forward.
        h.State.State.MovedAwaySlots[int.MaxValue - 1] = 1;
        h.Right.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task<SplitResult?>>(_ => throw ReplayRefusal());

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await h.Grain.SetManyAsync(Batch("a", "x")));

        Assert.That(thrown!.UnappliedKeys, Is.Null,
            "a committed key still owes its forward to the slot's new owner, so it cannot be confirmed");
    }
}
