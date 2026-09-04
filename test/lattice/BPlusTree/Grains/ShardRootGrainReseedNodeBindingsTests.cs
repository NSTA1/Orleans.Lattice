using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="ShardRootGrain.ReseedNodeBindingsAsync"/> - the
/// post-recovery repair that re-asserts every node's tree id and shard index
/// so an interrupted purge cannot leave a routable node unbound and therefore
/// rejecting typed CRDT writes.
/// <para>
/// The internal-rooted descent, the best-effort catch arm, and the
/// <c>MaxReseedNodes</c> budget truncation are all structurally unreachable
/// from a flat-tree fixture, which is why they need their own topology shapes
/// here.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainReseedNodeBindingsTests
{
    private const string ShardKey = "reseed-tree/3";

    /// <summary>Mirrors the private <c>ShardRootGrain.MaxReseedNodes</c> repair budget.</summary>
    private const int MaxReseedNodes = 4096;

    [Test]
    public async Task ReseedNodeBindingsAsync_is_a_no_op_when_shard_has_no_root()
    {
        var harness = new ReseedHarness();

        await harness.Grain.ReseedNodeBindingsAsync();

        Assert.That(harness.Logger.Warnings, Is.Empty);
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_rebinds_the_single_root_leaf_when_tree_is_flat()
    {
        var harness = new ReseedHarness();
        var l0 = harness.Leaf("L0");
        harness.State.State.RootNodeId = l0.Id;
        harness.State.State.RootIsLeaf = true;

        await harness.Grain.ReseedNodeBindingsAsync();

        await l0.Grain.Received(1).SetTreeIdAsync("reseed-tree");
        await l0.Grain.Received(1).SetShardIndexAsync(3);
        Assert.That(harness.Logger.Warnings, Is.Empty);
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_descends_internal_nodes_and_rebinds_every_leaf()
    {
        // I0 (children internal) -> [I1, I2]; I1 -> [L0, L1]; I2 -> [L2].
        // The descent must reach every leaf routing can still deliver to, not
        // just the leftmost one, because a split inherits its donor's binding
        // verbatim and can mint an unbound sibling anywhere in the key range.
        var harness = new ReseedHarness();
        var l0 = harness.Leaf("L0");
        var l1 = harness.Leaf("L1");
        var l2 = harness.Leaf("L2");
        var i1 = harness.Internal("I1", childrenAreLeaves: true, children: [l0.Id, l1.Id]);
        var i2 = harness.Internal("I2", childrenAreLeaves: true, children: [l2.Id]);
        var i0 = harness.Internal("I0", childrenAreLeaves: false, children: [i1.Id, i2.Id]);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        await harness.Grain.ReseedNodeBindingsAsync();

        foreach (var leaf in new[] { l0, l1, l2 })
        {
            await leaf.Grain.Received(1).SetTreeIdAsync("reseed-tree");
            await leaf.Grain.Received(1).SetShardIndexAsync(3);
        }

        foreach (var node in new[] { i0, i1, i2 })
        {
            await node.Grain.Received(1).SetTreeIdAsync("reseed-tree");
        }

        Assert.That(harness.Logger.Warnings, Is.Empty);
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_degrades_to_no_repair_and_warns_when_the_walk_throws()
    {
        // A topology whose internal root was itself cleared has nothing to
        // descend, and a node's silo may be momentarily unreachable. Recovery
        // succeeded before this repair existed, so degrading to "no repair" is
        // strictly no worse than the status quo - throwing would make recovery
        // newly fragile.
        var harness = new ReseedHarness();
        var l0 = harness.Leaf("L0");
        var i0 = harness.Internal("I0", childrenAreLeaves: true, children: [l0.Id]);
        i0.Grain.AreChildrenLeavesAsync().Throws(new TimeoutException("node silo unreachable"));

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        Assert.DoesNotThrowAsync(async () => await harness.Grain.ReseedNodeBindingsAsync());

        // No binding was re-asserted, and the degradation is reported.
        await l0.Grain.DidNotReceive().SetTreeIdAsync(Arg.Any<string>());
        Assert.That(harness.Logger.Warnings, Has.Count.EqualTo(1));
        Assert.That(harness.Logger.Warnings[0], Does.Contain("re-assert node bindings after recovery"));
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_truncates_at_the_repair_budget_and_warns()
    {
        // The repair runs inside one grain call, and an unbounded node walk in
        // one grain call is what stranded the topology in the first place, so
        // the walk is capped at MaxReseedNodes and the overrun is reported
        // rather than allowed to run long.
        //
        // Each pushed child contributes one internal node plus one leaf, so a
        // root fanning out to (MaxReseedNodes / 2) + 8 children overruns the
        // budget with room to spare.
        var harness = new ReseedHarness();
        var childCount = (MaxReseedNodes / 2) + 8;
        var sharedLeaf = harness.Leaf("Lshared");
        var children = new List<GrainId>(childCount);
        for (var i = 0; i < childCount; i++)
        {
            children.Add(harness.Internal($"I{i}", childrenAreLeaves: true, children: [sharedLeaf.Id]).Id);
        }

        var root = harness.Internal("Iroot", childrenAreLeaves: false, children: children);
        harness.State.State.RootNodeId = root.Id;
        harness.State.State.RootIsLeaf = false;

        await harness.Grain.ReseedNodeBindingsAsync();

        Assert.That(harness.Logger.Warnings, Has.Count.EqualTo(1));
        Assert.That(harness.Logger.Warnings[0], Does.Contain("repair budget"));
        // The walk stopped at the budget rather than visiting every child.
        await root.Grain.Received(1).SetTreeIdAsync("reseed-tree");
        Assert.That(harness.InternalNodesWalked, Is.LessThan(childCount));
    }

    /// <summary>
    /// Directly-constructed <see cref="ShardRootGrain"/> plus node substitutes,
    /// with a capturing logger so the best-effort warning arms are observable.
    /// </summary>
    private sealed class ReseedHarness
    {
        public ReseedHarness()
        {
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("shard", ShardKey));

            Grain = new ShardRootGrain(
                context,
                State,
                Factory,
                TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: Factory),
                Logger,
                TestMutationObservers.NoObservers());
        }

        public IGrainFactory Factory { get; } = Substitute.For<IGrainFactory>();

        public FakePersistentState<ShardRootState> State { get; } = new();

        public CapturingLogger<ShardRootGrain> Logger { get; } = new();

        public ShardRootGrain Grain { get; }

        /// <summary>Number of distinct internal nodes the descent asked for children.</summary>
        public int InternalNodesWalked { get; private set; }

        public LeafNode Leaf(string key)
        {
            var id = GrainId.Create("leaf", $"{ShardKey}:{key}");
            var grain = Substitute.For<IBPlusLeafGrain>();
            grain.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
            grain.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
            Factory.GetGrain<IBPlusLeafGrain>(id).Returns(grain);
            return new LeafNode(id, grain);
        }

        public InternalNode Internal(string key, bool childrenAreLeaves, IReadOnlyList<GrainId> children)
        {
            var id = GrainId.Create("internal", $"{ShardKey}:{key}");
            var grain = Substitute.For<IBPlusInternalGrain>();
            grain.AreChildrenLeavesAsync().Returns(Task.FromResult(childrenAreLeaves));
            grain.GetChildIdsAsync().Returns(_ =>
            {
                InternalNodesWalked++;
                return Task.FromResult(new List<GrainId>(children));
            });
            grain.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
            Factory.GetGrain<IBPlusInternalGrain>(id).Returns(grain);
            return new InternalNode(id, grain);
        }

        public readonly record struct LeafNode(GrainId Id, IBPlusLeafGrain Grain);

        public readonly record struct InternalNode(GrainId Id, IBPlusInternalGrain Grain);
    }

    /// <summary>
    /// Minimal <see cref="ILogger{T}"/> that records formatted warnings. A
    /// null or provider-less logger would leave every best-effort warning arm
    /// dark while the test still passed.
    /// </summary>
    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
