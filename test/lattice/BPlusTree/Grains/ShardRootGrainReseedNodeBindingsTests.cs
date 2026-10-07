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
/// post-recovery repair that re-binds every node a shard routes to, re-creating
/// the leaves an interrupted purge cleared (issue #4700) through
/// <see cref="IBPlusLeafGrain.RecoverBindingAsync"/>.
/// <para>
/// The internal-rooted descent, the paging of the leaf work and the propagation
/// of a walk fault are structurally unreachable from a flat-tree fixture, which is
/// why they need their own topology shapes here.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainReseedNodeBindingsTests
{
    private const string ShardKey = "reseed-tree/3";

    private static async Task<int> ReseedAllAsync(ShardRootGrain grain, List<int>? pageStarts = null)
    {
        var next = 0;
        var calls = 0;
        do
        {
            pageStarts?.Add(next);
            next = await grain.ReseedNodeBindingsAsync(next);
            calls++;
        }
        while (next >= 0);

        return calls;
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_is_a_no_op_when_shard_has_no_root()
    {
        var harness = new ReseedHarness();

        Assert.That(await harness.Grain.ReseedNodeBindingsAsync(0), Is.EqualTo(-1));
        Assert.That(harness.Logger.Warnings, Is.Empty);
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_recovers_the_single_root_leaf_when_tree_is_flat()
    {
        var harness = new ReseedHarness();
        var l0 = harness.Leaf("L0");
        harness.State.State.RootNodeId = l0.Id;
        harness.State.State.RootIsLeaf = true;

        Assert.That(await harness.Grain.ReseedNodeBindingsAsync(0), Is.EqualTo(-1));

        await l0.Grain.Received(1).RecoverBindingAsync("reseed-tree", 3);
        Assert.That(harness.Logger.Warnings, Is.Empty);
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_descends_internal_nodes_and_recovers_every_leaf()
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

        await ReseedAllAsync(harness.Grain);

        foreach (var leaf in new[] { l0, l1, l2 })
        {
            await leaf.Grain.Received(1).RecoverBindingAsync("reseed-tree", 3);
        }

        foreach (var node in new[] { i0, i1, i2 })
        {
            await node.Grain.Received(1).SetTreeIdAsync("reseed-tree");
        }

        Assert.That(harness.Logger.Warnings, Is.Empty);
    }

    [Test]
    public void ReseedNodeBindingsAsync_fails_the_recovery_when_the_walk_throws()
    {
        // Issue #4700: the leaves a purge cleared are re-created only by this
        // repair, so a repair skipped would leave them failed closed with no path
        // back. The fault propagates and the recovery is retried.
        var harness = new ReseedHarness();
        var l0 = harness.Leaf("L0");
        var i0 = harness.Internal("I0", childrenAreLeaves: true, children: [l0.Id]);
        i0.Grain.AreChildrenLeavesAsync().Throws(new TimeoutException("node silo unreachable"));

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        Assert.ThrowsAsync<TimeoutException>(async () => await harness.Grain.ReseedNodeBindingsAsync(0));
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_pages_the_leaf_work_and_reaches_every_leaf()
    {
        // Issue #4700: more routed leaves than one call handles are re-bound over
        // several calls, each resuming where the last stopped, so no leaf beyond
        // the first page is left behind.
        var harness = new ReseedHarness();
        var leafCount = ShardRootGrain.ReseedLeafPageSize + 10;
        var leaves = Enumerable.Range(0, leafCount).Select(i => harness.Leaf($"L{i}")).ToList();
        var root = harness.Internal("Iroot", childrenAreLeaves: true, children: leaves.Select(l => l.Id).ToList());
        harness.State.State.RootNodeId = root.Id;
        harness.State.State.RootIsLeaf = false;

        var pageStarts = new List<int>();
        var calls = await ReseedAllAsync(harness.Grain, pageStarts);

        Assert.Multiple(async () =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(pageStarts, Is.EqualTo(new[] { 0, ShardRootGrain.ReseedLeafPageSize }));
            await leaves[0].Grain.Received(1).RecoverBindingAsync("reseed-tree", 3);
            await leaves[^1].Grain.Received(1).RecoverBindingAsync("reseed-tree", 3);
        });
        await root.Grain.Received(1).SetTreeIdAsync("reseed-tree");
    }

    [Test]
    public async Task ReseedNodeBindingsAsync_leaves_a_refused_leaf_failed_closed_and_recovers_the_rest()
    {
        // A rowless leaf no purge cleared is refused (its row may have been lost);
        // the refusal is reported and the rest of the shard is still recovered.
        var harness = new ReseedHarness();
        var lost = harness.Leaf("Llost");
        var fine = harness.Leaf("Lfine");
        lost.Grain.RecoverBindingAsync(Arg.Any<string>(), Arg.Any<int>())
            .Returns(Task.FromException(new LeafStateRowLostException("leaf", "reseed-tree", "no purge cleared it", null)));
        var root = harness.Internal("I0", childrenAreLeaves: true, children: [lost.Id, fine.Id]);
        harness.State.State.RootNodeId = root.Id;
        harness.State.State.RootIsLeaf = false;

        Assert.That(await harness.Grain.ReseedNodeBindingsAsync(0), Is.EqualTo(-1));

        await fine.Grain.Received(1).RecoverBindingAsync("reseed-tree", 3);
        Assert.That(harness.Logger.Warnings, Has.Count.EqualTo(1));
        Assert.That(harness.Logger.Warnings[0], Does.Contain("does not show that a purge cleared it"));
    }

    [Test]
    public void ReseedNodeBindingsAsync_fails_the_recovery_when_a_leaf_cannot_be_recovered()
    {
        // A fault other than a refusal - the leaf's record could not be read - fails
        // the recovery, which is retried, rather than strand a cleared leaf.
        var harness = new ReseedHarness();
        var l0 = harness.Leaf("L0");
        l0.Grain.RecoverBindingAsync(Arg.Any<string>(), Arg.Any<int>())
            .Returns(Task.FromException(new TimeoutException("record store unreachable")));
        harness.State.State.RootNodeId = l0.Id;
        harness.State.State.RootIsLeaf = true;

        Assert.ThrowsAsync<TimeoutException>(async () => await harness.Grain.ReseedNodeBindingsAsync(0));
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
            grain.RecoverBindingAsync(Arg.Any<string>(), Arg.Any<int>()).Returns(Task.CompletedTask);
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
