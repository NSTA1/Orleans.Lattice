using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>Tests for <see cref="ShardRootGrain.GetRootNodeRefAsync"/>.</summary>
[TestFixture]
public sealed class ShardRootGrainRootNodeRefTests
{
    private const string ShardKey = "rootref-tree/0";

    [Test]
    public async Task GetRootNodeRefAsync_returns_null_for_empty_shard()
    {
        var grain = CreateGrain(out _);

        var rootRef = await grain.GetRootNodeRefAsync();

        Assert.That(rootRef, Is.Null);
    }

    [Test]
    public async Task GetRootNodeRefAsync_returns_root_when_seeded()
    {
        var grain = CreateGrain(out var state);
        var rootId = GrainId.Create("leaf", "r0");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = true;

        var rootRef = await grain.GetRootNodeRefAsync();

        Assert.That(rootRef, Is.Not.Null);
        Assert.That(rootRef!.Value.NodeId, Is.EqualTo(rootId));
        Assert.That(rootRef.Value.IsLeaf, Is.True);
    }

    [Test]
    public async Task GetRootNodeRefAsync_reports_internal_root()
    {
        var grain = CreateGrain(out var state);
        var rootId = GrainId.Create("internal", "i0");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var rootRef = await grain.GetRootNodeRefAsync();

        Assert.That(rootRef, Is.Not.Null);
        Assert.That(rootRef!.Value.NodeId, Is.EqualTo(rootId));
        Assert.That(rootRef.Value.IsLeaf, Is.False);
    }

    private static ShardRootGrain CreateGrain(out FakePersistentState<ShardRootState> state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        state = new FakePersistentState<ShardRootState>();

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        return new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
    }
}
