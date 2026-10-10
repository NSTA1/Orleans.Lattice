using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Initiating a resize reads the current alias from the registry entry it
/// already captures for undo, rather than paying a separate
/// <c>ResolveAsync</c> turn for a projection of that entry.
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task InitiateResize_derives_the_old_physical_tree_from_the_captured_entry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
                PhysicalTreeId = "aliased-tree",
            }));
        registry.ClearReceivedCalls();

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.OldPhysicalTreeId, Is.EqualTo("aliased-tree"));
            Assert.That(state.State.OldRegistryEntry!.PhysicalTreeId, Is.EqualTo("aliased-tree"));
            Assert.That(registry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)), Is.Zero);
        });
    }
}
