using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class LatticeBootstrapCoordinatorGrainTests
{
    [TestCase(null, false, false)]
    [TestCase(null, false, true)]
    [TestCase("boot-tree", false, true)]
    [TestCase("boot-physical", false, true)]
    [TestCase("boot-physical", true, true)]
    public async Task Shadow_kickoff_reuses_registration_entry_but_checks_alias_target(
        string? physicalTreeId, bool targetExists, bool registered)
    {
        var (grain, state, factory, _, _, _, _, _) = Create();
        Seed(state);
        state.State.UseShadowCopy = true;
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(Tree).Returns(Task.FromResult<TreeRegistryEntry?>(
            registered ? new TreeRegistryEntry { PhysicalTreeId = physicalTreeId } : null));
        registry.ExistsAsync("boot-physical").Returns(targetExists);
        var resize = factory.GetGrain<ITreeResizeGrain>(Tree);
        resize.IsBootstrapCopyReadyAsync(Arg.Any<string>()).Returns(false);
        registry.ClearReceivedCalls();

        await grain.ProcessNextPhaseAsync();

        await registry.Received(1).GetEntryAsync(Tree);
        await registry.Received(1).ResolveAsync(Tree);
        await registry.DidNotReceive().ExistsAsync(Tree);
        if (physicalTreeId == "boot-physical")
            await registry.Received(1).ExistsAsync("boot-physical");
        else
            await registry.DidNotReceive().ExistsAsync(Arg.Any<string>());
        if (!registered || (physicalTreeId == "boot-physical" && !targetExists))
            await registry.Received(1).RegisterAsync(physicalTreeId ?? Tree);
        else
            await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
    }
}
