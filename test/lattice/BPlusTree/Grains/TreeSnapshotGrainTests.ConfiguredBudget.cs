using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeSnapshotGrainTests
{
    [TestCase("slice")]
    [TestCase("phase")]
    [TestCase("drain")]
    public async Task Copy_budget_skips_registry_and_preserves_progress(string path)
    {
        var rig = CreateSlicingSnapshot(1, 2, TimeSpan.Zero, new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 1,
            BackgroundDrainMaxDuration = TimeSpan.FromTicks(1),
        });
        var registry = rig.Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ClearReceivedCalls();

        switch (path)
        {
            case "slice":
                Assert.That(await rig.Grain.RunSnapshotSliceAsync(), Is.False);
                break;
            case "phase":
                await rig.Grain.ProcessNextPhaseAsync();
                break;
            case "drain":
                await rig.Grain.DrainAllShardsOnlineAsync();
                break;
        }

        Assert.That(rig.MergedKeys[0], Has.Count.EqualTo(path == "drain" ? 2 : 1));
        Assert.That(rig.State.WriteCount, Is.GreaterThan(0));
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }
}
