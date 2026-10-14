using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TombstoneCompactionGrainTests
{
    [TestCase("operator", 1)]
    [TestCase("reminder", 1)]
    [TestCase("scoped", 0)]
    public async Task Pass_schedule_skips_registry_beyond_topology(string path, int topologyReads)
    {
        var (grain, _, _, factory, _) = CreateGrain(new LatticeOptions
        {
            CompactionLeafBatchSize = 0,
            CompactionShardTickInterval = TimeSpan.Zero,
        });
        SetupShardWithLeaves(factory, 0);
        SetupShardWithLeaves(factory, 1);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ClearReceivedCalls();

        switch (path)
        {
            case "operator":
                await grain.RunCompactionPassAsync();
                break;
            case "reminder":
                await grain.BeginCompactionStateAsync(0);
                break;
            case "scoped":
                await grain.BeginScopedCompactionStateAsync(TreeId, [0]);
                break;
        }

        Assert.That(grain.CurrentLeafBatchSizeForTests, Is.EqualTo(LatticeOptions.MinCompactionLeafBatchSize));
        if (path != "operator")
            Assert.That(grain.CurrentTickIntervalForTests, Is.EqualTo(LatticeOptions.MinCompactionShardTickInterval));
        await registry.Received(topologyReads).GetEntryAsync(TreeId);
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }
}
