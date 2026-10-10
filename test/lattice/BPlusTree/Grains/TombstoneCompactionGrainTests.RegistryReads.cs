using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The pass topology is derived from one registry entry read:
/// <c>ResolveAsync</c> and <c>GetShardMapAsync</c> are projections of that
/// entry, so asking for them separately paid extra serial registry turns.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task BeginCompactionState_reads_the_topology_from_one_registry_entry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var registry = StubRegistryEntry(grainFactory, "physical-tree", new ShardMap { Slots = [0, 0, 3, 3], Version = 2 });

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PhysicalTreeId, Is.EqualTo("physical-tree"));
            Assert.That(state.State.PhysicalShardIndices, Is.EqualTo(new[] { 0, 3 }));
            Assert.That(registry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)), Is.Zero);
            // One topology read plus the pass-start options snapshot.
            Assert.That(registry.ReceivedCalls().Count(), Is.EqualTo(2));
        });
    }
}
