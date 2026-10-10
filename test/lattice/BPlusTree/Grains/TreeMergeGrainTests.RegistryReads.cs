using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Initiating a merge reads the source's alias and shard map from one registry
/// entry: <c>ResolveAsync</c> and <c>GetShardMapAsync</c> are projections of
/// that entry, so asking for them separately paid extra serial registry turns.
/// </summary>
public partial class TreeMergeGrainTests
{
    [Test]
    public async Task InitiateMergeState_reads_the_source_topology_from_one_registry_entry()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var registry = StubSourceEntry(grainFactory, "source-physical", new ShardMap { Slots = [0, 0, 4, 4], Version = 2 });
        registry.ResolveAsync(TargetTreeId).Returns("target-physical");
        registry.ClearReceivedCalls();

        await grain.InitiateMergeStateAsync(SourceTreeId, ShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo("source-physical"));
            Assert.That(state.State.TargetPhysicalTreeId, Is.EqualTo("target-physical"));
            Assert.That(state.State.SourcePhysicalShards, Is.EqualTo(new[] { 0, 4 }));
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)
                && (string)c.GetArguments()[0]! == SourceTreeId), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)), Is.Zero);
            // One source entry read plus the target alias.
            Assert.That(registry.ReceivedCalls().Count(), Is.EqualTo(2));
        });
    }
}
