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

    [Test]
    public async Task MergeShardAsync_reads_target_entry_once_for_options_and_shard_map()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;
        state.State.SourcePhysicalShards = [0];
        state.State.NextShardIndex = 0;

        registry.ClearReceivedCalls();

        await grain.RunMergePassAsync();

        // Shard drain reads target registry entry once for options + shard map,
        // without calling GetShardMapAsync or ResolveAsync.
        Assert.Multiple(() =>
        {
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetShardMapAsync)), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)
                && (string)c.GetArguments()[0]! == TargetTreeId), Is.Zero);
            Assert.That(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetEntryAsync)
                && (string)c.GetArguments()[0]! == TargetTreeId), Is.GreaterThanOrEqualTo(1));
        });
    }
}
