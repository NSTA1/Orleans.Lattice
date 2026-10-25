using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeSnapshotGrainTests
{
    [TestCase(SnapshotMode.Online)]
    [TestCase(SnapshotMode.Offline)]
    public async Task Snapshot_reuses_one_source_entry_for_sizing_alias_map_and_high_water(SnapshotMode mode)
    {
        var (grain, state, reminders, _, factory) = CreateGrainWithTimerRegistry();
        SetupKeepalive(reminders);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var map = ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, 3);
        registry.GetEntryAsync(SourceTreeId).Returns(new TreeRegistryEntry
        {
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
            ShardCount = 3,
            PhysicalTreeId = AliasedPhysicalTreeId,
            ShardMap = map,
            NextShardIndex = 17,
        });

        await grain.SnapshotAsync(DestTreeId, mode);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardCount, Is.EqualTo(3));
            Assert.That(state.State.ShardIndices, Is.EqualTo(new[] { 0, 1, 2 }));
            Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo(AliasedPhysicalTreeId));
            Assert.That(state.State.SourceShardMap, Is.SameAs(map));
        });
        await registry.Received(1).GetEntryAsync(SourceTreeId);
        await registry.Received(1).ExistsAsync(DestTreeId);
        await registry.Received(1).RegisterAsync(DestTreeId,
            Arg.Is<TreeRegistryEntry?>(entry => entry != null && entry.NextShardIndex == 17));
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Snapshot_with_a_separate_logical_owner_keeps_its_physical_resolution()
    {
        const string logical = "snapshot-logical-owner";
        var (grain, state, reminders, _, factory) = CreateGrainWithTimerRegistry();
        SetupKeepalive(reminders);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(AliasedPhysicalTreeId);

        await grain.SnapshotWithOperationIdAsync(DestTreeId, SnapshotMode.Offline, null, null, "copy-op", logical);

        Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo(AliasedPhysicalTreeId));
        await registry.Received(1).GetEntryAsync(logical);
        await registry.Received(1).GetEntryAsync(SourceTreeId);
        await registry.Received(1).ResolveAsync(SourceTreeId);
    }
}
