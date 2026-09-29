using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for snapshotting a tree whose logical id is aliased to
/// another physical tree, the state every tree is in after a resize. The
/// snapshot used to address the shards under the logical id itself - the
/// resize's retired copy - so an offline snapshot missed every write made after
/// the resize (and un-deleted the retired shards), and an online one failed
/// outright against the retired shards' rejecting phase.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private const string AliasedPhysicalTreeId = SourceTreeId + "/resized/op1";

    private static void AliasSource(IGrainFactory grainFactory, string physicalTreeId = AliasedPhysicalTreeId)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(Task.FromResult(physicalTreeId));
    }

    [Test]
    public async Task Initiate_pins_the_physical_tree_the_source_alias_resolves_to()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        AliasSource(grainFactory);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Offline, ShardCount);

        Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo(AliasedPhysicalTreeId));
    }

    [Test]
    public async Task Initiate_pins_the_source_itself_when_it_is_not_aliased()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        AliasSource(grainFactory, SourceTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Offline, ShardCount);

        Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo(SourceTreeId));
    }

    [Test]
    public async Task Offline_snapshot_of_an_aliased_tree_locks_the_physical_shards_not_the_retired_ones()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        AliasSource(grainFactory);
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, AliasedPhysicalTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Offline, ShardCount);
        await grain.LockSourceShardsAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{AliasedPhysicalTreeId}/{i}").Received(1).MarkDeletedAsync();
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}").DidNotReceive().MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Online_snapshot_of_an_aliased_tree_shadow_forwards_the_physical_shards()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        AliasSource(grainFactory);
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, AliasedPhysicalTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount);
        await grain.BeginShadowForwardAllShardsAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{AliasedPhysicalTreeId}/{i}").Received(1)
                .BeginShadowForwardAsync(DestTreeId, Arg.Any<string>(), SourceTreeId);
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}").DidNotReceive()
                .BeginShadowForwardAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
        }
    }

    [Test]
    public async Task Copy_of_an_aliased_tree_drains_the_physical_shards()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var liveLeaf = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var retiredLeaf = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardForSnapshot(grainFactory, AliasedPhysicalTreeId, 0,
            new Dictionary<string, byte[]> { ["live-a"] = [1], ["live-b"] = [2] }, liveLeaf);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0,
            new Dictionary<string, byte[]> { ["stale"] = [3] }, retiredLeaf);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "test-op";
        state.State.SourcePhysicalTreeId = AliasedPhysicalTreeId;

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0").Received(1).BulkLoadRawAsync(
            "test-op-snapshot-0",
            Arg.Is<List<LwwEntry>>(e => e.Count == 2 && e.All(x => x.Key.StartsWith("live-"))));
        await grainFactory.GetGrain<IBPlusLeafGrain>(retiredLeaf).DidNotReceive().GetLiveRawEntriesAsync();
    }

    [Test]
    public async Task Legacy_state_without_a_pinned_physical_tree_addresses_the_source_id()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);
        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.ShardCount = ShardCount;
        state.State.SourcePhysicalTreeId = "";

        await grain.LockSourceShardsAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}").Received(1).MarkDeletedAsync();
        }
    }
}
