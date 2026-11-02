using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The journey host's WAL materialiser pins, reachable only when the fault seams are enabled
/// (A12, issue #5382). Each kind leaves exactly the durable state a real cluster can be left
/// in, so the Explorer's WAL reclamation section reads it as it would in production:
/// <list type="bullet">
/// <item><description><see cref="Stranded"/> - a leaf that published a pin at a WAL offset and
/// crashed before it ever persisted a checkpoint: reclamation is blocked and does not clear on
/// its own.</description></item>
/// <item><description><see cref="Unchecked"/> - a leaf that has applied writes but not yet
/// checkpointed, so its pin reports no offset: reclamation waits for the checkpoint.</description></item>
/// <item><description><see cref="Unidentified"/> - a pin whose consumer names no leaf the cluster
/// can read: whether reclamation is blocked cannot be established.</description></item>
/// </list>
/// Every planted pin, and the leaf record behind it, is removed by <see cref="IJourneyWalPinGrain.ReleaseAsync"/>.
/// </summary>
internal interface IJourneyWalPinGrain : IGrainWithStringKey
{
    /// <summary>Plants a pin of <paramref name="kind"/> on the tree this grain is keyed by.</summary>
    /// <param name="kind">One of <c>stranded</c>, <c>unchecked</c> or <c>unidentified</c>.</param>
    /// <returns>The pin's consumer id.</returns>
    Task<string> PlantAsync(string kind);

    /// <summary>Removes a pin this grain planted, and the leaf record behind it.</summary>
    /// <param name="consumerId">The consumer id <see cref="PlantAsync"/> returned.</param>
    Task ReleaseAsync(string consumerId);
}

/// <inheritdoc cref="IJourneyWalPinGrain" />
internal sealed class JourneyWalPinGrain(
    IOptionsMonitor<LatticeOptions> options,
    LatticeOptionsResolver resolver,
    [FromKeyedServices(LatticeOptions.StorageProviderName)] IGrainStorage leafStorage) : Grain, IJourneyWalPinGrain
{
    /// <summary>A pin at a WAL offset whose leaf never persisted a checkpoint.</summary>
    public const string Stranded = "stranded";

    /// <summary>A pin with no offset yet, from a leaf that has not checkpointed.</summary>
    public const string Unchecked = "unchecked";

    /// <summary>A pin whose consumer id names no readable leaf.</summary>
    public const string Unidentified = "unidentified";

    private readonly Dictionary<string, GrainId> _leaves = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async Task<string> PlantAsync(string kind)
    {
        var physical = await GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(this.GetPrimaryKeyString());
        var partitions = await resolver.GetWalPartitionsAsync(physical);
        string consumerId;
        long offset;
        switch (kind)
        {
            case Stranded or Unchecked:
            {
                // The leaf's durable row as a leaf writes it before its first checkpoint: it
                // belongs to the tree, and no partition has a persisted checkpoint offset.
                var leaf = GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
                await leafStorage.WriteStateAsync(WalFloorHolderReader.LeafStateName, leaf, new GrainState<LeafNodeState>(new LeafNodeState { TreeId = physical }));
                consumerId = partitions > 1
                    ? $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{physical}_{leaf}_0"
                    : $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{physical}_{leaf}";
                _leaves[consumerId] = leaf;
                offset = kind == Stranded ? 0 : -1;
                break;
            }

            case Unidentified:
                consumerId = $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{physical}_journey-unidentified-consumer";
                offset = 0;
                break;

            default:
                throw new ArgumentException($"Unknown pin kind '{kind}'.", nameof(kind));
        }

        await PinGrain(physical, consumerId).SeedManyAsync([new MaterialiserPinReport(consumerId, HybridLogicalClock.Zero, offset)]);
        return consumerId;
    }

    /// <inheritdoc />
    public async Task ReleaseAsync(string consumerId)
    {
        var physical = await GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(this.GetPrimaryKeyString());
        await PinGrain(physical, consumerId).RemoveAsync(consumerId);
        if (_leaves.Remove(consumerId, out var leaf))
        {
            var state = new GrainState<LeafNodeState>(new LeafNodeState());
            await leafStorage.ReadStateAsync(WalFloorHolderReader.LeafStateName, leaf, state);
            if (state.RecordExists)
            {
                await leafStorage.ClearStateAsync(WalFloorHolderReader.LeafStateName, leaf, state);
            }
        }
    }

    private IWalMaterialiserPinGrain PinGrain(string physical, string consumerId) =>
        GrainFactory.GetGrain<IWalMaterialiserPinGrain>(
            WalMaterialiserPinRouting.ShardKey(physical, consumerId, WalMaterialiserPinRouting.ResolveShardCount(options)));
}
