using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    private async Task<RoutingInfo> ReadPhysicalSourceRoutingAsync(string sourceTreeId, string physicalTreeId)
    {
        var registry = grainFactory.GetLatticeRegistry();
        var logical = await registry.GetEntryAsync(sourceTreeId)
            ?? throw new InvalidOperationException($"Source '{sourceTreeId}' has no routing registry entry.");
        ShardMap map;
        if (string.Equals(logical.PhysicalTreeId ?? sourceTreeId, physicalTreeId, StringComparison.Ordinal))
        {
            // The active logical row owns live split/reshard topology.
            map = AliasCutoverShardMaps.EffectiveMap(logical);
        }
        else if (string.Equals(sourceTreeId, physicalTreeId, StringComparison.Ordinal))
        {
            map = logical.UnaliasedShardMap
                ?? throw new InvalidOperationException($"Original physical source '{physicalTreeId}' has no retained routing map.");
        }
        else
        {
            // Cutover stamps the final replaced layout on its physical row.
            var physical = await registry.GetEntryAsync(physicalTreeId)
                ?? throw new InvalidOperationException($"Physical source '{physicalTreeId}' has no routing registry entry.");
            map = physical.PhysicalTreeId is null
                ? AliasCutoverShardMaps.EffectiveMap(physical)
                : physical.UnaliasedShardMap
                    ?? throw new InvalidOperationException($"Physical source '{physicalTreeId}' now aliases another tree without retaining its own map.");
        }

        return new RoutingInfo(physicalTreeId, map);
    }

    /// <summary>
    /// Reads one source row in its <b>stored</b> form (version envelope intact) for a
    /// rebuild or reconcile, so the re-derivation hands
    /// <see cref="ProjectionInput"/> exactly the bytes incremental apply read from the
    /// WAL (#5240). An unaliased source reads through
    /// <see cref="IReplicationApplyGrain.ReadStoredWithVersionAsync"/>, the
    /// non-decoding counterpart of <see cref="ILattice.GetWithVersionAsync"/> - the
    /// public read would strip and upcast to the target; an aliased source reads its
    /// physical shards directly, which never decode.
    /// </summary>
    private Task<VersionedValue> ReadStoredSourceValueAsync(
        string sourceTreeId, string physicalTreeId, string key, CancellationToken cancellationToken)
    {
        if (!string.Equals(sourceTreeId, physicalTreeId, StringComparison.Ordinal))
        {
            return ReadPhysicalSourceValueAsync(sourceTreeId, physicalTreeId, key, cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return grainFactory.GetGrain<IReplicationApplyGrain>(physicalTreeId).ReadStoredWithVersionAsync(key);
    }

    private async Task<VersionedValue> ReadPhysicalSourceValueAsync(
        string sourceTreeId, string physicalTreeId, string key, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var routing = await ReadPhysicalSourceRoutingAsync(sourceTreeId, physicalTreeId);
            try
            {
                return await grainFactory.GetGrain<IShardRootGrain>(
                    $"{physicalTreeId}/{routing.Map.Resolve(key)}").GetWithVersionAsync(key);
            }
            catch (Exception ex) when ((ex is StaleShardRoutingException or StaleTreeRoutingException)
                && attempt < SourceVersionRoutingAttempts)
            {
            }
        }
    }
}
