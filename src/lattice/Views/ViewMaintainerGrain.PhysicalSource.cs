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
