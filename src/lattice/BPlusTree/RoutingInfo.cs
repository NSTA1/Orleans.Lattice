using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// Snapshot of the effective routing context for a Lattice tree at a point
/// in time. Returned by <see cref="ILattice.GetRoutingAsync"/> for callers
/// — typically infrastructure helpers such as the streaming bulk loader —
/// that need to address shard grains directly without re-implementing
/// alias resolution and shard-map fetching.
/// </summary>
/// <param name="PhysicalTreeId">
/// The physical tree ID after resolving any registry alias. Shard grain
/// keys must be constructed as <c>{PhysicalTreeId}/{shardIndex}</c>.
/// </param>
/// <param name="Map">
/// The effective <see cref="ShardMap"/> for the tree. Use
/// <see cref="ShardMap.Resolve"/> to map a key to a physical shard index
/// and <see cref="ShardMap.GetPhysicalShardIndices"/> to enumerate the
/// shards that may contain data.
/// </param>
[GenerateSerializer]
[Alias(TypeAliases.RoutingInfo)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record RoutingInfo(
    [property: Id(0)] string PhysicalTreeId,
    [property: Id(1)] ShardMap Map);