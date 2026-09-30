namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// One physical shard, as the Shards tab lists it: the virtual slots routed to it
/// from the live shard map, its structure from diagnostics, and its load from the
/// hotness sample. Any part the caller could not read is <see langword="null"/>.
/// </summary>
/// <param name="ShardIndex">The physical shard index.</param>
/// <param name="Slots">Virtual slots the live shard map routes to the shard.</param>
/// <param name="Depth">The shard's B+ tree depth.</param>
/// <param name="LiveKeys">Live keys.</param>
/// <param name="Tombstones">Tombstones, counted only by a deep read.</param>
/// <param name="Reads">Reads in the hotness window.</param>
/// <param name="Writes">Writes in the hotness window.</param>
/// <param name="OpsPerSecond">Observed operations per second.</param>
/// <param name="Splitting">Whether a split is in progress.</param>
/// <param name="BulkPending">Whether a bulk operation is pending.</param>
internal sealed record ClusterShardRow(
    int ShardIndex,
    int? Slots,
    int? Depth,
    long? LiveKeys,
    long? Tombstones,
    long? Reads,
    long? Writes,
    double? OpsPerSecond,
    bool Splitting,
    bool BulkPending);
