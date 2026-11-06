namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable state of a WAL partition's clock floor (issue #4586). The floor is
/// written before it is published to a replication shipper, so it never
/// regresses across a reactivation or a move to a silo whose clock is behind.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalShardFloorState)]
internal sealed class WalShardFloorState
{
    /// <summary>
    /// The highest floor this partition has published. A freshly authored local
    /// write stamped below it is refused. <see cref="HybridLogicalClock.Zero"/>
    /// until a replication shipper first reads the partition with the cluster's
    /// capability gate open.
    /// </summary>
    [Id(0)]
    public HybridLogicalClock Floor { get; set; }

    /// <summary>
    /// The partition's durable incarnation (issue #5896, condition I1): advanced
    /// on every path that resets, truncates or rewrites the partition under the
    /// same physical tree id, before the partition admits a new append. A cached
    /// replay page is served only while the partition is still at the
    /// incarnation it was filled at. Never cleared, so it stays monotonic across
    /// a purge and a recreate of the same id. Zero on state written before it
    /// existed.
    /// </summary>
    [Id(1)]
    public long Incarnation { get; set; }
}
