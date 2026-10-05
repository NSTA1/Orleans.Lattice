namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The sender's per-peer view of its replicated trees' applied low watermarks
/// (issue #4586 part 2b), keyed by the peer's cluster id. Each tree's shipper
/// toward the peer reports its tree watermark and the receiver lineage it was
/// computed under; the aggregate is the minimum over every tree the cluster
/// replicates, zero - vouching for nothing - while any of them has no fresh
/// non-zero report. It also paces the re-seeds receiver lineage changes force,
/// so a rollout does not re-seed every tree of the peer at once.
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationSourceFrontierAggregateGrain)]
internal interface IReplicationSourceFrontierAggregateGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records <paramref name="treeId"/>'s watermark toward the peer
    /// (<see cref="HybridLogicalClock.Zero"/> when it has none) under
    /// <paramref name="receiverLineage"/>, renews its lineage re-seed slot when
    /// <paramref name="holdsLineageReseed"/>, and returns the aggregate with its
    /// generation. The generation rises whenever any tree's receiver lineage
    /// changes or the set of replicated trees grows, and on every activation, so
    /// the receiver can ignore an aggregate computed before such a change.
    /// </summary>
    Task<(HybridLogicalClock OriginLowWatermark, long Generation)> ReportAsync(
        string treeId, Guid receiverLineage, HybridLogicalClock treeLowWatermark, bool holdsLineageReseed);

    /// <summary>
    /// Grants <paramref name="treeId"/> a slot to re-seed the peer for a receiver
    /// lineage change, when fewer than the pacing limit are held (or it already
    /// holds one). A slot not renewed through <see cref="ReportAsync"/> lapses.
    /// </summary>
    Task<bool> TryAcquireLineageReseedAsync(string treeId);

    /// <summary>Releases <paramref name="treeId"/>'s lineage re-seed slot. Idempotent.</summary>
    Task ReleaseLineageReseedAsync(string treeId);
}
