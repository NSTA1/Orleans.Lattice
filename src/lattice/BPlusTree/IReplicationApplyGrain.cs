using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal apply-side seam used by <c>Orleans.Lattice.Replication</c> to
/// install a remote mutation onto the local tree while preserving the
/// authoring cluster's <see cref="HybridLogicalClock"/> and origin-cluster
/// id verbatim. Unlike the public <see cref="ILattice"/> write surface —
/// which always stamps a fresh local HLC at commit time — these methods
/// route the incoming entry through the LWW-merge path so the persisted
/// <see cref="LwwValue{T}"/> carries the source HLC and source
/// <see cref="LwwValue{T}.OriginClusterId"/> exactly as authored on the
/// remote cluster.
/// </summary>
/// <remarks>
/// <para>
/// Implemented by the per-tree <c>LatticeGrain</c> stateless worker so the
/// existing routing machinery (<see cref="LatticeOptionsResolver"/>,
/// shard-map resolution, system-tree guard) is reused. Apply calls for
/// system-prefixed trees are rejected for the same reason public writes
/// are.
/// </para>
/// <para>
/// Set / Delete apply paths route via
/// <see cref="IShardRootGrain.MergeManyAsync"/> — the same primitive used
/// by shard-split shadow-forward and tree-merge — because that is the
/// only entry point that preserves the source HLC end-to-end. Range
/// applies route via the standard <see cref="IShardRootGrain.DeleteRangeAsync"/>
/// wrapped in a <see cref="LatticeOriginContext"/> scope so the
/// receiver-side observer publishes a <see cref="MutationKind.DeleteRange"/>
/// notification stamped with the remote origin (and is therefore filtered
/// back out by the outbound replication ship loop).
/// </para>
/// </remarks>
[Alias(TypeAliases.IReplicationApplyGrain)]
internal interface IReplicationApplyGrain : IGrainWithStringKey
{
    /// <summary>
    /// Installs a Set mutation authored on the remote cluster identified
    /// by <paramref name="originClusterId"/>. The persisted entry carries
    /// <paramref name="sourceHlc"/> as its <see cref="HybridLogicalClock"/>
    /// timestamp, <paramref name="originClusterId"/> as its
    /// <see cref="LwwValue{T}.OriginClusterId"/>, and
    /// <paramref name="expiresAtTicks"/> as its absolute UTC expiry
    /// (<c>0</c> for non-expiring entries).
    /// </summary>
    /// <param name="key">The key the remote write targeted.</param>
    /// <param name="value">The committed value bytes.</param>
    /// <param name="sourceHlc">The HLC stamped by the remote cluster.</param>
    /// <param name="originClusterId">The id of the remote cluster that authored the write.</param>
    /// <param name="expiresAtTicks">Absolute UTC tick expiry; <c>0</c> means no expiry.</param>
    Task ApplySetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        long expiresAtTicks);

    /// <summary>
    /// Installs a Delete tombstone authored on the remote cluster
    /// identified by <paramref name="originClusterId"/>. The tombstone is
    /// stamped with <paramref name="sourceHlc"/> so LWW resolution against
    /// concurrent local writes is deterministic across clusters.
    /// </summary>
    /// <param name="key">The key the remote delete targeted.</param>
    /// <param name="sourceHlc">The HLC stamped by the remote cluster.</param>
    /// <param name="originClusterId">The id of the remote cluster that authored the delete.</param>
    Task ApplyDeleteAsync(
        string key,
        HybridLogicalClock sourceHlc,
        string originClusterId);

    /// <summary>
    /// Installs a range delete authored on the remote cluster identified
    /// by <paramref name="originClusterId"/>. The receiver walks the leaf
    /// chain locally and stamps each tombstone with a freshly-ticked local
    /// HLC — range deletes do not carry a single source HLC because the
    /// remote walk produced many per-leaf timestamps. The
    /// <see cref="LatticeOriginContext"/> scope ensures the receiver-side
    /// <see cref="MutationKind.DeleteRange"/> observer publishes the
    /// remote origin so the outbound ship loop does not loop the range
    /// back to the authoring cluster.
    /// </summary>
    /// <param name="startInclusive">Inclusive start key of the range.</param>
    /// <param name="endExclusive">Exclusive end key of the range.</param>
    /// <param name="originClusterId">The id of the remote cluster that authored the range delete.</param>
    Task ApplyDeleteRangeAsync(
        string startInclusive,
        string endExclusive,
        string originClusterId);
}
