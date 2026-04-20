using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>ShardRootGrain</c> when an incoming operation targets a key
/// whose virtual slot has been migrated to a different physical shard during
/// an adaptive split and the calling <c>LatticeGrain</c> is using a
/// stale <see cref="ShardMap"/>. The <c>LatticeGrain</c> catches this
/// exception, invalidates its cached routing snapshot, refreshes the
/// <see cref="ShardMap"/> from the registry, and retries the operation
/// against the new physical shard.
/// <para>
/// This exception is part of the internal coordination protocol between
/// <c>LatticeGrain</c> and <c>ShardRootGrain</c> during shard splits — it is
/// never surfaced to external callers because <c>LatticeGrain</c> always
/// catches and recovers from it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.StaleShardRouting)]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class StaleShardRoutingException : Exception
{
    /// <summary>
    /// The physical shard index that the request was routed to, which is no
    /// longer responsible for the requested key.
    /// </summary>
    [Id(0)] public int SourceShardIndex { get; set; }

    /// <summary>
    /// The physical shard index that now owns the requested key.
    /// </summary>
    [Id(1)] public int TargetShardIndex { get; set; }

    /// <summary>
    /// The virtual slot that was migrated.
    /// </summary>
    [Id(2)] public int VirtualSlot { get; set; }

    /// <summary>Creates a new <see cref="StaleShardRoutingException"/>.</summary>
    public StaleShardRoutingException(int sourceShardIndex, int targetShardIndex, int virtualSlot)
        : base($"Virtual slot {virtualSlot} has been migrated from shard {sourceShardIndex} to shard {targetShardIndex}. Refresh the shard map and retry.")
    {
        SourceShardIndex = sourceShardIndex;
        TargetShardIndex = targetShardIndex;
        VirtualSlot = virtualSlot;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public StaleShardRoutingException() { }
}
