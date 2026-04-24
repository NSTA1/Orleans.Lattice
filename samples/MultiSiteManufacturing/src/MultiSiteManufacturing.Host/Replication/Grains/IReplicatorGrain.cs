using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// One replicator per <c>(tree, peer-cluster)</c> pair. Grain key
/// format: <c>"{treeName}|{peerName}"</c>. Drives a reminder-based
/// tick that tails the local replog for its tree, ships new entries
/// to the peer, and advances the local cursor on ack.
/// </summary>
/// <remarks>
/// <b>FUTURE seam.</b> When <c>Orleans.Lattice</c> ships cross-tree
/// continuous merge, this grain shrinks to a thin adapter — the
/// replog-tailing logic is replaced by the library primitive, and
/// the HTTP shipping step either stays (for cross-cluster hops) or
/// is replaced by a distributed merge (for single-cluster
/// replication).
/// </remarks>
internal interface IReplicatorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Runs one shipment tick: scans the replog above the current
    /// cursor, ships up to a batch's worth of entries to the peer,
    /// and advances the cursor on ack. Safe to invoke concurrently
    /// — reentrancy is guarded by the grain's single-activation
    /// serialization.
    /// </summary>
    Task TickAsync();

    /// <summary>Returns an immutable snapshot of the replicator's progress.</summary>
    Task<ReplicatorStatus> GetStatusAsync();
}

/// <summary>Public read-model for <see cref="IReplicatorGrain.GetStatusAsync"/>.</summary>
[GenerateSerializer]
public sealed record ReplicatorStatus
{
    /// <summary>Tree this replicator ships.</summary>
    [Id(0)] public required string Tree { get; init; }

    /// <summary>Peer cluster name.</summary>
    [Id(1)] public required string Peer { get; init; }

    /// <summary>Highest HLC successfully acked by the peer.</summary>
    [Id(2)] public required HybridLogicalClock Cursor { get; init; }

    /// <summary>Timestamp of the last successful ack.</summary>
    [Id(3)] public required DateTimeOffset LastContactUtc { get; init; }

    /// <summary>Total entries shipped since activation.</summary>
    [Id(4)] public required long TotalRowsShipped { get; init; }

    /// <summary>Consecutive error count — reset on every success.</summary>
    [Id(5)] public required int ConsecutiveErrors { get; init; }
}
