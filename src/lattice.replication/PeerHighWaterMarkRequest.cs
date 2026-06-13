namespace Orleans.Lattice.Replication;

/// <summary>
/// A read-only request asking a peer cluster for the high-water-mark clock it
/// has durably applied for a given <c>(TreeName, OriginClusterId)</c> replication
/// stream. Used by the anti-entropy targeted leaf re-replay repair stage to bound
/// which retained write-ahead-log entries it re-ships to a diverged peer: only
/// entries whose clock is strictly greater than the returned watermark are
/// re-shipped. Strictly read-only - answering this request must never mutate
/// data or any replication cursor.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.PeerHighWaterMarkRequest)]
[Immutable]
public readonly record struct PeerHighWaterMarkRequest
{
    /// <summary>The logical replicated-tree name whose per-origin watermark is requested.</summary>
    [Id(0)]
    public string TreeName { get; init; }

    /// <summary>
    /// The origin cluster id whose applied watermark is requested. This is the
    /// re-shipping cluster's own id, because targeted re-replay only re-ships
    /// entries that the local cluster originated.
    /// </summary>
    [Id(1)]
    public string OriginClusterId { get; init; }
}
