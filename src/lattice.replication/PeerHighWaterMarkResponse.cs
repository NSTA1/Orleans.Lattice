using Orleans.Lattice;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A peer's read-only answer to a <see cref="PeerHighWaterMarkRequest"/>: the
/// hybrid-logical clock the peer has durably applied for the requested
/// <c>(TreeName, OriginClusterId)</c> stream. The anti-entropy leaf re-replay
/// stage re-ships only entries whose clock is strictly greater than
/// <see cref="Clock"/>. An origin the peer has never applied yields
/// <see cref="HybridLogicalClock.Zero"/>, which the re-replay stage treats as
/// "re-ship the whole in-range retained set" and relies on the receiver's
/// per-origin idempotent dedup to discard duplicates.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.PeerHighWaterMarkResponse)]
[Immutable]
public readonly record struct PeerHighWaterMarkResponse
{
    /// <summary>
    /// The peer's durably-applied high-water-mark clock for the requested
    /// <c>(TreeName, OriginClusterId)</c> stream, or
    /// <see cref="HybridLogicalClock.Zero"/> when the peer has never applied an
    /// entry from that origin.
    /// </summary>
    [Id(0)]
    public HybridLogicalClock Clock { get; init; }
}
