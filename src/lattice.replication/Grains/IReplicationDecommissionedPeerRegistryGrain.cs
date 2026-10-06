using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The durable, cluster-wide record of every peer cluster id that has been
/// decommissioned (issue #4684's decommission verb), as distinct from a mere
/// detach from <c>ReplicationPeers</c>. A decommissioned peer is gone for
/// good: a later re-add of the same cluster id is treated as a fresh
/// bootstrap rather than a resumed one (#4701 runs
/// <c>StalePendingClearer</c> on every full bootstrap), so this grain exists
/// only to answer "was this peer ever decommissioned" for diagnostics and
/// for the decommission verb's own idempotency - the cross-tree decision
/// hold itself needs no lookup here, because removing the peer from each
/// tree's <see cref="ICrossTreePeerEnrolmentGrain"/> enrolment is what stops
/// the hold from waiting on it.
/// </summary>
/// <remarks>
/// Grain key: the fixed literal <see cref="SingletonKey"/>. One activation
/// cluster-wide; silo loss triggers automatic migration via the standard
/// Orleans cluster-singleton model.
/// </remarks>
[Alias(ReplicationTypeAliases.IReplicationDecommissionedPeerRegistryGrain)]
internal interface IReplicationDecommissionedPeerRegistryGrain : IGrainWithStringKey
{
    /// <summary>The fixed grain key every caller must address this singleton with.</summary>
    internal const string SingletonKey = "replication-decommissioned-peers";

    /// <summary>
    /// Reports whether <paramref name="peerClusterId"/> has ever been
    /// decommissioned.
    /// </summary>
    [AlwaysInterleave]
    Task<bool> IsDecommissionedAsync(string peerClusterId);

    /// <summary>
    /// Durably records that <paramref name="peerClusterId"/> was
    /// decommissioned at <paramref name="decommissionedAtUtc"/>. Idempotent:
    /// re-marking an already-decommissioned peer leaves its original
    /// timestamp in place.
    /// </summary>
    Task MarkDecommissionedAsync(string peerClusterId, DateTimeOffset decommissionedAtUtc);
}
