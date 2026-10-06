using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The durable, cluster-wide record of every peer cluster id that has been
/// decommissioned (issue #4684's decommission verb), as distinct from a mere
/// detach from <c>ReplicationPeers</c>. A decommissioned peer is gone for
/// good: a later re-add of the same cluster id is treated as a fresh
/// bootstrap rather than a resumed one (#4701 runs
/// <c>StalePendingClearer</c> on every full bootstrap). This registry is also
/// consulted directly: <see cref="ICrossTreePeerEnrolmentGrain.EnrolAsync"/>
/// refuses to re-enrol a shipper for a peer this registry still marks
/// decommissioned, so a decommissioned peer can only come back through an
/// explicit fresh bootstrap rather than a stale or racing shipper activation
/// silently re-enrolling it. The marker is cleared when the operator
/// re-configures the peer into <c>ReplicationPeers</c> - see
/// <see cref="ClearDecommissionedAsync"/> - not when any drain or bootstrap
/// completes, because the drain/bootstrap path runs on the receiving side
/// while this registry and the enrolment grains it gates both live
/// origin-side.
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

    /// <summary>
    /// Clears the decommissioned marker for <paramref name="peerClusterId"/>,
    /// called when the operator re-configures the peer into
    /// <c>ReplicationPeers</c> (<see cref="ReplicationDriverActivationService"/>'s
    /// peer-added handling). Idempotent: clearing a peer that is not marked
    /// decommissioned is a no-op. This is what lets a re-added peer's
    /// shippers re-enrol via <see cref="ICrossTreePeerEnrolmentGrain.EnrolAsync"/>
    /// again.
    /// </summary>
    Task ClearDecommissionedAsync(string peerClusterId);
}
