using Orleans.Concurrency;
using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The durable set of peers that have ever attached a shipper to a tree
/// (issue #4684), keyed by the logical tree name. The cross-tree decision
/// purge hold waits for every one of them as well as every configured peer:
/// removing a peer from the topology only detaches it, and a detached peer
/// that is added back resumes against barriers that still need the decision.
/// </summary>
[Alias(ReplicationTypeAliases.ICrossTreePeerEnrolmentGrain)]
internal interface ICrossTreePeerEnrolmentGrain : IGrainWithStringKey
{
    /// <summary>Every enrolled peer.</summary>
    [AlwaysInterleave]
    Task<ImmutableArray<string>> GetAsync();

    /// <summary>Durably enrols <paramref name="peerClusterId"/>. Idempotent.</summary>
    Task EnrolAsync(string peerClusterId);

    /// <summary>
    /// Durably removes <paramref name="peerClusterId"/> from this tree's
    /// enrolment, for good (issue #4684's decommission verb). Unlike removing
    /// a peer from <c>ReplicationPeers</c> - a detach that this grain's
    /// enrolment deliberately survives so the cross-tree decision hold keeps
    /// waiting for a peer that may return - this permanently drops the peer
    /// from the set the hold waits on for this tree, so the hold stops
    /// waiting on it here. Idempotent: removing a peer that was never
    /// enrolled, or already decommissioned, is a no-op.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the peer was actually enrolled on this
    /// tree and was removed; <see langword="false"/> when it was already
    /// absent (never enrolled, or already decommissioned), so the caller can
    /// skip per-tree work that only matters for a tree the peer actually
    /// touched.
    /// </returns>
    Task<bool> DecommissionAsync(string peerClusterId);
}
