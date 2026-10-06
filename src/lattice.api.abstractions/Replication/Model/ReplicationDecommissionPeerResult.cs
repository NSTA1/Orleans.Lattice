namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The transport-agnostic outcome of a
/// <see cref="ILatticeReplicationControl.DecommissionPeerAsync"/> call.
/// Decommissioning is permanent: unlike removing a peer from
/// <c>ReplicationPeers</c> (a detach, which keeps every tree's durable
/// enrolment so the peer can be re-added without a fresh bootstrap),
/// decommissioning drops the peer's enrolment from every replicated tree, which
/// releases any cross-tree decision hold still waiting on that peer. A later
/// re-add of the same peer cluster id is therefore always a fresh bootstrap.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationDecommissionPeerResult)]
[Immutable]
public sealed record ReplicationDecommissionPeerResult
{
    /// <summary>Initializes a new <see cref="ReplicationDecommissionPeerResult"/>.</summary>
    /// <param name="peerClusterId">The decommissioned peer cluster id. Must not be <c>null</c>.</param>
    /// <param name="treeCount">The number of replicated trees whose enrolment the peer was removed from.</param>
    /// <param name="alreadyDecommissioned">
    /// Whether the peer was already decommissioned and the call was an
    /// idempotent no-op.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="peerClusterId"/> is <c>null</c>.</exception>
    public ReplicationDecommissionPeerResult(string peerClusterId, int treeCount, bool alreadyDecommissioned)
    {
        ArgumentNullException.ThrowIfNull(peerClusterId);
        PeerClusterId = peerClusterId;
        TreeCount = treeCount;
        AlreadyDecommissioned = alreadyDecommissioned;
    }

    /// <summary>The decommissioned peer cluster id.</summary>
    [Id(0)] public string PeerClusterId { get; init; }

    /// <summary>The number of replicated trees whose enrolment the peer was removed from.</summary>
    [Id(1)] public int TreeCount { get; init; }

    /// <summary>
    /// <c>true</c> when the peer was already decommissioned and the call was an
    /// idempotent no-op; <c>false</c> when a fresh decommission was authored.
    /// </summary>
    [Id(2)] public bool AlreadyDecommissioned { get; init; }
}
