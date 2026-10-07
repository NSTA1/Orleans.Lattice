namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The status of one replication link - one tree, one peer region, one
/// direction - carried in a <see cref="ReplicationPeerStatusPage"/>. Every
/// measured field is one the cluster already tracks per link;
/// <see cref="Health"/> is derived from them against the facade's configured
/// thresholds.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationPeerStatusEntry)]
[Immutable]
public sealed record ReplicationPeerStatusEntry
{
    /// <summary>Initializes a new <see cref="ReplicationPeerStatusEntry"/>.</summary>
    /// <param name="treeId">The effective tree id (see <see cref="TreeId"/>). Must not be <c>null</c>.</param>
    /// <param name="peerRegionId">The peer region (cluster) id. Must not be <c>null</c>.</param>
    /// <param name="direction">Which way the link carries entries.</param>
    /// <param name="entriesBehind">WAL entries not yet shipped to the peer (outbound only).</param>
    /// <param name="bytesBehind">Payload bytes not yet shipped to the peer (outbound only).</param>
    /// <param name="consecutiveErrors">Consecutive failed contact attempts since the last success.</param>
    /// <param name="timeSinceLastContact">Time since the last successful contact, or <c>null</c> if there has been none.</param>
    /// <param name="inFlight">Shipped but unacknowledged batches (outbound only).</param>
    /// <param name="health">The derived link health.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> or <paramref name="peerRegionId"/> is <c>null</c>.</exception>
    public ReplicationPeerStatusEntry(
        string treeId,
        string peerRegionId,
        ReplicationLinkDirection direction,
        long entriesBehind,
        long bytesBehind,
        long consecutiveErrors,
        TimeSpan? timeSinceLastContact,
        long inFlight,
        ReplicationLinkHealth health)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(peerRegionId);
        TreeId = treeId;
        PeerRegionId = peerRegionId;
        Direction = direction;
        EntriesBehind = entriesBehind;
        BytesBehind = bytesBehind;
        ConsecutiveErrors = consecutiveErrors;
        TimeSinceLastContact = timeSinceLastContact;
        InFlight = inFlight;
        Health = health;
    }

    /// <summary>
    /// The effective tree id: the bare name for a default-tenant tree, and the
    /// tenant-qualified <c>t/{tenant}/{name}</c> id for a tree of an asserted,
    /// non-default tenant. It is the id
    /// <see cref="ReplicationTreeConfigEntry.TreeId"/> carries for the same tree,
    /// so a link joins to its enrolment on it.
    /// </summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The id (cluster id) of the peer region at the other end of the link.</summary>
    [Id(1)] public string PeerRegionId { get; init; }

    /// <summary>Which way the link carries entries, relative to the local region.</summary>
    [Id(2)] public ReplicationLinkDirection Direction { get; init; }

    /// <summary>
    /// WAL entries the local region has yet to ship to the peer. Outbound links
    /// only; always zero on an inbound link, which tracks no backlog.
    /// </summary>
    [Id(3)] public long EntriesBehind { get; init; }

    /// <summary>
    /// Payload bytes the local region has yet to ship to the peer. Outbound links
    /// only; always zero on an inbound link.
    /// </summary>
    [Id(4)] public long BytesBehind { get; init; }

    /// <summary>
    /// Consecutive failed contact attempts in this direction since the last
    /// successful one: failed shipments for an outbound link, failed applies of
    /// the peer's entries for an inbound link.
    /// </summary>
    [Id(5)] public long ConsecutiveErrors { get; init; }

    /// <summary>
    /// Time since the last successful contact in this direction, or
    /// <see langword="null"/> when there has never been one. An outbound link is
    /// refreshed by the periodic liveness probe even when idle; an inbound link is
    /// refreshed only when the peer's entries are applied, so an idle peer's
    /// inbound link ages without being unhealthy.
    /// </summary>
    [Id(6)] public TimeSpan? TimeSinceLastContact { get; init; }

    /// <summary>
    /// Batches shipped to the peer and not yet acknowledged. Outbound links only;
    /// always zero on an inbound link.
    /// </summary>
    [Id(7)] public long InFlight { get; init; }

    /// <summary>The link health derived from the fields above against the facade's configured thresholds.</summary>
    [Id(8)] public ReplicationLinkHealth Health { get; init; }

    /// <summary>
    /// The reason a link is stalled by a re-seed requirement or a full
    /// dead-letter queue; <see langword="null"/> when neither condition applies.
    /// </summary>
    [Id(9)] public ReplicationLinkStallReason? StallReason { get; init; }
}
