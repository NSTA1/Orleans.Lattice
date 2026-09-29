using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// One peer region as the estate diagram draws it: a node with an outbound and an
/// inbound edge to this region, each rolled up over the trees along it.
/// </summary>
/// <param name="PeerRegionId">The peer region id.</param>
/// <param name="Outbound">What this region ships to the peer, or <see langword="null"/> when nothing does.</param>
/// <param name="Inbound">What this region receives from the peer, or <see langword="null"/> when nothing does.</param>
internal sealed record ReplicationPeerSummary(
    string PeerRegionId,
    ReplicationEdgeSummary? Outbound,
    ReplicationEdgeSummary? Inbound)
{
    /// <summary>The worse of the two edges' healths.</summary>
    public ReplicationLinkHealth Health =>
        (Outbound, Inbound) switch
        {
            ({ } outbound, { } inbound) => ReplicationHealth.Worse(outbound.Health, inbound.Health),
            ({ } outbound, null) => outbound.Health,
            (null, { } inbound) => inbound.Health,
            _ => ReplicationLinkHealth.Unknown,
        };

    /// <summary>
    /// Groups <paramref name="links"/> by peer region, worst peer first and then by
    /// region id, each with its two edges rolled up.
    /// </summary>
    /// <param name="links">The links to summarise.</param>
    public static IReadOnlyList<ReplicationPeerSummary> Summarise(IEnumerable<ReplicationPeerStatusEntry> links)
    {
        ArgumentNullException.ThrowIfNull(links);
        return
        [
            .. links
                .GroupBy(link => link.PeerRegionId, StringComparer.Ordinal)
                .Select(peer => new ReplicationPeerSummary(
                    peer.Key,
                    ReplicationEdgeSummary.From(ReplicationLinkDirection.Outbound, [.. peer.Where(link => link.Direction == ReplicationLinkDirection.Outbound)]),
                    ReplicationEdgeSummary.From(ReplicationLinkDirection.Inbound, [.. peer.Where(link => link.Direction == ReplicationLinkDirection.Inbound)])))
                .OrderByDescending(peer => ReplicationHealth.Severity(peer.Health))
                .ThenBy(peer => peer.PeerRegionId, StringComparer.Ordinal),
        ];
    }
}
