using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// The regions this cluster knows, as R1's peer report describes them: this
/// region, and each peer it replicates with.
/// </summary>
/// <param name="LocalRegionId">This region's id.</param>
/// <param name="Peers">The peers, ordered by region id.</param>
/// <param name="Truncated">Whether the report was cut off at <see cref="MaximumPages"/>.</param>
internal sealed record ClusterRegionPicture(string LocalRegionId, IReadOnlyList<ClusterRegionPeer> Peers, bool Truncated)
{
    /// <summary>The most report pages the picture follows.</summary>
    public const int MaximumPages = 100;

    /// <summary>Reads the whole peer report, page by page, and rolls it up by peer.</summary>
    /// <param name="status">The replication peer report.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The picture.</returns>
    public static async Task<ClusterRegionPicture> ReadAsync(ILatticeReplicationStatus status, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(status);

        var links = new List<ReplicationPeerStatusEntry>();
        var local = string.Empty;
        string? token = null;
        var pages = 0;
        do
        {
            var page = await status
                .GetPeerStatusAsync(new ReplicationPeerStatusQuery { ContinuationToken = token }, cancellationToken)
                .ConfigureAwait(false);
            local = page.LocalRegionId;
            links.AddRange(page.Peers);
            token = page.ContinuationToken;
            pages++;
        }
        while (token is not null && pages < MaximumPages);

        return new ClusterRegionPicture(local, RollUp(links), token is not null);
    }

    /// <summary>Rolls links up by peer region.</summary>
    /// <param name="links">The links.</param>
    /// <returns>One peer per region, ordered by id.</returns>
    internal static IReadOnlyList<ClusterRegionPeer> RollUp(IEnumerable<ReplicationPeerStatusEntry> links) =>
        links
            .GroupBy(link => link.PeerRegionId, StringComparer.Ordinal)
            .Select(group => new ClusterRegionPeer(
                group.Key,
                group.Count(),
                group.Count(link => link.Health == ReplicationLinkHealth.Healthy),
                group.Count(link => link.Health == ReplicationLinkHealth.Lagging),
                group.Count(link => link.Health == ReplicationLinkHealth.Stalled),
                group.Select(link => link.TreeId).Distinct(StringComparer.Ordinal).Count()))
            .OrderBy(peer => peer.RegionId, StringComparer.Ordinal)
            .ToArray();
}
