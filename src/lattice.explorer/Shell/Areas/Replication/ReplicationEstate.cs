using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// A snapshot of every replication link this region reports - one per
/// <c>(tree, peer, direction)</c> - read by paging
/// <see cref="ILatticeReplicationStatus"/> until its continuation token runs out.
/// </summary>
/// <param name="LocalRegionId">The reporting (local) region.</param>
/// <param name="Links">The links, in the order the facade reported them.</param>
/// <param name="Truncated">Whether the read stopped at the page ceiling before the report ended.</param>
/// <param name="ReadAt">When the snapshot was read.</param>
internal sealed record ReplicationEstate(
    string LocalRegionId,
    IReadOnlyList<ReplicationPeerStatusEntry> Links,
    bool Truncated,
    DateTimeOffset ReadAt)
{
    /// <summary>How many links have <paramref name="health"/>.</summary>
    /// <param name="health">The health.</param>
    public int Count(ReplicationLinkHealth health) => Links.Count(link => link.Health == health);

    /// <summary>The distinct peer regions, ordered by id.</summary>
    public IReadOnlyList<string> PeerRegions =>
        [.. Links.Select(link => link.PeerRegionId).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)];

    /// <summary>Orders <paramref name="links"/> worst first, then by tree, peer and direction.</summary>
    /// <param name="links">The links to order.</param>
    public static IReadOnlyList<ReplicationPeerStatusEntry> WorstFirst(IEnumerable<ReplicationPeerStatusEntry> links) =>
    [
        .. links
            .OrderByDescending(link => ReplicationHealth.Severity(link.Health))
            .ThenBy(link => link.TreeId, StringComparer.Ordinal)
            .ThenBy(link => link.PeerRegionId, StringComparer.Ordinal)
            .ThenBy(link => link.Direction),
    ];
}
