using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// One page of per-peer replication link status, returned by
/// <see cref="ILatticeReplicationStatus.GetPeerStatusAsync"/>. Carries the id of
/// the region that produced it, so a caller can place "here" among the peers it
/// reports.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationPeerStatusPage)]
[Immutable]
public sealed record ReplicationPeerStatusPage
{
    /// <summary>Initializes a new <see cref="ReplicationPeerStatusPage"/>.</summary>
    /// <param name="localRegionId">The id (cluster id) of the region that produced the page. Must not be <c>null</c>.</param>
    /// <param name="peers">The link rows on this page, in report order. Must not be <c>null</c>.</param>
    /// <param name="continuationToken">
    /// The token that resumes after the last row, or <c>null</c> when the report is complete.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="localRegionId"/> or <paramref name="peers"/> is <c>null</c>.</exception>
    public ReplicationPeerStatusPage(
        string localRegionId,
        IReadOnlyList<ReplicationPeerStatusEntry> peers,
        string? continuationToken)
    {
        ArgumentNullException.ThrowIfNull(localRegionId);
        ArgumentNullException.ThrowIfNull(peers);
        LocalRegionId = localRegionId;
        Peers = peers;
        ContinuationToken = continuationToken;
    }

    /// <summary>The id (cluster id) of the region that produced this page - the caller's "here".</summary>
    [Id(0)] public string LocalRegionId { get; init; }

    /// <summary>The link rows on this page, ordered by tree id, then peer region id, then direction.</summary>
    [Id(1)] public IReadOnlyList<ReplicationPeerStatusEntry> Peers { get; init; }

    /// <summary>
    /// The opaque token that resumes the report after the last row of this page,
    /// or <see langword="null"/> when there is nothing further to read. Pass it
    /// back unaltered in <see cref="ReplicationPeerStatusQuery.ContinuationToken"/>.
    /// </summary>
    [Id(2)] public string? ContinuationToken { get; init; }

    /// <summary>Creates an empty, final page for <paramref name="localRegionId"/>.</summary>
    /// <param name="localRegionId">The id of the region that produced the page. Must not be <c>null</c>.</param>
    /// <returns>A page with no rows and no continuation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="localRegionId"/> is <c>null</c>.</exception>
    public static ReplicationPeerStatusPage Empty(string localRegionId) =>
        new(localRegionId, ImmutableArray<ReplicationPeerStatusEntry>.Empty, continuationToken: null);
}
