using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// The regions this cluster knows: its own region and every peer region the
/// replication peer report names.
/// </summary>
/// <remarks>
/// Read once per circuit and tenant, for at most the freshness window. A head
/// that serves no replication report cannot name its regions; the field then
/// accepts a typed region id and says why.
/// </remarks>
/// <param name="status">The replication peer report, or <see langword="null"/> when the head serves none.</param>
/// <param name="tenant">The circuit's asserted tenant.</param>
/// <param name="time">The clock the freshness window is measured on.</param>
internal sealed class RegionSuggestionSource(ILatticeReplicationStatus? status, ShellAssertedTenant? tenant, TimeProvider? time)
    : CachedSuggestionSource(tenant, time)
{
    /// <summary>The detail beside this cluster's own region.</summary>
    public const string LocalDetail = "This region";

    /// <summary>The detail beside a peer region.</summary>
    public const string PeerDetail = "Peer region";

    /// <inheritdoc />
    protected override string UnavailableReason => "The cluster's regions could not be listed, so the region id is used as typed.";

    /// <inheritdoc />
    protected override async Task<IReadOnlyList<LtSuggestion>?> LoadAsync(CancellationToken cancellationToken)
    {
        if (status is null)
        {
            return null;
        }

        var picture = await ClusterRegionPicture.ReadAsync(status, cancellationToken).ConfigureAwait(false);
        var values = new List<LtSuggestion>(picture.Peers.Count + 1);
        if (!string.IsNullOrEmpty(picture.LocalRegionId))
        {
            values.Add(new LtSuggestion(picture.LocalRegionId, LocalDetail));
        }

        foreach (var peer in picture.Peers)
        {
            if (!string.Equals(peer.RegionId, picture.LocalRegionId, StringComparison.Ordinal))
            {
                values.Add(new LtSuggestion(peer.RegionId, PeerDetail));
            }
        }

        return values;
    }
}
