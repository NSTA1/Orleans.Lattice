using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The <c>?health=</c>, <c>?region=</c> and <c>?app=</c> filters an address
/// carries. A value that names nothing (an unknown health, say) is ignored rather
/// than emptying the page.
/// </summary>
/// <param name="Health">The health a link must have, or <see langword="null"/> for any.</param>
/// <param name="Region">The peer region a link must reach, or <see langword="null"/> for any.</param>
/// <param name="App">The app whose trees are shown, or <see langword="null"/> for every tree.</param>
internal sealed record ReplicationFilter(ReplicationLinkHealth? Health, string? Region, string? App)
{
    /// <summary>No filter.</summary>
    public static ReplicationFilter None { get; } = new(null, null, null);

    /// <summary>Whether any filter is set.</summary>
    public bool IsActive => Health is not null || Region is not null || App is not null;

    /// <summary>Reads the filters from <paramref name="address"/>.</summary>
    /// <param name="address">The address.</param>
    public static ReplicationFilter From(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        ReplicationLinkHealth? health = ReplicationHealth.TryParse(address.GetQuery(ReplicationAddresses.HealthQuery), out var parsed)
            ? parsed
            : null;
        var region = NullIfEmpty(address.GetQuery(ReplicationAddresses.RegionQuery));
        var app = NullIfEmpty(address.GetQuery(ReplicationAddresses.AppQuery));
        return new ReplicationFilter(health, region, app);
    }

    /// <summary>Whether a link passes every filter.</summary>
    /// <param name="link">The link.</param>
    public bool Matches(ReplicationPeerStatusEntry link)
    {
        ArgumentNullException.ThrowIfNull(link);
        return (Health is not { } health || link.Health == health)
            && (Region is null || string.Equals(link.PeerRegionId, Region, StringComparison.Ordinal))
            && MatchesTree(link.TreeId);
    }

    /// <summary>Whether a tree passes the app filter.</summary>
    /// <param name="treeId">The logical tree id.</param>
    public bool MatchesTree(string treeId) =>
        App is null
        || (ReplicationTreeOwnership.TryGetAppSlug(treeId, out var slug) && string.Equals(slug, App, StringComparison.Ordinal));

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
