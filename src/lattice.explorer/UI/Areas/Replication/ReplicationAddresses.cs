using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The Replication area's addresses: <c>/replication</c> (the estate),
/// <c>/replication/trees</c> (enrolled trees) and
/// <c>/replication/trees/{tree-path}</c> (one tree's links), with the
/// <c>?health=</c>, <c>?region=</c> and <c>?app=</c> filters. Every address is
/// tenant-free here; the navigator roots it at the active tenant.
/// </summary>
internal static class ReplicationAddresses
{
    /// <summary>The area key.</summary>
    public const string AreaKey = "replication";

    /// <summary>The literal segment of the enrolled-trees section.</summary>
    public const string TreesSegment = "trees";

    /// <summary>The query key filtering links by health.</summary>
    public const string HealthQuery = "health";

    /// <summary>The query key filtering links by peer region.</summary>
    public const string RegionQuery = "region";

    /// <summary>The query key filtering trees by owning app.</summary>
    public const string AppQuery = "app";

    /// <summary>The estate view.</summary>
    public static ExplorerAddress Estate { get; } = ExplorerAddress.ForArea(AreaKey);

    /// <summary>The enrolled trees.</summary>
    public static ExplorerAddress Trees { get; } = ExplorerAddress.ForArea(AreaKey, TreesSegment);

    /// <summary>The estate filtered to one peer region.</summary>
    /// <param name="regionId">The peer region id.</param>
    public static ExplorerAddress ForRegion(string regionId) => Estate.WithQuery(RegionQuery, regionId);

    /// <summary>The estate filtered to one app's trees.</summary>
    /// <param name="slug">The app slug.</param>
    public static ExplorerAddress ForApp(string slug) => Estate.WithQuery(AppQuery, slug);

    /// <summary>
    /// One tree's detail page, or <see langword="null"/> when the logical tree id
    /// cannot be written as an address (an empty part, for instance).
    /// </summary>
    /// <param name="treeId">The logical tree id.</param>
    public static ExplorerAddress? ForTree(string treeId)
    {
        if (string.IsNullOrEmpty(treeId))
        {
            return null;
        }

        var parts = treeId.Split('/');
        if (parts.Any(part => part.Length == 0 || !ExplorerAddressEncoding.IsWellFormed(part)))
        {
            return null;
        }

        return ExplorerAddress.Create(null, AreaKey, [TreesSegment, .. parts]);
    }

    /// <summary>
    /// The logical tree id a tree-detail address names, or <see langword="null"/>
    /// when the address is not one.
    /// </summary>
    /// <param name="address">The address.</param>
    public static string? TreeIdOf(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return string.Equals(address.Area, AreaKey, StringComparison.Ordinal)
            && address.Path.Count > 1
            && string.Equals(address.Path[0], TreesSegment, StringComparison.Ordinal)
                ? string.Join('/', address.Path.Skip(1))
                : null;
    }
}
