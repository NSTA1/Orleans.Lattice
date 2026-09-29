using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The Schema area's addresses: <c>/schema</c> lists the governed trees, and
/// <c>/schema/{tree-path}?tab=...</c> is one tree's workspace. The tab is a query
/// parameter, because every path segment below the area belongs to the logical
/// tree id.
/// </summary>
internal static class SchemaAddresses
{
    /// <summary>The area's route segment.</summary>
    public const string AreaKey = "schema";

    /// <summary>The Apps area's route segment, which a declaring app links to.</summary>
    public const string AppsAreaKey = "apps";

    /// <summary>The query key naming the open tab; absent means <see cref="SchemaTabs.Policy"/>.</summary>
    public const string TabQuery = "tab";

    /// <summary>The query key choosing which trees the directory lists: <see cref="ShowAll"/> or absent.</summary>
    public const string ShowQuery = "show";

    /// <summary>The <see cref="ShowQuery"/> value that lists every tree, governed or not.</summary>
    public const string ShowAll = "all";

    /// <summary>The query key the directory's filter text travels in.</summary>
    public const string FilterQuery = "filter";

    /// <summary>
    /// The query key that asks the compliance tab to start a scan once, on
    /// arrival. A compliance scan is a pure read, so an address may start one.
    /// </summary>
    public const string ScanQuery = "scan";

    /// <summary>The <see cref="ScanQuery"/> value that starts a scan.</summary>
    public const string ScanStart = "start";

    /// <summary>The directory of governed trees.</summary>
    public static ExplorerAddress Directory { get; } = ExplorerAddress.ForArea(AreaKey);

    /// <summary>The directory listing every tree.</summary>
    public static ExplorerAddress AllTrees { get; } = Directory.WithQuery(ShowQuery, ShowAll);

    /// <summary>One tree's workspace, on <paramref name="tab"/> (the policy tab when <see langword="null"/>).</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="tab">The tab, or <see langword="null"/> for the default.</param>
    /// <returns>The address.</returns>
    public static ExplorerAddress Tree(string treeId, string? tab = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var address = ExplorerAddress.ForTree(AreaKey, treeId);
        return tab is null || tab == SchemaTabs.Policy ? address : address.WithQuery(TabQuery, tab);
    }

    /// <summary>The compliance tab of <paramref name="treeId"/>, asked to start a scan on arrival.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The address.</returns>
    public static ExplorerAddress StartScan(string treeId) =>
        Tree(treeId, SchemaTabs.Compliance).WithQuery(ScanQuery, ScanStart);

    /// <summary>The logical tree id <paramref name="address"/> names, or <see langword="null"/> when it names none.</summary>
    /// <param name="address">The address.</param>
    /// <returns>The logical tree id.</returns>
    public static string? TreeIdOf(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return string.Equals(address.Area, AreaKey, StringComparison.Ordinal) ? address.TreeId : null;
    }

    /// <summary>The page in the Apps area of the app <paramref name="slug"/>.</summary>
    /// <param name="slug">The app slug.</param>
    /// <returns>The address.</returns>
    public static ExplorerAddress App(string slug)
    {
        ArgumentException.ThrowIfNullOrEmpty(slug);
        return ExplorerAddress.ForArea(AppsAreaKey, slug);
    }
}
