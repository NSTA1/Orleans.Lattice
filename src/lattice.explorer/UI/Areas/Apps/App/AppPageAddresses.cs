using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.App;

/// <summary>
/// The addresses an app's page links to: its own sections, the in-app path of its Open
/// section, and the other areas it points into - Data for a tree, Replication for its
/// intent, and the catalogue's review for re-consent - each an address, never an import
/// of the other area's code.
/// </summary>
/// <remarks>
/// Every address is built rooted at <c>tenant</c> as given; the page passes each through
/// the navigator's canonicalisation, which drops the root when tenancy is off or the
/// target area is cluster-wide.
/// </remarks>
internal static class AppPageAddresses
{
    /// <summary>The Apps area's key: the first segment of every app page.</summary>
    public const string AppsArea = "apps";

    /// <summary>The Data area's key.</summary>
    public const string DataArea = "data";

    /// <summary>The Replication area's key.</summary>
    public const string ReplicationArea = "replication";

    /// <summary>The catalogue's literal segment below <c>/apps</c>, owned by the catalogue pages (A1).</summary>
    public const string CatalogueSegment = "catalogue";

    /// <summary>The query key the Replication area filters by app on.</summary>
    public const string AppQuery = "app";

    /// <summary>The query key the catalogue searches on.</summary>
    public const string SearchQuery = "q";

    /// <summary>The query key that carries the query part of an app's in-frame path.</summary>
    public const string InAppQuery = "query";

    /// <summary>
    /// The query key that carries an in-frame path deeper than <see cref="MaxInAppSegments"/>
    /// segments, which the route's optional parameters cannot hold.
    /// </summary>
    public const string InAppPath = "path";

    /// <summary>
    /// The most in-frame path segments an open address carries as path segments: the page's
    /// routes declare optional parameters, never a catch-all, and four of them follow
    /// <c>/apps/{slug}/open</c>.
    /// </summary>
    public const int MaxInAppSegments = 4;

    /// <summary>An app's page, at <paramref name="tab"/> or, when <see langword="null"/>, at its bare address.</summary>
    /// <param name="tenant">The tenant root, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="tab">The section, or <see langword="null"/>.</param>
    /// <returns>The address <c>[/t/{tenant}]/apps/{slug}[/{tab}]</c>.</returns>
    public static ExplorerAddress Page(string? tenant, string slug, string? tab = null) =>
        ExplorerAddress.Create(tenant, AppsArea, tab is null ? [slug] : [slug, tab]);

    /// <summary>A tree of the app in the Data area.</summary>
    /// <param name="tenant">The tenant root, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="tree">The app-local tree name.</param>
    /// <returns>The address <c>[/t/{tenant}]/data/a/{slug}/{tree}</c>.</returns>
    public static ExplorerAddress DataTree(string? tenant, string slug, string tree) =>
        ExplorerAddress.Create(tenant, DataArea, ["a", slug, tree]);

    /// <summary>The Replication area filtered to the app.</summary>
    /// <param name="tenant">The tenant root, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    /// <returns>The address <c>[/t/{tenant}]/replication?app={slug}</c>.</returns>
    public static ExplorerAddress Replication(string? tenant, string slug) =>
        ExplorerAddress.Create(tenant, ReplicationArea, null, [new(AppQuery, slug)]);

    /// <summary>
    /// The catalogue's review of the app, where its re-consent flow lives: the entry for
    /// its source when the source is recorded, and otherwise the catalogue searched for it.
    /// </summary>
    /// <param name="tenant">The tenant root, or <see langword="null"/>.</param>
    /// <param name="sourceKey">The key of the source the installed version came from, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    /// <returns>The review address.</returns>
    public static ExplorerAddress Reconsent(string? tenant, string? sourceKey, string slug) =>
        string.IsNullOrEmpty(sourceKey)
            ? ExplorerAddress.Create(tenant, AppsArea, [CatalogueSegment], [new(SearchQuery, slug)])
            : ExplorerAddress.Create(tenant, AppsArea, [CatalogueSegment, sourceKey, slug]);

    /// <summary>
    /// The in-frame path an Open address names: its segments after <c>/apps/{slug}/open</c>
    /// (or its <see cref="InAppPath"/> value, for a deep path) behind a leading <c>/</c>, with
    /// the <see cref="InAppQuery"/> value after a <c>?</c>. <see langword="null"/> when it
    /// names no path, so the app starts at its own start.
    /// </summary>
    /// <param name="address">An Open address.</param>
    /// <returns>The in-frame path, or <see langword="null"/>.</returns>
    public static string? FramePath(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        var segments = address.Path.Skip(2).ToArray();
        var deep = address.GetQuery(InAppPath);
        var query = address.GetQuery(InAppQuery);
        if (segments.Length == 0 && string.IsNullOrEmpty(deep) && string.IsNullOrEmpty(query))
        {
            return null;
        }

        var path = !string.IsNullOrEmpty(deep)
            ? (deep[0] == '/' ? deep : "/" + deep)
            : "/" + string.Join('/', segments);
        return string.IsNullOrEmpty(query) ? path : path + "?" + query;
    }

    /// <summary>
    /// The Open address for an in-frame path the app reported: up to
    /// <see cref="MaxInAppSegments"/> segments below <c>/apps/{slug}/open</c> (a deeper path
    /// goes in <see cref="InAppPath"/>) and its query in <see cref="InAppQuery"/>. A fragment
    /// is dropped and empty segments collapse. <see langword="null"/> when the path cannot be
    /// an address.
    /// </summary>
    /// <param name="tenant">The tenant root, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="framePath">The path from <c>nav.sync</c>, starting with <c>/</c>.</param>
    /// <returns>The address, or <see langword="null"/>.</returns>
    public static ExplorerAddress? FromFramePath(string? tenant, string slug, string? framePath)
    {
        if (string.IsNullOrEmpty(framePath))
        {
            return null;
        }

        var text = framePath;
        var fragment = text.IndexOf('#', StringComparison.Ordinal);
        if (fragment >= 0)
        {
            text = text[..fragment];
        }

        string? query = null;
        var question = text.IndexOf('?', StringComparison.Ordinal);
        if (question >= 0)
        {
            query = text[(question + 1)..];
            text = text[..question];
        }

        try
        {
            var segments = text.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var address = segments.Length <= MaxInAppSegments
                ? ExplorerAddress.Create(tenant, AppsArea, [slug, AppPageTabs.Open, .. segments])
                : ExplorerAddress.Create(tenant, AppsArea, [slug, AppPageTabs.Open]).WithQuery(InAppPath, "/" + string.Join('/', segments));
            return string.IsNullOrEmpty(query) ? address : address.WithQuery(InAppQuery, query);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }
}
