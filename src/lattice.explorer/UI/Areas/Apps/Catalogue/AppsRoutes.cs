using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's address grammar: <c>/apps</c> ("Your apps"),
/// <c>/apps/catalogue?source=&amp;filter=&amp;q=</c>, and the pre-install review at
/// <c>/apps/catalogue/{source}/{slug}[@{version}]</c>. The app pages under
/// <c>/apps/{slug}/...</c> belong to A2; this type only builds links to them. An app's UI
/// is opened in a window of its own, so a link to it carries <see cref="NewWindowTarget"/>
/// and <see cref="NewWindowRel"/>.
/// </summary>
internal static class AppsRoutes
{
    /// <summary>The <c>target</c> of every link that opens an app's window.</summary>
    public const string NewWindowTarget = "_blank";

    /// <summary>
    /// The <c>rel</c> of every link that opens an app's window: the new window has no handle on
    /// the console that opened it and is sent no referrer, so it shares nothing with it.
    /// </summary>
    public const string NewWindowRel = "noopener noreferrer";

    /// <summary>The area key and first route segment.</summary>
    public const string AreaKey = "apps";

    /// <summary>The catalogue's route segment, reserved: no app slug can be browsed at <c>/apps/catalogue</c>.</summary>
    public const string CatalogueSegment = "catalogue";

    /// <summary>
    /// The app page segment that once hosted an app's UI inside the console. The UI now opens
    /// only in a window of its own, so an address under this segment is redirected to the same
    /// in-app path under <see cref="WindowSegment"/>, and nothing links to it.
    /// </summary>
    public const string OpenSegment = "open";

    /// <summary>
    /// The app page segment that hosts an app's UI on its own, filling a browser window of
    /// its own: <c>/apps/{slug}/window[/{in-app path}]</c>.
    /// </summary>
    public const string WindowSegment = "window";

    /// <summary>The query key selecting a source, or <see cref="AllSources"/>.</summary>
    public const string SourceQuery = "source";

    /// <summary>The query key selecting the installed-state filter.</summary>
    public const string FilterQuery = "filter";

    /// <summary>The query key carrying the text filter.</summary>
    public const string TextQuery = "q";

    /// <summary>The <see cref="SourceQuery"/> value for every source.</summary>
    public const string AllSources = "all";

    /// <summary>The separator between a slug and a version in a review address.</summary>
    public const char VersionSeparator = '@';

    /// <summary>"Your apps", in <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant the address is rooted at, or <see langword="null"/>.</param>
    public static ExplorerAddress Landing(string? tenant) => ExplorerAddress.Create(tenant, AreaKey);

    /// <summary>The catalogue, filtered.</summary>
    /// <param name="tenant">The tenant the address is rooted at, or <see langword="null"/>.</param>
    /// <param name="view">The source, filter and text.</param>
    public static ExplorerAddress Catalogue(string? tenant, AppsCatalogueView view)
    {
        ArgumentNullException.ThrowIfNull(view);

        var query = new List<KeyValuePair<string, string>>(3)
        {
            new(SourceQuery, view.SourceKey ?? AllSources),
            new(FilterQuery, FilterText(view.Filter)),
        };

        if (!string.IsNullOrWhiteSpace(view.Text))
        {
            query.Add(new(TextQuery, view.Text.Trim()));
        }

        return ExplorerAddress.Create(tenant, AreaKey, [CatalogueSegment], query);
    }

    /// <summary>The pre-install review of <paramref name="slug"/> as <paramref name="sourceKey"/> offers it.</summary>
    /// <param name="tenant">The tenant the address is rooted at, or <see langword="null"/>.</param>
    /// <param name="sourceKey">The source key.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="version">An exact version, or <see langword="null"/> for the source's newest.</param>
    public static ExplorerAddress Review(string? tenant, string sourceKey, string slug, string? version = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(slug);

        var segment = string.IsNullOrWhiteSpace(version) ? slug : slug + VersionSeparator + version;
        return ExplorerAddress.Create(tenant, AreaKey, [CatalogueSegment, sourceKey, segment]);
    }

    /// <summary>An installed app's overview page (A2).</summary>
    /// <param name="tenant">The tenant the address is rooted at, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    public static ExplorerAddress App(string? tenant, string slug) => ExplorerAddress.Create(tenant, AreaKey, [slug]);

    /// <summary>An installed app's UI in a browser window of its own (A2), where every "Open" control leads.</summary>
    /// <param name="tenant">The tenant the address is rooted at, or <see langword="null"/>.</param>
    /// <param name="slug">The app slug.</param>
    public static ExplorerAddress Window(string? tenant, string slug) => ExplorerAddress.Create(tenant, AreaKey, [slug, WindowSegment]);

    /// <summary>
    /// Whether <paramref name="address"/> is an app's window, <c>[/t/{tenant}]/apps/{slug}/window[/...]</c>,
    /// for any slug but the reserved catalogue segment.
    /// </summary>
    /// <param name="address">The address.</param>
    public static bool IsWindow(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return string.Equals(address.Area, AreaKey, StringComparison.Ordinal)
            && address.Path.Count >= 2
            && !string.Equals(address.Path[0], CatalogueSegment, StringComparison.Ordinal)
            && string.Equals(address.Path[1], WindowSegment, StringComparison.Ordinal);
    }

    /// <summary>
    /// Reads a review address's <c>{slug}[@{version}]</c> segment.
    /// </summary>
    /// <param name="segment">The segment.</param>
    /// <param name="slug">The slug.</param>
    /// <param name="version">The version, or <see langword="null"/> for the newest.</param>
    /// <returns>Whether the segment names a slug.</returns>
    public static bool TryReadSlugSegment(string? segment, out string slug, out string? version)
    {
        slug = string.Empty;
        version = null;
        if (string.IsNullOrWhiteSpace(segment))
        {
            return false;
        }

        var at = segment.IndexOf(VersionSeparator, StringComparison.Ordinal);
        if (at < 0)
        {
            slug = segment;
            return true;
        }

        if (at == 0 || at == segment.Length - 1)
        {
            return false;
        }

        slug = segment[..at];
        version = segment[(at + 1)..];
        return true;
    }

    /// <summary>The query text of <paramref name="filter"/>.</summary>
    /// <param name="filter">The filter.</param>
    public static string FilterText(AvailableAppFilter filter) => filter switch
    {
        AvailableAppFilter.Installed => "installed",
        AvailableAppFilter.Available => "available",
        AvailableAppFilter.Updates => "updates",
        _ => "all",
    };

    /// <summary>Reads a filter's query text, defaulting to <see cref="AvailableAppFilter.All"/>.</summary>
    /// <param name="text">The query text.</param>
    public static AvailableAppFilter ReadFilter(string? text) => text?.Trim().ToLowerInvariant() switch
    {
        "installed" => AvailableAppFilter.Installed,
        "available" => AvailableAppFilter.Available,
        "updates" => AvailableAppFilter.Updates,
        _ => AvailableAppFilter.All,
    };
}
