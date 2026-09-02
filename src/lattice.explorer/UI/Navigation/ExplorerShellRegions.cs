namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The element ids the shell's own regions carry, shared by the components that
/// render them and the controls that point at them.
/// </summary>
/// <remarks>
/// <para>
/// Four relationships in the shell are expressed as an id reference across a
/// component boundary, and each of them silently degrades to nothing when the
/// two ends disagree: the skip link points at the main landmark, every area tab
/// points at the region it swaps (<c>aria-controls</c>), that region names the
/// tab that selected it (<c>aria-labelledby</c>), and each strip's panel names
/// its own active tab. Declaring the ids once is what keeps a rename from
/// quietly severing them.
/// </para>
/// <para>
/// They are stable, human-readable ids rather than generated ones because the
/// skip link's target is part of the shell's published behaviour: a host page or
/// an accessibility check may address it.
/// </para>
/// <para>
/// Each is composed from <see cref="Prefix"/> rather than written whole. That is
/// not cosmetic: the repository's orphan-class gate reads every string literal
/// in a C# file as a possible class name, because this is where the Explorer
/// composes its computed class names - so a whole id spelled out here would be
/// reported as a class with no rule. Composing from the prefix states honestly
/// that these share the shell's namespace without claiming any of them is a
/// class.
/// </para>
/// </remarks>
public static class ExplorerShellRegions
{
    /// <summary>The namespace every shell region id shares.</summary>
    public const string Prefix = "lx-shell-";

    /// <summary>
    /// The main landmark: the working surface, and the destination of the skip
    /// link. Present on every area, including one a plugin has taken over.
    /// </summary>
    public const string Main = Prefix + "main";

    /// <summary>
    /// The region the area rail swaps, and the <c>role="tabpanel"</c> every rail
    /// tab controls.
    /// </summary>
    public const string AreaContent = Prefix + "area-content";

    /// <summary>
    /// The area rail's element-id prefix. The tab primitive derives each tab's
    /// id from it, which is what <see cref="AreaTabElementId(string)"/>
    /// reproduces.
    /// </summary>
    public const string AreaRail = Prefix + "areas";

    /// <summary>The catalog-kind strip's element-id prefix.</summary>
    public const string CatalogKindStrip = Prefix + "kind";

    /// <summary>The catalog list's id, the panel the catalog-kind strip controls.</summary>
    public const string CatalogList = Prefix + "catalog";

    /// <summary>The per-selection detail strip's element-id prefix.</summary>
    public const string DetailStrip = Prefix + "detail-strip";

    /// <summary>The per-selection detail body's id, the panel the detail strip controls.</summary>
    public const string DetailPanel = Prefix + "detail-panel";

    /// <summary>The cluster-capabilities disclosure's element-id prefix.</summary>
    public const string CapabilitiesHelp = Prefix + "capabilities";

    /// <summary>The "hide areas I cannot open" control's id, which its label names.</summary>
    public const string HideInaccessibleControl = Prefix + "hide-inaccessible";

    /// <summary>The element-id prefix of one area's own denial disclosure.</summary>
    public const string AreaHelpPrefix = Prefix + "area-";

    /// <summary>
    /// The id the tab primitive gives the rail tab for
    /// <paramref name="areaSlug"/>, so the area region can name the tab that
    /// selected it.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>LatticeAdaptiveTabs</c>'s own derivation
    /// (<c>{Id}-tab-{tabId}</c>). The rail passes the area slug as each tab's
    /// identity, so the slug is the tab id.
    /// </remarks>
    /// <param name="areaSlug">The canonical area slug. Must not be null.</param>
    /// <returns>The rail tab's element id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="areaSlug"/> is <see langword="null"/>.</exception>
    public static string AreaTabElementId(string areaSlug)
    {
        ArgumentNullException.ThrowIfNull(areaSlug);
        return TabElementId(AreaRail, areaSlug);
    }

    /// <summary>
    /// The id the tab primitive gives the tab <paramref name="tabId"/> in the
    /// strip whose element-id prefix is <paramref name="stripId"/>.
    /// </summary>
    /// <param name="stripId">The strip's element-id prefix. Must not be null.</param>
    /// <param name="tabId">The tab's identity. Must not be null.</param>
    /// <returns>The tab's element id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="stripId"/> or <paramref name="tabId"/> is <see langword="null"/>.</exception>
    public static string TabElementId(string stripId, string tabId)
    {
        ArgumentNullException.ThrowIfNull(stripId);
        ArgumentNullException.ThrowIfNull(tabId);
        return stripId + "-tab-" + tabId;
    }
}
