using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// <b>Keeps two adjacent navigation tiers from saying the same word.</b> An area
/// and its own first sub-surface must not share a name: when they do, the rail
/// says "Tenants", the strip inside it says "Tenants", and neither tells the
/// caller which one they are on.
/// </summary>
/// <remarks>
/// <para>
/// The collision is structural, not a naming mistake. An area's root sub-surface
/// is genuinely "the area itself", so the obvious label for it is the area's own
/// label, and every plugin author reaches for it independently. Rather than ask
/// each of them to remember, the shell resolves it at the seam where both tiers
/// are known: a sub-surface whose label is its area's label is relabelled to
/// <see cref="AreaRootSurfaceLabel"/>, which is what the tier already means.
/// </para>
/// <para>
/// The identity and the description are untouched, so a relabelled surface keeps
/// its URL slug, its retained preference, and its explanation. Only the word on
/// the control changes.
/// </para>
/// <para>
/// The common case allocates nothing: when no label collides, the caller's own
/// list is handed straight back.
/// </para>
/// </remarks>
public static class ExplorerAreaSurfaceLabels
{
    /// <summary>
    /// The label a sub-surface takes when its own label duplicates its area's.
    /// The conventional name for an area's root surface, and already the
    /// spelling the My Tenant area uses for exactly that surface.
    /// </summary>
    public const string AreaRootSurfaceLabel = "Overview";

    /// <summary>
    /// Returns <paramref name="tabs"/> with any tab whose label duplicates
    /// <paramref name="areaLabel"/> relabelled to
    /// <see cref="AreaRootSurfaceLabel"/>.
    /// </summary>
    /// <remarks>
    /// Comparison is case-insensitive and ordinal: "Tenants" and "tenants" are
    /// the same word to a reader, and that is who the rule is for. Returns the
    /// same instance when nothing collides, so a plugin that never duplicates
    /// its area's name pays nothing for the check.
    /// </remarks>
    /// <param name="areaLabel">
    /// The active area's label, or <see langword="null"/> when the sub-surfaces
    /// are not nested inside a named area - in which case there is nothing to
    /// collide with and the list is returned unchanged.
    /// </param>
    /// <param name="tabs">The sub-surface tabs, or <see langword="null"/>.</param>
    /// <returns>The tabs to render.</returns>
    public static IReadOnlyList<LatticeTabItem>? Disambiguate(
        string? areaLabel,
        IReadOnlyList<LatticeTabItem>? tabs)
    {
        if (tabs is null || tabs.Count == 0 || string.IsNullOrEmpty(areaLabel))
        {
            return tabs;
        }

        var collision = IndexOfCollision(areaLabel, tabs);
        if (collision < 0)
        {
            // The overwhelmingly common case. Handing back the caller's own
            // instance keeps this free for every plugin that already names its
            // surfaces distinctly, and keeps the cached-list identity the tab
            // primitive diffs against.
            return tabs;
        }

        var resolved = new LatticeTabItem[tabs.Count];
        for (var i = 0; i < tabs.Count; i++)
        {
            var tab = tabs[i];
            resolved[i] = i == collision
                ? new LatticeTabItem(tab.Id, AreaRootSurfaceLabel)
                {
                    IsEnabled = tab.IsEnabled,
                    Description = tab.Description,
                }
                : tab;
        }

        return resolved;
    }

    /// <summary>
    /// The index of the first tab whose label duplicates
    /// <paramref name="areaLabel"/>, or <c>-1</c> when none does.
    /// </summary>
    /// <param name="areaLabel">The area's label, or <see langword="null"/>.</param>
    /// <param name="tabs">The sub-surface tabs, or <see langword="null"/>.</param>
    public static int IndexOfCollision(string? areaLabel, IReadOnlyList<LatticeTabItem>? tabs)
    {
        if (tabs is null || string.IsNullOrEmpty(areaLabel))
        {
            return -1;
        }

        // Indexed rather than a LINQ predicate: this runs on the render path of
        // every plugin area that hosts sub-surfaces.
        for (var i = 0; i < tabs.Count; i++)
        {
            if (string.Equals(tabs[i].Label, areaLabel, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }
}
