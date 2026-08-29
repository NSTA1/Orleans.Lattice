using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Access.Views;

/// <summary>
/// The Access plugin's three internal sub-surfaces: Groups administers the
/// membership directory, Policies authors the authorization rules, and Explain
/// drives the facade's Explain / EffectivePermissions introspection.
/// <para>
/// These are <em>internal to this plugin</em>. They are deliberately not
/// registered with <see cref="Plugins.IExplorerPluginCatalog"/>: the plugin
/// model keys a whole surface, its access decision, and its preference
/// namespace, none of which a sub-view of one panel wants. Expressing them as
/// plugins would put three entries in the shell's area strip that are really
/// one area.
/// </para>
/// <para>
/// What they are <em>not</em> is a third tab registry. The strip is rendered by
/// the design system's single tab primitive
/// (<see cref="DesignSystem.Components.LatticeAdaptiveTabs"/>) over the shared
/// <see cref="LatticeTabItem"/> vocabulary, so the Explorer has exactly one tab
/// mechanism: the retired <c>AccessTab</c> enum and its <c>SetTab</c> switch,
/// with their bespoke markup and no keyboard support, are gone. The plugin
/// contributes a list of tab items, not a mechanism.
/// </para>
/// </summary>
internal static class AccessSurfaces
{
    /// <summary>The membership-directory surface: groups and their direct members.</summary>
    public const string Groups = "groups";

    /// <summary>The authorization-rule authoring surface, scoped to the selected tree.</summary>
    public const string Policies = "policies";

    /// <summary>The Explain / effective-permissions introspection surface.</summary>
    public const string Explain = "explain";

    /// <summary>
    /// The tab items in display order. A single cached array, so the strip costs
    /// no allocation per render and every re-render diffs against the same
    /// instances.
    /// </summary>
    public static IReadOnlyList<LatticeTabItem> Tabs { get; } =
    [
        new LatticeTabItem(Groups, "Groups")
        {
            Description = "Administer the membership directory: groups and their direct members.",
        },
        new LatticeTabItem(Policies, "Policies")
        {
            Description = "Author the authorization rules for the selected tree.",
        },
        new LatticeTabItem(Explain, "Explain")
        {
            Description = "Explain a decision, or list a subject's effective permissions.",
        },
    ];
}
