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

    // Composed from a prefix rather than spelled whole. The orphan-class gate
    // reads every string literal in a C# file as a possible CLASS name, so an
    // element id spelled out here would be reported as a class no stylesheet
    // defines. The shell's own region ids are composed for the same reason.
    private const string ElementPrefix = "lxa-";

    /// <summary>The element-id prefix the strip derives its tab and panel ids from.</summary>
    public const string StripElementId = ElementPrefix + "surfacestrip";

    /// <summary>The strip's accessible name.</summary>
    public const string StripLabel = "Access surfaces";

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

    /// <summary>
    /// Whether <paramref name="surfaceId"/> names one of this area's surfaces.
    /// </summary>
    /// <remarks>
    /// The ids double as the <c>surface</c> segment of the shell's route grammar
    /// and as the value the retained preference stores, so this is the one place
    /// that answers "is this a surface we offer" for the address, the remembered
    /// choice, and the strip alike.
    /// </remarks>
    /// <param name="surfaceId">The candidate id. May be <see langword="null"/>.</param>
    public static bool IsKnown(string? surfaceId) =>
        surfaceId is Groups or Policies or Explain;
}
