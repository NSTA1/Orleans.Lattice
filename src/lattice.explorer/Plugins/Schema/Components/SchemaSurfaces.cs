using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The Schema area's three internal sub-surfaces: the enforcement policy (with
/// the compliance audit scoped to it), envelope versioning and remediation, and
/// the strict-mode dead-letter view.
/// </summary>
/// <remarks>
/// <para>
/// These are sub-surfaces of one area, not areas of their own: they share the
/// plugin's id, its access decision, and its preference namespace, none of which
/// a separately registered plugin would.
/// </para>
/// <para>
/// The ids are canonical lower-case slugs so they are addressable as the
/// <c>surface</c> segment of the shell's route grammar, and are the same strings
/// the retained preference stores, so a remembered surface and a linked one
/// cannot spell it differently.
/// </para>
/// </remarks>
internal static class SchemaSurfaces
{
    /// <summary>The enforcement-policy surface.</summary>
    public const string Policy = "policy";

    /// <summary>The envelope-versioning and remediation surface.</summary>
    public const string Versions = "versions";

    /// <summary>The strict-mode dead-letter surface.</summary>
    public const string DeadLetters = "dead-letters";

    // Composed from a prefix rather than spelled whole. The orphan-class gate
    // reads every string literal in a C# file as a possible CLASS name, so an
    // element id spelled out here would be reported as a class no stylesheet
    // defines. The shell's own region ids are composed for the same reason.
    private const string ElementPrefix = "lx-schema-";

    /// <summary>The element-id prefix the strip derives its tab and panel ids from.</summary>
    public const string StripElementId = ElementPrefix + "surfacestrip";

    /// <summary>The id of the panel the strip controls, which the panel renders itself.</summary>
    public const string PanelElementId = ElementPrefix + "surfacepanel";

    /// <summary>The strip's accessible name.</summary>
    public const string StripLabel = "Schema surfaces";

    /// <summary>
    /// The tab items in display order. A single cached list, so the strip costs
    /// no allocation per render and every re-render diffs against the same
    /// instances.
    /// </summary>
    public static IReadOnlyList<LatticeTabItem> Tabs { get; } =
    [
        new LatticeTabItem(Policy, "Policy")
        {
            Description = "Control how the selected tree's values are validated, and audit what already fits.",
        },
        new LatticeTabItem(Versions, "Versions")
        {
            Description = "Control how the selected tree's value shape evolves, and remediate older writes.",
        },
        new LatticeTabItem(DeadLetters, "Dead letters")
        {
            Description = "Inspect the writes strict enforcement rejected and set aside.",
        },
    ];

    /// <summary>The surface slug <paramref name="tab"/> is addressed by.</summary>
    /// <param name="tab">The sub-tab to name.</param>
    public static string SlugFor(SchemaPanel.SchemaTab tab) => tab switch
    {
        SchemaPanel.SchemaTab.Versions => Versions,
        SchemaPanel.SchemaTab.DeadLetters => DeadLetters,
        _ => Policy,
    };

    /// <summary>
    /// The sub-tab <paramref name="slug"/> names, or <see langword="null"/> when
    /// it names none.
    /// </summary>
    /// <remarks>
    /// Returning null rather than falling back to the default is what lets a
    /// caller tell "the address asked for something that is not here" from "the
    /// address asked for the first surface".
    /// </remarks>
    /// <param name="slug">The slug to resolve. May be <see langword="null"/>.</param>
    public static SchemaPanel.SchemaTab? FromSlug(string? slug) => slug switch
    {
        Policy => SchemaPanel.SchemaTab.Policy,
        Versions => SchemaPanel.SchemaTab.Versions,
        DeadLetters => SchemaPanel.SchemaTab.DeadLetters,
        _ => null,
    };
}
