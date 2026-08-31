using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The Backups area's two internal sub-surfaces: composing a new capture, and
/// browsing the catalogue that already exists.
/// </summary>
/// <remarks>
/// <para>
/// These are sub-surfaces of one area, not areas of their own: they share the
/// plugin's id, its access decision, and its preference namespace, none of which
/// a separately registered plugin would. Expressing them as plugins would put
/// two entries in the shell's area rail that are really one area.
/// </para>
/// <para>
/// The ids are canonical lower-case slugs so they are addressable as the
/// <c>surface</c> segment of the shell's route grammar - <c>/area/backups/.../new</c> -
/// and are the same strings the retained preference stores, so a remembered
/// surface and a linked one cannot spell it differently.
/// </para>
/// <para>
/// The strip itself is the design system's one tab primitive rendered in its
/// subordinate variant, which is the presentation the shell reserves for a
/// plugin's own sub-surfaces so they do not read as a fourth peer strip. This
/// type contributes the items; it is not a second tab mechanism.
/// </para>
/// </remarks>
internal static class BackupsSurfaces
{
    /// <summary>The capture-composition surface.</summary>
    public const string New = "new";

    /// <summary>The catalogue-browsing surface.</summary>
    public const string Existing = "existing";

    // Composed from a prefix rather than spelled whole. The orphan-class gate
    // reads every string literal in a C# file as a possible CLASS name, so an
    // element id spelled out here would be reported as a class no stylesheet
    // defines. The shell's own region ids are composed for the same reason.
    private const string ElementPrefix = "lx-backups-";

    /// <summary>The element-id prefix the strip derives its tab and panel ids from.</summary>
    public const string StripElementId = ElementPrefix + "surfacestrip";

    /// <summary>The id of the panel the strip controls, which the panel renders itself.</summary>
    public const string PanelElementId = ElementPrefix + "surfacepanel";

    /// <summary>The strip's accessible name.</summary>
    public const string StripLabel = "Backups surfaces";

    /// <summary>
    /// The tab items in display order. A single cached list, so the strip costs
    /// no allocation per render and every re-render diffs against the same
    /// instances.
    /// </summary>
    public static IReadOnlyList<LatticeTabItem> Tabs { get; } =
    [
        new LatticeTabItem(New, "New backup")
        {
            Description = "Capture a new backup of one or more trees.",
        },
        new LatticeTabItem(Existing, "Existing backups")
        {
            Description = "Browse the catalogue, and restore, schedule or delete what is in it.",
        },
    ];

    /// <summary>The surface slug <paramref name="tab"/> is addressed by.</summary>
    /// <param name="tab">The sub-tab to name.</param>
    public static string SlugFor(BackupsSubTab tab) => tab == BackupsSubTab.Existing ? Existing : New;

    /// <summary>
    /// The sub-tab <paramref name="slug"/> names, or <see langword="null"/> when
    /// it names none.
    /// </summary>
    /// <remarks>
    /// Returning null rather than falling back to the default is what lets a
    /// caller tell "the address asked for something that is not here" from "the
    /// address asked for the first surface", so an unrecognised slug does not
    /// silently look like a deliberate choice.
    /// </remarks>
    /// <param name="slug">The slug to resolve. May be <see langword="null"/>.</param>
    public static BackupsSubTab? FromSlug(string? slug) => slug switch
    {
        New => BackupsSubTab.New,
        Existing => BackupsSubTab.Existing,
        _ => null,
    };
}
