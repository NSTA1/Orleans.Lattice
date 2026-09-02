using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The My Tenant plugin's internal sub-surfaces, and the tab items the design
/// system's adaptive strip renders for them.
/// <para>
/// These are internal to this plugin, exactly as the Access plugin's are: the
/// plugin model keys a whole area, its access decision, and its preference
/// namespace, none of which a sub-view of one panel wants. Expressing them as
/// plugins would put six entries in the shell's area strip that are really one
/// area.
/// </para>
/// </summary>
public static class MyTenantSurfaces
{
    /// <summary>The tenant's own descriptor, status, and accessible-tenant list.</summary>
    public const string Overview = "overview";

    /// <summary>The tenant's admin subjects: list, add, and remove.</summary>
    public const string Members = "members";

    /// <summary>Consumption against each quota ceiling.</summary>
    public const string Quota = "quota";

    /// <summary>Region residency within the operator-authorized allowed set.</summary>
    public const string Regions = "regions";

    /// <summary>This tenant's side of the two-step cross-tenant grant agreement.</summary>
    public const string Sharing = "sharing";

    /// <summary>
    /// The tenant-metrics section. A declared seam with a placeholder body until
    /// the metrics issue fills it, so the surface exists in the strip from the
    /// start rather than appearing later and moving every tab beside it.
    /// </summary>
    public const string Metrics = "metrics";

    /// <summary>
    /// The tab items in display order. A single cached array, so the strip costs
    /// no allocation per render and every re-render diffs against the same
    /// instances.
    /// </summary>
    public static IReadOnlyList<LatticeTabItem> Tabs { get; } =
    [
        new LatticeTabItem(Overview, "Overview")
        {
            Description = "Your tenant's descriptor and lifecycle status.",
        },
        new LatticeTabItem(Members, "Members")
        {
            Description = "The subjects holding admin authority over your tenant.",
        },
        new LatticeTabItem(Quota, "Quota")
        {
            Description = "Your consumption against each quota ceiling.",
        },
        new LatticeTabItem(Regions, "Regions")
        {
            Description = "Where your tenant is resident, within the regions an operator has allowed.",
        },
        new LatticeTabItem(Sharing, "Sharing")
        {
            Description = "Cross-tenant grants your tenant has offered, and offers awaiting your decision.",
        },
        new LatticeTabItem(Metrics, "Metrics")
        {
            Description = "Your tenant's own metrics.",
        },
    ];

    /// <summary>
    /// Whether <paramref name="surfaceId"/> names one of this plugin's surfaces.
    /// Used to reject a retained preference that no longer matches a surface, so
    /// a renamed or retired tab cannot leave the panel showing nothing.
    /// </summary>
    /// <param name="surfaceId">The candidate surface id.</param>
    /// <returns><see langword="true"/> when the id names a declared surface.</returns>
    public static bool IsKnown(string? surfaceId) => surfaceId switch
    {
        Overview or Members or Quota or Regions or Sharing or Metrics => true,
        _ => false,
    };

    /// <summary>
    /// The glossary term explaining <paramref name="surfaceId"/>, or
    /// <see langword="null"/> when the id names no declared surface. Lets the
    /// panel hang one help disclosure off the active sub-surface rather than
    /// baking prose beside each tab.
    /// </summary>
    /// <remarks>
    /// A frozen-dictionary probe per call and no allocation, so it is safe on the
    /// render path.
    /// </remarks>
    /// <param name="surfaceId">The sub-surface id.</param>
    /// <returns>The term, or <see langword="null"/>.</returns>
    public static ExplorerTerm? TermFor(string? surfaceId) => surfaceId switch
    {
        Overview => ExplorerGlossary.Find(ExplorerTermIds.Tenant),
        Members => ExplorerGlossary.Find(ExplorerTermIds.AdminSubject),
        Quota => ExplorerGlossary.Find(ExplorerTermIds.Quota),
        Regions => ExplorerGlossary.Find(ExplorerTermIds.Residency),
        Sharing => ExplorerGlossary.Find(ExplorerTermIds.Grant),
        Metrics => ExplorerGlossary.Find(ExplorerTermIds.Tenant),
        _ => null,
    };

    /// <summary>
    /// The element-id prefix the panel gives the active sub-surface's help
    /// disclosure, so its trigger and its explanation get stable, unique ids.
    /// </summary>
    /// <remarks>
    /// Deliberately not an <c>lx</c>-prefixed name: it is an element id, not a
    /// class, and the repository's orphan-class gate reads every
    /// <c>lx</c>-prefixed literal in a C# file as a class that must have a rule.
    /// </remarks>
    public const string HelpIdPrefix = "my-tenant-help-";

    /// <summary>
    /// The help disclosure's element id for <paramref name="surfaceId"/>, or
    /// <see langword="null"/> when the id names no declared surface.
    /// </summary>
    /// <remarks>
    /// Every arm is a compile-time constant, so the panel spends no allocation
    /// composing one per render.
    /// </remarks>
    /// <param name="surfaceId">The sub-surface id.</param>
    /// <returns>The element id, or <see langword="null"/>.</returns>
    public static string? HelpIdFor(string? surfaceId) => surfaceId switch
    {
        Overview => HelpIdPrefix + Overview,
        Members => HelpIdPrefix + Members,
        Quota => HelpIdPrefix + Quota,
        Regions => HelpIdPrefix + Regions,
        Sharing => HelpIdPrefix + Sharing,
        Metrics => HelpIdPrefix + Metrics,
        _ => null,
    };
}
