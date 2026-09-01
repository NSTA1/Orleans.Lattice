using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// The tenant administration plugin's internal sub-surfaces: the stable ids they
/// are keyed by, the tab strip the panel renders them through, and the glossary
/// terms that explain each one at the point the caller meets it.
/// <para>
/// The ids are the plugin's own vocabulary, are compared ordinally, and are
/// canonical lower case because they address a sub-surface in the URL. They are
/// separate from the plugin id, which keys the area itself in the shell.
/// </para>
/// </summary>
/// <remarks>
/// The first sub-surface is called <see cref="Overview"/> and never
/// <c>Tenants</c>. The area itself is
/// <see cref="ExplorerVocabulary.TenantAdministrationArea"/>, and a sub-surface
/// that repeated its parent's word put "Tenants" in two adjacent navigation
/// tiers at once - the naming defect this plugin's half of issue #1856 closes.
/// The shell carries a de-duplicating backstop for that collision; naming the
/// surface properly here is what keeps the backstop from ever having to fire.
/// </remarks>
public static class TenantsSurfaces
{
    /// <summary>
    /// The area's root surface: the tenant list, its lifecycle operations, and
    /// the action that makes one tenant the active scope.
    /// </summary>
    public const string Overview = "overview";

    /// <summary>The selected tenant's quota ceilings and usage.</summary>
    public const string Quotas = "quotas";

    /// <summary>The selected tenant's allowed regions and per-region residency.</summary>
    public const string Regions = "regions";

    /// <summary>The selected tenant's admin subjects and cross-tenant grants.</summary>
    public const string Access = "access";

    /// <summary>
    /// The sub-surfaces in display order, as one shared cached list, so
    /// rendering the strip allocates nothing.
    /// </summary>
    public static IReadOnlyList<LatticeTabItem> Tabs { get; } =
    [
        new LatticeTabItem(Overview, "Overview")
        {
            Description = "Every tenant on the cluster, with its lifecycle state and headline usage.",
        },
        new LatticeTabItem(Quotas, "Quotas")
        {
            Description = "The selected tenant's quota ceilings and its usage against them.",
        },
        new LatticeTabItem(Regions, "Regions")
        {
            Description = "The selected tenant's allowed regions and its per-region residency.",
        },
        new LatticeTabItem(Access, "Tenant access")
        {
            Description = "The selected tenant's admin subjects and its cross-tenant grants.",
        },
    ];

    /// <summary>
    /// Whether <paramref name="surfaceId"/> is one of the three sub-surfaces that
    /// only mean something once a tenant is selected.
    /// </summary>
    /// <param name="surfaceId">The sub-surface id to test.</param>
    /// <returns><see langword="true"/> for a tenant-scoped sub-surface.</returns>
    public static bool RequiresTenant(string surfaceId) =>
        !string.Equals(surfaceId, Overview, StringComparison.Ordinal);

    /// <summary>
    /// Whether <paramref name="surfaceId"/> names one of this plugin's surfaces.
    /// Used to reject a remembered preference or an address naming a surface that
    /// no longer exists, so a renamed or retired tab cannot leave the panel
    /// showing nothing.
    /// </summary>
    /// <param name="surfaceId">The candidate surface id.</param>
    /// <returns><see langword="true"/> when the id names a declared surface.</returns>
    public static bool IsKnown(string? surfaceId) => surfaceId switch
    {
        Overview or Quotas or Regions or Access => true,
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
        Quotas => ExplorerGlossary.Find(ExplorerTermIds.Quota),
        Regions => ExplorerGlossary.Find(ExplorerTermIds.Region),
        Access => ExplorerGlossary.Find(ExplorerTermIds.Grant),
        Overview => ExplorerGlossary.Find(ExplorerTermIds.Tenant),
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
    public const string HelpIdPrefix = "tenant-admin-help-";

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
        Quotas => HelpIdPrefix + Quotas,
        Regions => HelpIdPrefix + Regions,
        Access => HelpIdPrefix + Access,
        _ => null,
    };
}
