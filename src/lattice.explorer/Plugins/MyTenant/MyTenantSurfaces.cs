using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.MyTenant;

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
}
