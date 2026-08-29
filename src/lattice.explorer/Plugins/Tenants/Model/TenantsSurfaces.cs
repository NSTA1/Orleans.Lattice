using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// The Tenants plugin's internal sub-surfaces: the stable ids they are keyed by
/// and the tab strip the panel renders them through.
/// <para>
/// The ids are the plugin's own vocabulary and are compared ordinally. They are
/// separate from the plugin id, which keys the area itself in the shell.
/// </para>
/// </summary>
public static class TenantsSurfaces
{
    /// <summary>The tenant list and its lifecycle operations.</summary>
    public const string Tenants = "tenants";

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
        new LatticeTabItem(Tenants, "Tenants")
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
        !string.Equals(surfaceId, Tenants, StringComparison.Ordinal);
}
