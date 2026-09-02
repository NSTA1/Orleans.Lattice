using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The My Tenant plugin's own key vocabulary: the stable id its access decision
/// is filed under, and the scopes the plugin publishes its own advisory
/// sub-decisions against.
/// <para>
/// The plugin owns these strings; nothing outside the My Tenant feature needs to
/// know them, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class MyTenantPluginKeys
{
    /// <summary>The stable plugin id the My Tenant area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.mytenant";

    /// <summary>
    /// The scope the plugin files its platform-operator-gate diagnostic under.
    /// <para>
    /// It is not an authorization decision: it records whether the head supplied
    /// a real <c>IExplorerTenantOperatorGate</c> or is still running on the
    /// navigation core's fail-closed placeholder, so a head that called
    /// <c>AddExplorerTenantView()</c> before <c>AddExplorerAccess()</c> is told
    /// so instead of silently losing every tenant switch.
    /// </para>
    /// </summary>
    public const string OperatorGateScope = "operator-gate";

    /// <summary>
    /// The query key carrying the open sub-surface in the address, so a link to
    /// the sharing surface reopens on the sharing surface.
    /// </summary>
    /// <remarks>
    /// Named for this area rather than a bare <c>surface</c>, so the two tenancy
    /// areas - and any other area adding one - carry their own state
    /// independently instead of overwriting each other's through the parameter
    /// set a route keeps when the area changes. Canonical lower case, as every
    /// route segment and query key in the shell is.
    /// </remarks>
    public const string SurfaceQueryKey = "my-tenant-surface";

    /// <summary>
    /// The area's canonical route slug, derived from <see cref="PluginId"/> the
    /// same way the shell derives it, so the plugin and the shell always agree on
    /// the address the area answers at.
    /// </summary>
    public static string AreaSlug { get; } = ExplorerRouteSlug.FromIdentifier(PluginId);

    /// <summary>
    /// The sub-surface the caller last had open here. Scoped per user and per
    /// cluster, because it names a surface over a particular cluster's tenant.
    /// </summary>
    /// <remarks>
    /// Registered by the panel when it mounts rather than by a head, so a
    /// deployment gains it by rendering the area and the reset-view affordance
    /// discloses and clears it with no further wiring.
    /// </remarks>
    public static ExplorerPreferenceKey SurfacePreference { get; } = new(
        "mytenant.surface",
        "the tenant surface you last had open");
}
