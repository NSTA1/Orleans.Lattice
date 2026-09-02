using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// The tenant administration plugin's own key vocabulary: the stable id its
/// access decision is filed under in the Explorer's keyed plugin access store,
/// the area slug it is addressed by, and the two keys that make its open
/// sub-surface both bookmarkable and remembered.
/// <para>
/// The plugin owns these strings; nothing outside the feature needs to know
/// them, which is what lets the decision be keyed without a shared record.
/// </para>
/// </summary>
public static class TenantsPluginKeys
{
    /// <summary>The stable plugin id the tenant administration area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.tenants";

    /// <summary>
    /// The query key carrying the open sub-surface in the address, so a link to
    /// the quota surface reopens on the quota surface.
    /// </summary>
    /// <remarks>
    /// Named for this area rather than a bare <c>surface</c>, so the two tenancy
    /// areas - and any other area adding one - carry their own state
    /// independently instead of overwriting each other's through the parameter
    /// set a route keeps when the area changes. Canonical lower case, as every
    /// route segment and query key in the shell is.
    /// </remarks>
    public const string SurfaceQueryKey = "tenant-admin-surface";

    /// <summary>
    /// The area's canonical route slug, derived from <see cref="PluginId"/> the
    /// same way the shell derives it, so the plugin and the shell always agree on
    /// the address the area answers at.
    /// </summary>
    public static string AreaSlug { get; } = ExplorerRouteSlug.FromIdentifier(PluginId);

    /// <summary>
    /// The sub-surface the caller last had open here. Scoped per user and per
    /// cluster, because it names a surface over a particular cluster's tenants.
    /// </summary>
    /// <remarks>
    /// Registered by the panel when it mounts rather than by a head, so a
    /// deployment gains it by rendering the area and the reset-view affordance
    /// discloses and clears it with no further wiring.
    /// </remarks>
    public static ExplorerPreferenceKey SurfacePreference { get; } = new(
        "tenants.surface",
        "the tenant administration surface you last had open");
}
