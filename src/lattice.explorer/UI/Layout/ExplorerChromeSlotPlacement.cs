namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The named regions of the shell's banner a feature may contribute a component
/// to, without that feature and the shell having to know about each other.
/// </summary>
/// <remarks>
/// <para>
/// The banner is shared chrome: the tenant scope control, the theme and density
/// controls, and the sign-in affordance all belong there, and each is owned by a
/// different package. Rather than have the layout name them - which would make
/// every one of them a reason to edit the layout - the layout renders a small,
/// fixed set of placements and a feature registers into one with
/// <c>AddExplorerChromeSlot</c>.
/// </para>
/// <para>
/// The set is deliberately small and closed. A placement is a promise about
/// where something appears and how prominent it is, so adding one is a shell
/// decision; contributing to one is not.
/// </para>
/// </remarks>
public enum ExplorerChromeSlotPlacement
{
    /// <summary>
    /// The scope control: which tenant, or which set of tenants, the view is
    /// pointed at. Rendered first in the banner's trailing group, before the
    /// view settings and the identity, because it changes what every surface
    /// below it shows.
    /// </summary>
    TenantScope = 0,

    /// <summary>
    /// View settings that change how the shell presents itself rather than what
    /// it shows - theme, density, contrast. Rendered as its own labelled region
    /// between the scope control and the identity, so it is discoverable in its
    /// own right rather than buried beside the sign-out control.
    /// </summary>
    ViewSettings = 1,
}
