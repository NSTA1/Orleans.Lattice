using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The single controlled domain contract the Access plugin operates against.
/// <para>
/// This is the plugin's whole reach, declared once in its own source and
/// resolved for it by the host through
/// <see cref="Plugins.IExplorerPluginHostContext.GetDomain{TDomain}"/>. The
/// views take this and nothing else - no container, no service locator, and no
/// cluster connection - so what the Access surface can touch is reviewable from
/// this file alone (epic decision D3).
/// </para>
/// </summary>
public interface IAccessDomain
{
    /// <summary>The membership directory and group administration surface.</summary>
    IMembershipAdminService Membership { get; }

    /// <summary>The authorization rule, explain, and effective-permissions surface.</summary>
    IPolicyAdminService Policy { get; }

    /// <summary>
    /// The tree catalog the Policies and Explain surfaces scope themselves to.
    /// Trees are the policy scope unit, so the plugin reads the catalog rather
    /// than asking an operator to type a tree id.
    /// </summary>
    ICatalogReader Catalog { get; }

    /// <summary>
    /// The cluster's best-effort active authentication mode, as last published
    /// by this plugin's own access gate.
    /// <see cref="ExplorerAccessAuthenticationMode.Unknown"/> until a probe has
    /// read the access model. Advisory display state that belongs to this
    /// plugin, not to any shared record - no other Explorer surface reads it.
    /// </summary>
    ExplorerAccessAuthenticationMode AuthenticationMode { get; }

    /// <summary>
    /// Creates a fresh principal-label cache, scoped to the view that owns it,
    /// so a panel's resolved display names are dropped with the panel.
    /// </summary>
    PrincipalLabelResolver CreateLabelResolver();

    /// <summary>
    /// Creates a fresh subject-picker model over its own single in-flight
    /// search debounce, so each picker instance debounces independently. The
    /// debounce is resolved through the injectable
    /// <see cref="ISubjectSearchDebounce"/> seam, so search timing stays
    /// substitutable in a test rather than tied to a wall clock.
    /// </summary>
    SubjectPickerModel CreateSubjectPicker();
}
