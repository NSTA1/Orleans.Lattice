namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// <b>The Explorer's preference contract.</b> Exactly what the shell remembers
/// about you between sessions, at what scope, and for how long.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is remembered.</b> Only the keys declared here and any a feature
/// registers with <see cref="IExplorerPreferenceCatalog.Register"/>. Nothing
/// else: a component that wants state to survive a reload declares a key, and a
/// component that does not declare one does not persist. That is the whole point
/// of the contract - the previous ad hoc arrangement is why the shell restored
/// your detail tab faithfully while forgetting which area you were in.
/// </para>
/// <para>
/// <b>At what scope.</b> Every key below is
/// <see cref="ExplorerPreferenceScope.UserAndCluster"/>, because each names
/// something that lives inside a cluster. Switching account or cluster therefore
/// shows a clean view rather than resurrecting somebody else's. Presentation
/// preferences that should follow the operator between clusters (theme, density)
/// are declared by their own feature at
/// <see cref="ExplorerPreferenceScope.User"/>.
/// </para>
/// <para>
/// <b>For how long.</b> Until the user resets the view
/// (<see cref="IExplorerShellPreferences.ResetAsync"/>), until the underlying
/// entry passes <see cref="UiPreferenceStore.DefaultRetention"/> untouched, or
/// until the browser's storage for the origin is cleared. Nothing here is
/// security state and nothing here is authoritative: a remembered value is a
/// hint that is always re-validated against the live cluster on restore, and
/// falls back with an explanation when it no longer resolves.
/// </para>
/// <para>
/// <b>What is deliberately not remembered.</b> Credentials and tokens (they live
/// in the credential store), anything the URL already carries for the current
/// view, and transient per-render state (that is
/// <see cref="IUiSessionStore"/>'s job).
/// </para>
/// </remarks>
public static class ExplorerPreferenceKeys
{
    /// <summary>The area the shell was last showing, as a route area slug.</summary>
    public static ExplorerPreferenceKey ActiveArea { get; } = new(
        "shell.area",
        "the area you were last in");

    /// <summary>The catalog kind the tree browser was last showing, as a route kind slug.</summary>
    public static ExplorerPreferenceKey CatalogKind { get; } = new(
        "shell.catalog-kind",
        "the catalog you were last browsing");

    /// <summary>The id of the item last selected in the catalog.</summary>
    public static ExplorerPreferenceKey Selection { get; } = new(
        "shell.selection",
        "the item you last selected");

    /// <summary>The detail surface the selection was last open on, as a route surface slug.</summary>
    public static ExplorerPreferenceKey DetailSurface { get; } = new(
        "shell.surface",
        "the surface you last had open");

    /// <summary>The tenant the view was last scoped to.</summary>
    public static ExplorerPreferenceKey ActiveTenant { get; } = new(
        "shell.tenant",
        "the tenant you were last scoped to");

    /// <summary>Whether the view last spanned every reachable tenant.</summary>
    public static ExplorerPreferenceKey AllTenantsVisible { get; } = new(
        "shell.all-tenants",
        "your all-tenants view setting");

    /// <summary>
    /// The shell's own declared keys, seeded into every
    /// <see cref="ExplorerPreferenceCatalog"/>. A feature adds its keys by
    /// registering them, not by editing this list.
    /// </summary>
    public static IReadOnlyList<ExplorerPreferenceKey> All { get; } =
    [
        ActiveArea,
        CatalogKind,
        Selection,
        DetailSurface,
        ActiveTenant,
        AllTenantsVisible,
    ];
}
