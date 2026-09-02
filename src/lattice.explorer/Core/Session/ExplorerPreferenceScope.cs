namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// How widely a remembered preference applies. The scope is baked into the
/// stored key, so two scopes never see each other's value.
/// </summary>
/// <remarks>
/// Scoping is a correctness requirement, not a nicety. The Explorer's preference
/// document lives in one browser-origin store, so without a scope a preference
/// written while signed in as one operator against one cluster would be read back
/// by the next operator against a different cluster - resurrecting somebody
/// else's view, and in the tenant case pointing the shell at a tenant the new
/// identity may not even be able to see.
/// </remarks>
public enum ExplorerPreferenceScope
{
    /// <summary>
    /// Remembered per signed-in user, across every cluster they connect to. For
    /// preferences about the person rather than the data: theme, density, and
    /// other presentation choices that should follow the operator around.
    /// </summary>
    User,

    /// <summary>
    /// Remembered per signed-in user <em>and</em> per connected cluster. The
    /// default, and the right choice for anything naming something that lives in
    /// a cluster - an area, a tenant, a tree, a detail surface - because those
    /// names mean nothing, or mean something different, on another cluster.
    /// </summary>
    UserAndCluster,
}
