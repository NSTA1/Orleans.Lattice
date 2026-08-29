namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The tenancy facts a plugin may read: whether tenant scoping is enabled at
/// all, which tenant is active, and the visibility the host has already
/// resolved for the caller.
/// <para>
/// The visibility here is the <em>effective</em> one: the host has already
/// validated any cross-tenant assertion and degraded it fail-closed if the
/// caller did not validate as a platform operator. A plugin reads the outcome
/// and cannot re-assert a wider scope, which keeps the validation at the single
/// narrowest seam rather than in each plugin.
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so reading it per render allocates nothing.
/// <c>default</c> is the inactive scope, which is what a cluster without the
/// tenancy add-on reports.
/// </para>
/// </summary>
/// <param name="IsActive">
/// <see langword="true"/> when tenant scoping is enabled. When
/// <see langword="false"/> the deployment has no tenancy add-on, so a tenancy
/// plugin's gate should report
/// <see cref="ExplorerPluginAccessState.Unavailable"/> and render nothing.
/// </param>
/// <param name="ActiveTenantId">
/// The caller's active tenant id, or <see langword="null"/> when none is
/// established.
/// </param>
/// <param name="Visibility">The already-resolved effective visibility.</param>
public readonly record struct ExplorerPluginTenantScope(
    bool IsActive,
    string? ActiveTenantId,
    ExplorerPluginTenantVisibility Visibility)
{
    /// <summary>
    /// The scope a deployment without the tenancy add-on reports: inactive, no
    /// active tenant, and the fail-closed active-tenant visibility.
    /// </summary>
    public static ExplorerPluginTenantScope Inactive { get; }
        = new(IsActive: false, ActiveTenantId: null, ExplorerPluginTenantVisibility.ActiveTenant);
}
