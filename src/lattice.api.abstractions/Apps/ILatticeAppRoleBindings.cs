namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Transport-independent re-binding of an installed app's roles to membership groups in
/// the caller's active tenant, or the cluster when tenancy is disabled. It sits beside
/// <see cref="ILatticeAppsControl"/> rather than on it, so that contract stays unchanged.
/// </summary>
/// <remarks>
/// Implementations authorize <see cref="LatticeOperation.AppInstall"/> over the cluster-wide
/// scope before reading anything, so a denied caller learns nothing about which apps
/// exist. Bindings are group-only, exactly as at install: a role is bound to a membership
/// group, never to a user. Responses and exception messages never disclose composed
/// physical tree ids.
/// </remarks>
public interface ILatticeAppRoleBindings
{
    /// <summary>
    /// Replaces every role-to-group binding of the installed app, only for the explicitly
    /// named installed version. A version mismatch, a role the installed manifest does not
    /// declare, or a concurrent change to the install fails without mutation. An enabled
    /// app is re-applied, so its compiled role rules are replaced and a removed binding
    /// leaves no stale grant; a disabled or merely installed app is never enabled.
    /// </summary>
    /// <param name="request">The non-null version-pinned replacement bindings.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bindings and lifecycle state read back after the update.</returns>
    Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default);
}
