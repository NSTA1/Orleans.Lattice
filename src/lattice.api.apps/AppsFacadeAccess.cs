using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The administrative gate the app catalogue shares with <see cref="LatticeAppsControl"/>: the caller's
/// active tenant, and <see cref="LatticeOperation.AppInstall"/> over the cluster-wide scope through the
/// shared access gate.
/// </summary>
internal static class AppsFacadeAccess
{
    /// <summary>
    /// Authorizes <see cref="LatticeOperation.AppInstall"/> over the cluster-wide scope. A key-filtered allow is
    /// refused; system-origin callers and the no-op gate skip enforcement.
    /// </summary>
    /// <param name="gate">The shared access gate.</param>
    /// <param name="membership">The membership context, or null for anonymous.</param>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns>A task that completes when the caller is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized.</exception>
    public static ValueTask AuthorizeInstallAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
            gate, membership, LatticeScope.ClusterWideTreeId, LatticeOperation.AppInstall, cancellationToken);

    /// <summary>
    /// Resolves the caller's active tenant, preferring the synchronous warm path. A resolver that denies by
    /// resolving the uninitialised tenant fails closed.
    /// </summary>
    /// <param name="tenants">The active-tenant resolver.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The active tenant.</returns>
    /// <exception cref="LatticeTenantAccessDeniedException">The tenant could not be resolved.</exception>
    public static ValueTask<TenantId> ResolveTenantAsync(ITenantContextResolver tenants, CancellationToken cancellationToken)
    {
        if (tenants.TryResolveCurrent(out var tenant))
        {
            return new ValueTask<TenantId>(Require(tenant));
        }

        return ResolveSlowAsync(tenants, cancellationToken);
    }

    private static async ValueTask<TenantId> ResolveSlowAsync(ITenantContextResolver tenants, CancellationToken cancellationToken) =>
        Require(await tenants.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false));

    private static TenantId Require(TenantId tenant) =>
        tenant.Value is null ? throw new LatticeTenantAccessDeniedException() : tenant;
}
