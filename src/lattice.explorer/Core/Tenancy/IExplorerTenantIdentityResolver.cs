namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// Establishes the caller's active tenant on the per-circuit
/// <see cref="IExplorerTenantContext"/> from the authenticated identity. It is the
/// single seam that maps "who is signed in" onto "which tenant this circuit is
/// scoped to", so the fail-closed <see cref="IExplorerTenantView"/> has an active
/// tenant to scope against once a user signs in.
/// </summary>
/// <remarks>
/// Activation follows tenancy being enabled: the default resolver is a no-op
/// whenever <see cref="IExplorerTenantView.IsActive"/> is <see langword="false"/>,
/// so a non-tenant deployment leaves <see cref="IExplorerTenantContext.ActiveTenant"/>
/// untouched (<see langword="null"/>) and the Explorer UI is byte-for-byte
/// unchanged. When the view is active the resolver is fail-closed: an anonymous
/// caller establishes no active tenant (and therefore sees nothing), and an
/// authenticated caller is mapped to a tenant by the registered resolver.
/// <para>
/// The default implementation maps every authenticated caller to
/// <see cref="ExplorerTenantId.Default"/> - the documented single-tenant and
/// development default. A production multi-tenant head replaces this seam with a
/// resolver that reads the authenticated principal's tenant claim (for example
/// from its identity provider) and maps it onto the circuit's active tenant.
/// </para>
/// </remarks>
public interface IExplorerTenantIdentityResolver
{
    /// <summary>
    /// Resolves the caller's active tenant from the current authenticated identity
    /// and applies it to <see cref="IExplorerTenantContext.ActiveTenant"/>. A no-op
    /// when the tenant view is inactive. Idempotent: calling it again for an
    /// unchanged sign-in re-establishes the same active tenant. Call it once per
    /// circuit after sign-in and whenever the authentication state changes.
    /// </summary>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>A task that completes when the active tenant has been resolved.</returns>
    ValueTask ResolveAsync(CancellationToken cancellationToken = default);
}
