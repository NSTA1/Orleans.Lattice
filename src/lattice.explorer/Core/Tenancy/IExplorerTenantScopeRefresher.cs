namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The seam a tenant-scope mutation uses to tell the host that the caller's
/// scope changed, so the host re-projects it and re-probes whatever it gates on
/// it.
/// <para>
/// It exists because switching tenant is a <em>refresh occasion</em>, exactly as
/// mounting the shell, a sign-in change, and a reconnect are: each of those
/// re-resolves the effective tenant scope and re-probes every gate, and a tenant
/// switch changes the same inputs. Without it a switch mutates
/// <see cref="IExplorerTenantContext.ActiveTenant"/> and nothing downstream
/// notices, leaving the published scope - and every decision derived from it -
/// describing the tenant the caller just left.
/// </para>
/// </summary>
/// <remarks>
/// The contract lives here, beside the switcher that raises it, and the host
/// supplies the implementation. That is what keeps the tenancy core free of any
/// knowledge of the plugin model: the switcher knows only that <em>something</em>
/// wants to hear about a scope change.
/// <para>
/// Implementations are expected to be fault-isolated and fail-closed - a refresh
/// that cannot complete must narrow rather than widen - because the mutation
/// that triggered it has already been applied and cannot be unwound by a failed
/// notification.
/// </para>
/// </remarks>
public interface IExplorerTenantScopeRefresher
{
    /// <summary>
    /// Re-resolves everything derived from the caller's tenant scope.
    /// </summary>
    /// <param name="cancellationToken">Cancels the refresh.</param>
    /// <returns>A task that completes once the refresh has been applied.</returns>
    Task RefreshAsync(CancellationToken cancellationToken = default);
}
