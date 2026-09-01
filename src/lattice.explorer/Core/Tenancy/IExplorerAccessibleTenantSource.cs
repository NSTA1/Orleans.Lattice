namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The single source of truth for <em>which tenants the current caller can
/// actually reach</em>. The shell's tenant scope control offers exactly this
/// list, and the identity resolver validates a remembered tenant against exactly
/// this list, so the picker and the tenant administration area can never diverge.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the seam lives here.</b> The list a tenant administration surface
/// renders comes from the cluster's control API, which the Explorer's navigation
/// core deliberately cannot reach. Declaring the contract here and letting the
/// administrative surface implement it keeps the dependency pointing the right
/// way: the core owns the question, the surface that already asks the cluster
/// owns the answer.
/// </para>
/// <para>
/// <b>Fail-closed by default.</b> When no surface supplies an implementation the
/// registered default is
/// <see cref="ActiveTenantOnlyAccessibleTenantSource"/>, which reports only the
/// tenant the caller is already scoped to. A deployment that cannot enumerate
/// tenants therefore offers no way to reach one, rather than guessing - and a
/// remembered tenant that cannot be shown to be reachable is never restored.
/// </para>
/// <para>
/// <b>What belongs in the list.</b> Only tenants this caller may read as. An
/// implementation filters out a tenant the caller has no grant for, and one whose
/// lifecycle makes it unusable, before returning - the consumers treat the result
/// as already authorized and do no further filtering.
/// </para>
/// </remarks>
public interface IExplorerAccessibleTenantSource
{
    /// <summary>
    /// The tenants the current caller may scope the Explorer to, best-first (the
    /// first entry is the one to fall back to when nothing better is known).
    /// Returns an empty list when none can be established.
    /// </summary>
    /// <remarks>
    /// Called on the resolve path and once per tenant-control refresh, not per
    /// render: a consumer caches the result for the render pass rather than
    /// re-asking. An implementation that reaches the cluster should therefore
    /// memoize per circuit.
    /// </remarks>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    /// <returns>The reachable tenants, or an empty list.</returns>
    ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(
        CancellationToken cancellationToken = default);
}
