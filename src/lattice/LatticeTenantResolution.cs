using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Resolves the effective, tenant-scoped tree id for an unqualified tree name at
/// the <see cref="ILattice"/> client boundary, consuming the
/// <see cref="ITenantContextResolver"/> seam. This is the single place that maps
/// a caller-supplied, tenant-local tree name to the tree id the
/// <c>LatticeGrain</c> is actually addressed by (its grain key), so tenant
/// isolation is established once, at grain resolution, rather than inside a
/// shared activation.
/// </summary>
/// <remarks>
/// <para>
/// The warm path is allocation- and await-free: the core no-op
/// <see cref="NullTenantContextResolver"/> resolves the reserved
/// <see cref="TenantId.Default"/> synchronously via
/// <see cref="ITenantContextResolver.TryResolveCurrent"/>, and the default
/// tenant returns the caller's bare tree name unchanged (the same
/// <see cref="string"/> reference), so a cluster with tenancy off is
/// byte-for-byte identical to today.
/// </para>
/// <para>
/// A resolver denies an operation by resolving the uninitialised
/// <c>default(TenantId)</c> "no tenant" value (a <c>null</c>
/// <see cref="TenantId.Value"/>), which is turned into a
/// <see cref="LatticeTenantAccessDeniedException"/> - the fail-closed contract:
/// a request that cannot be attributed to a tenant is denied, not silently
/// defaulted.
/// </para>
/// </remarks>
internal static class LatticeTenantResolution
{
    /// <summary>
    /// Resolves the effective tree id for <paramref name="treeName"/> under the
    /// caller's active tenant. Prefers the synchronous
    /// <see cref="ITenantContextResolver.TryResolveCurrent"/> fast path and
    /// falls back to <see cref="ITenantContextResolver.ResolveCurrentAsync"/>
    /// only when a synchronous resolution is unavailable.
    /// </summary>
    /// <param name="resolver">The active-tenant context resolver seam.</param>
    /// <param name="treeName">The caller-supplied, tenant-local tree name.</param>
    /// <param name="cancellationToken">Cancels an asynchronous resolution.</param>
    /// <returns>The effective tree id the tree grain should be addressed by.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="resolver"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="treeName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeTenantAccessDeniedException">
    /// The resolver denied the operation (no valid active tenant).
    /// </exception>
    public static ValueTask<string> ResolveEffectiveTreeIdAsync(
        ITenantContextResolver resolver,
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        ArgumentException.ThrowIfNullOrEmpty(treeName);

        // Warm path: a synchronous resolution avoids the async state machine and
        // any allocation entirely (the null resolver always resolves here).
        if (resolver.TryResolveCurrent(out var tenant))
        {
            return new ValueTask<string>(ComposeEffectiveTreeId(tenant, treeName));
        }

        return ResolveEffectiveTreeIdSlowAsync(resolver, treeName, cancellationToken);
    }

    private static async ValueTask<string> ResolveEffectiveTreeIdSlowAsync(
        ITenantContextResolver resolver,
        string treeName,
        CancellationToken cancellationToken)
    {
        var tenant = await resolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        return ComposeEffectiveTreeId(tenant, treeName);
    }

    /// <summary>
    /// Composes the effective tree id for <paramref name="treeName"/> under
    /// <paramref name="tenant"/>. A denying "no tenant" value fails closed; the
    /// reserved <see cref="TenantId.Default"/> returns the bare name unchanged
    /// (default-tenant adoption); a non-default tenant scopes an unqualified
    /// name into its <c>t/{tenant}/{name}</c> namespace. A name the caller
    /// already qualified (the reserved <c>t/</c> tenant namespace or a
    /// <c>_lattice_</c> / <c>sys-</c> system namespace) is returned unchanged
    /// and never double-composed.
    /// </summary>
    /// <param name="tenant">The resolved active tenant.</param>
    /// <param name="treeName">The caller-supplied tree name.</param>
    /// <returns>The effective tree id.</returns>
    /// <exception cref="LatticeTenantAccessDeniedException">
    /// <paramref name="tenant"/> is the uninitialised "no tenant" value.
    /// </exception>
    public static string ComposeEffectiveTreeId(TenantId tenant, string treeName)
    {
        // Fail-closed: a resolver signals a denial with the uninitialised
        // "no tenant" value (Value == null), distinct from TenantId.Default.
        if (tenant.Value is null)
        {
            throw new LatticeTenantAccessDeniedException();
        }

        // Default-tenant adoption / tenancy off: the bare name is returned
        // unchanged (same reference), so behaviour is byte-for-byte identical to
        // today. Checked first so the warm path does no prefix inspection.
        if (tenant.IsDefault)
        {
            return treeName;
        }

        // A name the caller already qualified is never double-composed: the
        // tenancy layer only scopes unqualified, tenant-local names, and the
        // reserved namespaces are governed by their own guards.
        if (IsReservedOrQualified(treeName))
        {
            return treeName;
        }

        return LatticeTenantTrees.Compose(tenant, treeName);
    }

    private static bool IsReservedOrQualified(string treeName) =>
        LatticeTenantTrees.IsTenantScoped(treeName)
        || treeName.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        || treeName.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal);
}
