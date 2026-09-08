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
            ThrowIfNamespaceEscape(tenant, treeName);
            return treeName;
        }

        return LatticeTenantTrees.Compose(tenant, treeName);
    }

    /// <summary>
    /// Fails closed when a confined tenant uses the pass-through of an
    /// already-qualified name to address a tree <em>outside</em> its own
    /// namespace.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Passing an already-qualified name through uncomposed is right for the
    /// first-party add-ons that own the <c>sys-</c> trees, and for the tenant-scoped
    /// facades that compose an id and then hand it back in for re-resolution - but
    /// it is the one way a tenant could name a tree that resolves outside its own
    /// namespace. Such an id stays global, so the tree is invisible to the
    /// per-tenant tree-count and footprint accounting that enumerates the tenant's
    /// <c>t/{tenant}/</c> prefix, it is shared with every other tenant that picks
    /// the same name, and it can collide with an add-on store. Refusing here rather
    /// than in each facade puts the check on the single seam every tenant-scoped
    /// resolution passes through, so a facade added later inherits it.
    /// </para>
    /// <para>
    /// Two shapes escape, and both are refused:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///   The <see cref="LatticeConstants.SystemDataTreePrefix"/> (<c>sys-</c>)
    ///   namespace, which holds first-party add-on state and sits outside every
    ///   tenant.
    ///   </description></item>
    ///   <item><description>
    ///   A <see cref="LatticeTenantTrees.SegmentPrefix"/> (<c>t/</c>) id that is
    ///   <em>malformed</em> - <c>t/x</c>, with no second segment. It has no owning
    ///   tenant at all, so <see cref="LatticeTenantTrees.GetOwner"/> reports it
    ///   platform-owned and the tenancy access gate admits it unconditionally,
    ///   yielding the same shared, accounting-invisible, cross-tenant namespace the
    ///   <c>sys-</c> escape did.
    ///   </description></item>
    /// </list>
    /// <para>
    /// A <em>well-formed</em> <c>t/{other}/{name}</c> is deliberately <b>not</b>
    /// refused here. It must reach the tenancy access gate, which is the component
    /// that adjudicates it: the gate denies the crossing by default, but admits it
    /// when the owning tenant has issued a matching cross-tenant grant. Refusing at
    /// this seam would pre-empt that adjudication and break cross-tenant grants
    /// entirely - the resolution layer cannot see grants, so it must not decide
    /// crossings. Only the malformed shape, which the gate structurally cannot
    /// adjudicate because it resolves to no tenant, is closed here.
    /// </para>
    /// <para>
    /// The <c>_lattice_</c> namespace needs no test here: the data plane rejects it
    /// outright and the tree-admin facade rejects it at create, whereas <c>sys-</c>
    /// trees are deliberately readable ordinary trees and so had no other guard.
    /// </para>
    /// <para>
    /// The default tenant has already returned above, so only a genuinely confined
    /// caller reaches this. First-party add-ons administering their own stores run
    /// inside a system-origin scope and are exempt, exactly as they are exempt from
    /// the reserved-namespace rejection in the data plane.
    /// </para>
    /// </remarks>
    private static void ThrowIfNamespaceEscape(TenantId tenant, string treeName)
    {
        if (LatticeAccessGateContext.IsSystemOrigin)
        {
            return;
        }

        if (treeName.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal))
        {
            throw new LatticeTenantAccessDeniedException(
                $"Tenant '{tenant}' may not address the reserved '{LatticeConstants.SystemDataTreePrefix}' "
                + "namespace: it holds first-party add-on state, sits outside every tenant, and is therefore "
                + "never composed into the calling tenant's namespace.");
        }

        if (!LatticeTenantTrees.IsTenantScoped(treeName))
        {
            return;
        }

        // Tenant-owned - the caller's own namespace, or a foreign one the access
        // gate will adjudicate against the owning tenant's grants.
        if (LatticeTenantTrees.GetOwner(treeName).IsTenantOwned)
        {
            return;
        }

        throw new LatticeTenantAccessDeniedException(
            $"Tenant '{tenant}' may not address a malformed '{LatticeTenantTrees.SegmentPrefix}' id that "
            + "belongs to no tenant at all: it resolves outside every tenant's namespace, so it can be "
            + "neither confined to the caller nor adjudicated against the owning tenant, and is refused.");
    }

    private static bool IsReservedOrQualified(string treeName) =>
        LatticeTenantTrees.IsTenantScoped(treeName)
        || treeName.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        || treeName.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal);
}
