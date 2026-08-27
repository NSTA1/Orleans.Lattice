using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice;

/// <summary>
/// Tenant-aware extension methods for resolving an <see cref="ILattice"/> handle
/// from an unqualified, tenant-local tree name. These are the tenant-scoping
/// entry point to the tree surface: they consume the
/// <see cref="ITenantContextResolver"/> seam to determine the caller's active
/// tenant and address the tree grain by its effective, tenant-scoped id, so
/// tenant isolation is established once, at grain resolution.
/// </summary>
/// <remarks>
/// <para>
/// With no tenancy add-on registered the core no-op resolver resolves the
/// reserved <see cref="TenantId.Default"/> synchronously and the bare tree name
/// is returned unchanged, so <c>GetLatticeAsync("my-tree")</c> addresses exactly
/// the same grain as <c>grainFactory.GetGrain&lt;ILattice&gt;("my-tree")</c> -
/// byte-for-byte identical, with no added allocation or <c>await</c> on the warm
/// path. When the tenancy add-on is registered, its resolver scopes an
/// unqualified name into the active tenant's <c>t/{tenant}/{name}</c> namespace
/// and fails closed (<see cref="LatticeTenantAccessDeniedException"/>) when the
/// caller has no valid active tenant.
/// </para>
/// </remarks>
public static class LatticeTenantExtensions
{
    /// <summary>
    /// Resolves the effective, tenant-scoped tree id for the caller-supplied,
    /// tenant-local <paramref name="treeName"/> under the caller's active tenant.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the seam an external API facade uses to scope a caller-supplied
    /// tree name before it authorizes or operates on it. Both must use the
    /// <em>same</em> effective id: authorizing the bare name and then operating on
    /// the composed one (or the reverse) would check one tree and act on another.
    /// </para>
    /// <para>
    /// With no tenancy add-on registered the core no-op resolver resolves
    /// <see cref="TenantId.Default"/> synchronously and the bare name is returned
    /// unchanged - the same <see cref="string"/> reference, with no allocation and
    /// no <c>await</c> on the warm path - so a cluster with tenancy off is
    /// byte-for-byte identical to not calling this at all. When the tenancy add-on
    /// is registered, an unqualified name is scoped into the active tenant's
    /// <c>t/{tenant}/{name}</c> namespace, an already-qualified or reserved name is
    /// returned unchanged, and a caller with no valid active tenant fails closed
    /// with <see cref="LatticeTenantAccessDeniedException"/>.
    /// </para>
    /// </remarks>
    /// <param name="tenantResolver">The active-tenant context resolver seam.</param>
    /// <param name="treeName">The caller-supplied, tenant-local tree name.</param>
    /// <param name="cancellationToken">Cancels an asynchronous tenant resolution.</param>
    /// <returns>The effective tree id to authorize and operate on.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenantResolver"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="treeName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeTenantAccessDeniedException">
    /// The resolver denied the operation (no valid active tenant).
    /// </exception>
    public static ValueTask<string> ResolveEffectiveTreeIdAsync(
        this ITenantContextResolver tenantResolver,
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tenantResolver);
        ArgumentException.ThrowIfNullOrEmpty(treeName);

        return LatticeTenantResolution.ResolveEffectiveTreeIdAsync(
            tenantResolver, treeName, cancellationToken);
    }

    /// <summary>
    /// Resolves the <see cref="ILattice"/> handle for the tenant-local tree
    /// <paramref name="treeName"/> under the caller's active tenant, resolving
    /// the <see cref="ITenantContextResolver"/> and <see cref="IGrainFactory"/>
    /// from <paramref name="services"/>.
    /// </summary>
    /// <param name="services">
    /// The service provider to resolve the grain factory and the active-tenant
    /// context resolver from (a cluster client's
    /// <see cref="IServiceProvider"/>, or a silo-injected one).
    /// </param>
    /// <param name="treeName">The caller-supplied, tenant-local tree name.</param>
    /// <param name="cancellationToken">Cancels an asynchronous tenant resolution.</param>
    /// <returns>A handle to the effective, tenant-scoped tree.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="treeName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeTenantAccessDeniedException">
    /// The resolver denied the operation (no valid active tenant).
    /// </exception>
    public static ValueTask<ILattice> GetLatticeAsync(
        this IServiceProvider services,
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(treeName);

        var grainFactory = services.GetRequiredService<IGrainFactory>();
        var resolver = services.GetRequiredService<ITenantContextResolver>();
        return grainFactory.GetLatticeAsync(resolver, treeName, cancellationToken);
    }

    /// <summary>
    /// Resolves the <see cref="ILattice"/> handle for the tenant-local tree
    /// <paramref name="treeName"/> under the caller's active tenant, using the
    /// supplied <paramref name="tenantResolver"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory to address the tree grain from.</param>
    /// <param name="tenantResolver">The active-tenant context resolver seam.</param>
    /// <param name="treeName">The caller-supplied, tenant-local tree name.</param>
    /// <param name="cancellationToken">Cancels an asynchronous tenant resolution.</param>
    /// <returns>A handle to the effective, tenant-scoped tree.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="grainFactory"/> or <paramref name="tenantResolver"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="treeName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeTenantAccessDeniedException">
    /// The resolver denied the operation (no valid active tenant).
    /// </exception>
    public static ValueTask<ILattice> GetLatticeAsync(
        this IGrainFactory grainFactory,
        ITenantContextResolver tenantResolver,
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(tenantResolver);
        ArgumentException.ThrowIfNullOrEmpty(treeName);

        var pending = LatticeTenantResolution.ResolveEffectiveTreeIdAsync(
            tenantResolver, treeName, cancellationToken);

        // Warm path: the effective id resolved synchronously (the null resolver
        // always does), so the grain is addressed with no await and no added
        // allocation over addressing it by name directly.
        if (pending.IsCompletedSuccessfully)
        {
            return new ValueTask<ILattice>(grainFactory.GetGrain<ILattice>(pending.Result));
        }

        return AwaitAndGetGrainAsync(grainFactory, pending);
    }

    private static async ValueTask<ILattice> AwaitAndGetGrainAsync(
        IGrainFactory grainFactory,
        ValueTask<string> pending)
    {
        var effectiveTreeId = await pending.ConfigureAwait(false);
        return grainFactory.GetGrain<ILattice>(effectiveTreeId);
    }
}
