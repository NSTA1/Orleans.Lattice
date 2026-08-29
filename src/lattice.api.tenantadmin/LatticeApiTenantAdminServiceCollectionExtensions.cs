using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.TenantAdmin</c> tenant-administration control facade on
/// an Orleans silo.
/// </summary>
public static class LatticeApiTenantAdminServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic tenant-administration control facade to the
    /// silo: binds <see cref="LatticeApiTenantAdminOptions"/>, registers the
    /// <see cref="ILatticeTenantAdmin"/> singleton every transport binding (for
    /// example gRPC, MCP) adapts over, along with the fail-closed authorization
    /// seam, the monotonic write clock, and the tenant-tree cascade seam. It adds
    /// no transport behaviour of its own.
    /// <para>
    /// Must be called <i>after</i> the tenancy add-on is registered
    /// (<c>siloBuilder.AddLatticeTenancy(...)</c>): the facade operates on the
    /// tenancy engine's <see cref="ITenantRegistry"/> lifecycle store, so that
    /// store must be registered first. Calling it out of order fails fast with a
    /// clear message, mirroring how the sibling control-API add-ons guard their
    /// ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiTenantAdminOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the tenancy add-on has not been registered on the same builder
    /// before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeTenantAdminApi(
        this ISiloBuilder builder,
        Action<LatticeApiTenantAdminOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: the facade operates on the tenancy engine's tenant
        // registry, so ITenantRegistry must already be registered (by
        // AddLatticeTenancy). Its absence means this facade would have no
        // lifecycle store to act on, so fail fast at registration with an
        // actionable message rather than failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ITenantRegistry)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenantAdminApi() must be called after AddLatticeTenancy(). Register the tenancy " +
                "add-on (siloBuilder.AddLatticeTenancy(...)) before adding the tenant-administration control " +
                "API, which operates on its tenant registry.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiTenantAdminOptions>();

        // The monotonic clock supplying strictly increasing last-writer-wins
        // stamps for registry writes.
        builder.Services.TryAddSingleton<ITenantAdminClock, MonotonicTenantAdminClock>();

        // The production tenant-tree cascade used by delete.
        builder.Services.TryAddSingleton<ITenantTreeCascade, GrainTenantTreeCascade>();

        // The fail-closed tenant-admin authorization seam the facade consults
        // before every mutating operation. It resolves the core access gate (the
        // no-op gate when no auth add-on is registered, so it is zero cost) and the
        // optional membership context.
        builder.Services.TryAddSingleton(sp => new TenantAdminAccessAuthorizer(
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetService<ILatticeMembershipContext>()));

        // The transport-agnostic control facade. Registered as a silo singleton
        // that every transport binding (for example gRPC, MCP) adapts over. The
        // membership context is resolved optionally so create can seed the calling
        // subject as the new tenant's admin subject; without it a create that
        // supplies no subjects leaves the tenant subject-less.
        builder.Services.TryAddSingleton<ILatticeTenantAdmin>(sp => new LatticeTenantAdmin(
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetRequiredService<TenantAdminAccessAuthorizer>(),
            sp.GetRequiredService<ITenantAdminClock>(),
            sp.GetRequiredService<ITenantTreeCascade>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetService<ILatticeMembershipContext>()));

        // T20 per-tenant region residency. The two-tier fail-closed authorizer
        // (operator authorizes the allowed set; tenant-admin sets residency within
        // it), the region-residency control facade every transport binding adapts
        // over, and the system-driven backfill/drain promotion driver. All are
        // append-only siblings of the tenant-lifecycle facade above.
        builder.Services.TryAddSingleton(sp => new TenantRegionResidencyAuthorizer(
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetService<ILatticeMembershipContext>()));
        builder.Services.TryAddSingleton<ILatticeTenantRegionAdmin, LatticeTenantRegionAdmin>();
        builder.Services.TryAddSingleton<TenantRegionLifecycleDriver>();

        // N1 tenant access administration. The tenant-tier surface that manages a
        // tenant's admin-subject set (list / add / remove), so membership can be
        // changed after creation instead of being frozen at the create-time seed.
        // It reuses the same two-tier authorizer as region residency - platform
        // operator OR a live admin subject of that tenant - deliberately not the
        // operator-only TenantAdminAccessAuthorizer that gates the lifecycle
        // mutations above. The identity directory is resolved optionally so a
        // granted subject id is validated against the upstream directory wherever
        // one is configured, matching the create path's seeding contract.
        builder.Services.TryAddSingleton<ILatticeTenantAccessAdmin>(sp => new LatticeTenantAccessAdmin(
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ITenantAdminClock>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetService<ILatticeIdentityDirectory>(),
            sp.GetService<IOptionsMonitor<LatticeIdentityDirectoryOptions>>()));

        // N2 cross-tenant grant administration. The two-step agreement surface -
        // the granting tenant offers, the grantee approves or rejects, and either
        // party may revoke - over the grants the tenancy engine's cross-tenant
        // resolution already consumes but which no facade could previously reach.
        // It reuses the same two-tier authorizer as region residency and access
        // administration, applied per operation to the side the step belongs to,
        // deliberately not the operator-only TenantAdminAccessAuthorizer: a grant
        // is a tenant-to-tenant agreement, so making an operator the bottleneck
        // for it would be the wrong shape.
        builder.Services.TryAddSingleton<ILatticeTenantGrantAdmin>(sp => new LatticeTenantGrantAdmin(
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ITenantAdminClock>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>()));

        // T21 tenant self-awareness. The read-only counterpart to the lifecycle
        // facade: it projects the caller's current tenant, the tenants it may
        // enumerate, and the read-only status/residency of one such tenant, scoped
        // fail-closed to the caller's subject through the tenancy policy engine. It
        // grants no lifecycle authority of its own, so it is registered whenever
        // tenancy is wired (this call already requires AddLatticeTenancy). The MCP
        // binding contributes its read-only tools exactly when this facade is
        // present, which is the single "tenancy enabled" signal it keys off.
        builder.Services.TryAddSingleton<ILatticeTenantSelfService>(sp => new LatticeTenantSelfService(
            sp.GetRequiredService<ITenantContextResolver>(),
            sp.GetRequiredService<ITenantPolicyEngine>(),
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetService<ILatticeMembershipContext>()));

        // N3 tenant usage against quota. The read-only counterpart to
        // SetTenantQuotasAsync: it projects the tenancy engine's warm per-tenant
        // usage index onto the control-API contract so a quota surface can render
        // a bar rather than only a ceiling. It reuses the two-tier region-residency
        // authorizer (operator, or a live admin subject of that tenant) because it
        // is a tenant-tier read, and it unifies an unauthorized tenant with an
        // absent one so it can never be used to probe for tenant existence.
        builder.Services.TryAddSingleton<ILatticeTenantQuotaUsage>(sp => new LatticeTenantQuotaUsage(
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ITenantUsageReader>()));

        // Idempotency marker: the structural wiring runs once regardless of how
        // many times the host calls this method. A repeat call still layers any
        // supplied configure delegate above, matching how the sibling add-ons
        // treat repeated registration.
        builder.Services.TryAddSingleton<LatticeApiTenantAdminMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeTenantAdminApi"/> call a no-op for the structural
    /// wiring while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiTenantAdminMarker
    {
    }
}
