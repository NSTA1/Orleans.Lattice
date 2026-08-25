using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Extension methods for configuring the <c>Orleans.Lattice.Tenancy</c> tenant
/// registry on an Orleans silo.
/// </summary>
public static class LatticeTenancyServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Tenancy</c> tenant registry to the silo: the
    /// durable, CRDT-backed <see cref="ITenantRegistry"/> backed by the reserved
    /// <c>sys-tenant-*</c> trees, its options, and the once-per-silo bootstrap
    /// that sets history retention and seeds the reserved default tenant with an
    /// unbounded quota. Also ensures the view infrastructure is present so the
    /// registry tree gets durable per-key history out of the box.
    /// <para>
    /// Enabling tenancy hard-depends on the core, membership, and auth add-ons,
    /// so this must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>,
    /// <c>AddLatticeMembership(...)</c>, and <c>AddLatticeAuth(...)</c>: the core
    /// registration owns the tree registry and options system the registry builds
    /// on, membership resolves the tenant-admin subjects the registry names, and
    /// auth is the enforcement seam that acts on tenant status, quotas, and
    /// grants. Calling it before any of them fails fast with an actionable
    /// message. When this method is never called, the add-on registers nothing
    /// and the core tenancy seams stay inert, so core behaves exactly as it did
    /// before tenancy existed.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeTenancyOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c>, <c>AddLatticeMembership(...)</c>, or <c>AddLatticeAuth(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeTenancy(
        this ISiloBuilder builder,
        Action<LatticeTenancyOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the registry
        // would have no tree registry to dogfood, so fail fast at registration
        // with an actionable message.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenancy() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding tenancy.");
        }

        // Ordering guard: enabling tenancy hard-depends on membership - the
        // tenant-admin subjects a registry names are resolved through the
        // membership directory. AddLatticeMembership is the only registrar of
        // ILatticeMembershipDirectory, so its absence is a misconfiguration.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeMembershipDirectory)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenancy() must be called after AddLatticeMembership(). Register " +
                "membership (siloBuilder.AddLatticeMembership(...)) before adding tenancy so the " +
                "registry's tenant-admin subjects can be resolved.");
        }

        // Ordering guard: enabling tenancy hard-depends on auth - the enforcement
        // seam that acts on tenant status, quotas, and cross-tenant grants.
        // AddLatticeAuth is the only registrar of ILatticeDecisionEngine, so its
        // absence means tenancy could never be enforced.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeDecisionEngine)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenancy() must be called after AddLatticeAuth(). Register " +
                "authorization (siloBuilder.AddLatticeAuth(...)) before adding tenancy so tenant " +
                "status, quotas, and grants can be enforced.");
        }

        // A repeat call still layers any supplied configure delegate but performs
        // the structural wiring only once.
        var alreadyRegistered = builder.Services.Any(d => d.ServiceType == typeof(TenancyRegistrationMarker));
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<TenancyRegistrationMarker>();

        // Durable per-key history for the sys-tenant-* trees rides on the view
        // infrastructure; ensure it is present (idempotent).
        builder.AddLatticeViews();

        // Fail-fast on an invalid options value at host build time rather than at
        // the first registry operation.
        builder.Services.AddOptions<LatticeTenancyOptions>().ValidateOnStart();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeTenancyOptions>, LatticeTenancyOptionsValidator>());

        // The registry persists TenantRecord state through the Orleans binary
        // serializer (not the lossy default JSON path), so register the wrapper
        // as an open generic that binds Orleans' Serializer<T> for the record.
        builder.Services.TryAddSingleton(typeof(OrleansLatticeSerializer<>));

        builder.Services.TryAddSingleton<TenantRegistryInitializer>();
        builder.Services.TryAddSingleton<ITenantRegistry, LatticeTenantRegistry>();

        // The compiled tenant-policy snapshot maintainer: a per-silo singleton
        // registered twice at the same instance - once as the concrete singleton
        // and once as an IMutationObserver - so a sys-tenant-registry write
        // refreshes the exact snapshot the tenant-policy engine reads. The
        // AddSingleton<IMutationObserver>(...) factory is intentionally not
        // idempotent under TryAdd, which is why the whole block runs only once
        // (guarded by TenancyRegistrationMarker above).
        builder.Services.TryAddSingleton<CompiledTenantPolicySnapshotMaintainer>();
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<CompiledTenantPolicySnapshotMaintainer>());

        // The tenant-policy decision engine: the in-memory decision surface that
        // resolves a subject's allowed tenants, validates an active tenant, and
        // resolves cross-tenant grants against the compiled snapshot. Registering
        // it is inert: nothing on the data path consults it until a later feature
        // wires enforcement in.
        builder.Services.TryAddSingleton<LatticeTenantPolicyEngine>();
        builder.Services.TryAddSingleton<ITenantPolicyEngine>(
            sp => sp.GetRequiredService<LatticeTenantPolicyEngine>());

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeTenancyOptions"/> configuration
    /// delegate. Use to adjust tenancy options after
    /// <see cref="AddLatticeTenancy"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeTenancy(
        this ISiloBuilder builder,
        Action<LatticeTenancyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
