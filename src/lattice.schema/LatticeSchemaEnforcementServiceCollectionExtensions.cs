using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Registration extensions for the <c>Orleans.Lattice.Schema</c> enforcement layer:
/// opt-in, per-tree, server-enforced value validation. Installing it replaces the
/// core no-op <see cref="ILatticeWriteInterceptor"/> with the schema-enforcement
/// interceptor, wires the reserved-tree policy and dead-letter stores, the cached
/// policy provider, and the <see cref="LatticeOperation.SchemaAdmin"/>-gated admin
/// surface. A tree with no policy pays a single cached lookup that short-circuits,
/// so enforcement is zero-overhead until a policy is set.
/// </summary>
public static class LatticeSchemaEnforcementServiceCollectionExtensions
{
    /// <summary>
    /// Adds schema enforcement to the silo. Must be called <i>after</i>
    /// <c>AddLattice(...)</c>: the core registration is the source of truth for the
    /// tree registry, the options system, and the no-op write interceptor this
    /// add-on replaces.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeSchemaEnforcementOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeSchemaEnforcement(
        this ISiloBuilder builder,
        Action<LatticeSchemaEnforcementOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the stores would
        // have no tree registry to dogfood, so fail fast with an actionable
        // message, mirroring how the other add-ons guard their ordering.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeSchemaEnforcement() must be called after AddLattice(). Register the " +
                "core lattice (siloBuilder.AddLattice(...)) before adding schema enforcement.");
        }

        // A repeat call still layers any supplied configure delegate but performs
        // the structural wiring only once.
        var alreadyRegistered = builder.Services.Any(d => d.ServiceType == typeof(SchemaEnforcementRegistrationMarker));
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<SchemaEnforcementRegistrationMarker>();
        builder.Services.AddOptions<LatticeSchemaEnforcementOptions>();

        // Deterministic clock for dead-letter timestamps; overridable by a host
        // that registered its own TimeProvider (for example a test fake).
        builder.Services.TryAddSingleton(TimeProvider.System);

        // Reserved-tree stores. Both dogfood a sys-schema-* ILattice tree, so they
        // depend only on the grain factory the core already provides.
        builder.Services.TryAddSingleton<ILatticeSchemaPolicyStore, LatticeSchemaPolicyStore>();
        builder.Services.TryAddSingleton<ILatticeSchemaDeadLetterStore, LatticeSchemaDeadLetterStore>();

        // The cached policy provider is registered once as the concrete singleton
        // and mapped both to the provider interface (read by the interceptor and
        // admin) and to IMutationObserver (so a sys-schema-policy write evicts the
        // affected tree's cache entry off the core change feed). The
        // AddSingleton<IMutationObserver>(...) factory is intentionally not
        // idempotent under TryAdd, which is why the whole block runs only once.
        builder.Services.TryAddSingleton<LatticeSchemaPolicyProvider>();
        builder.Services.TryAddSingleton<ILatticeSchemaPolicyProvider>(
            sp => sp.GetRequiredService<LatticeSchemaPolicyProvider>());
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<LatticeSchemaPolicyProvider>());

        // Enforcement wiring: replace the core default NullLatticeWriteInterceptor
        // (registered by AddLattice via TryAddSingleton) with the schema
        // interceptor. Replace (not TryAdd) guarantees exactly one interceptor
        // resolves and it is the enforcing one.
        builder.Services.TryAddSingleton<LatticeSchemaWriteInterceptor>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(
                sp => sp.GetRequiredService<LatticeSchemaWriteInterceptor>()));

        // The SchemaAdmin-gated control plane over the stores + provider cache.
        builder.Services.TryAddSingleton<ILatticeSchemaAdmin, LatticeSchemaAdmin>();

        // CRDT merge-result observer: opt-in, because registering a non-null merge
        // observer makes every merge in the silo pay the observer round-trip. Read
        // the flag from a probe of the configure delegate so the merge path keeps
        // its zero-overhead default unless the host explicitly turns it on.
        var probe = new LatticeSchemaEnforcementOptions();
        configure?.Invoke(probe);
        if (probe.ValidateCrdtMergeResults)
        {
            builder.Services.TryAddSingleton<LatticeSchemaMergeObserver>();
            builder.Services.Replace(
                ServiceDescriptor.Singleton<ILatticeMergeObserver>(
                    sp => sp.GetRequiredService<LatticeSchemaMergeObserver>()));
        }

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeSchemaEnforcementOptions"/>
    /// configuration delegate after <see cref="AddLatticeSchemaEnforcement"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeSchemaEnforcement(
        this ISiloBuilder builder,
        Action<LatticeSchemaEnforcementOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
