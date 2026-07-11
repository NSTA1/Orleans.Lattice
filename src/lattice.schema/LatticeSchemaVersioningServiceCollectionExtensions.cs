using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Registration extensions for the <c>Orleans.Lattice.Schema</c> versioning layer:
/// opt-in, per-tree, self-describing per-value schema-version envelopes with
/// read-time upcasting. Installing it replaces the core no-op
/// <see cref="ILatticeValueDecoder"/> with the envelope-stripping / upcasting
/// decoder, composes an envelope-stamping <see cref="ILatticeWriteInterceptor"/>
/// stage onto the write path (after schema enforcement, when that is also
/// registered), wires the reserved-tree version-config store, the cached version
/// provider, the host-supplied schema registry, and the
/// <see cref="LatticeOperation.SchemaAdmin"/>-gated version admin. A tree with no
/// version config pays a single cached lookup on write and a single leading-byte
/// check on read, so versioning is zero-overhead until a tree opts in.
/// </summary>
public static class LatticeSchemaVersioningServiceCollectionExtensions
{
    /// <summary>
    /// Adds schema versioning to the silo. Must be called <i>after</i>
    /// <c>AddLattice(...)</c>, and - when schema enforcement is also used - <i>after</i>
    /// <c>AddLatticeSchemaEnforcement(...)</c>, so the enforcement validation stage is
    /// composed ahead of the versioning envelope stage on the write path.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configureRegistry">
    /// Optional delegate populating the schema registry (descriptors and upcasters).
    /// Omit it for a pure Phase-1 stamping deployment with no upcasters; a stale read
    /// then surfaces <see cref="NotSupportedException"/> until upcasters are declared.
    /// </param>
    /// <param name="configureOptions">Optional delegate that populates <see cref="LatticeSchemaVersioningOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeSchemaVersioning(
        this ISiloBuilder builder,
        Action<LatticeSchemaRegistryBuilder>? configureRegistry = null,
        Action<LatticeSchemaVersioningOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator. Its
        // absence means the stores would have no tree registry to dogfood, so fail
        // fast, mirroring how the enforcement add-on guards its ordering.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeSchemaVersioning() must be called after AddLattice(). Register the core lattice " +
                "(siloBuilder.AddLattice(...)) before adding schema versioning.");
        }

        var alreadyRegistered = builder.Services.Any(
            d => d.ServiceType == typeof(SchemaVersioningRegistrationMarker));
        if (configureOptions is not null)
        {
            builder.Services.Configure(configureOptions);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<SchemaVersioningRegistrationMarker>();
        builder.Services.AddOptions<LatticeSchemaVersioningOptions>();

        // Deterministic clock for dead-letter timestamps; overridable by a host.
        builder.Services.TryAddSingleton(TimeProvider.System);

        // The DI transform registry resolves any DI-backed upcaster id and is shared
        // with the transform-registration extension.
        builder.Services.TryAddSingleton<ILatticeValueTransformRegistry, LatticeValueTransformRegistry>();

        // Reserved-tree config store + the strict-ingest dead-letter store (shared
        // with enforcement; TryAdd so either add-on may register it).
        builder.Services.TryAddSingleton<ILatticeSchemaVersionStore, LatticeSchemaVersionStore>();
        builder.Services.TryAddSingleton<ILatticeSchemaDeadLetterStore, LatticeSchemaDeadLetterStore>();

        // Cached provider, mapped to the interface and to IMutationObserver so a
        // sys-schema-version write evicts the affected tree's cache entry.
        builder.Services.TryAddSingleton<LatticeSchemaVersionProvider>();
        builder.Services.TryAddSingleton<ILatticeSchemaVersionProvider>(
            sp => sp.GetRequiredService<LatticeSchemaVersionProvider>());
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<LatticeSchemaVersionProvider>());

        // Host-supplied schema registry (descriptors + upcasters), built once.
        builder.Services.AddSingleton<ILatticeSchemaRegistry>(sp =>
        {
            var registryBuilder = new LatticeSchemaRegistryBuilder();
            configureRegistry?.Invoke(registryBuilder);
            return registryBuilder.Build(sp.GetService<ILatticeValueTransformRegistry>());
        });

        // Write path: stamp the envelope. Compose after the enforcement validation
        // stage when enforcement is also registered (its concrete type is resolved
        // optionally, so versioning works with or without enforcement).
        builder.Services.TryAddSingleton<LatticeSchemaVersionWriteInterceptor>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(sp =>
                new CompositeLatticeWriteInterceptor(
                    sp.GetRequiredService<LatticeSchemaVersionWriteInterceptor>(),
                    sp.GetService<LatticeSchemaWriteInterceptor>())));

        // Read path: replace the core no-op decoder with the envelope-stripping /
        // upcasting decoder.
        builder.Services.TryAddSingleton<LatticeSchemaVersionDecoder>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ILatticeValueDecoder>(
                sp => sp.GetRequiredService<LatticeSchemaVersionDecoder>()));

        // The SchemaAdmin-gated control plane over the config store + provider cache.
        builder.Services.TryAddSingleton<ILatticeSchemaVersionAdmin, LatticeSchemaVersionAdmin>();

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeSchemaVersioningOptions"/>
    /// configuration delegate after <see cref="AddLatticeSchemaVersioning"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeSchemaVersioning(
        this ISiloBuilder builder,
        Action<LatticeSchemaVersioningOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
