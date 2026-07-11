using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Registration extensions for the <c>Orleans.Lattice.Schema</c> DI escape hatch.
/// They register host-supplied <see cref="ILatticeValueTransform"/> instances and
/// ensure the <see cref="ILatticeValueTransformRegistry"/> that resolves them by
/// id is present. A full <c>AddLatticeSchema(...)</c> registrar arrives with the
/// schema-enforcement / versioning layers; this foundation intentionally exposes
/// only the transform seam.
/// </summary>
public static class LatticeSchemaServiceCollectionExtensions
{
    /// <summary>
    /// Registers <paramref name="transform"/> as a resolvable
    /// <see cref="ILatticeValueTransform"/> and ensures the
    /// <see cref="ILatticeValueTransformRegistry"/> is registered.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="transform">The transform instance to register.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="transform"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeValueTransform(
        this IServiceCollection services,
        ILatticeValueTransform transform)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(transform);
        EnsureRegistry(services);
        services.AddSingleton(transform);
        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="TTransform"/> as a resolvable
    /// <see cref="ILatticeValueTransform"/> and ensures the
    /// <see cref="ILatticeValueTransformRegistry"/> is registered.
    /// </summary>
    /// <typeparam name="TTransform">The transform implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeValueTransform<TTransform>(this IServiceCollection services)
        where TTransform : class, ILatticeValueTransform
    {
        ArgumentNullException.ThrowIfNull(services);
        EnsureRegistry(services);
        services.AddSingleton<ILatticeValueTransform, TTransform>();
        return services;
    }

    /// <summary>
    /// Registers <paramref name="transform"/> on the silo. See
    /// <see cref="AddLatticeValueTransform(IServiceCollection, ILatticeValueTransform)"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="transform">The transform instance to register.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="transform"/> is <c>null</c>.</exception>
    public static ISiloBuilder AddLatticeValueTransform(
        this ISiloBuilder builder,
        ILatticeValueTransform transform)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(transform);
        builder.Services.AddLatticeValueTransform(transform);
        return builder;
    }

    /// <summary>
    /// Registers <typeparamref name="TTransform"/> on the silo. See
    /// <see cref="AddLatticeValueTransform{TTransform}(IServiceCollection)"/>.
    /// </summary>
    /// <typeparam name="TTransform">The transform implementation type.</typeparam>
    /// <param name="builder">The silo builder.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    public static ISiloBuilder AddLatticeValueTransform<TTransform>(this ISiloBuilder builder)
        where TTransform : class, ILatticeValueTransform
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeValueTransform<TTransform>();
        return builder;
    }

    private static void EnsureRegistry(IServiceCollection services) =>
        services.TryAddSingleton<ILatticeValueTransformRegistry, LatticeValueTransformRegistry>();
}
