using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for registering <see cref="ILatticeCompressor"/>
/// implementations on a silo's DI container. Compression in
/// Orleans.Lattice is configured purely by the set of registered
/// <see cref="ILatticeCompressor"/> singletons; the
/// <see cref="LatticeCompression"/> enum is just the on-wire tag.
/// A host that wants to ship a new algorithm registers a custom
/// <see cref="ILatticeCompressor"/> whose
/// <see cref="ILatticeCompressor.Algorithm"/> is a byte cast into
/// <see cref="LatticeCompression"/> from the host-reserved
/// <c>[0x80, 0xFF]</c> tag range. See
/// <c>docs/lattice/compression.md</c> for the full walk-through.
/// </summary>
public static class LatticeCompressionServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="TCompressor"/> as a singleton
    /// <see cref="ILatticeCompressor"/> on the supplied
    /// <paramref name="services"/> container. Uses
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddEnumerable(IServiceCollection, ServiceDescriptor)"/>
    /// so calling this overload twice with the same compressor type
    /// is idempotent - the second call is a no-op rather than a
    /// duplicate registration. Use this overload when the
    /// compressor type has a parameterless constructor or is fully
    /// resolvable from the container.
    /// </summary>
    /// <typeparam name="TCompressor">
    /// The concrete compressor type. Must implement
    /// <see cref="ILatticeCompressor"/>.
    /// </typeparam>
    /// <param name="services">The DI service collection.</param>
    /// <returns>
    /// The same <paramref name="services"/> instance for fluent
    /// chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> is <see langword="null"/>.
    /// </exception>
    public static IServiceCollection AddLatticeCompressor<TCompressor>(this IServiceCollection services)
        where TCompressor : class, ILatticeCompressor
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILatticeCompressor, TCompressor>());
        return services;
    }

    /// <summary>
    /// Registers a pre-constructed <paramref name="compressor"/>
    /// instance as a singleton <see cref="ILatticeCompressor"/> on
    /// the supplied <paramref name="services"/> container. Two
    /// different <see cref="ILatticeCompressor"/> instances that
    /// share the same <see cref="ILatticeCompressor.Algorithm"/>
    /// tag are rejected by the consuming encoder at construction
    /// time, so pre-instance registration is reserved for the case
    /// where the host needs to supply non-default constructor
    /// parameters (e.g. a custom compression level) that the
    /// container cannot resolve on its own.
    /// </summary>
    /// <param name="services">The DI service collection.</param>
    /// <param name="compressor">The compressor instance to register.</param>
    /// <returns>
    /// The same <paramref name="services"/> instance for fluent
    /// chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="compressor"/>
    /// is <see langword="null"/>.
    /// </exception>
    public static IServiceCollection AddLatticeCompressor(this IServiceCollection services, ILatticeCompressor compressor)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(compressor);
        services.TryAddEnumerable(ServiceDescriptor.Singleton(compressor));
        return services;
    }
}