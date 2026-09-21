using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// Installs <see cref="LatticeGrainStorageSerializer"/> as the silo's
/// grain-storage serializer.
/// </summary>
internal static class LatticeGrainStorageSerializerRegistration
{
    /// <summary>
    /// Registers <see cref="LatticeGrainStorageSerializer"/> as the
    /// <see cref="IGrainStorageSerializer"/> the silo resolves, wrapping
    /// whichever serializer was registered before it as the fallback for
    /// unmarked state types.
    /// <para>
    /// The registration is deliberately unconditional rather than a
    /// <c>TryAdd</c>. Orleans always registers its JSON serializer first,
    /// so a <c>TryAdd</c> would lose to it and silently do nothing. It is
    /// still non-destructive: a serializer a host registered itself is not
    /// discarded but becomes the fallback, and is therefore still what
    /// writes every state type Lattice has not marked. Storage providers
    /// pick this up through Orleans' own post-configure step, which fills
    /// in a provider's serializer from the container only when the provider
    /// did not set one explicitly, so a provider configured with its own
    /// serializer keeps it.
    /// </para>
    /// </summary>
    /// <param name="services">The silo's service collection.</param>
    public static IServiceCollection AddLatticeGrainStorageSerializer(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(d => d.ServiceType == typeof(LatticeGrainStorageSerializerMarker)))
        {
            return services;
        }

        services.AddSingleton<LatticeGrainStorageSerializerMarker>();

        var prior = services.LastOrDefault(
            d => d.ServiceType == typeof(IGrainStorageSerializer) && !d.IsKeyedService);

        services.AddSingleton<IGrainStorageSerializer>(sp => new LatticeGrainStorageSerializer(
            sp.GetRequiredService<Serializer>(),
            ResolveFallback(sp, prior)));

        return services;
    }

    private static IGrainStorageSerializer ResolveFallback(IServiceProvider provider, ServiceDescriptor? prior)
    {
        if (prior is null)
        {
            // Defensive: Orleans registers a JSON serializer on every silo,
            // so this only happens in a bare container.
            return ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(provider);
        }

        // The prior descriptor is materialised directly rather than resolved
        // from the container, because the container's IGrainStorageSerializer
        // is now this serializer and resolving it would recurse.
        if (prior.ImplementationInstance is IGrainStorageSerializer instance)
        {
            return instance;
        }

        if (prior.ImplementationFactory is { } factory)
        {
            return (IGrainStorageSerializer)factory(provider);
        }

        if (prior.ImplementationType is { } implementationType)
        {
            return (IGrainStorageSerializer)ActivatorUtilities.CreateInstance(provider, implementationType);
        }

        return ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(provider);
    }

    /// <summary>
    /// Presence in the service collection records that the serializer has
    /// already been installed, so repeated calls are idempotent and cannot
    /// stack one wrapper on another.
    /// </summary>
    private sealed class LatticeGrainStorageSerializerMarker
    {
    }
}
