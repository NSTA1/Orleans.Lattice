using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Caching.AzureBlob;

/// <summary>
/// DI extensions for registering the Azure Blob Storage
/// <see cref="IDistributedCache"/>.
/// </summary>
public static class LatticeAzureBlobCacheServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="AzureBlobDistributedCache"/> as the application's
    /// <see cref="IDistributedCache"/>. Because it is registered through the
    /// standard <see cref="IDistributedCache"/> seam, any consumer that resolves
    /// the abstraction - for example a Microsoft.Identity.Web distributed token
    /// cache, ASP.NET session state, or output caching - uses the Azure Blob
    /// backend without further wiring. This is registered as the last
    /// <see cref="IDistributedCache"/>, so it wins over an earlier
    /// <c>AddDistributedMemoryCache</c>; call it once.
    /// <para>
    /// A <see cref="TimeProvider"/> is resolved from the container when one is
    /// registered (for deterministic tests) and otherwise defaults to
    /// <see cref="TimeProvider.System"/>. The container client is built once from
    /// the populated authentication mode when the cache is first resolved.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Callback that populates <see cref="LatticeAzureBlobCacheOptions"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddAzureBlobDistributedCache(
        this IServiceCollection services,
        Action<LatticeAzureBlobCacheOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<LatticeAzureBlobCacheOptions>();
        services.Configure(configure);

        // Append (not TryAdd): this call selects the blob backend even when an
        // in-memory distributed cache was registered earlier, since the last
        // IDistributedCache registration is the one resolved.
        services.AddSingleton<IDistributedCache>(static sp =>
        {
            var options = sp.GetRequiredService<IOptions<LatticeAzureBlobCacheOptions>>().Value;
            var timeProvider = sp.GetService<TimeProvider>() ?? TimeProvider.System;
            return new AzureBlobDistributedCache(options.BuildContainerClient(), options.KeyPrefix, timeProvider);
        });

        return services;
    }
}
