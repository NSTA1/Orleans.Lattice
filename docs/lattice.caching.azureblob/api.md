# Orleans.Lattice.Caching.AzureBlob API reference

The package's public surface is deliberately small: one options type and one registration extension. The cache implementation itself is internal and reached only through the `IDistributedCache` abstraction.

## `LatticeAzureBlobCacheOptions`

A sealed class configuring the account, container, key layout, and client. Every property and the `DefaultContainerName` constant are covered in [configuration](configuration.md).

## `LatticeAzureBlobCacheServiceCollectionExtensions`

A static class with one extension method.

```csharp
public static IServiceCollection AddAzureBlobDistributedCache(
    this IServiceCollection services,
    Action<LatticeAzureBlobCacheOptions> configure)
```

Registers the Azure Blob-backed cache as the application's `IDistributedCache`.

- **Throws** `ArgumentNullException` when `services` or `configure` is null.
- The cache is added as the **last** `IDistributedCache` registration, so it wins over an earlier `AddDistributedMemoryCache`. Call it once.
- A `TimeProvider` is resolved from the container when one is registered (for deterministic tests) and otherwise defaults to `TimeProvider.System`.
- The container client is built once, from the single populated authentication mode, when the cache is first resolved. Option validation therefore happens at first resolution, not at registration.

Because the registration targets the standard `IDistributedCache` seam, no consumer references this package at the call site: a Microsoft.Identity.Web distributed token cache, ASP.NET session state, or output caching all resolve the abstraction and get the blob backend.

```csharp verify
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Caching.AzureBlob;

public static class CacheResolution
{
    public static IDistributedCache Resolve()
    {
        var services = new ServiceCollection();
        services.AddAzureBlobDistributedCache(o => o.ConnectionString = "UseDevelopmentStorage=true");

        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IDistributedCache>();
    }
}
```
