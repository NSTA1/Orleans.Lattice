# Orleans.Lattice.Caching.AzureBlob

A durable [Azure Blob Storage](https://learn.microsoft.com/azure/storage/blobs/) [`IDistributedCache`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.caching.distributed.idistributedcache) for the Orleans.Lattice family.

## What is it?

`Orleans.Lattice.Caching.AzureBlob` implements the standard ASP.NET Core `IDistributedCache` abstraction over an Azure Blob Storage container. It exists so a hosted-web component that needs a shared, multi-replica-safe cache - most notably the Microsoft.Identity.Web distributed token cache behind [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md) - has a durable backend without taking a dependency on Redis or SQL Server. There is no official Azure Storage `IDistributedCache` adapter, so this package provides one scoped to the family's needs.

Each cache entry is a single block blob whose name is the SHA-256 hash of the cache key (optionally under a virtual-directory `KeyPrefix`), with the absolute and sliding expiry stored in blob metadata. A read that finds an expired entry deletes it and reports a miss, so the container self-prunes on access.

## Core properties

- **Standard seam, no bespoke API.** The package adds one options type and one registration extension; everything else is driven through `IDistributedCache`. Any consumer that resolves the abstraction uses the blob backend automatically.
- **Multi-replica safe.** Because the store is external, every host replica sees the same entries. A token cached by one replica is readable by another, so a Blazor Server user whose circuit lands on a cold replica does not silently lose their session.
- **Flexible authentication.** Exactly one of a connection string, a service URI with an Azure AD token credential, a service URI with a shared-key credential, or a pre-built `BlobServiceClient` is configured. The mode is read once at construction and a long-lived container client is built from it.
- **Provisionless and self-pruning.** The container is created on first use (idempotent); expired entries are removed lazily when read.
- **Deterministic expiry.** Absolute and sliding expiry arithmetic is pure and clock-injectable via `TimeProvider`, so behaviour is testable without wall-clock waits.

## Setup

Register the cache once on the host. Because it is the standard `IDistributedCache`, downstream consumers pick it up automatically.

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Caching.AzureBlob;

public static class CacheRegistration
{
    public static void Configure(IServiceCollection services) =>
        services.AddAzureBlobDistributedCache(options =>
        {
            options.ConnectionString = "UseDevelopmentStorage=true";
            options.ContainerName = "orleans-lattice-cache";
            options.KeyPrefix = "tokens/";
        });
}
```

> In production, prefer a service URI with a managed-identity `TokenCredential`
> (for example `new DefaultAzureCredential()` from `Azure.Identity`) over a
> connection string. See [configuration](configuration.md) for the authentication
> modes.

## Reference

- [API reference](api.md) - the public options type and registration extension.
- [Configuration](configuration.md) - every public options property, its type, and its default, and the authentication-mode rules.
- [Architecture](architecture.md) - the blob layout, the expiry-metadata protocol, and how the cache attaches to `IDistributedCache` consumers.

## See also

- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md) - the hosted-web Entra sign-in provider whose distributed token cache this package can back.
