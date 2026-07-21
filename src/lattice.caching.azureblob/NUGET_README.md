# Orleans.Lattice.Caching.AzureBlob

Optional, opt-in **Azure Blob Storage** `IDistributedCache` for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

Implements the ASP.NET Core `IDistributedCache` abstraction against an Azure
Storage account, so a multi-replica web head has a shared, off-cluster cache
without needing Redis or SQL:

- Each entry is a single **block blob**; the value is the blob content and the
  expiry (absolute cap, sliding window, and current effective instant) lives in
  **blob metadata**.
- Arbitrary caller keys are hashed to a fixed, storage-legal blob name, so any
  key is safe.
- **Sliding expiration** is supported and advanced on read; expiry is enforced
  lazily (an expired entry reads as a miss and is best-effort evicted).

It targets low-churn, small-value workloads - most notably a
**Microsoft.Identity.Web distributed token cache** for a hosted, multi-replica
console such as the Orleans.Lattice Explorer. Register it once:

```csharp
builder.Services.AddAzureBlobDistributedCache(options =>
{
    options.ConnectionString = "UseDevelopmentStorage=true";
    options.ContainerName = "orleans-lattice-cache";
});
```

The cache is registered through the standard `IDistributedCache` seam, so any
consumer that resolves the abstraction uses the Azure Blob backend. The last
registration wins, so this call selects the blob backend over an earlier
`AddDistributedMemoryCache`.
