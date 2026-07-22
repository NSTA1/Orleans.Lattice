# Orleans.Lattice.Caching.AzureBlob architecture

The package is a single `IDistributedCache` implementation, `AzureBlobDistributedCache` (internal), plus two internal helpers that keep the blob-name and expiry logic pure and testable.

## Blob layout

Each cache entry is one **block blob** in the configured container:

- **Blob name** is `{KeyPrefix}{hash}`, where `hash` is the lowercase hex SHA-256 of the UTF-8 cache key, computed by `BlobCacheKeyMap`. Hashing guarantees a fixed-length, storage-legal name for any caller-supplied key (Microsoft.Identity.Web keys contain characters that are not valid in a blob name), and the optional `KeyPrefix` acts as a virtual directory so several logical caches can share one container.
- **Blob content** is the cached value verbatim (`byte[]`). Values are held whole in memory during a read or write, so the cache targets small entries - tokens and session state, not large blobs.
- **Blob metadata** carries the expiry: the absolute expiration cap, the sliding window, and the current effective expiry instant. Keeping expiry in metadata means a read fetches content and expiry in one download, and a sliding renewal is a cheap metadata-only `SetMetadata` call.

## Expiry protocol

`BlobCacheEntryExpiration` is a pure, `TimeProvider`-driven helper - no I/O - so every expiry decision is unit-testable against an injected clock:

- **Compute** turns a `DistributedCacheEntryOptions` (absolute, absolute-relative-to-now, or sliding) plus the current instant into the stored expiry values.
- **FromMetadata / ToMetadata** round-trip those values through the blob's metadata dictionary.
- **IsExpired** compares the effective instant to now.
- **Slide** advances the effective expiry by the sliding window on each read, capped at the absolute expiration, and returns null when there is nothing to slide.

Enforcement is **lazy on read**. `Get`/`Refresh` download the entry, and if it is expired they best-effort delete it and report a miss; otherwise a sliding entry has its effective expiry advanced. There is no background sweeper, so an entry written and never read again lingers until overwritten or removed. That is acceptable for the low-churn, per-subject workloads (a token cache) this backend targets, and it keeps the implementation free of a timer or lease.

## Container lifecycle

The container is created on first use behind a one-shot async gate (`EnsureContainerAsync`): the first operation calls `CreateIfNotExists` under a `SemaphoreSlim`, flips a ready flag, and every subsequent operation skips straight through. Hosts therefore never provision the container out of band, and the create cost is paid once per process.

## Concurrency and failure semantics

- **Writes** are last-writer-wins blob uploads; there is no read-modify-write race because `Set` replaces the whole blob.
- **Sliding renewals and expired-entry deletes are best-effort.** A `RequestFailedException` from a concurrent delete or rewrite is swallowed: a lost slide only shortens a window (never corrupts the value), and a failed delete is harmless because the entry already read as a miss.
- **404s are misses.** A missing blob on `Get`/`Refresh`/`Remove` is treated as an absent entry, not an error.

## How it attaches

`AddAzureBlobDistributedCache` registers the cache as the last `IDistributedCache` singleton, building the container client from the validated options and resolving a `TimeProvider` (or `TimeProvider.System`). Consumers - the Explorer's Microsoft.Identity.Web distributed token cache, ASP.NET session state, output caching - resolve `IDistributedCache` and transparently use the blob backend.

## See also

- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/architecture.md) - the primary consumer, whose distributed token cache this backs on a multi-replica host.
