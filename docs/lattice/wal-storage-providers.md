# WAL Storage Providers

The write-ahead log (WAL) is the per-shard, durable, ordered record of every committed mutation. `IWalStorageProvider` is the pluggable seam that lets a host swap the WAL's underlying storage backend - in-memory for tests and single-process samples, Azure Table Storage for cross-region replicated production deployments, future durable backends as they are introduced - without touching the rest of the commit-log pipeline.

## Contract

`IWalStorageProvider` lives in `Orleans.Lattice` (core). Implementations are provider-agnostic; they see only `(treeId, shardIndex, WalEntry)` triples and must satisfy four invariants:

| Invariant | Meaning |
|---|---|
| **All-or-nothing append** | `AppendBatchAsync` (and its zero-copy `AppendEncodedBatchAsync` overload) is atomic per call. Either every entry in the supplied list is durably persisted before the returned task completes, or none of them are. A backend that cannot satisfy this for a particular batch (e.g. a multi-partition write on a backend without cross-partition transactions) must reject the batch at validation time rather than silently fragmenting it. |
| **Dense offsets** | Caller-assigned offsets are dense (gap-free) per shard. Implementations must preserve offsets verbatim so `GetHighestOffsetAsync` on activation always returns a value exactly one less than the next offset the caller will assign. |
| **Read order** | `ReadAsync` yields entries with `Offset` strictly greater than `fromOffsetExclusive`, in ascending offset order, capped at `maxEntries`. Pass `-1` to read from the start. |
| **Trim is idempotent** | `TrimAsync` removes every entry with offset `<= throughOffsetInclusive`. Trimming through an offset that has already been trimmed is a no-op; trimming through an offset that does not yet exist reserves the trim point for a future append. The persisted head sentinel is never rolled back. |
| **Lowest-offset query** | `GetLowestOffsetAsync` returns the lowest still-persisted offset, or `-1` for an empty / fully-trimmed shard. Together with `GetHighestOffsetAsync` it lets a caller compute the live entry count (`highest - lowest + 1`) without scanning the log, so trim-aware diagnostics observe the persisted footprint rather than the monotonically-growing offset counter. |

The contract:

```text
Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken);
Task AppendEncodedBatchAsync(string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries, ReadOnlyMemory<long> offsets, IWalMutationEncoder encoder, CancellationToken cancellationToken);
IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken);
Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken);
Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken);
Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken);
```

### Zero-copy append (`AppendEncodedBatchAsync`)

The WAL grain encodes each captured `LatticeMutation` exactly once via the configured `IWalMutationEncoder` at append time and tracks that encoded length against `LatticeOptions.WalMaxBatchBytes`. On flush it hands the **same** payload bytes to the provider through `AppendEncodedBatchAsync`, so backends that natively store binary blobs (`AzureTableWalStorageProvider`, file-backed providers) avoid the second encode that the legacy `AppendBatchAsync` shape forced on them.

`IWalStorageProvider.AppendEncodedBatchAsync` ships with a **default interface implementation** that decodes each segment through the supplied `IWalMutationEncoder.Decode(ReadOnlySpan<byte>)` back into a `WalEntry` and delegates to `AppendBatchAsync`. Third-party providers that have not been recompiled against the zero-copy overload therefore keep working unchanged; providers that want the fast path override `AppendEncodedBatchAsync` and skip the round-trip.

The default encoder, `OrleansBinaryWalMutationEncoder`, wraps the canonical `Serializer<LatticeMutation>` from `Orleans.Serialization`; hosts that wish to substitute a different wire format register their own `IWalMutationEncoder` singleton before `AddLattice` (the default registration uses `TryAddSingleton`).

A `WalEntry` pairs the `LatticeMutation` with its dense per-shard offset:

```csharp verify
var entry = new WalEntry
{
    Offset = 0L,
    Mutation = new LatticeMutation
    {
        TreeId = "tree",
        Kind = MutationKind.Set,
        Key = "k",
        Value = new byte[] { 1 },
        Timestamp = default,
        OriginClusterId = "site-a",
    },
};
```

## Registering a provider

Hosts wire a provider into the silo via `ISiloBuilder.AddWalStorage`. The default registration if no explicit provider is supplied is `InMemoryWalStorageProvider`. The extension takes a `Func<IServiceProvider, IWalStorageProvider>` factory so the provider can resolve its own dependencies from DI:

```csharp verify
siloBuilder.AddWalStorage(sp => new InMemoryWalStorageProvider());
```

The replication package additionally exposes per-tree overrides via `LatticeReplicationOptions.WalStorageProvider` for trees that should opt out of the silo-wide default.

## Provider catalogue

### `InMemoryWalStorageProvider`

Default implementation shipped in core. Stores every appended entry in a thread-safe per-shard list. State is kept entirely in process memory and is lost on silo restart. Suitable for tests, single-process samples, and as the registered default until a host wires up a durable provider.

- **Atomicity**: validates the supplied offsets are dense ahead of any state mutation; a rejected batch leaves observable state untouched.
- **Throughput**: lock-per-shard append (uncontended outside fan-in benchmarks).
- **Recovery**: none - the provider is process-scoped and reports an empty log on restart.

### `AzureTableWalStorageProvider`

Durable Azure Table Storage implementation shipped in the optional `Orleans.Lattice.Storage.AzureTable` NuGet package. Translates each `AppendBatchAsync` (or `AppendEncodedBatchAsync`) call into a single `TableClient.SubmitTransactionAsync` within the target partition. The provider overrides the zero-copy `AppendEncodedBatchAsync` overload so the WAL grain's already-encoded payload bytes are stored verbatim into the row's `Payload` column - no second encode.

#### Configuration

The package exposes one DI extension - `AddAzureTableWalStorage(this ISiloBuilder, Action<AzureTableWalStorageOptions>)` - that layers on top of core's `AddWalStorage`. The host supplies a delegate that populates `AzureTableWalStorageOptions`; the provider is built once at first use from the resolved options.

```text
using Orleans.Lattice.Storage.AzureTable;
using Azure.Identity;

// Connection-string mode (Azurite, dev, simple deployments):
siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
    o.TableName = "OrleansLatticeWal"; // optional; this is the default
});

// Token-credential mode (managed identity, production):
siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceUri = new Uri("https://myaccount.table.core.windows.net");
    o.TokenCredential = new DefaultAzureCredential();
});

// Shared-key mode:
siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceUri = new Uri("https://myaccount.table.core.windows.net");
    o.SharedKeyCredential = new TableSharedKeyCredential("myaccount", "<base64 key>");
});
```

Exactly one authentication mode must be configured. `Validate()` throws `InvalidOperationException` at first use if zero or more than one mode is set, or if `ServiceUri` is supplied without a credential. The optional `ConfigureClientOptions` callback lets the host attach custom retry policies, diagnostics, or transport without the provider having to surface a pass-through option per setting.

#### Storage layout

The provider uses one Azure Table partition per `(treeId, shardIndex)` pair plus a per-partition head-pointer sentinel:

| Element | Format | Purpose |
|---|---|---|
| `PartitionKey` | `{percent-encoded treeId}\|{shardIndex}` | One partition per shard. Disallowed characters (`/`, `\`, `#`, `?`, control bytes, surrogates) and `%` itself are percent-encoded byte-wise over UTF-8 so non-ASCII tree ids round-trip. |
| Entry `RowKey` | `E{Offset:D19}` | 19-digit zero-padded so lexicographic order matches numeric order (`long.MaxValue` is 19 digits). |
| Head `RowKey` | `HEAD` | Per-partition sentinel. `'H'` (0x48) sorts after `'E'` (0x45), so the entry-range query uses a tight upper bound (`RowKey lt 'HEAD'`). |
| Entity columns | `Offset` (long), `Payload` (byte[]?) | The payload is the Orleans-binary-serialised `LatticeMutation`; it is `null` on the head sentinel. |

The table is created on first use (idempotent) so hosts do not need to provision it out-of-band. Specify a non-default `TableName` to share an account across multiple Lattice clusters without WAL collisions.

#### Atomicity and capacity

Every `AppendBatchAsync` (or `AppendEncodedBatchAsync`) call is translated to **one** `SubmitTransactionAsync` containing one head-sentinel upsert plus one `Add` per appended entry. Azure Tables commits the transaction atomically across the partition or fails the whole batch, satisfying the all-or-nothing append contract.

Azure Tables caps a single transaction at **100 actions and 4 MiB**. Because every batch reserves one action for the head upsert, the provider rejects batches of more than `MaxEntriesPerBatch = 99` entries with `ArgumentException`. The replication package's `LatticeReplicationOptions.WalMaxBatchEntries` (default 100, validated against this cap) already keeps batches well below this limit in the canonical pipeline; callers writing the WAL directly should chunk larger batches before invoking the provider.

#### Operational characteristics

- **Recovery**: `GetHighestOffsetAsync` resolves in **one point read** of the head sentinel - O(1) regardless of log length. `GetLowestOffsetAsync` resolves in **one ascending `Top(1)` query** over the entry-row range - O(1) symmetric to the high-water-mark read.
- **Reads**: `ReadAsync` issues a tightly-bounded `(PartitionKey, RowKey)` range query so paging is server-side. The provider yields entries lazily through `IAsyncEnumerable<WalEntry>` so a caller that only needs the first N entries pays for one Azure Tables page rather than the full log.
- **Trim**: chunked delete in 100-action transactions with `ETag.All` (unconditional). A crash mid-trim leaves a contiguous live tail and a stale prefix; the next trim resumes from the new head. The head sentinel is never deleted, so the monotonic offset counter survives both trims and silo restarts.
- **Concurrency**: instances are safe for concurrent calls across distinct partitions. Concurrent calls targeting the same partition rely on Azure Tables' partition-level transactional serialisation; the WAL grain is single-writer per shard so this is the documented usage.
- **Per-call allocations**: the partition key is built per call (UTF-8 encode + percent-encode + concatenate). The hot path (`AppendEncodedBatchAsync`) materialises the row payload by copying each `ArraySegment<byte>` into a freshly-owned `byte[]` for the entity (the segment's backing buffer is pooled upstream by the WAL grain) and allocates a single `List<TableTransactionAction>` sized to `entries.Count + 1` for the batch. The legacy `AppendBatchAsync` overload additionally reuses a single `ArrayBufferWriter<byte>` across every entry in the batch for the per-entry encode.

## Testing

The package's unit tests run without any infrastructure. The end-to-end integration tests are tagged `[Category("AzureTableEmulator")]` and require [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite) to be running on the default development endpoint.

The default dev loop excludes the category implicitly, because the fixture's `[OneTimeSetUp]` probes Azurite reachability and falls through to `Assert.Inconclusive` if the probe fails - so a plain `dotnet test --filter "TestCategory!=Chaos"` reports the integration tests as inconclusive rather than failing the suite. CI excludes the category explicitly via `--filter "TestCategory!=Chaos&TestCategory!=AzureTableEmulator"` so the unreachable-Azurite probe never runs (saving the per-build wall-clock cost of the SDK's default retry budget).

```text
# Start Azurite via Docker (one-shot):
docker run -d --name lattice-azurite \
    -p 10000:10000 -p 10001:10001 -p 10002:10002 \
    mcr.microsoft.com/azure-storage/azurite:latest

# Run the integration tests:
dotnet test test/lattice.storage.azuretable/Orleans.Lattice.Storage.AzureTable.Tests.csproj \
    --filter "TestCategory=AzureTableEmulator"
```

If Azurite is not reachable, the fixture's `[OneTimeSetUp]` falls through to `Assert.Inconclusive` rather than failing the suite. Each test creates a fresh GUID-named table and tears it down on completion, so cross-test bleed is impossible even on a shared emulator instance.

## Implementing a custom provider

Authoring a custom provider is purely an exercise in implementing the contract. The guide-rails are:

1. Validate the supplied offsets are dense (`entries[i].Offset == entries[0].Offset + i`) **before** issuing any I/O so a rejected batch leaves observable state untouched.
2. Treat `cancellationToken` as a pre-condition - check it on entry to every public method.
3. Persist a head pointer (or equivalent O(1)-readable structure) so `GetHighestOffsetAsync` does not require scanning the log on activation. Symmetrically, expose the lowest still-persisted offset in O(1) so `GetLowestOffsetAsync` does not scan either - the live entry count is computed from both endpoints on the hot diagnostic path.
4. Make `TrimAsync` idempotent and safe to interrupt - a crash mid-trim must leave the WAL in a state where a subsequent trim resumes correctly.
5. **Optional fast path.** Override `AppendEncodedBatchAsync` when the backend stores binary payloads natively. The default implementation decodes each segment through the supplied `IWalMutationEncoder` and delegates to `AppendBatchAsync`, so a provider that only implements `AppendBatchAsync` keeps working - overriding the zero-copy overload skips the round-trip and stores the grain's already-encoded bytes directly.

The `InMemoryWalStorageProvider` source under `src/lattice/InMemoryWalStorageProvider.cs` is the canonical reference implementation; the `AzureTableWalStorageProvider` source under `src/lattice.storage.azuretable/` is the canonical durable reference implementation.

Once the implementation is in place, register it through the standard `AddWalStorage` extension - no other wiring is required:

```csharp verify
siloBuilder.AddWalStorage(sp => new InMemoryWalStorageProvider());
