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
Task AppendEncodedBatchAsync(string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries, ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken);
IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken);
Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken);
Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken);
Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken);
Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken); // optional, default no-op
Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken); // optional, default no-op
```

### Reclamation without a trim (`EvaluateCompactionAsync`)

`EvaluateCompactionAsync` asks a provider to decide whether a shard is due for physical reclamation, and to perform it if its own policy says so. The WAL GC calls it wherever a pass ends without invoking `TrimAsync` for a shard: when that shard's scan released nothing, and - because the collector can also return before the per-shard scan begins at all - when the tree holds no usable trim predicate on any partition and no TTL is configured. The second case is the state an unusable durable materialiser pin produces, it can persist indefinitely, and it is distinguishable in telemetry because such a pass reports no trim-stop series at all rather than a zero one.

It exists because trimming and reclamation are separable concerns that a log-structured backend can accidentally weld together. Trimming decides which *live* entries may be released and is gated by consumers that have not yet applied them; reclamation returns space belonging to entries that are *already* trimmed and that nothing references. A provider that reclaims only as a side effect of being trimmed cannot reclaim at all while the retention floor is held - and a held floor is precisely the state in which the backlog is largest. Issue #3207 measured that shape: a shard whose scan stopped on the offset floor at its **first** entry was never evaluated against any compaction threshold, at any dead ratio, for as long as the stop persisted, so no value of any compaction option could reach it. The same issue measured the stronger form of the shape on a tree whose pins had become unusable: the collector returned before the scan, so not even the stop was recorded.

The call carries no watermark and must not move one. It is not a trim, it grants no additional release, and the shard's **logical** contents must be identical either side of it: the offsets `ReadAsync`, `GetLowestOffsetAsync`, and `GetHighestOffsetAsync` report are unchanged whether or not a compaction ran. Whether anything happens is entirely the provider's decision, taken against the same policy - and reported through the same `orleans.lattice.wal.compactions` arms - as the evaluation `TrimAsync` already performs.

The default interface implementation is a no-op. That is correct rather than an omission for a backend whose trim deletes its storage outright: deletion *is* reclamation for the in-memory and Azure Table providers, so no dead bytes exist and there is nothing to evaluate. Only a backend that reclaims by rewriting - the file provider - overrides it.

Because the evaluation is threshold-gated it is self-limiting, and so needs no frequency bound of its own: a compaction zeroes the shard's dead bytes, after which every further evaluation returns at the minimum-dead floor until new trims accumulate, and an evaluation that declines rewrites nothing.

### Activation-time recovery (`ReconcileAsync`)

`ReconcileAsync` is the optional activation-time recovery seam for providers whose commit protocol can leave the durable state inconsistent across crash boundaries. The WAL grain calls it in `OnActivateAsync` before reading the highest offset, so the activation hook runs while the grain is quiescent. It also calls it from the post-failure resync after a flush fails, when the grain's own flush chain has drained but the provider may still hold work the grain has already been told about (for example a pipelined phase-2 commit), so an override must tolerate writes to the same shard that are still settling - the Azure Tables provider waits them out before scanning. The default interface implementation is a no-op, suitable for backends whose append is atomic in a single operation (`InMemoryWalStorageProvider` inherits the default). The Azure Tables provider overrides it to repair phase-1/phase-2 orphans (see *Crash recovery* below).

### Zero-copy append (`AppendEncodedBatchAsync`)

The WAL grain encodes each captured `WalRecord` exactly once via the configured `IWalRecordEncoder` at append time and tracks that encoded length against `LatticeOptions.WalMaxBatchBytes`. On flush it hands the **same** payload bytes to the provider through `AppendEncodedBatchAsync`, so backends that natively store binary blobs (`AzureTableWalStorageProvider`, file-backed providers) avoid the second encode that the legacy `AppendBatchAsync` shape forced on them.

`IWalStorageProvider.AppendEncodedBatchAsync` ships with a **default interface implementation** that decodes each segment through the supplied `IWalRecordEncoder.Decode(ReadOnlySpan<byte>)` back into a `WalEntry` and delegates to `AppendBatchAsync`. Third-party providers that have not been recompiled against the zero-copy overload therefore keep working unchanged; providers that want the fast path override `AppendEncodedBatchAsync` and skip the round-trip.

The default encoder, `OrleansBinaryWalRecordEncoder`, wraps the canonical `Serializer<WalRecord>` from `Orleans.Serialization`; hosts that wish to substitute a different wire format register their own `IWalRecordEncoder` singleton before `AddLattice` (the default registration uses `TryAddSingleton`).

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

`AddWalStorage` has **two different registration semantics depending on the overload**, and the difference is load-bearing because `AddLattice` self-registers the in-memory baseline as part of its own setup:

| Overload | Registration | Wins against |
|---|---|---|
| `AddWalStorage()` (no factory) | `TryAddSingleton` - first registration wins | nothing (only installs if no provider is registered yet) |
| `AddWalStorage(factory)` | `Services.Replace` - last call wins | the in-memory baseline, any prior factory |

Net effect: a host-supplied factory is **order-independent** with respect to `AddLattice`. `siloBuilder.AddLattice(...)` followed by `siloBuilder.AddAzureTableWalStorage(...)` produces the same effective registration as the reverse order - the Azure factory wins either way. Calling `AddWalStorage(factory)` (or one of the package-level overloads that wraps it, such as `AddAzureTableWalStorage`) multiple times follows last-call-wins.

This contract was tightened to fix a silent-drop bug: previously both branches used `TryAddSingleton`, so a host that called `AddLattice` before `AddAzureTableWalStorage` would silently end up on the in-memory baseline because `AddLattice`'s own `AddWalStorage()` call had already won the `TryAdd` race.

The replication package additionally exposes per-tree overrides via `LatticeReplicationOptions.WalStorageProvider` for trees that should opt out of the silo-wide default.

For production deployments, the canonical durable WAL backend is the Azure Table Storage provider in the optional `Orleans.Lattice.Storage.AzureTable` package - register it with `AddAzureTableWalStorage` so the commit log survives silo restarts. See [Orleans.Lattice.Storage.AzureTable](../lattice.storage.azuretable/README.md) for its setup, configuration, and operations guide.

## Multi-account fan-out: named providers and pinned placement

A single storage account has a throughput ceiling (Azure Table tops out around 22-24 ke/s per account). A tree whose write rate exceeds one account's ceiling needs its WAL partitions spread across **several** accounts. The seam for that is a per-silo **provider catalogue** keyed by string, plus a per-tree **placement pin** that maps each WAL partition to a catalogue key. The pin is durable cluster state; a partition's placement only ever changes through the managed `ILatticeAdmin` move surface, never as a side effect of a config edit.

### Naming providers in the catalogue

`AddWalStorage(factory)` still registers the **baseline** provider under the reserved key `default`. Register every *additional* backend under a distinct key with `AddLatticeWalStorageProvider`:

```csharp verify
siloBuilder.AddWalStorage(sp => new InMemoryWalStorageProvider());
siloBuilder.AddLatticeWalStorageProvider("table-account-b", sp => new InMemoryWalStorageProvider());
siloBuilder.AddLatticeWalStorageProvider("table-account-c", sp => new InMemoryWalStorageProvider());
```

**Cluster contract.** Every silo must register an identical key set. A partition pinned to a key a given silo did not register **fails closed** on that silo (`LatticeWalProviderMissingException`) rather than silently falling back to the baseline. The reserved key `default` cannot be registered through `AddLatticeWalStorageProvider` - it always names the `AddWalStorage` baseline. Re-registering a key is last-call-wins.

Hosts that never call `AddLatticeWalStorageProvider` are unaffected: every tree's pin defaults to `default`, so the resolution path is identical to the pre-placement behaviour.

### Inspecting placement

`ILatticeAdmin` is the cluster-wide administrative singleton; resolve it with `grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin")`. `GetWalPlacementAsync` returns the durable pin (the default key plus any per-partition overrides and the pin's CAS version). `AuditWalPlacementAsync` additionally reports, for the silo that serves the call, whether every pinned key is resolvable there - the cheapest way to catch a missing-key misconfiguration before it fails a partition closed:

```csharp verify
var admin = grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin");
WalPlacement placement = await admin.GetWalPlacementAsync("orders", cancellationToken);
WalPlacementAudit audit = await admin.AuditWalPlacementAsync("orders", cancellationToken);
if (!audit.AllResolvableOnThisSilo)
{
    // One or more partitions are pinned to a key this silo did not register.
}
```

### Moving a partition to another account

Moving a partition is a two-call workflow: `PlanWalMoveAsync` is a read-only dry run that reports what a move would copy (offset range, entry count, whether the target is already current, whether the target key resolves on the serving silo); `ExecuteWalMoveAsync` performs the move:

```csharp verify
var admin = grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin");
WalMovePlan plan = await admin.PlanWalMoveAsync("orders", partition: 0, "table-account-b", cancellationToken);
WalMoveReceipt receipt = await admin.ExecuteWalMoveAsync(
    "orders", partition: 0, "table-account-b", WalMoveOptions.Default, cancellationToken);
```

`ExecuteWalMoveAsync` runs a quiesce-copy-cutover saga: it fences the partition's WAL grain (briefly refusing appends), copies the retained entry range to the target provider **preserving every offset**, re-converges on any entries that landed during the copy, flips the durable pin under a compare-and-swap, then forces the WAL grain to deactivate so its next activation - on any silo - reads the new pin and binds the new provider. Appends resume against the target with no offset discontinuity. The move is **non-destructive**: the source's entries are retained (`receipt.SourceRetained == true`) so the operation is recoverable.

`WalMoveOptions.Default` is fine for most moves; override `QuiesceLease`, `CopyPageSize`, or `VerifyAfterCopy` for large partitions or stricter post-copy verification.

### Reverting and reclaiming

Because a move only rewrites the pin, **reverting is just another move** back to the original key - the source still holds every entry, so the reverse move copies nothing new and flips the pin back:

```csharp verify
var admin = grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin");
WalMoveReceipt reverted = await admin.ExecuteWalMoveAsync(
    "orders", partition: 0, "default", WalMoveOptions.Default, cancellationToken);
```

Once you are confident a forward move is permanent, reclaim the now-redundant copy on the **source** provider with `ReclaimMovedWalSourceAsync`. It refuses (throws) if the partition is still pinned to that source - you can only reclaim a placement the pin has already moved away from:

```csharp verify
var admin = grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin");
WalMoveReceipt reclaim = await admin.ReclaimMovedWalSourceAsync(
    "orders", partition: 0, "default", cancellationToken);
```

> **Per-call scope.** A `ReclaimMovedWalSourceAsync` call reclaims exactly one partition's former source. To discard the retained sources after a batch move, reclaim each moved partition in turn.

Re-running a reclaim is idempotent: once the former source holds no live entries the call reports `WalMoveOutcome.NoOp` and trims nothing, even though the source still reports its high-water mark (a trim never lowers `GetHighestOffsetAsync`).

Reclaiming is what makes a move irreversible. A later move back onto the reclaimed provider is refused with `InvalidOperationException` while that provider's high-water mark covers offsets the partition still retains, because it no longer holds them and resuming past its mark would skip live entries. Move to a different provider key instead, or retry once the partition's retained range starts above the reclaimed mark.

> **A partition with no live entries.** When WAL GC has trimmed a partition completely there is nothing to copy, but the target must still continue the partition's offsets. The move reserves the source's high-water mark on the target as a trim point, and refuses the cutover if the target does not then report it: the file provider records a reserved trim, while the Azure Table and in-memory providers do not, so on those a fully-trimmed partition is movable once it holds live entries again.

### Moving several partitions at once

To relocate a whole tree (or a subset of its partitions) to a different account, use the batch overloads of `PlanWalMoveAsync` / `ExecuteWalMoveAsync`, which take a sequence of `(partition, targetProviderKey)` pairs. The batch flips the placement pin **once**, under a single compare-and-swap, so every partition moves together to the same new placement version - no intermediate placement is ever observable:

```csharp verify
var admin = grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin");
var moves = new (int Partition, string TargetProviderKey)[]
{
    (0, "table-account-b"),
    (1, "table-account-b"),
    (2, "table-account-b"),
};
WalMoveBatchPlan batchPlan = await admin.PlanWalMoveAsync("orders", moves, cancellationToken);
WalMoveBatchReceipt batchReceipt = await admin.ExecuteWalMoveAsync(
    "orders", moves, new WalMoveOptions { MaxConcurrentPartitionMoves = 2 }, cancellationToken);
```

Each partition runs the same quiesce-copy-verify phases as a single move; `WalMoveOptions.MaxConcurrentPartitionMoves` (default `1` = sequential) bounds how many run in parallel, so you can trade a faster cutover against the extra storage-tier pressure of concurrent copies. `batchReceipt.Moves` carries one `WalMoveReceipt` per requested partition, in request order, and `batchReceipt.Outcome` is `Moved` when at least one partition was relocated (or `AlreadyAtTarget` when every partition was already pinned to its target).

The batch is **all-or-nothing**: if any partition's phase fails, the pin is never flipped, every fenced source is released back to service, and the partial target copies are retained so a re-execute resumes without recopying. The batch fails closed before touching any log (throwing `LatticeWalProviderMissingException`) if **any** target key is unresolvable on the serving silo, and rejects an empty batch or a partition named more than once with `ArgumentException`. As with a single move, sources are never trimmed - reclaim each one explicitly with `ReclaimMovedWalSourceAsync` once the move is permanent.


## Provider catalogue

### `InMemoryWalStorageProvider`

Default implementation shipped in core. Stores every appended entry in a thread-safe per-shard list. State is kept entirely in process memory and is lost on silo restart. Suitable for tests, single-process samples, and as the registered default until a host wires up a durable provider.

- **Atomicity**: validates the supplied offsets are dense ahead of any state mutation; a rejected batch leaves observable state untouched.
- **Throughput**: lock-per-shard append (uncontended outside fan-in benchmarks).
- **Recovery**: none - the provider is process-scoped and reports an empty log on restart.

### `AzureTableWalStorageProvider`

Durable Azure Table Storage implementation shipped in the optional `Orleans.Lattice.Storage.AzureTable` NuGet package. It overrides the zero-copy append overload so the WAL grain's already-encoded payload bytes are stored verbatim, and implements activation-time reconciliation so its multi-phase commit protocol stays consistent across crash boundaries.

Its storage and row layout, transactional append pipeline, retry and saturation handling, compression, capacity planning, and the Azurite-backed test setup are documented in the dedicated [Azure Table WAL docs](../lattice.storage.azuretable/README.md) - start with [configuration](../lattice.storage.azuretable/configuration.md), [architecture](../lattice.storage.azuretable/architecture.md), and [chaos tests](../lattice.storage.azuretable/chaos-tests.md).

## Implementing a custom provider

Authoring a custom provider is purely an exercise in implementing the contract. The guide-rails are:

1. Validate the supplied offsets are dense (`entries[i].Offset == entries[0].Offset + i`) **before** issuing any I/O so a rejected batch leaves observable state untouched.
2. Treat `cancellationToken` as a pre-condition - check it on entry to every public method.
3. Persist a head pointer (or equivalent O(1)-readable structure) so `GetHighestOffsetAsync` does not require scanning the log on activation. Symmetrically, expose the lowest still-persisted offset in O(1) so `GetLowestOffsetAsync` does not scan either - the live entry count is computed from both endpoints on the hot diagnostic path.
4. Make `TrimAsync` idempotent and safe to interrupt - a crash mid-trim must leave the WAL in a state where a subsequent trim resumes correctly.
5. **Optional fast path.** Override `AppendEncodedBatchAsync` when the backend stores binary payloads natively. The default implementation decodes each segment through the supplied `IWalRecordEncoder` and delegates to `AppendBatchAsync`, so a provider that only implements `AppendBatchAsync` keeps working - overriding the zero-copy overload skips the round-trip and stores the grain's already-encoded bytes directly.
6. **Optional activation-time recovery.** Override `ReconcileAsync` if the backend's commit protocol can leave the durable state inconsistent across crash boundaries (e.g. a multi-phase commit, as in the Azure Tables provider). The default implementation is a no-op, suitable for backends whose append is atomic in a single operation. The WAL grain calls `ReconcileAsync` in `OnActivateAsync` before reading the highest offset, so the activation seam is quiescent for the duration.

The `InMemoryWalStorageProvider` source under `src/lattice/InMemoryWalStorageProvider.cs` is the canonical reference implementation; the `AzureTableWalStorageProvider` source under `src/lattice.storage.azuretable/` is the canonical durable reference implementation.

Once the implementation is in place, register it through the standard `AddWalStorage` extension - no other wiring is required:

```csharp verify
siloBuilder.AddWalStorage(sp => new InMemoryWalStorageProvider());
