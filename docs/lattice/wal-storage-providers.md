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
Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken); // optional, default no-op
```

### Activation-time recovery (`ReconcileAsync`)

`ReconcileAsync` is the optional activation-time recovery seam for providers whose commit protocol can leave the durable state inconsistent across crash boundaries. The WAL grain calls it in `OnActivateAsync` before reading the highest offset, so the activation hook runs while the grain is quiescent. The default interface implementation is a no-op, suitable for backends whose append is atomic in a single operation (`InMemoryWalStorageProvider` inherits the default). The Azure Tables provider overrides it to repair phase-1/phase-2 orphans (see *Crash recovery* below).

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

> **Single-partition scope.** Each `ExecuteWalMoveAsync` / `ReclaimMovedWalSourceAsync` call moves exactly one partition. Spreading a tree across N accounts today means N-1 sequential move calls. A batch / whole-tree rebalance API is tracked as a follow-up.


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

// Pre-built TableServiceClient mode (shared with other Orleans components):
siloBuilder.AddAzureTableWalStorage(o =>
{
    // Host already constructs one TableServiceClient (typically with
    // DefaultAzureCredential) and routes every Azure-backed component
    // through it - the same shape AddAzureTableGrainStorage's
    // options.TableServiceClient slot expects.
    o.ServiceClient = sharedTableServiceClient;
});
```

Exactly one authentication mode must be configured. `Validate()` throws `InvalidOperationException` at first use if zero or more than one mode is set, or if `ServiceUri` is supplied without a credential. The optional `ConfigureClientOptions` callback lets the host attach custom retry policies, diagnostics, or transport without the provider having to surface a pass-through option per setting - except in pre-built `ServiceClient` mode, where the callback is ignored because the host already owns the client's `TableClientOptions` and lifetime.

#### Storage layout

The provider uses a **per-batch partition + per-shard manifest** schema. Each `AppendBatchAsync` (or `AppendEncodedBatchAsync`) call lands in its own batch partition; each shard also owns one manifest partition that records every committed batch plus the shard's monotonic tail.

| Element | Format | Purpose |
|---|---|---|
| Batch `PartitionKey` | `_b_\|{percent-encoded treeId}\|{shardIndex}\|S{startOffset:D19}` | One partition per appended batch. Distinct batch partitions sit on distinct Azure Tables partition servers, so concurrent appends against the same shard run in parallel rather than serialising on a single server. The D19 width makes batch partition keys sort lexicographically iff their start offsets sort numerically. |
| Manifest `PartitionKey` | `_m_\|{percent-encoded treeId}\|{shardIndex}` | One manifest partition per shard. Holds the candidate-row, manifest, and tail rows described below. |
| Entry `RowKey` | `E{Offset:D19}` (inside a batch partition) | One row per appended `WalEntry`. The 19-digit zero pad makes lexicographic order match numeric order. |
| Candidate `RowKey` | `C{startOffset:D19}` (inside the manifest partition) | Phase-0 stamp written alongside phase-1 entry rows. The row's `Offset` column carries `endOffsetInclusive`. Deleted atomically with the matching manifest row by phase 2; a remaining candidate-row after a restart is exactly the orphan signal the reconciler looks for. |
| Manifest `RowKey` | `M{startOffset:D19}` (inside the manifest partition) | Phase-2 commit. The row's `Offset` column carries `endOffsetInclusive`. A `RowKey` range scan returns committed batches in commit-offset order. |
| Tail `RowKey` | `TAIL` (inside the manifest partition) | Per-shard tail pointer. The `Offset` column holds the maximum committed `endOffsetInclusive` across every batch in the shard. `'T' > 'M'`, so the manifest range query uses `RowKey lt 'TAIL'` as a tight upper bound. |
| Entity columns | `Offset` (long), `Payload` (byte[]?) | The payload is the Orleans-binary-serialised `LatticeMutation` on entry rows; it is `null` on candidate, manifest, and tail rows. |

Disallowed characters (`/`, `\`, `#`, `?`, control bytes, surrogates) and `%` itself are percent-encoded byte-wise over UTF-8 so non-ASCII tree ids round-trip; the percent-encoded form is cached process-wide so the per-call partition-key build path allocates only the assembled key string. The table is created on first use (idempotent) so hosts do not need to provision it out-of-band. Specify a non-default `TableName` to share an account across multiple Lattice clusters without WAL collisions.

#### Atomicity and capacity

Each append is committed in **three phases** so concurrent batches against the same shard get true partition-server parallelism while still presenting a monotonic, all-or-nothing visible tail:

1. **Phase 0** stamps the candidate-row (`C{startOffset:D19}`) into the shard's manifest partition in parallel with phase 1. The row carries the batch's `endOffsetInclusive` so reconciliation can describe the batch without reading its entry rows.
2. **Phase 1** writes every entry row into the batch's own partition in a single `SubmitTransactionAsync`. Azure Tables commits the transaction atomically across the partition or fails the whole batch, so phase 1 is either fully durable or invisible.
3. **Phase 2** is handed off to a per-shard `PhaseTwoWorker`. The worker drains pending commits in strict ascending `startOffset` order and coalesces up to **49 manifest commits** plus a single `TAIL` upsert into one transaction (`2 * 49 + 1 = 99` actions per chunk, fitting under the 100-action cap). The strict-offset drain order makes `TAIL` unconditionally monotonic regardless of phase-1 completion order; the coalescing collapses N round-trips into one under burst load. `AppendBatchAsync` awaits the phase-2 completion, so post-append `GetHighestOffsetAsync` observes the new tail.

Azure Tables caps a single transaction at **100 actions and 4 MiB**. Phase 1 holds entry rows only (no head sentinel) so the full 100-action budget is available for entries; the provider rejects batches of more than `MaxEntriesPerBatch = 100` entries with `ArgumentException`. The replication package's `LatticeReplicationOptions.WalMaxBatchEntries` already keeps batches well below this limit in the canonical pipeline; callers writing the WAL directly should chunk larger batches before invoking the provider.

#### Crash recovery

A silo crash between phase 0/1 and phase 2 leaves an **orphan**: a batch partition with phase-1 entry rows plus a phase-0 candidate-row in the manifest partition, but no phase-2 manifest row. The provider's `ReconcileAsync` activation-time hook discovers orphans with a **single anchored range query** against the shard's manifest partition (`RowKey ge 'C' and RowKey lt 'D'`) - no cross-partition scan over the shard's live batch partitions.

- Orphans whose `startOffset` contiguously extends the current `TAIL` are **rolled forward**: their manifest rows are added in strict offset order, their candidate-rows are deleted, and `TAIL` advances.
- Orphans below or above a gap are **rolled back**: every entry row in the orphan's batch partition is deleted and the candidate-row is deleted. The producer's WAL grain restarts at `TAIL + 1` and would otherwise observe an unreferenced batch sitting above the offset it expects to be the next monotonic append slot.

Reconciliation is idempotent: a second pass with no intervening writes is a no-op because no candidate-rows remain (phase 2 or rollback already deleted them). The work is also bounded by `WalMaxPendingBatches`, which is typically 0 in steady state, so the activation cost is O(in-flight batches at the moment of the crash) rather than O(live batches in the shard).

When the optional `EliminateCandidateRowOnHotPath` mode is enabled (see [below](#eliminating-the-phase-0-candidate-row)), the phase-0 candidate-row write is skipped on the hot path and the reconciler additionally enumerates batch partitions sitting above `TAIL` to discover orphans without a candidate-row.

The activation seam itself - `IWalStorageProvider.ReconcileAsync` - ships with a default no-op implementation, so providers that do not need recovery (the in-memory provider, file-backed providers that commit atomically) inherit the no-op without code.

#### Operational characteristics

- **Recovery**: `GetHighestOffsetAsync` resolves in **one point read** of the shard's `TAIL` row - O(1) regardless of log length. `GetLowestOffsetAsync` walks the manifest partition forward until it finds a non-empty batch partition, so its cost is O(trimmed batches with surviving manifest rows), typically 0 outside the trim hot path.
- **Reads**: `ReadAsync` walks the manifest partition in ascending start-offset order, then streams the matching batch partitions' entry rows. Paging is server-side; the provider yields entries lazily through `IAsyncEnumerable<WalEntry>` so a caller that only needs the first N entries pays for one Azure Tables page per overlapping batch.
- **Trim**: chunked delete in 100-action transactions with `ETag.All` (unconditional) per batch partition, plus a per-batch manifest-row delete in commit order. A crash mid-trim leaves a contiguous live tail and a stale prefix; the next trim resumes from the new head. `TAIL` is never moved back by trim.
- **Concurrency**: instances are safe for concurrent calls across distinct shards. Concurrent calls into the same shard land in distinct batch partitions during phase 1 (no contention) and serialise through the per-shard phase-2 worker for phase 2. With the post-v6.2 default `WalMaxPendingBatches = 16` up to sixteen `AppendBatchAsync` calls can be in flight per shard; the worker's phase-2 coalescing window collapses overlapping commits into one Azure-Tables transaction. The combined fan-out across all shards (`WalPartitions * WalMaxPendingBatches` = 128 at default) sits at the edge of a single Azure Tables Standard storage account's sustained throughput budget (~2,500 transactions/sec); see [WAL tuning](wal-tuning.md) for the envelope above which throttling collapses per-flush latency.
- **Per-call allocations**: percent-encoded tree-id segments are cached process-wide so a repeated tree id pays the encode cost exactly once. The hot path (`AppendEncodedBatchAsync`) materialises the row payload by copying each `ArraySegment<byte>` into a freshly-owned `byte[]` for the entity (the segment's backing buffer is pooled upstream by the WAL grain) and allocates a single `List<TableTransactionAction>` sized to `entries.Count` for the batch. The legacy `AppendBatchAsync` overload additionally reuses a single `ArrayBufferWriter<byte>` across every entry in the batch for the per-entry encode.

#### Phase-2 pipelining

Setting `AzureTableWalStorageOptions.PipelinePhaseTwoCommits = true` opts the provider into a **pipelined phase-2** mode in which `AppendBatchAsync` returns as soon as the **previous** batch's phase-2 commit lands, rather than waiting for the **current** batch's own phase-2. Phase 0 (candidate-row stamp) and phase 1 (entry-row transactional batch) remain synchronous and durable on every call, so a crash at any point still produces a state the activation-time reconciler can roll forward or roll back deterministically. Pipelining only changes when the caller of the *current* `AppendBatchAsync` learns that phase-2 succeeded - it does not relax atomicity, ordering, or recovery.

**As of v6.0.1 the default is `PipelinePhaseTwoCommits = true`** (the operating-point default selected by the throughput campaign). The mode is now opt-out rather than opt-in; hosts that need the pre-v6.0 strict-serial-phase-2 behaviour (phase-2 failure surfaces on the failing call rather than the next one) set it back to `false` explicitly. The default flip is paired with `PhaseTwoCoalescingWindow = 5 ms` (also flipped in v6.0.1) so the worker's 49-row coalescing window actually engages under burst load instead of committing one-row transactions every call.

The mode is purely a throughput-vs-latency knob:

| Property | Pre-v6.0.1 / opt-out (`= false`) | v6.0.1 default / pipelined (`= true`) |
|---|---|---|
| Caller-visible append latency | own phase-0+1+2 | own phase-0+1 + previous phase-2 |
| Phase-2 commit ordering on a shard | strict ascending start-offset | strict ascending start-offset (unchanged) |
| Crash recovery semantics | reconciler rolls forward / rolls back | reconciler rolls forward / rolls back (unchanged) |
| Failure surfacing on the *next* append | the failing call observes its own phase-2 fault | the *next* `AppendBatchAsync` on the shard observes the previous append's phase-2 fault |
| Failure surfacing when the producer goes quiescent | n/a (every caller observes its own fault) | configurable via `PipelinedPhaseTwoFaultHandler`; default is silent |

##### Surfacing faults on a quiescent shard

In pipelined mode the slot occupant - the previous batch's still-in-flight phase-2 task - is observed only when a successor `AppendBatchAsync` arrives. If the producer drains, idles, or crashes after issuing the *last* batch, that batch's phase-2 fault has no canonical observer: `DisposeAsync` swallows at shutdown by design (the data is recoverable on next activation, but the application that issued the append has no signal). To close that gap, set `PipelinedPhaseTwoFaultHandler` to a log-only delegate at host startup:

```text
siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceUri = new Uri("https://myaccount.table.core.windows.net");
    o.TokenCredential = new DefaultAzureCredential();
    o.PipelinePhaseTwoCommits = true;
    o.PipelinedPhaseTwoFaultHandler = ex => logger.LogError(ex,
        "Pipelined phase-2 commit failed; data is recoverable on next activation.");
});
```

The handler fires **exactly once per faulted pipelined phase-2 task**, on a thread-pool continuation chained off the slot occupant the moment the fault becomes observable - regardless of whether a successor call later arrives (which would also surface the fault to its own caller). Implementations should be **idempotent and observability-only**; exceptions thrown by the handler are swallowed so a misbehaving observer cannot corrupt the pipeline's task graph. The handler is not on any append's request path.

##### Cancellation semantics

`AppendBatchAsync`'s `cancellationToken` is wired through the phase-2 wait via `Task.WaitAsync(CancellationToken)`. In default mode this cancels the caller's wait on its own phase-2 task. In pipelined mode it cancels the caller's wait on the *previous* batch's phase-2 task without disturbing that task itself - the predecessor remains in flight and any other observer (the next call, the fault handler, `DisposeAsync`) will continue to see it through to its terminal state. The phase-2 commit itself is not cancellable; cancellation only releases the caller's blocking wait.

##### When to enable

Pipelining trades a phase-2 wait for additional concurrent writer pressure on the WAL backend. The trade is positive only while the backend has spare write concurrency to absorb the overlapped batch, and **the relationship is non-monotonic in offered load** - throughput at saturation can be worse with pipelining than without it.

A representative single-silo Azurite measurement (current-state-no-replication scenario, 30 s window, three load points relative to the host's calibrated saturation knee) shows the shape:

| Offered load                     | Commits/s Δ | WAL append p99 Δ | Verdict                                  |
|----------------------------------|------------:|-----------------:|------------------------------------------|
| Well below knee                  |      ~+2 %  |          ~0 %    | near-noise; little phase-2 idle to overlap |
| Near knee (sustained pressure)   |     ~+11 %  |        ~-36 %    | clear win; phase-2 overlaps next batch     |
| Past knee (backend saturated)    |     ~-16 %  |        ~+45 %    | regression; concurrent writes contend     |

Two practical rules follow:

- Enable pipelining only on backends that scale write concurrency (e.g. real Azure Tables with multiple WAL shards spreading across partitions). On a single-writer backend - Azurite, a single-shard configuration, or any WAL whose effective write width is one - pipelining can amplify saturation rather than relieve it.
- **As of v6.0.1 the default is `PipelinePhaseTwoCommits = true`** because the throughput campaign measured a large, sustained win at the Azure-Tables operating point - the single highest-impact entry on the campaign's library-default-flip ladder - with the recovery contract unchanged and failures still sticky on the next call. Hosts running against a single-writer backend (Azurite, single-shard configurations, or any WAL whose effective write width is one) should explicitly opt **out** (`PipelinePhaseTwoCommits = false`) - the table above's "past knee" row applies. The same rule covers any host that wants the pre-v6.0 behaviour where a phase-2 failure surfaces on the failing call rather than the next one.

#### Eliminating the phase-0 candidate row

Setting `AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath = true` opts the provider into a hot-path shape that **skips the phase-0 candidate-row write entirely** and shrinks the phase-2 transaction from three actions (`{delete C, insert M, upsert TAIL}`) to two (`{insert M, upsert TAIL}`). Orphans are still recovered, but recovery now discovers them by enumerating batch partitions whose start offset sits above `TAIL` instead of by scanning candidate-rows in the manifest partition.

**As of v6.0.1 the default is `EliminateCandidateRowOnHotPath = true`.** An A/B against real Azure Tables at the 25k-writer saturation rung moved the sustained-ingest watermark from ~58k entries (C-row written inline) to ~1.38M entries (C-row elided) - a ~24x capacity gain - because the phase-0 upsert no longer contends with the per-shard `PhaseTwoWorker` on the shared manifest partition (Azure Tables serialises writes within a `PartitionKey` server-side). Hosts that need the pre-v6.0 recovery contract - the C-row written inline and orphans discovered via the manifest-partition C-row scan - should explicitly opt **out** (`EliminateCandidateRowOnHotPath = false`). It is a throughput knob that trades the per-batch C-row round-trip for a different recovery-scan shape:

| Property | Legacy (`= false`) | Default (`= true`) |
|---|---|---|
| Hot-path Azure Tables round-trips per append | phase-0 C-row write + phase-1 entry transaction + phase-2 transaction | phase-1 entry transaction + phase-2 transaction |
| Phase-2 transaction shape | `{delete C, insert M, upsert TAIL}` per coalesced commit, plus one TAIL | `{insert M}` per coalesced commit, plus one TAIL |
| In-flight orphan signal | C-row in manifest partition (`RowKey ge 'C' and RowKey lt 'D'`) | batch partition above TAIL (`PartitionKey ge '_b_\|...\|S{TAIL+1:D19}' and PartitionKey lt '_b_\|...\|T'`) |
| Recovery discovery cost | O(in-flight batches at the moment of the crash) | O(in-flight batches at the moment of the crash), but coupled to batch-partition GC hygiene |
| Atomicity, ordering, idempotence | unchanged | unchanged |

The reconciler always performs both scans when the flag is on, so a silo started with the flag enabled recovers **both** legacy orphans (left by a previous flag-off activation) and D-mode orphans (left by a previous flag-on activation) - upgrading from flag-off to flag-on is safe and lossless.

**Always drain pending appends and let `ReconcileAsync` complete on a flag-on silo before flipping the flag back to off.** This is the load-bearing reason the legacy code path is retained now that the flag defaults on.

Like `PipelinePhaseTwoCommits`, this default flip is a change in the recovery contract, not just a perf tweak - which is why the legacy C-row code path is retained behind `EliminateCandidateRowOnHotPath = false`. Before opting out in production, bake the chosen arm into a representative workload.

The `benchmark/azure-throughput/` harness exposes this flag via the `BENCH_WAL_ELIMINATE_CANDIDATE_ROW` environment variable so the same single-silo, real-Azure-Tables deployment can drive both arms of an A/B run with no code change. See `benchmark/azure-throughput/README.md` for the A/B runbook.

#### Retry-attempt observability

The provider attaches a per-retry pipeline policy (`RetryAttemptTrackingPolicy`) to every `TableServiceClient` it constructs from `ConnectionString`, `ServiceUri` + credential, or `ServiceUri` + shared-key. The policy increments the counter `orleans.lattice.provider.retry.attempts` exactly once per retry attempt the Azure SDK performs, tagged with the HTTP status code that triggered the retry (or `0` for a transport-level failure with no HTTP exchange). The counter is purely additive - it sits alongside the existing `orleans.lattice.provider.retry.exhausted` counter, which only fires when the SDK gives up. Together the two counters separate two distinct failure shapes:

| Counter | Fires when | Hot-path attribution |
|---|---|---|
| `orleans.lattice.provider.retry.exhausted` | The SDK has burned its retry budget and is about to surface an exception. | Sustained throttling at the partition / account ceiling. |
| `orleans.lattice.provider.retry.attempts` | The SDK has decided to retry an attempt, regardless of the eventual outcome. | Transient throttling / 503 / 429 storms whose retries ultimately succeed - the canonical fingerprint of wall-time inflation while server-timing stays low. |

The policy is registered at `HttpPipelinePosition.PerRetry` **after** the host's `ConfigureClientOptions` callback runs, so a host that customises the retry policy cannot accidentally drop the observability hook. In pre-built `ServiceClient` mode the provider returns the host-supplied client verbatim and does not attach the policy; hosts using that path can opt in by calling `clientOptions.AddPolicy(RetryAttemptTrackingPolicy.Instance, HttpPipelinePosition.PerRetry)` themselves before constructing the client they hand to the provider. The counter's cardinality is intentionally bounded to the small set of HTTP status codes the SDK surfaces on the WAL hot path; it deliberately omits tree / shard tags, because per-shard failure attribution is already covered by `orleans.lattice.provider.retry.exhausted`.

#### Retry-budget tuning

The provider surfaces five nullable knobs on `AzureTableWalStorageOptions` that map straight onto `TableClientOptions.Retry`. All default to `null`, which leaves the Azure SDK defaults in place (4 attempts, 0.8 s base delay, 60 s cap, 100 s per-attempt deadline, exponential backoff); set any subset of them to bound the WAL hot path's per-call retry budget without surrendering the SDK's transient-fault classification.

| Option | Maps to | SDK default | Typical Phase-C value |
|---|---|---|---|
| `RetryMaxAttempts` | `Retry.MaxRetries` (retries after the initial attempt) | `3` | `1` or `2` to clip long backoff trains |
| `RetryDelay` | `Retry.Delay` (base backoff) | `0.8 s` | `40 ms` – `100 ms` to keep the first retry inside a single Orleans turn |
| `RetryMaxDelay` | `Retry.MaxDelay` (per-attempt cap) | `60 s` | `400 ms` – `1 s` to bound the worst-case backoff |
| `RetryNetworkTimeout` | `Retry.NetworkTimeout` (per-attempt deadline) | `100 s` | `2 s` – `10 s` so a stuck request cannot park a WAL slot indefinitely |
| `RetryMode` | `Retry.Mode` | `Exponential` | usually unchanged |

The knobs are applied to `clientOptions.Retry` **before** the host's `ConfigureClientOptions` callback runs, so a host that wants final say can override any of them inside that callback (or replace `clientOptions.Retry` wholesale). The provider's `Validate()` rejects negative values, `RetryDelay > RetryMaxDelay`, and a non-positive `RetryNetworkTimeout`. In pre-built `ServiceClient` mode the knobs are ignored: the host already owns the `TableClientOptions.Retry` on the client it constructed.

The motivation is the diagnostic finding that the WAL hot path's wall-time p99 sat 5–100x above Azure Tables' server-timing p99 - the canonical signature of a retry storm whose retries ultimately succeed. The `orleans.lattice.provider.retry.attempts` counter from the observability section above is the diagnostic that tells operators whether to reach for these knobs, and `RetryNetworkTimeout` in particular is the per-attempt deadline budget that prevents a single stuck request from holding a WAL slot for the full SDK default of 100 s.

#### Bounding a wedged phase-2 commit

`RetryNetworkTimeout` bounds each *attempt* the SDK makes, but it does not bound the per-shard `PhaseTwoWorker`'s manifest commit as a whole: the worker commits phase-2 transactions one coalesced group at a time, and the next group cannot start until the current `SubmitTransactionAsync` returns (the strict offset-FIFO ordering invariant requires it). The grain-side `WalShardGrain` back-pressure and flush deadline bound the *foreground* append path, but neither timer covers this *background* commit. If a commit's Azure Tables transaction stops making progress (a hung socket, a server-side partition stall, or an SDK retry loop that keeps re-issuing past the network timeout) the drain loop blocks indefinitely and every later pending commit on that shard wedges behind it.

`AzureTableWalStorageOptions.PhaseTwoCommitTimeout` is a finite per-commit deadline for exactly this case. It defaults to `AzureTableWalStorageOptions.DefaultPhaseTwoCommitTimeout` (3 seconds), so a wedged manifest commit faults instead of stalling the per-shard drain loop indefinitely out of the box. Set it to `null` to restore the historical unbounded behaviour, or to a different positive `TimeSpan` to tune the deadline:

```csharp verify
var options = new AzureTableWalStorageOptions
{
    ConnectionString = "UseDevelopmentStorage=true",
    // Abandon a phase-2 manifest commit that has not returned within
    // 30 s. The faulted batch (and every later pending commit on the
    // shard) is recovered by the same sticky-failure resync path that
    // already handles a phase-2 transaction error.
    PhaseTwoCommitTimeout = TimeSpan.FromSeconds(30),
};
```

When the deadline fires, the commit's batch and every later still-pending commit on the shard fault with a `TimeoutException` (the same fault-the-window semantics as a transaction error, because their tail offsets are now stale relative to the recovered `TAIL`), the worker keeps draining new arrivals, and the `orleans.lattice.provider.phase2.commit.timeouts` counter increments once - tagged `tree` and `shard` - so operators can prove whether the deadline ever fired. Size it comfortably above the observed phase-2 commit p99 (so healthy commits never trip it) but below the silo-level activation / request timeout (so a wedged shard is broken before it cascades). `Validate()` rejects a zero or negative value.

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
5. **Optional fast path.** Override `AppendEncodedBatchAsync` when the backend stores binary payloads natively. The default implementation decodes each segment through the supplied `IWalRecordEncoder` and delegates to `AppendBatchAsync`, so a provider that only implements `AppendBatchAsync` keeps working - overriding the zero-copy overload skips the round-trip and stores the grain's already-encoded bytes directly.
6. **Optional activation-time recovery.** Override `ReconcileAsync` if the backend's commit protocol can leave the durable state inconsistent across crash boundaries (e.g. a multi-phase commit, as in the Azure Tables provider). The default implementation is a no-op, suitable for backends whose append is atomic in a single operation. The WAL grain calls `ReconcileAsync` in `OnActivateAsync` before reading the highest offset, so the activation seam is quiescent for the duration.

The `InMemoryWalStorageProvider` source under `src/lattice/InMemoryWalStorageProvider.cs` is the canonical reference implementation; the `AzureTableWalStorageProvider` source under `src/lattice.storage.azuretable/` is the canonical durable reference implementation.

Once the implementation is in place, register it through the standard `AddWalStorage` extension - no other wiring is required:

```csharp verify
siloBuilder.AddWalStorage(sp => new InMemoryWalStorageProvider());
