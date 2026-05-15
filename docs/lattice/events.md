# Tree Events

Orleans.Lattice publishes metadata-only event notifications on a per-tree Orleans stream so that caches, projections, audit pipelines, and dashboards can react to tree mutations without polling.

Publication is opt-in (`LatticeOptions.PublishEvents` silo-wide default, overridable per tree via `ILattice.SetPublishEventsEnabledAsync`), **fire-and-forget**, and never affects the write-path outcome: a missing stream provider or a downstream queue failure is logged at `Warning` and swallowed. Events carry only metadata - **key name and operation kind, never the value bytes** - so subscribers that need the new value must `GetAsync` it themselves.

> **Tree events vs. [mutation observers](api.md#mutation-observers).** Pick events when you need out-of-process, fire-and-forget, metadata-only notifications for UI updates, cache invalidation, dashboards, or audit projections. Pick [`IMutationObserver`](api.md#mutation-observers) when you need an in-process, synchronous hook with the full value bytes on the write path - typically to feed a replication WAL or transactional outbox in another library. Observers add latency to every write in the silo; events do not.

## Event shape

```csharp verify
// LatticeTreeEvent is a readonly record struct with these fields:
LatticeTreeEvent evt = default;
LatticeTreeEventKind kind = evt.Kind;        // Set, Delete, DeleteRange, SplitCommitted, ...
string treeId = evt.TreeId;                   // logical tree name
string? key = evt.Key;                        // single-key events; null for tree-level events
int? shardIndex = evt.ShardIndex;             // physical shard index when relevant
string? opId = evt.OperationId;               // correlation id for saga writes
DateTimeOffset at = evt.AtUtc;                // silo-side timestamp
```

### `LatticeTreeEventKind`

| Kind | `Key` | `ShardIndex` | Emitted by |
|---|---|---|---|
| `Set` | key written | routing shard | `ILattice.SetAsync`, `SetAsync` + TTL, `GetOrSetAsync` (only when newly written), `SetIfVersionAsync` (only when applied), `SetManyAsync` (per entry), `SetManyAtomicAsync` (per entry, stamped with the saga's `OperationId`; emitted as the prepare-phase `SetAsync` calls return - see [the rollback note below](#rollback-emits-prepared-set-events-but-no-terminal-event)) |
| `Delete` | key deleted | routing shard | `ILattice.DeleteAsync` (only when the key existed) |
| `DeleteRange` | `"{start}..{end}"` | `null` | `ILattice.DeleteRangeAsync` (one event when ≥ 1 key was deleted) |
| `AtomicWriteCompleted` | `null` | `null` | `AtomicWriteGrain` on terminal success only. `OperationId` is the saga's idempotency key. Rolled-back sagas do **not** publish a completion event. |
| `SplitCommitted` | `null` | source shard | `TreeShardSplitGrain` after the finalise phase. |
| `CompactionCompleted` | `null` | compacted shard | `TombstoneCompactionGrain` after a successful pass. |
| `SnapshotCompleted` | `null` | `null` | `TreeSnapshotGrain` on terminal success. |
| `ResizeCompleted` | `null` | `null` | `TreeResizeGrain` on terminal success. |
| `ReshardCompleted` | `null` | `null` | `TreeReshardGrain` on terminal success. |
| `TreeDeleted` | `null` | `null` | `ILattice.DeleteTreeAsync` (via `TreeDeletionGrain.DeleteTreeAsync`). |
| `TreeRecovered` | `null` | `null` | `ILattice.RecoverTreeAsync` (via `TreeDeletionGrain.RecoverAsync`). |
| `TreePurged` | `null` | `null` | `ILattice.PurgeTreeAsync` (via `TreeDeletionGrain.PurgeNowAsync`) or the soft-delete-expiry purge reminder. |

### Correlation

Atomic writes (`ILattice.SetManyAtomicAsync(entries, operationId)`) stamp every per-entry `Set` event **and** the terminal `AtomicWriteCompleted` event with the caller-supplied `operationId`. This lets subscribers stitch the per-key and saga-complete records into a single logical transaction.

Non-saga writes leave `OperationId` as `null`.

### Rollback emits prepared-set events but no terminal event

`SetManyAtomicAsync` writes each entry through `ILattice.SetAsync` during its prepare/execute phase, so a per-key `Set` event with the saga's `OperationId` fires for every entry the saga successfully prepared. These writes are stamped as prepared and routed into the receiving leaf's per-transaction pending bucket; they are **not** visible to readers until the saga's terminal commit broadcast arrives. If the saga later aborts, the leaf's terminal-abort handler drops the pending bucket - no compensating `Set` / `Delete` writes are issued, so subscribers see the prepared-side events with **no** matching `AtomicWriteCompleted`. The absence of the terminal event is the signal that the saga rolled back; a `GetAsync` on any of those keys after the abort terminal lands returns the pre-saga value.

The strict atomic-visibility cleanup avoids the older pattern of emitting reverse compensating writes - which would have generated additional `Set` / `Delete` events tagged with the same `OperationId` - because compensation writes would themselves become visible and reorder against concurrent reads. Subscribers that need stronger durability semantics (e.g. "only act on events for sagas that actually committed") should buffer per-key events keyed by `OperationId` and discard the buffer if `AtomicWriteCompleted` does not arrive within a bounded window.

### Operations that deliberately do not emit events

The following APIs intentionally skip event publication to keep their bulk I/O profile predictable:

- `ILattice.BulkLoadAsync` - bulk-import path is optimised for throughput and assumes the importer already knows the full keyset.
- `ILattice.DeleteRangeStepAsync` - stateful cursor-driven range delete advances one batch at a time. The unbounded `ILattice.DeleteRangeAsync` (executed as a single logical range operation) is the only range-delete API that emits a `DeleteRange` event.
- Cursor-page reads (`OpenKeyCursorAsync` / `OpenEntryCursorAsync` / `NextKeysAsync` / `NextEntriesAsync` / `CloseCursorAsync`) - read-only, never emit events.
- Receiver-side replication apply seam (`IReplicationApplyGrain.ApplySetAsync` / `ApplyDeleteAsync` / `ApplyDeleteRangeAsync` / `ApplyPreparedSetAsync` / `ApplyPreparedDeleteAsync` / `ApplyMergeManyAsync`) - inbound writes from a peer cluster are merged directly into the owning shard via the LWW apply path and do **not** publish events at the receiving silo. Each cluster only emits events for writes that originated locally; subscribers that need a cluster-of-record view should attach at every cluster.

## Delivery semantics

- **Stream addressing.** Namespace is `"orleans.lattice.events"` (constant `LatticeEventConstants.StreamNamespace`) and the stream id is the **logical tree id**. One subscription per tree id, no fan-out filter grains.
- **Metadata-only.** Values are never included. Subscribers that need the new bytes must issue a follow-up `ILattice.GetAsync(evt.Key)`.
- **Best-effort.** Publication happens after the write is durable but is not part of the write commit. The underlying Orleans stream provider determines redelivery and ordering guarantees (e.g. MemoryStreams is at-most-once per activation; EventHub/AzureQueue streams are at-least-once with ordering per partition).
- **Fail-silent.** Missing provider, serialization failures, and downstream queue exceptions are logged and discarded. The write-path return value is unchanged.
- **No default provider.** Lattice does not register a stream provider on your behalf. You must add one explicitly (e.g. `siloBuilder.AddMemoryStreams("Default")` plus `AddMemoryGrainStorage("PubSubStore")`) and name it in `LatticeOptions.EventStreamProviderName`.

## Setup

On the silo:

```csharp verify
siloBuilder
    .AddLattice((services, name) => services.AddMemoryGrainStorage(name))
    .ConfigureLattice(o =>
    {
        o.PublishEvents = true;
        o.EventStreamProviderName = "Default";
    })
    .AddMemoryStreams("Default")
    .AddMemoryGrainStorage("PubSubStore");
```

On the cluster client, register a matching stream provider (same name) and call `SubscribeToEventsAsync`:

```csharp verify
// Assumes the client was configured with .AddMemoryStreams("Default") during build.
var tree = client.GetGrain<ILattice>("my-tree");
var handle = await tree.SubscribeToEventsAsync(
    client,
    async evt =>
    {
        Console.WriteLine($"[{evt.AtUtc:O}] {evt.Kind} key={evt.Key}");
        await Task.CompletedTask;
    },
    providerName: "Default",
    cancellationToken);

// Later, when shutting down:
await handle.UnsubscribeAsync();
```

### Missing provider

If the silo has `PublishEvents = true` but no matching `IStreamProvider` registered, `SubscribeToEventsAsync` throws `InvalidOperationException` with an actionable message ("register one via siloBuilder.AddMemoryStreams/AddEventHubStreams/etc."). This is the one hard-fail in the pipeline - publication itself continues to noop, but subscribing on a mis-configured client is treated as a programming error.

## Per-tree override

`LatticeOptions.PublishEvents` is the **silo-wide default**. Individual trees can opt in or out at runtime via `ILattice.SetPublishEventsEnabledAsync(bool?)`:

```csharp verify
// Force publication on for this tree regardless of the silo default.
await tree.SetPublishEventsEnabledAsync(true, cancellationToken);

// Force publication off (e.g. for a high-volume tree whose events are uninteresting).
await tree.SetPublishEventsEnabledAsync(false, cancellationToken);

// Clear the override and inherit the silo default again.
await tree.SetPublishEventsEnabledAsync(null, cancellationToken);
```

The override is persisted on the tree's registry entry (`TreeRegistryEntry.PublishEvents`) and survives silo restarts. Resolution order on every publish site:

1. Per-tree override if set.
2. Otherwise `LatticeOptions.PublishEvents`.

**Propagation.** The activation that handled the call observes the change immediately. Other activations (on other silos, or other stateless-worker instances on the same silo) refresh their cached value within ~5 seconds, so writes landing on a different silo may emit under the previous setting for a brief window. This is intentional: the per-site cache keeps publication latency negligible and avoids a registry round-trip on every write.

**System trees.** Trees whose id starts with `_lattice_` (e.g. the internal registry tree) never consult the registry for their own override - doing so would deadlock the non-reentrant registry activation that is currently servicing the write. For system trees the silo-wide `LatticeOptions.PublishEvents` is always authoritative.

## What **not** to rely on

- **Not a change log.** `LatticeTreeEvent` is not persisted; a silo restart without subscribers attached loses any in-flight events. For durable audit trails use a durable stream provider (EventHubs, AzureQueue) or maintain a secondary projection tree.
- **Not transactional.** Events are published after the write is durable, not as part of the same commit. A silo crash between the write and the publish loses the event; the write survives.
- **Not totally ordered across shards.** Events for the same key are ordered by the originating `IShardRootGrain`'s single-activation serialisation. Events for different keys (or different shards) may be interleaved by the stream provider.

## See also

- [Configuration](configuration.md#publishevents) - option reference.
- [Atomic Writes](atomic-writes.md) - how `SetManyAtomicAsync` stamps `OperationId` on every per-entry event.
- [Mutation observers](api.md#mutation-observers) - the in-process, synchronous, value-carrying alternative for write-path integrations.
- [`ILattice` API reference](api.md#ilattice) - full method surface.
