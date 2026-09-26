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
| `Set` | key written | `null` | `ILattice.SetAsync`, `SetAsync` + TTL, `GetOrSetAsync` (only when newly written), `SetIfVersionAsync` (only when applied), `SetManyAsync` (per entry), `SetManyWherePredicateAsync` (per key actually written), `ApplyCrdtDeltaAsync` with or without TTL and `ApplyCrdtDeltaManyAsync` (per entry) - the path the typed CRDT accessors write through - and `SetManyAtomicAsync` / `SetManyAtomicWhereAsync` (per staged entry, stamped with the saga's `OperationId`; emitted as the saga's execute phase stages each batch, before the commit decision - see [the rollback note below](#rollback-emits-prepared-set-events-but-no-terminal-event); a guarded batch whose precondition fails stages nothing and emits nothing) |
| `Delete` | key deleted | `null` | `ILattice.DeleteAsync` (only when the key existed) |
| `DeleteRange` | `"{start}..{end}"` | `null` | `ILattice.DeleteRangeAsync` and `DeleteRangeWherePredicateAsync` (one event when >= 1 key was deleted), and each `DeleteRangeStepAsync` step of a delete-range cursor that deleted >= 1 key (keyed by that step's sub-range) |
| `AtomicWriteCompleted` | `null` | `null` | The atomic-write coordinator on terminal success only. `OperationId` is the saga's idempotency key. Rolled-back sagas do **not** publish a completion event. |
| `SplitCommitted` | `null` | source shard | The shard-split coordinator after the finalise phase. |
| `CompactionCompleted` | `null` | `null` | The tombstone-compaction pass after a successful run - one event per pass, however many shards it covered. |
| `CompactionTriggered` | `null` | affected shard | Reserved / not yet emitted. A declared kind for a leaf asking the tree's compaction grain to schedule an out-of-cycle pass; no producer currently publishes it. |
| `SnapshotCompleted` | `null` | `null` | The snapshot coordinator on terminal success. |
| `ResizeCompleted` | `null` | `null` | The tree-resize coordinator on terminal success. |
| `ReshardCompleted` | `null` | `null` | The reshard coordinator on terminal success. |
| `TreeDeleted` | `null` | `null` | `ILattice.DeleteTreeAsync`. |
| `TreeRecovered` | `null` | `null` | `ILattice.RecoverTreeAsync`. |
| `TreePurged` | `null` | `null` | `ILattice.PurgeTreeAsync` or the soft-delete-expiry purge reminder. |

### Correlation

Atomic writes (`ILattice.SetManyAtomicAsync(entries, operationId)`) stamp every per-entry `Set` event **and** the terminal `AtomicWriteCompleted` event with the caller-supplied `operationId`. This lets subscribers stitch the per-key and saga-complete records into a single logical transaction.

Non-saga writes leave `OperationId` as `null`.

### Rollback emits prepared-set events but no terminal event

`SetManyAtomicAsync` stages its entries through batched `ILattice.SetManyAsync` calls during its execute phase, so a per-key `Set` event with the saga's `OperationId` fires for every entry of each batch the saga successfully staged. These writes are stamped as prepared and routed into the receiving leaf's per-transaction pending bucket; they are **not** visible to readers until the saga's terminal commit broadcast arrives. If the saga later aborts, the leaf's terminal-abort handler drops the pending bucket - no compensating `Set` / `Delete` writes are issued, so subscribers see the prepared-side events with **no** matching `AtomicWriteCompleted`. The absence of the terminal event is the signal that the saga rolled back; a `GetAsync` on any of those keys after the abort terminal lands returns the pre-saga value.

The strict atomic-visibility cleanup avoids the older pattern of emitting reverse compensating writes - which would have generated additional `Set` / `Delete` events tagged with the same `OperationId` - because compensation writes would themselves become visible and reorder against concurrent reads. Subscribers that need stronger durability semantics (e.g. "only act on events for sagas that actually committed") should buffer per-key events keyed by `OperationId` and discard the buffer if `AtomicWriteCompleted` does not arrive within a bounded window.

### A resize also publishes events for its internal steps

`ResizeAsync` copies the tree through an internal online snapshot of its current physical tree and, after the alias swap, soft-deletes that physical tree. Those steps publish their own events, stamped with the old physical tree's id rather than the logical tree id: `SnapshotCompleted` when the copy finishes, `TreeDeleted` when the old physical tree is soft-deleted, and `TreePurged` when it is purged once `LatticeOptions.SoftDeleteDuration` has elapsed. `ResizeCompleted` itself is published under the logical tree id. For a tree that has never been resized, its physical tree id is its own id, so a subscriber to that tree receives `SnapshotCompleted` and `TreeDeleted` before `ResizeCompleted`, and `TreePurged` later; the deleted and purged tree in those events is the retired physical copy. A later resize publishes its internal events under the previous resize's physical tree id, a stream the logical tree's subscribers do not receive. `UndoResizeAsync` likewise publishes `TreeRecovered` for the old physical tree when it had already been soft-deleted, and `TreeDeleted` for the discarded copy.

### Operations that deliberately do not emit events

The following APIs intentionally skip event publication to keep their bulk I/O profile predictable:

- `ILattice.BulkLoadAsync` - bulk-import path is optimised for throughput and assumes the importer already knows the full keyset.
- Cursor-page reads (`OpenKeyCursorAsync` / `OpenEntryCursorAsync` / `NextKeysAsync` / `NextEntriesAsync` / `CloseCursorAsync`) - read-only, never emit events.
- Inbound replicated last-writer-wins applies - a set, delete, range delete, or merged batch arriving from a peer cluster is merged directly into the owning shard and does **not** publish events at the receiving silo.

Two inbound replication shapes are the exception and **do** publish at the receiving silo when publication is enabled there: a replicated atomic batch's prepared writes are re-applied through the local set / delete path, and a replicated CRDT delta is folded through the local CRDT apply path, so both emit the same per-key events a local write on that path would. A subscriber that needs every write should therefore still attach at every cluster - each cluster emits the writes it originated - and treat a replicated atomic batch or CRDT delta that it also observes on a receiving cluster as a duplicate.

A delete-range cursor is **not** event-silent: each `DeleteRangeStepAsync` step issues a range delete on the tree and so emits a `DeleteRange` event for its sub-range whenever it deleted at least one key.

## Delivery semantics

- **Stream addressing.** Namespace is `"orleans.lattice.events"` (constant `LatticeEventConstants.StreamNamespace`) and the stream id is the **logical tree id**. One subscription per tree id, no fan-out filter grains.
- **Metadata-only.** Values are never included. Subscribers that need the new bytes must issue a follow-up `ILattice.GetAsync(evt.Key)`.
- **Best-effort.** Publication happens after the write is durable but is not part of the write commit. The underlying Orleans stream provider determines redelivery and ordering guarantees (e.g. MemoryStreams is at-most-once per activation; EventHub/AzureQueue streams are at-least-once with ordering per partition).
- **Fail-silent.** Missing provider, serialization failures, and downstream queue exceptions are logged and discarded. The write-path return value is unchanged.
- **No default provider.** Lattice does not register a stream provider on your behalf. You must add one explicitly (e.g. `siloBuilder.AddMemoryStreams("Default")` plus `AddMemoryGrainStorage("PubSubStore")`) under the name set in `LatticeOptions.EventStreamProviderName` (default `"Default"`).

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

If no `IStreamProvider` with the requested name is registered on the cluster client, `SubscribeToEventsAsync` throws `InvalidOperationException` with an actionable message ("No Orleans stream provider named '{providerName}' is registered on the cluster client. Register one via clientBuilder.AddMemoryStreams("{providerName}") (or the Event Hub / Azure Queue equivalent) and ensure every silo hosting Lattice grains has the same provider registered."), carrying the provider lookup failure as its inner exception. The check runs on the client whatever the silos' `PublishEvents` setting. This is the one hard-fail in the pipeline - publication itself continues to noop, but subscribing on a mis-configured client is treated as a programming error.

## Metrics

The publish pipeline emits two counters under the `orleans.lattice` meter:

| Instrument | Type | Unit | Tags | Meaning |
|---|---|---|---|---|
| `orleans.lattice.events.published` | `Counter<long>` | `{event}` | `tree`, `kind` = the event kind name (e.g. `Set`, `SnapshotCompleted`), `tenant` | Incremented once per `LatticeTreeEvent` successfully dispatched to the configured stream provider. |
| `orleans.lattice.events.dropped` | `Counter<long>` | `{event}` | `tree`, `reason` = `missing_provider` (no stream provider by the configured name) or `publish_error` (the stream provider threw during dispatch), `tenant` | Incremented once per event drop. |

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

The override is persisted on the tree's registry entry and survives silo restarts. Resolution order on every publish site:

1. Per-tree override if set.
2. Otherwise `LatticeOptions.PublishEvents` - also used whenever the registry cannot be read, so a registry outage never blocks a write.

**Propagation.** The activation that handled the call observes the change immediately. Other activations (on other silos, or other stateless-worker instances on the same silo) refresh their cached value within ~5 seconds, so writes landing on a different silo may emit under the previous setting for a brief window. This is intentional: the per-site cache keeps publication latency negligible and avoids a registry round-trip on every write.

**System trees.** Trees whose id starts with `_lattice_` (e.g. the internal registry tree) never consult the registry for their own override - doing so would deadlock the non-reentrant registry activation that is currently servicing the write. For system trees the silo-wide `LatticeOptions.PublishEvents` is always authoritative.

## What **not** to rely on

- **Not a change log.** `LatticeTreeEvent` is not persisted; a silo restart without subscribers attached loses any in-flight events. For durable audit trails use a durable stream provider (EventHubs, AzureQueue) or maintain a secondary projection tree.
- **Not transactional.** Events are published after the write is durable, not as part of the same commit. A silo crash between the write and the publish loses the event; the write survives.
- **Not totally ordered, even per key.** Events are published by the tree's stateless-worker front end after each write returns, not by the shard that serialised the write, so the events of concurrent writes - even to the same key - can be published in a different order from the one the shard applied them in. A caller that awaits each write before issuing the next publishes its own events in write order; delivery order from there on is up to the stream provider. Consumers that need causal order should re-read with `GetWithVersionAsync`.

## See also

- [Configuration](configuration.md#publishevents) - option reference.
- [Atomic Writes](atomic-writes.md) - how `SetManyAtomicAsync` stamps `OperationId` on every per-entry event.
- [Mutation observers](api.md#mutation-observers) - the in-process, synchronous, value-carrying alternative for write-path integrations.
- [`ILattice` API reference](api.md#ilattice) - full method surface.
